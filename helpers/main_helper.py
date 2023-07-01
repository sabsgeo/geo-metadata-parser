from geo import geo_mongo, geo, model_data
from helpers import general_helper

from pymongo import UpdateOne

import time

def get_diff_between_geo_and_all_geo_series_sync_info(modified_gse_ids, get_gse_status):
    all_series_data_to_add = []
    all_series_data_to_update = []
    for modified_gse_id in modified_gse_ids:
        if geo.has_soft_file(modified_gse_id):
            selected_one = get_gse_status.get(modified_gse_id)
            series_data = geo.get_series_metadata_from_soft_file(
                modified_gse_id)
            last_updated_date = "-"
            try:
                try:
                    last_updated_date = series_data.get("SERIES").get(
                        modified_gse_id).get("Series_last_update_date")
                except Exception as err:
                    _modified_gse_id = "GSE" + str(int(modified_gse_id[3:]))
                    print("Trying modified gse_id {} original one was {}".format(
                        _modified_gse_id, modified_gse_id))
                    last_updated_date = series_data.get("SERIES").get(
                        _modified_gse_id).get("Series_last_update_date")
            except Exception as err:
                print("There is some unknown issue with GSE ID {}".format(
                    modified_gse_id))
                continue

            if selected_one == None:
                data_to_add = {
                    "_id": modified_gse_id,
                    "gse_patten": geo.gse_pattern_from_gse_id(modified_gse_id),
                    "gse_id": modified_gse_id,
                    "last_updated": last_updated_date,
                    "status": "not_up_to_date",
                    "access": "public",
                    "sample_status": "invalid"
                }
                all_series_data_to_add.append(data_to_add)
                print("GSE ID has to be added: " +
                      modified_gse_id)
            elif not (selected_one.get("last_updated") == last_updated_date):
                update_to_add = {
                    "_id": modified_gse_id,
                    "status": "not_up_to_date",
                    "last_updated": last_updated_date
                }
                all_series_data_to_update.append(update_to_add)
                print("GSE ID has to be updated: " +
                      modified_gse_id)
        else:
            data_to_add = {
                "_id": modified_gse_id,
                "gse_patten": geo.gse_pattern_from_gse_id(modified_gse_id),
                "gse_id": modified_gse_id,
                "last_updated": '-',
                "status": "up_to_date",
                "access": "private",
                "sample_status": "valid"
            }
            all_series_data_to_add.append(data_to_add)
            print("GSE ID is private: " +
                  modified_gse_id)
    return all_series_data_to_add, all_series_data_to_update


def add_geo_sync_info_to_mongo(all_params):
    geo_mongo_instance = geo_mongo.GeoMongo()
    modified_gse_ids = all_params.get("list_to_parallel")
    get_gse_status = all_params.get("get_gse_status")
    time_to_wait_in_min = all_params.get("run_interval")
    data_to_be_add, data_to_be_update = get_diff_between_geo_and_all_geo_series_sync_info(
        modified_gse_ids, get_gse_status)

    add_oper = []
    update_oper = []

    for data_to_add in data_to_be_add:
        add_oper.append(UpdateOne({"_id": data_to_add.get("_id")}, {
            "$set": data_to_add}, upsert=True))

    for data_to_update in data_to_be_update:
        update_oper.append(UpdateOne({"_id": data_to_update.get("_id")}, {
            "$set": data_to_update}, upsert=True))

    if len(add_oper) > 0:
        geo_mongo_instance.all_geo_series_collection.bulk_write(
            add_oper, ordered=False)

    if len(update_oper) > 0:
        geo_mongo_instance.all_geo_series_collection.bulk_write(
            update_oper, ordered=False)
    time.sleep(time_to_wait_in_min * 60)

def add_series_and_sample_metadata(all_params):
    list_to_add = all_params.get("list_to_parallel")
    data_model = model_data.ModelData()
    geo_instance = geo_mongo.GeoMongo()

    for gse_id in list_to_add:
        print("Started adding/updating " + gse_id.get("gse_id"))
        updated_series_data, updated_sample_data, pubmed_metadata, pmc_metadata, pmc_assets = data_model.extract_all_metadata_info_from_softfile(
            gse_id.get("gse_id"))

        if bool(updated_series_data):
            # update series
            series_id = updated_series_data.get("_id")
            geo_instance.series_metadata_collection.update_one(
                {"_id": series_id}, {"$set": updated_series_data}, upsert=True)

            #update sample
            oper = []
            for each_sample in updated_sample_data:
                sample_id = each_sample.get("_id")
                oper.append(UpdateOne({"_id": sample_id}, {
                            "$set": each_sample}, upsert=True))
            if len(oper) > 0:
                geo_instance.sample_metadata_collection.bulk_write(
                    oper, ordered=False)
            
            #update pubmed metadata
            oper = []
            for each_pubmed in pubmed_metadata:
                if len(each_pubmed) < 1:
                    continue
                pubmed_id = each_pubmed.get("_id")
                oper.append(UpdateOne({"_id": pubmed_id}, {
                            "$set": each_pubmed}, upsert=True))
            if len(oper) > 0:
                geo_instance.pubmed_metadata_collection.bulk_write(
                    oper, ordered=False)
            
            #update pmc metadata
            oper = []
            for each_pmc in pmc_metadata:
                if len(each_pmc) < 1:
                    continue
                pmc_id = each_pmc.get("_id")
                oper.append(UpdateOne({"_id": pmc_id}, {
                            "$set": each_pmc}, upsert=True))
            if len(oper) > 0:
                geo_instance.pmc_metadata_collection.bulk_write(
                    oper, ordered=False)
            
            # uploading assets
            for upload_data in pmc_assets:
                for upload_types in upload_data:
                    if len(upload_types) < 1:
                        continue
                    for data_to_upload in upload_data.get(upload_types):
                        _id = data_to_upload
                        content_to_upload = general_helper.tar_gz_compress_string(data_to_upload, upload_data.get(upload_types).get(data_to_upload))          
                        
                        geo_instance.fs.delete(_id)
                        geo_instance.fs.put(content_to_upload, _id = _id)
                
            # update status
            geo_instance.all_geo_series_collection.update_one({"_id": gse_id.get(
                "gse_id")}, {"$set": {"status": "up_to_date", "sample_status": "valid"}}, upsert=True)
        else:
            print("GSE ID {} is probably private".format(gse_id.get("gse_id")))
        

def diff_bw_pmc_and_pubmed():
    geo_mongo_instance = geo_mongo.GeoMongo()
    studies_with_pmc = geo_mongo_instance.pubmed_metadata_collection.distinct("pmc_id", {})
    data_collected = geo_mongo_instance.pmc_metadata_collection.distinct("_id", {})
    return list(set(studies_with_pmc) - set(data_collected))
