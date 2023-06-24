from geo import geo_mongo
from geo import geo
from geo import model_data
from geo import pubmed


from pymongo import UpdateOne
import time
import json
import csv
import random
import hashlib

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

def add_metadata_from_pmc(all_params):
    geo_mongo_instance = geo_mongo.GeoMongo()
    data_inst = model_data.ModelData()
    list_to_add = all_params.get("list_to_parallel")
    for each_study in list_to_add:
        pmc_id = each_study
        db_data, upload_data = data_inst.extract_pmc_metadata(pmc_id)
        if len(db_data) < 1 and len(upload_data) < 1:
            continue
        for upload_types in upload_data:
            for data_to_upload in upload_data.get(upload_types):
                _id = hashlib.sha256(data_to_upload.encode()).hexdigest()
                geo_mongo_instance.fs.put(upload_data.get(upload_types).get(data_to_upload), _id = _id)
        geo_mongo_instance.pmc_metadata_collection.insert_one(db_data)

def add_series_and_sample_metadata(all_params):
    list_to_add = all_params.get("list_to_parallel")
    data_model = model_data.ModelData()
    geo_instance = geo_mongo.GeoMongo()

    for gse_id in list_to_add:
        print("Started adding/updating " + gse_id.get("gse_id"))
        updated_series_data, updated_sample_data = data_model.extract_all_metadata_info_from_softfile(
            gse_id.get("gse_id"))

        if bool(updated_series_data):
            # update series
            series_id = updated_series_data.get("_id")
            geo_instance.series_metadata_collection.update_one(
                {"_id": series_id}, {"$set": updated_series_data}, upsert=True)

            oper = []
            for each_sample in updated_sample_data:
                sample_id = each_sample.get("_id")
                oper.append(UpdateOne({"_id": sample_id}, {
                            "$set": each_sample}, upsert=True))
            if len(oper) > 0:
                geo_instance.sample_metadata_collection.bulk_write(
                    oper, ordered=False)

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

def get_into_from_pubmed(all_params):
    pubmed_id_added = set()
    list_to_add = all_params.get("list_to_parallel")
    headers = ['pmid', 'pmc_id', 'title', 'transliterated_title', 'journal_title', 'journal_title_abbreviation',
               'publication_type', 'abstract', 'medical_subject_headings', 'source', 'article_identifier',
               'general_note', 'substance_name', 'registry_number']
    pro_num = random.getrandbits(128)
    with open('/mnt/pmbmed_id_info_{}.csv'.format(str(pro_num)), 'w') as write_file:
        writer = csv.DictWriter(write_file, fieldnames=headers)
        for each_data in list_to_add:
            pubmed_ids = json.loads(each_data).get("Series_pubmed_id")
            if len(pubmed_ids) > 0:
                for pubmed_id in pubmed_ids:
                    if not (pubmed_id in pubmed_id_added):
                        print("Trying to add pubmed id {}".format(pubmed_id))
                        pub_json = pubmed.parse_medline(pubmed_id)
                        writer.writerow({
                            "pmid": pubmed_id,
                            "pmc_id": pub_json.get("PMC", ""),
                            "title": pub_json.get("TI", ""),
                            "transliterated_title": pub_json.get("TT", ""),
                            "journal_title": pub_json.get("JT", ""),
                            "journal_title_abbreviation": pub_json.get("TA", ""),
                            "publication_type": pub_json.get("PT", []),
                            "abstract": pub_json.get("AB", ""),
                            "medical_subject_headings": pub_json.get("MH", []),
                            "source": pub_json.get("SO", ""),
                            "article_identifier": pub_json.get("AID", []),
                            "general_note": pub_json.get("GN", ""),
                            "substance_name": pub_json.get("NM", ""),
                            "registry_number": pub_json.get("RN", [])
                        })
                        pubmed_id_added.add(pubmed_id)
                        print("Added pubmed id {}".format(pubmed_id))
            else:
                print("Not present")
