
from geo import geo_mongo
from geo import geo


def get_diff_between_all_geo_series_and_series_metadata():
    geo_mongo_instance = geo_mongo.GeoMongo()
    gse_id_list = list(geo_mongo_instance.all_geo_series_collection.find(
        {}, projection={"_id": False, "gse_patten": False, "last_updated": False, "status": False}))

    gse_id_list_sub = list(geo_mongo_instance.series_metadata_collection.find(
        {}, projection={"_id": False, "Series_title": False, "Series_status": False, "Series_pubmed_id": False,
                        "Series_web_link": False,
                        "Series_summary": False,
                        "Series_summary": False,
                        "Series_overall_design": False,
                        "Series_type": False,
                        "Series_contributor": False,
                        "Series_sample_id": False,
                        "Series_contact_institute": False,
                        "Series_supplementary_file": False,
                        "Series_platform_id": False,
                        "Series_relation": False,
                        "Platform_title": False,
                        "Platform_technology": False,
                        "Platform_organism": False
                        }))

    full_lst = [item.get("gse_id") for item in gse_id_list]
    sub_lst = [item.get("gse_id") for item in gse_id_list_sub]
    diff_lst = list(set(full_lst) - set(sub_lst))
    obj_sub_lst = [{"gse_id": item} for item in diff_lst]
    return obj_sub_lst


def get_diff_between_geo_and_all_geo_series_sync_info(gse_pattern_list):
    geo_mongo_instance = geo_mongo.GeoMongo()
    for each_pattern in gse_pattern_list:
        gse_ids = geo.get_gse_ids_from_pattern(each_pattern['gse_patten'])
        for gse_id in gse_ids:
            print(gse_id['gse_id'])
            if geo.has_soft_file(gse_id['gse_id']):
                selected_one = geo_mongo_instance.all_geo_series_collection.find_one(
                    {"_id": gse_id.get('gse_id')})
                last_updated_date = geo.read_series_metadata_from_soft_file(gse_id.get(
                    'gse_id')).get("SERIES").get(gse_id.get('gse_id')).get("Series_last_update_date")
                if selected_one == None:
                    data_to_add = {
                        "_id": gse_id.get('gse_id'),
                        "gse_patten": each_pattern['gse_patten'],
                        "gse_id": gse_id.get('gse_id'),
                        "last_updated": last_updated_date,
                        "status": "not_up_to_date"
                    }
                    geo_mongo_instance.all_geo_series_collection.insert_one(
                        data_to_add)
                else:
                    data_to_update = {
                        "status": "up_to_date",
                        "last_updated": last_updated_date
                    }
                    geo_mongo_instance.all_geo_series_collection.update_one(
                        {"_id": gse_id.get('gse_id')}, {"$set": data_to_update}, upsert=True)
                    # if (selected_one.get("status") == "not_up_to_date"):
                    #     print("No db update")
                    #     continue

                    # if (selected_one.get("last_updated") == gse_id['last_modified']):
                    #     geo_mongo_instance.all_geo_series_collection.update_one(
                    #         {"_id": gse_id.get('gse_id')}, {"$set": {"status": "up_to_date"}}, upsert=True)
                    # else:
                    #     geo_mongo_instance.all_geo_series_collection.update_one(
                    #         {"_id": gse_id.get('gse_id')}, {"$set": {"status": "not_up_to_date"}}, upsert=True)

            else:
                print("GSE ID skipped as soft file not present: " +
                      gse_id.get('gse_id'))
