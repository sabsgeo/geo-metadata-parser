
from geo import geo_mongo

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
