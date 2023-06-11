from geo import geo_mongo
from geo import model_data
from geo import parallel_runner
from geo import geo
from pymongo import UpdateOne

import argparse
import traceback


def __get_diff_between_geo_and_all_geo_series_sync_info(gse_pattern_list, get_gse_status):
    for each_pattern in gse_pattern_list:
        gse_ids = geo.get_gse_ids_from_pattern(each_pattern['gse_patten'])
        all_series_data_to_add = []
        all_series_data_to_update = []
        for gse_id in gse_ids:
            print(gse_id['gse_id'])
            if geo.has_soft_file(gse_id['gse_id']):
                selected_one = get_gse_status.get(gse_id['gse_id'])
                last_updated_date = geo.get_series_metadata_from_soft_file(gse_id.get(
                    'gse_id')).get("SERIES").get(gse_id.get('gse_id')).get("Series_last_update_date")
                if selected_one == None:
                    data_to_add = {
                        "_id": gse_id.get('gse_id'),
                        "gse_patten": each_pattern['gse_patten'],
                        "gse_id": gse_id.get('gse_id'),
                        "last_updated": last_updated_date,
                        "status": "not_up_to_date",
                        "access": "public",
                        "sample_status": "invalid"
                    }
                    all_series_data_to_add.append(data_to_add)
                    print("GSE ID has to be added: " +
                          gse_id.get('gse_id'))
                elif not (selected_one.get("last_updated") == last_updated_date):
                    update_to_add = {"status": "not_up_to_date"}
                    all_series_data_to_update.append(update_to_add)
                    print(gse_id)
                    print("GSE ID has to be updated: " +
                          gse_id.get('gse_id'))
                    exit(0)
            else:
                data_to_add = {
                    "_id": gse_id.get('gse_id'),
                    "gse_patten": each_pattern['gse_patten'],
                    "gse_id": gse_id.get('gse_id'),
                    "last_updated": '-',
                    "status": "up_to_date",
                    "access": "private",
                    "sample_status": "valid"
                }
                all_series_data_to_add.append(update_to_add)
                print("GSE ID is private: " +
                      gse_id.get('gse_id'))
                exit(0)
        if len(all_series_data_to_add) > 0 or len(all_series_data_to_update) > 0:
            yield all_series_data_to_add, all_series_data_to_update


def __add_geo_sync_info_to_mongo(all_params):
    geo_mongo_instance = geo_mongo.GeoMongo()
    sub_series_pattern = all_params.get("list_to_parallel")
    get_gse_status = all_params.get("get_gse_status")
    sync_iter = __get_diff_between_geo_and_all_geo_series_sync_info(
        sub_series_pattern, get_gse_status)
    for data_to_add, data_to_update in sync_iter:
        oper = []
        print(data_to_update)
        for gse in data_to_add:
            oper.append(UpdateOne({"_id": gse.get("_id")}, {"$set": gse}, upsert=True))
        
        if len(oper) > 0:
            geo_mongo_instance.all_geo_series_collection.bulk_write(oper, ordered=False)
        exit(0)


def sync_status_from_geo(number_of_process, min_memory, shuffle=False):
    geo_mongo_instance = geo_mongo.GeoMongo()
    series_pattern = geo.get_series_parrerns_for_geo()
    get_gse_status = list(
        geo_mongo_instance.all_geo_series_collection.find({}))
    final_get_gse_status = {}
    for gse_id in get_gse_status:
        final_get_gse_status[gse_id.get("_id")] = gse_id

    __add_geo_sync_info_to_mongo(
        {"list_to_parallel": series_pattern, "get_gse_status": final_get_gse_status})
    # parallel_runner.add_data_in_parallel(
    #    __add_geo_sync_info_to_mongo, {"list_to_parallel": series_pattern, "get_gse_status": final_get_gse_status}, number_of_process, min_memory, shuffle)


def validate_sample():
    pipeline = [
        {"$group": {"_id": "$gse_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]

    geo_mongo_instance = geo_mongo.GeoMongo()
    print("Getting all series metadata")
    gse_id_list = list(geo_mongo_instance.series_metadata_collection.find(
        {}, projection={
            "_id": False,
            "Series_title": False,
            "Series_status": False,
            "Series_pubmed_id": False,
            "Series_web_link": False,
            "Series_summary": False,
            "Series_overall_design": False,
            "Series_type": False,
            "Series_contributor": False,
            "Series_contact_institute": False,
            "Series_supplementary_file": False,
            "Series_platform_id": False,
            "Series_relation": False,
            "Platform_title": False,
            "Platform_technology": False,
            "Platform_organism": False
        }
    ))
    print("Getting sample level count")
    count_details = geo_mongo_instance.sample_metadata_collection.aggregate(
        pipeline)

    sample_count_from_db = {}
    for result in count_details:
        sample_count_from_db[result['_id']] = result['count']

    print("Going to update db")
    oper = []
    for gse_id in gse_id_list:
        number_samples_from_geo = len(gse_id.get("Series_sample_id"))
        number_samples_from_db = sample_count_from_db.get(
            gse_id.get("gse_id"), 0)
        sample_status = "valid"

        if not (number_samples_from_geo == number_samples_from_db):
            status = geo_mongo_instance.all_geo_series_collection.find_one(
                {"_id": gse_id.get("gse_id")}).get("access")
            if status == "private":
                continue
            sample_status = "invalid"
            print("There is a sample number mismatch of " + str(number_samples_from_geo) +
                  "--" + str(number_samples_from_db) + ' ' + gse_id.get("gse_id"))

        oper.append(UpdateOne({"_id": gse_id.get("gse_id")}, {
                    "$set": {"sample_status": sample_status}}))

    geo_mongo_instance.all_geo_series_collection.bulk_write(
        oper, ordered=False)
    print("Update complete")


def __add_series_and_sample_metadata(all_params):
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


def add_update_metadata(number_of_process, min_memory, shuffle=False):
    geo_mongo_instance = geo_mongo.GeoMongo()
    list_to_add = list(geo_mongo_instance.all_geo_series_collection.find(
        {"$or": [{"$and": [{"access": {"$eq": "public"}}, {"sample_status": {"$eq": "invalid"}}, {"status": {"$not": {"$eq": "need_investination"}}}]}, {"$and": [{"access": {"$eq": "public"}}, {"status": {"$eq": "not_up_to_date"}}]}]}))

    print("Number of data to be updated/added: " + str(len(list_to_add)))

    parallel_runner.add_data_in_parallel(__add_series_and_sample_metadata, {
                                         "list_to_parallel": list_to_add}, number_of_process, min_memory, shuffle)


def main(function_call, process_number=None, min_memory=None, shuffle=None):

    if function_call.startswith("__"):
        raise NotImplementedError("Method %s not callable" % function_call)

    possibles = globals().copy()
    possibles.update(locals())
    method = possibles.get(function_call)

    if not method:
        raise NotImplementedError("Method %s not implemented" % function_call)

    while True:
        try:
            if function_call == "validate_sample":
                method()
            else:
                method(process_number, min_memory, shuffle)
        except Exception as err:
            print(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Choose the functions to run')
    parser.add_argument('--function', required=True,
                        help='functions that need to be called')

    process_number = None
    min_memory = None
    shuffle = None

    parser.add_argument('--number_of_process', required=True,
                        help='Number of process to run')
    parser.add_argument('--min_memory', required=True,
                        help='Min amount of memory in MB that need to be there for the process to run. If not there the process will restart')
    parser.add_argument('--shuffle', action=argparse.BooleanOptionalAction,
                        help='Should the list that is running in parallel shuffle or not by default its False')
    parser.set_defaults(shuffle=False)
    args = parser.parse_args()
    process_number = int(args.number_of_process)
    min_memory = int(args.min_memory)
    shuffle = args.shuffle

    main(args.function, process_number, min_memory, shuffle)
