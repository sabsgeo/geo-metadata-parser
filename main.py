from geo import geo_mongo
from geo import model_data
from geo import parallel_runner
from geo import general_helper
from geo import geo
from pymongo import UpdateOne, DeleteOne, InsertOne, DeleteMany, InsertMany


import argparse
import traceback
import json


# Step that can be made parallel for add_series_metadata
def __add_series_metadata_to_mongo(all_params):
    gse_ids = all_params.get("list_to_parallel")
    geo_mongo_instance = geo_mongo.GeoMongo()
    data_model = model_data.ModelData()
    for gse_id in gse_ids:
        updated_data, _ = data_model.extract_all_metadata_info_from_softfile(
            gse_id.get("gse_id"))
        if bool(updated_data):
            geo_mongo_instance.series_metadata_collection.insert_one(
                updated_data)

# Function that adds data


def add_series_metadata(number_of_process, min_memory, shuffle):
    diff_list = general_helper.get_diff_between_all_geo_series_and_series_metadata()
    parallel_runner.add_data_in_parallel(
        __add_series_metadata_to_mongo, {"list_to_parallel": diff_list}, number_of_process, min_memory, shuffle)


# Step that can be made parallel for sync_metadata_status_from_geo
def __add_geo_sync_info_to_mongo(all_params):
    sub_series_pattern = all_params.get("list_to_parallel")
    general_helper.get_diff_between_geo_and_all_geo_series_sync_info(
        sub_series_pattern)

# Function that syncs status of updated data


def sync_metadata_status_from_geo(number_of_process, min_memory, shuffle):
    series_pattern = geo.get_series_parrerns_for_geo()
    parallel_runner.add_data_in_parallel(
        __add_geo_sync_info_to_mongo, {"list_to_parallel": series_pattern}, number_of_process, min_memory, shuffle)


def __add_geo_sample_info_to_mongo(all_params):
    gse_ids = all_params.get("list_to_parallel")
    geo_mongo_instance = geo_mongo.GeoMongo()
    data_model = model_data.ModelData()
    for gse_id in gse_ids:
        _, updated_data = data_model.extract_all_metadata_info_from_softfile(
            gse_id.get("gse_id"))
        if bool(updated_data):
            geo_mongo_instance.sample_metadata_collection.insert_many(
                updated_data, ordered=False)


def add_sample_metadata(number_of_process, min_memory, shuffle):
    gse_ids = general_helper.get_diff_between_all_geo_series_and_sample_metadata()
    print("Remaining samples to be added" + str(len(gse_ids)))
    parallel_runner.add_data_in_parallel(__add_geo_sample_info_to_mongo, {
                                         "list_to_parallel": gse_ids}, number_of_process, min_memory, shuffle)


def __check_sample_level_validity(all_params):
    gse_id_list = all_params.get("list_to_parallel")
    db_sample_count = all_params.get("db_sample_count")
    geo_mongo_instance = geo_mongo.GeoMongo()

    for gse_id in gse_id_list:
        samples = geo.get_samples_ids(gse_id.get("gse_id"))

        if len(samples) == 1 and samples[0] == -1:
            print("Not checking for samples from id " + gse_id.get("gse_id"))
            continue

        number_samples_from_geo = len(samples)
        number_samples_from_db = db_sample_count.get(gse_id.get("gse_id"), 0)
        sample_status = "valid"
        if not (number_samples_from_geo == number_samples_from_db):
            sample_status = "invalid"
            print("There is a sample number mismatch for " + gse_id.get("gse_id"))

        geo_mongo_instance.all_geo_series_collection.update_one({"_id": gse_id.get(
            "gse_id")},  {"$set": {"sample_status": sample_status}}, upsert=True)


def validate_sample(number_of_process, min_memory, shuffle):
    # get_non_status_entry = {"$and": [{"sample_status": {
    #     "$not": {"$eq": "invalid"}}}, {"sample_status": {"$not": {"$eq": "valid"}}}]}
    pipeline = [
        {"$group": {"_id": "$gse_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]

    geo_mongo_instance = geo_mongo.GeoMongo()
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

    count_details = geo_mongo_instance.sample_metadata_collection.aggregate(
        pipeline)

    sample_count_from_db = {}
    for result in count_details:
        sample_count_from_db[result['_id']] = result['count']
    oper = []
    for gse_id in gse_id_list:
        number_samples_from_geo = len(gse_id.get("Series_sample_id"))
        number_samples_from_db = sample_count_from_db.get(
            gse_id.get("gse_id"), 0)
        sample_status = "valid"

        if not (number_samples_from_geo == number_samples_from_db):
            sample_status = "invalid"
            print("There is a sample number mismatch of " + str(number_samples_from_geo) +
                  "--" + str(number_samples_from_db) + ' ' + gse_id.get("gse_id"))

        oper.append(UpdateOne({"_id": gse_id.get("gse_id")}, {
                    "$set": {"sample_status": sample_status}}))

    geo_mongo_instance.all_geo_series_collection.bulk_write(
        oper, ordered=False)


def __add_series_and_sample_metadata(all_params):
    list_to_add = all_params.get("list_to_parallel")
    data_model = model_data.ModelData()
    geo_instance = geo_mongo.GeoMongo()

    count = 0
    for gse_id in list_to_add:
        count = count + 1
        updated_series_data, updated_sample_data = data_model.extract_all_metadata_info_from_softfile(
            gse_id.get("gse_id"))
        
        # update series
        geo_instance.series_metadata_collection.delete_one(
            {"_id": gse_id.get("gse_id")})
        geo_instance.series_metadata_collection.insert_one(updated_series_data)
        
        # update sample
        geo_instance.sample_metadata_collection.delete_many(
            {"gse_id": gse_id.get("gse_id")})
        geo_instance.sample_metadata_collection.insert_many(
            updated_sample_data, ordered=False)
        # update status
        geo_instance.all_geo_series_collection.update_one({"_id": gse_id.get(
            "gse_id")}, {"$set": {"status": "up_to_date", "sample_status": "valid"}})
        print(gse_id)
        if count > 0:
            exit(0)


def add_update_metadata(number_of_process, min_memory, shuffle):
    list_to_add = general_helper.series_to_update_or_add()
    __add_series_and_sample_metadata({"list_to_parallel": list_to_add})
    # parallel_runner.add_data_in_parallel(__add_series_and_sample_metadata, {"list_to_parallel": list_to_add}, number_of_process, min_memory, shuffle)


def main(function_call, process_number, min_memory, shuffle):

    if function_call.startswith("__"):
        raise NotImplementedError("Method %s not callable" % function_call)

    possibles = globals().copy()
    possibles.update(locals())
    method = possibles.get(function_call)

    if not method:
        raise NotImplementedError("Method %s not implemented" % function_call)

    while True:
        try:
            method(process_number, min_memory, shuffle)
        except Exception as err:
            print(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Choose the functions to run')
    parser.add_argument('--function', required=True,
                        help='functions that need to be called')
    parser.add_argument('--process', required=True,
                        help='Number of process to run')
    parser.add_argument('--min_memory', required=True,
                        help='Min amount of memory in MB that need to be there for the process to run. If not there the process will restart')
    parser.add_argument('--shuffle', action=argparse.BooleanOptionalAction,
                        help='Should the list that is running in parallel shuffle or not by default its False')
    parser.set_defaults(shuffle=False)
    args = parser.parse_args()
    main(args.function, int(args.process), int(args.min_memory), args.shuffle)
