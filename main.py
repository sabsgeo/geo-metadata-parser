from geo import geo_mongo
from geo import model_data
from geo import parallel_runner
from geo import general_helper
from geo import geo

import argparse
import traceback


# Step that can be made parallel for add_series_metadata
def __add_series_metadata_to_mongo(gse_ids):
    geo_mongo_instance = geo_mongo.GeoMongo()
    data_model = model_data.ModelData()
    for gse_id in gse_ids:
        updated_data = data_model.extract_series_metadata_info_from_softfile(
            gse_id.get("gse_id"))
        if bool(updated_data):
            geo_mongo_instance.series_metadata_collection.insert_one(
                updated_data)

# Function that adds data
def add_series_metadata(number_of_process, min_memory, shuffle):
    diff_list = general_helper.get_diff_between_all_geo_series_and_series_metadata()
    parallel_runner.add_data_in_parallel(
        __add_series_metadata_to_mongo, diff_list, number_of_process, min_memory, shuffle)


# Step that can be made parallel for sync_metadata_status_from_geo
def __add_geo_sync_info_to_mongo(sub_series_pattern):
    general_helper.get_diff_between_geo_and_all_geo_series_sync_info(
        sub_series_pattern)

# Function that syncs status of updated data
def sync_metadata_status_from_geo(number_of_process, min_memory, shuffle):
    series_pattern = geo.get_series_parrerns_for_geo()
    parallel_runner.add_data_in_parallel(
        __add_geo_sync_info_to_mongo, series_pattern, number_of_process, min_memory, shuffle)

def __add_geo_sample_info_to_mongo(gse_ids):
    geo_mongo_instance = geo_mongo.GeoMongo()
    data_model = model_data.ModelData()
    for gse_id in gse_ids:
        updated_data = data_model.extract_sample_metadata_info_from_softfile(
            gse_id.get("gse_id"))
        if bool(updated_data):
            geo_mongo_instance.sample_metadata_collection.insert_many(updated_data, ordered=False)

def add_sample_metadata(number_of_process, min_memory, shuffle):
    gse_ids = general_helper.get_diff_between_all_geo_series_and_sample_metadata()
    print("Remaining samples to be added" + str(len(gse_ids)))
    parallel_runner.add_data_in_parallel(__add_geo_sample_info_to_mongo, gse_ids, number_of_process, min_memory, shuffle)


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
