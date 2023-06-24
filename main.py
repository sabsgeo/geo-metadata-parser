from geo import geo, parallel_runner, geo_mongo, model_data
from pymongo import UpdateOne
from helpers import main_helper, general_helper

import argparse
import traceback
import time
import inspect


def sync_status_from_geo(for_n_days, number_of_process, min_memory, run_interval=60):
    """
    Synchronizes the data status of GEO to internal database.

    Args:
        for_n_days (int): Number of days back from todays date to be checked to find the modified GSE ID
        number_of_process (int): The number of parallel processes to run.
        min_memory (int): The minimum memory that shouuld be conserved in the system in which function runs.
        run_interval (int): The interval in minutes for running the validation process. Default is 60 minutes.

    Returns:
        None

    Raises:
        None

    """
    geo_mongo_instance = geo_mongo.GeoMongo()
    modified_gse_ids = geo.get_recently_modified_gse_ids(for_n_days)
    get_gse_status = list(
        geo_mongo_instance.all_geo_series_collection.find({}))
    final_get_gse_status = {}
    for gse_id in get_gse_status:
        final_get_gse_status[gse_id.get("_id")] = gse_id
    shuffle = True
    parallel_runner.add_data_in_parallel(
        main_helper.add_geo_sync_info_to_mongo, {"list_to_parallel": modified_gse_ids, "get_gse_status": final_get_gse_status, "run_interval": run_interval}, number_of_process, min_memory, shuffle)


def validate_sample(run_interval=30):
    """
    Validates the sample level metadata in the internal database and updates the sample status accordingly.

    Args:
        run_interval (int): The interval in minutes for running the validation process. Default is 30 minutes.

    Returns:
        None

    Raises:
        None

    """
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

    print("Going to update db")
    oper = []
    for gse_id in gse_id_list:
        sample_ids = gse_id.get("Series_sample_id")
        number_samples_from_geo = len(sample_ids)
        number_samples_from_db = geo_mongo_instance.sample_metadata_collection.count_documents({
                                                                                               "_id": {"$in": sample_ids}})
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
    time.sleep(run_interval * 60)


def add_update_metadata(number_of_process, min_memory, shuffle=False):
    """
    Adds or updates metadata for GEO database to internal database.

    Args:
        number_of_process (int): The number of parallel processes to run.
        min_memory (int): The minimum memory that should be conserved in the system in which function runs.
        shuffle (bool, optional): Flag indicating whether to shuffle the data. Defaults to False.

    Returns:
        None

    Raises:
        None

    """
    geo_mongo_instance = geo_mongo.GeoMongo()
    list_to_add = list(geo_mongo_instance.all_geo_series_collection.find(
        {"$or": [{"$and": [{"access": {"$eq": "public"}}, {"sample_status": {"$eq": "invalid"}}, {"status": {"$not": {"$eq": "need_investination"}}}]}, {"$and": [{"access": {"$eq": "public"}}, {"status": {"$eq": "not_up_to_date"}}]}]}))

    print("Number of data to be updated/added: " + str(len(list_to_add)))

    parallel_runner.add_data_in_parallel(main_helper.add_series_and_sample_metadata, {
                                         "list_to_parallel": list_to_add}, number_of_process, min_memory, shuffle)


def get_data_from_pubmed(number_of_process, min_memory):
    """
    Gets the data from pubmed

    Args:
        number_of_process (int): The number of parallel processes to run.
        min_memory (int): The minimum memory that should be conserved in the system in which function runs.

    Returns:
        None

    Raises:
        None

    """
    json_data = open('/mnt/series_metadataMonday_Jun_19_2023_04:22:06.json')
    json_data = json_data.read()
    each_json = json_data.split("\n")
    parallel_runner.add_data_in_parallel(main_helper.get_into_from_pubmed, {
                                         "list_to_parallel": each_json}, number_of_process, min_memory, False)


def add_data_from_pmc(number_of_process, min_memory):
    """
    Add the pmc data into mongodb

    Args:
        number_of_process (int): The number of parallel processes to run.
        min_memory (int): The minimum memory that should be conserved in the system in which function runs.

    Returns:
        None

    Raises:
        None

    """
    # geo_mongo_instance = geo_mongo.GeoMongo()
    # studies_with_pmc = geo_mongo_instance.pubmed_metadata_collection.find({"pmc_id": {"$ne": ""}}, projection={
    #     "title": False,
    #     "journal_title": False,
    #     "transliterated_title": False,
    #     "journal_title_abbreviation": False,
    #     "publication_type": False,
    #     "abstract": False,
    #     "medical_subject_headings": False,
    #     "source": False,
    #     "article_identifier": False,
    #     "general_note": False,
    #     "substance_name": False,
    #     "registry_number": False,
    # })
    to_be_added = main_helper.diff_bw_pmc_and_pubmed()
    general_helper.save_pmc_tar_path()
    parallel_runner.add_data_in_parallel(main_helper.add_metadata_from_pmc, {
                                         "list_to_parallel": to_be_added}, number_of_process, min_memory, False)


def main(function_call, all_func_args):
    wait_time_in_minutes = 5
    if function_call.startswith("__"):
        raise NotImplementedError("Method %s not callable" % function_call)

    possibles = globals().copy()
    possibles.update(locals())
    method = possibles.get(function_call)

    if not method:
        raise NotImplementedError("Method %s not implemented" % function_call)

    while True:
        try:
            method(**all_func_args)
        except Exception as err:
            print(traceback.format_exc())
        time.sleep(wait_time_in_minutes * 60)


if __name__ == "__main__":
    possibles = globals().copy()
    possibles.update(locals())
    avail_func = [k for k in possibles.keys() if inspect.isfunction(
        possibles.get(k)) and not (k.startswith("__")) and not (k == "main")]

    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.add_argument('--function', required=True, choices=avail_func,
                             help='functions that need to be called')
    main_args, _ = main_parser.parse_known_args()

    parser = argparse.ArgumentParser(parents=[main_parser])

    method = possibles.get(main_args.function)
    signature = inspect.signature(method)
    parse_doc = general_helper.parse_arguments_from_docstring(method)
    for k, v in signature.parameters.items():
        if v.default is inspect.Parameter.empty:
            parser.add_argument('--{}'.format(k), required=True,
                                help=parse_doc.get(k).get("description"))
        else:
            if isinstance(v.default, bool):
                parser.add_argument(
                    '--{}'.format(k), action=argparse.BooleanOptionalAction, help=parse_doc.get(k).get("description"))
            else:
                parser.add_argument(
                    '--{}'.format(k), help=parse_doc.get(k).get("description"))
            pos_arg = {k: v.default}
            parser.set_defaults(**pos_arg)
    args = parser.parse_args()
    func_args = args.__dict__.copy()
    del func_args['function']
    for val in parse_doc:
        converted_val = eval("{}({})".format(
            parse_doc.get(val).get('type'), func_args.get(val)))
        func_args[val] = converted_val

    main(args.function, func_args)
