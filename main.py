from geo import geo_mongo
from geo import model_data
from geo import parallel_runner
from geo import geo
from pymongo import UpdateOne

import argparse
import traceback
import time
import inspect
import re 


def __get_diff_between_geo_and_all_geo_series_sync_info(gse_pattern_list, get_gse_status):
    time_to_sync_in_minutes = 2
    for each_pattern in gse_pattern_list:
        gse_ids = geo.get_gse_ids_from_pattern(each_pattern['gse_patten'])
        all_series_data_to_add = []
        all_series_data_to_update = []
        start_time = time.time()
        for gse_id in gse_ids:
            if geo.has_soft_file(gse_id['gse_id']):
                selected_one = get_gse_status.get(gse_id['gse_id'])
                series_data = geo.get_series_metadata_from_soft_file(gse_id.get('gse_id'))
                last_updated_date = "-"
                if "SERIES" in series_data:
                    last_updated_date = series_data.get("SERIES").get(gse_id.get('gse_id')).get("Series_last_update_date")
                else:
                    continue
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
                    update_to_add = {
                        "_id": gse_id.get('gse_id'),
                        "status": "not_up_to_date",
                        "last_updated": last_updated_date
                    }
                    all_series_data_to_update.append(update_to_add)
                    print("GSE ID has to be updated: " +
                          gse_id.get('gse_id'))
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
                all_series_data_to_add.append(data_to_add)
                print("GSE ID is private: " +
                      gse_id.get('gse_id'))
            end_time = time.time()
            if end_time - start_time > time_to_sync_in_minutes * 60:
                if len(all_series_data_to_add) > 0 or len(all_series_data_to_update) > 0:
                    yield all_series_data_to_add, all_series_data_to_update    
                start_time = time.time()
                all_series_data_to_add = []
                all_series_data_to_update = []

        if len(all_series_data_to_add) > 0 or len(all_series_data_to_update) > 0:
            yield all_series_data_to_add, all_series_data_to_update


def __add_geo_sync_info_to_mongo(all_params):
    geo_mongo_instance = geo_mongo.GeoMongo()
    sub_series_pattern = all_params.get("list_to_parallel")
    get_gse_status = all_params.get("get_gse_status")
    sync_iter = __get_diff_between_geo_and_all_geo_series_sync_info(
        sub_series_pattern, get_gse_status)
    for data_to_add, data_to_update in sync_iter:
        add_oper = []
        update_oper = []
        for add_gse in data_to_add:
            add_oper.append(UpdateOne({"_id": add_gse.get("_id")}, {
                            "$set": add_gse}, upsert=True))

        for update_gse in data_to_update:
            update_oper.append(UpdateOne({"_id": update_gse.get("_id")}, {
                               "$set": update_gse}, upsert=True))

        if len(add_oper) > 0:
            geo_mongo_instance.all_geo_series_collection.bulk_write(
                add_oper, ordered=False)

        if len(update_oper) > 0:
            geo_mongo_instance.all_geo_series_collection.bulk_write(
                update_oper, ordered=False)


def sync_status_from_geo(number_of_process, min_memory, shuffle=False):
    """
    Synchronizes the data status of GEO to internal database.

    Args:
        number_of_process (int): The number of parallel processes to run.
        min_memory (int): The minimum memory that shouuld be conserved in the system in which function runs.
        shuffle (bool, optional): Flag indicating whether to shuffle the data. Defaults to False.

    Returns:
        None

    Raises:
        None

    """
    geo_mongo_instance = geo_mongo.GeoMongo()
    series_pattern = geo.get_series_parrerns_for_geo()
    get_gse_status = list(
        geo_mongo_instance.all_geo_series_collection.find({}))
    final_get_gse_status = {}
    for gse_id in get_gse_status:
        final_get_gse_status[gse_id.get("_id")] = gse_id

    parallel_runner.add_data_in_parallel(
        __add_geo_sync_info_to_mongo, {"list_to_parallel": series_pattern, "get_gse_status": final_get_gse_status}, number_of_process, min_memory, shuffle)


def validate_sample():
    """
    Validates the sample level metadata in the internal database and updates the sample status accordingly.

    Args:
        None

    Returns:
        None

    Raises:
        None

    """
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

    parallel_runner.add_data_in_parallel(__add_series_and_sample_metadata, {
                                         "list_to_parallel": list_to_add}, number_of_process, min_memory, shuffle)

def __parse_arguments_from_docstring(func):
    docstring = inspect.getdoc(func)
    arg_pattern = r'\s+([\w_]+)\s*\(([^)]+)\):\s*([^:\n]+)'
    type_pattern = r'([^:,]+)'
    arg_matches = re.findall(arg_pattern, docstring)
    arguments = {}
    for arg_match in arg_matches:
        arg_name = arg_match[0]
        arg_type = ""

        type_match = re.search(type_pattern, arg_match[1])
        if type_match:
            arg_type = type_match.group(1).strip()

        arg_description = arg_match[2].strip()

        arguments[arg_name] = {"type": arg_type, "description": arg_description}

    return arguments

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
        time.sleep( wait_time_in_minutes * 60 )


if __name__ == "__main__":
    possibles = globals().copy()
    possibles.update(locals())
    avail_func = [ k for k in possibles.keys() if inspect.isfunction(possibles.get(k)) and not(k.startswith("__")) and not(k == "main")]
    
    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.add_argument('--function', required=True, choices=avail_func,
                        help='functions that need to be called')
    main_args, _ = main_parser.parse_known_args()

    parser = argparse.ArgumentParser(parents=[main_parser])

    method = possibles.get(main_args.function)
    signature = inspect.signature(method)
    parse_doc = __parse_arguments_from_docstring(method)
    for k, v in signature.parameters.items():
        if v.default is inspect.Parameter.empty:
            parser.add_argument('--{}'.format(k), required=True, help=parse_doc.get(k).get("description"))
        else:
            if isinstance(v.default, bool):
                parser.add_argument('--{}'.format(k), action=argparse.BooleanOptionalAction, help=parse_doc.get(k).get("description"))
            else:
                parser.add_argument('--{}'.format(k), help=parse_doc.get(k).get("description"))
            pos_arg = {k:v.default}
            parser.set_defaults(**pos_arg)
    args = parser.parse_args()
    func_args = args.__dict__.copy()
    del func_args['function']
    main(args.function, func_args)

