import json
from geo import geo_mongo
from geo import geo
from geo import parallel_runner
from geo import general_helper

import re
import json
from pymongo import MongoClient, InsertOne, DeleteOne, ReplaceOne, UpdateOne
list_k = general_helper.series_to_update_or_add()
print(len(list_k))
inst = geo_mongo.GeoMongo()
# for p in list_k:
#     all_data_ = list(inst.sample_metadata_collection.find(
#         {"gse_id": p.get('gse_id')}))
#     print("===============")
#     print(p.get('gse_id'))
#     print(str(len(all_data_)))
#     print(str(len(geo.get_samples_ids(p.get('gse_id')))))
#     print(len(inst.series_metadata_collection.find_one({"_id": p.get('gse_id')}).get("Series_sample_id")))
# print(len(geo.get_samples_ids("GSE229113")))
# count = 0
# update_list = {}
# oper = []
# for d in all_data_:
#     date_string = d.get("last_updated")
#     pattern = r'([A-Za-z]+) (\d{2}) (\d{4})'
#     matches = re.match(pattern, date_string)
#     if (matches):
#         oper.append(UpdateOne({"_id": d.get("_id")}, {"$set": {"access": "public" }}))

# inst.all_geo_series_collection.bulk_write(oper, ordered=False)
# print(count1)
# def __func(all_params):
#     all_data = all_params.get("list_to_parallel")
#     for d in all_data:
#         date_string = d.get("last_updated")
#         pattern = r'([A-Za-z]+) (\d{2}) (\d{4})'
#         matches = re.match(pattern, date_string)
#         if not (matches):
#             inst = geo_mongo.GeoMongo()
#             data = geo.get_series_metadata_from_soft_file(d.get("_id"))
#             if bool(data):
#                 dd = data.get("SERIES").get(d.get('_id')).get("Series_last_update_date")
#                 inst.all_geo_series_collection.update_one({"_id": d.get("_id")}, {"$set": {"last_updated":dd, "access": "public" }})
#             else:
#                 inst.all_geo_series_collection.update_one({"_id": d.get("_id")}, {"$set": {"sample_status": "invalid", "last_updated": "-", "access": "private" }})

# parallel_runner.add_data_in_parallel(__func, {"list_to_parallel": all_data_}, 100, 100)


# print(geo.get_series_metadata_from_soft_file("GSE228308"))


# geo_mongo_instance = geo_mongo.GeoMongo()
# gse_id_list = list(geo_mongo_instance.all_geo_series_collection.find(
#         {"$and" : [{"sample_status": {"$not": {"$eq": "invalid"}}}, {"sample_status": {"$not": {"$eq": "valid"}}}]}, projection={"_id": False, "gse_patten": False, "last_updated": False, "status": False}))

# geo_mongo_instance = geo_mongo.GeoMongo()
# gse_id_list = list(geo_mongo_instance.all_geo_series_collection.find(
#     {"sample_status": {"$not": {"$eq": "invalid"}}}, projection={"_id": False, "gse_patten": False, "last_updated": False, "status": False}))

# print(list(gse_id_list))
