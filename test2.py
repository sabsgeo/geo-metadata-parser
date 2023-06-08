import json
from geo import geo_mongo


geo_mongo_instance = geo_mongo.GeoMongo()
gse_id_list = list(geo_mongo_instance.all_geo_series_collection.find(
        {"$and" : [{"sample_status": {"$not": {"$eq": "invalid"}}}, {"sample_status": {"$not": {"$eq": "valid"}}}]}, projection={"_id": False, "gse_patten": False, "last_updated": False, "status": False}))
 
# geo_mongo_instance = geo_mongo.GeoMongo()
# gse_id_list = list(geo_mongo_instance.all_geo_series_collection.find(
#     {"sample_status": {"$not": {"$eq": "invalid"}}}, projection={"_id": False, "gse_patten": False, "last_updated": False, "status": False}))

print(list(gse_id_list))
