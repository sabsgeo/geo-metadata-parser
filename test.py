from geo import geo_mongo

import json

from pymongo import InsertOne

inst = geo_mongo.GeoMongo()

with open("pubmed_info_final.json") as f:
    json_data = json.load(f)
    opera = []
    for k in json_data:
        data_write = json_data[k].copy()
        data_write["_id"] = data_write.get("pmid")
        opera.append(InsertOne(data_write))
        print(data_write)
    inst.pubmed_metadata_collection.bulk_write(opera, ordered=False)
