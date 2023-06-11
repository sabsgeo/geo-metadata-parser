import json
from geo import geo_mongo
from geo import geo
from geo import parallel_runner
from geo import general_helper

import re
import json
from pymongo import MongoClient, InsertOne, DeleteOne, ReplaceOne, UpdateOne

inst = geo_mongo.GeoMongo()

# with open('to_update.json', 'r') as openfile:
#     json_object = json.load(openfile)

# for id in json_object:
#     inst.all_geo_series_collection.update_one({"_id": id}, {"$set": {"status": "not_up_to_date"}})
#     print(id)

collection = inst.sample_metadata_collection

# with open('new_probl.json', 'r') as openfile:
#     json_object = json.load(openfile)
# gse_id = []
# for k in json_object:
#     gse_id.append(k.get("_id").split("__")[0])

# gse_id = list(set(gse_id))
# print(len(gse_id))
# with open("to_update.json", "w") as outfile:
#     json.dump(gse_id, outfile)

field_name = 'ch1'
min_key_value_pairs = 4

# Aggregation pipeline
pipeline = [
    {
        '$project': {
            'numKeyValuePairs': {'$size': {'$objectToArray': '$' + field_name}}
        }
    },
    {
        '$match': {
            'numKeyValuePairs': {'$lt': min_key_value_pairs}
        }
    }
]

# Execute the aggregation pipeline
result = collection.aggregate(pipeline)
doc = []
# Print the matched documents
for document in result:
    doc.append(document)

with open("new_probl1.json", "w") as outfile:
    json.dump(doc, outfile)
# inst = geo_mongo.GeoMongo()
# k = list(inst.sample_metadata_collection.find({"gse_id": "GSE230004"}))
# print(k)
