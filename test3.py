import json
from geo import geo_mongo
from geo import geo
from geo import parallel_runner
from geo import general_helper

import re
import json
from pymongo import MongoClient, InsertOne, DeleteOne, ReplaceOne, UpdateOne

inst = geo_mongo.GeoMongo()
k = list(inst.sample_metadata_collection.find({"gse_id": "GSE221267"}))
print(len(k))
