import json
from geo import geo_mongo
from geo import geo
from geo import parallel_runner
from geo import general_helper
from geo import model_data

import re
import json
from pymongo import MongoClient, InsertOne, DeleteOne, ReplaceOne, UpdateOne

inst = geo_mongo.GeoMongo()
data_st = model_data.ModelData()

print(len(list(inst.sample_metadata_collection.find({"gse_id": "GSE225566"}))))

# print(data_st.extract_all_metadata_info_from_softfile("GSE195609"))