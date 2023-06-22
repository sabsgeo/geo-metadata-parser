# from geo import pmc
# from helpers import general_helper

# general_helper.save_pmc_tar_path()
# print(pmc.parse_pmc_info("PMC10053706"))

import json
from pymongo import InsertOne
from geo import geo_mongo


k = open("pubmed_info_final.json")
p = json.load(k)
opers = []
f = geo_mongo.GeoMongo()
for data in p:
    c = p.get(data)
    d = c.copy()
    d["_id"] = d.get("pmid")
    opers.append(InsertOne(d))

f.pubmed_metadata_collection.bulk_write(opers, ordered=False)
