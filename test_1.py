from geo import geo_mongo
from geo import geo_helper

print(geo_helper.validate_sample_metadata("GSE3363"))
# geo_mongo_instance = geo_mongo.GeoMongo()
# gse_id_list_sub = list(geo_mongo_instance.series_metadata_collection.find(
#     {}, projection={"_id": False, 
#                     "Series_title": False, 
#                     "Series_status": False,
#                     "Series_web_link": False,
#                     "Series_summary": False,
#                     "Series_summary": False,
#                     "Series_overall_design": False,
#                     "Series_type": False,
#                     "Series_contributor": False,
#                     "Series_sample_id": False,
#                     "Series_contact_institute": False,
#                     "Series_supplementary_file": False,
#                     "Series_platform_id": False,
#                     "Series_relation": False,
#                     "Platform_title": False,
#                     "Platform_technology": False,
#                     "Platform_organism": False
#                     }))
# for element in gse_id_list_sub:
#     if isinstance(element['Series_pubmed_id'], int):
#         print(element["gse_id"])
#         geo_mongo_instance.series_metadata_collection.update_one(
#             {"_id": element["gse_id"]},
#             {"$set": {"Series_pubmed_id": [element['Series_pubmed_id']]}},
#             upsert=True
#         )