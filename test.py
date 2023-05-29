# from Bio import Entrez

# def get_journal_name(pubmed_id):
#     # Set your email address (required by PubMed API
#     Entrez.email = "sabu.george@elucidata.io"
#     try:
#         # Search for a publication by PubMed ID
#         handle = Entrez.efetch(db="pubmed", id=pubmed_id, rettype="xml")
#         record = Entrez.read(handle)
#     except:
#         return ""

#     journal_name = ""

#     try:
#     # Extract the journal name from the publication record
#         journal_name = record['PubmedArticle'][0]['MedlineCitation']['Article']['Journal']['Title']
#     except:
#         pass

#     return journal_name

# def get_pubmed_contributors(pmid):
#     """
#     Given a PubMed ID (PMID), retrieve the contributors of the publication using the Entrez module from Biopython.
#     """
#     # Provide your own email address to use with Entrez
#     Entrez.email = "sabu.george@elucidata.io"
#     try:
#         # Use Entrez to retrieve the summary of the PubMed record
#         handle = Entrez.esummary(db="pubmed", id=pmid)
#         record = Entrez.read(handle)[0]
#         handle.close()
#     except:
#         return []

#     # Extract the list of contributors from the summary and return it
#     if "AuthorList" in record:
#         return record["AuthorList"]
#     else:
#         return []

# get_pubmed_contributors("36253052")
# # Traceback (most recent call last):
# #   File "/Users/sabugeorge-elucidata/Personal/GEO-parsed/main.py", line 168, in get_all_gse_id_information
# #     get_pubmed_contributors(pubmed_id))
# #   File "/Users/sabugeorge-elucidata/Personal/GEO-parsed/main.py", line 20, in get_pubmed_contributors
# #     record = Entrez.read(handle)[0]
# #   File "/Users/sabugeorge-elucidata/Personal/GEO-parsed/venv/lib/python3.9/site-packages/Bio/Entrez/__init__.py", line 503, in read
# #     record = handler.read(handle)
# #   File "/Users/sabugeorge-elucidata/Personal/GEO-parsed/venv/lib/python3.9/site-packages/Bio/Entrez/Parser.py", line 392, in read
# #     self.parser.ParseFile(handle)
# #   File "/System/Volumes/Data/SWE/Apps/DT/BuildRoots/BuildRoot7/ActiveBuildRoot/Library/Caches/com.apple.xbs/Sources/python3/python3-133.100.1.1/Python/Modules/pyexpat.c", line 461, in EndElement
# #   File "/Users/sabugeorge-elucidata/Personal/GEO-parsed/venv/lib/python3.9/site-packages/Bio/Entrez/Parser.py", line 790, in endErrorElementHandler
# #     raise RuntimeError(data)
# # RuntimeError: UID=36253052: cannot get document summary

# from geo import geo
# from geo import model_data
# x = model_data.ModelData()
# p = x.extract_series_metadata_info_from_softfile("GSE65047")
# print(p)
# sample_id = "GSM459121"
# channel_data = {}
# if "Sample_channel_count" in soft_file["SAMPLE"][sample_id]:
#     for channel_count in range(int(soft_file["SAMPLE"][sample_id]["Sample_channel_count"])):
#         updated_channel_count = channel_count + 1
#         channel_key = "ch" + str(updated_channel_count)
#         if not (channel_key in channel_data.keys()):
#             channel_data[channel_key] = {}

#     for sample_keys in soft_file["SAMPLE"][sample_id].keys():
#         last_key = sample_keys.split("_")[-1]
#         if "ch" in last_key:
#             channel_data[last_key][sample_keys] = soft_file["SAMPLE"][sample_id][sample_keys]
# final_array_channel_data = []
# for channel_sh_keys in channel_data.keys():
#     channel_data[channel_sh_keys]["char_name"] = channel_sh_keys
#     final_array_channel_data.append(
#         channel_data[channel_sh_keys])

# final_array_channel_data = sorted(
#     final_array_channel_data, key=lambda x: x["char_name"])
# temp_array = []
# import json
# for obj_data in final_array_channel_data:
#     k = json.dumps(obj_data)
#     temp_array.append(k)

# final_array_channel_data = temp_array
# print(final_array_channel_data)

# import hashlib
# sha = hashlib.sha256()
# some_string = "all_geo_series"
# print(hashlib.sha1(some_string.encode("UTF-8")).hexdigest()[:24])


# import requests
# import json
# import re
# import urllib
# import io

# gse_id = "GSE215106"
# url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + gse_id + "&targ=all&form=text&view=full"

# with urllib.request.urlopen(url) as response:
#     text_data = response.read()

# stream = io.StringIO(text_data.decode())

# for line in stream:
#     print(line)

# response = requests.head(url)
# if re.search( gse_id + ".txt", json.dumps(dict(response.headers))):
#     print("Soft file exists")
# else:
#     print("Soft file does not exist")

from geo import geo_mongo
from geo import geo

# print(geo.read_full_soft_file("GSE136706"))
# import requests

# gse_id = "GSE85566"
# url = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE" + \
#         str("85") + "nnn/" + gse_id + "/soft/" + gse_id + "_family.soft.gz"
# # url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + gse_id + "&targ=all&form=text&view=full"
# print(url)
# print(int(requests.get(url, stream=True).headers.get("Content-Length")) >> 20)


geo_mongo_instance = geo_mongo.GeoMongo()
# # gse_id_list = list(geo_mongo_instance.all_geo_series_collection.find(
# #     {}, projection={"_id": False, "gse_patten": False, "last_updated": False, "status": False}))

# # x = geo_mongo_instance.series_metadata_collection.delete_many({"Series_title": ""})
# gse_id_list_sub = list(geo_mongo_instance.series_metadata_collection.find(
#     {"Series_title": ""}, projection={"_id": False, "Series_title": False, "Series_status": False, "Series_pubmed_id": False,
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
# print(gse_id_list_sub)
final_types = {
    "_id": set(),
    "gse_id": set(),
    "Series_title": set(),
    "Series_status": set(),
    "Series_pubmed_id": set(),
    "Series_web_link": set(),
    "Series_summary": set(),
    "Series_overall_design":set(),
    "Series_type":set(),
    "Series_contributor":set(),
    "Series_sample_id":set(),
    "Series_contact_institute":set(),
    "Series_supplementary_file":set(),
    "Series_platform_id":set(),
    "Series_relation":set(),
    "Platform_title":set(),
    "Platform_technology":set(),
    "Platform_organism":set()
}

gse_id_list_sub = list(geo_mongo_instance.series_metadata_collection.find(
    {}, projection={"_id": False, "Series_title": False, "Series_status": False, "Series_pubmed_id": False,
                    "Series_web_link": False,
                    "Series_summary": False,
                    "Series_summary": False,
                    "Series_overall_design": False,
                    "Series_type": False,
                    "Series_contributor": False,
                    "Series_sample_id": False,
                    "Series_contact_institute": False,
                    "Series_supplementary_file": False,
                    "Series_platform_id": False,
                    "Series_relation": False,
                    "Platform_title": False,
                    "Platform_technology": False,
                    "Platform_organism": False
                    }))
for gse_id in gse_id_list_sub:
    gse_series_metadata = geo_mongo_instance.series_metadata_collection.find_one({"_id": gse_id['gse_id']})
    for keys in gse_series_metadata:
        final_types[keys].add(type(gse_series_metadata.get(keys)))

print(final_types)

# full_lst = [item.get("gse_id") for item in gse_id_list]
# sub_lst = [item.get("gse_id") for item in gse_id_list_sub]
# diff_lst = list(set(full_lst) - set(sub_lst))
# print(len(diff_lst))

# obj_sub_lst = [ {"gse_id": item } for item in diff_lst]
# print(obj_sub_lst[:5])


# def get_diff_list():
#     geo_mongo_instance = geo_mongo.GeoMongo()
#     gse_id_list = list(geo_mongo_instance.all_geo_series_collection.find(
#         {}, projection={"_id": False, "gse_patten": False, "last_updated": False, "status": False}))

#     gse_id_list_sub = list(geo_mongo_instance.series_metadata_collection.find(
#         {}, projection={"_id": False, "Series_title": False, "Series_status": False, "Series_pubmed_id": False,
#                         "Series_web_link": False,
#                         "Series_summary": False,
#                         "Series_summary": False,
#                         "Series_overall_design": False,
#                         "Series_type": False,
#                         "Series_contributor": False,
#                         "Series_sample_id": False,
#                         "Series_contact_institute": False,
#                         "Series_supplementary_file": False,
#                         "Series_platform_id": False,
#                         "Series_relation": False,
#                         "Platform_title": False,
#                         "Platform_technology": False,
#                         "Platform_organism": False
#                         }))


#     full_lst = [item.get("gse_id") for item in gse_id_list]
#     sub_lst = [item.get("gse_id") for item in gse_id_list_sub]
#     diff_lst = list(set(full_lst) - set(sub_lst))
#     obj_sub_lst = [{"gse_id": item} for item in diff_lst]
#     return obj_sub_lst


# list1 = get_diff_list()
# list1 = [item.get("gse_id") for item in list1]
# print(list1)


# list2 = list(geo_mongo_instance.all_geo_series_collection.find({"status": "not_up_to_date"}))

# list2 = [item.get("gse_id") for item in list2]
# print(len(list2))
# print(len(list1))
# list3 = list(set(list1).intersection(list2))
# print(len(list3))
# print(list3)



