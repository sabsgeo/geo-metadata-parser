from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd

import traceback
import json
import ast


class GeoMongo():

    def __init__(self):
        self.uri = "mongodb+srv://read_write_access:EcNPNcoW6T5Dsz6P@geocluster.f6yfva6.mongodb.net"
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        self.geo_db_name = "geodatasets"
        self.geo_db = None
        self.all_geo_series_collection_name = "all_geo_series"
        self.all_geo_series_collection = None
        self.state_management_system_collection_name = "state_management_system"
        self.state_management_system_collection = None
        self.series_metadata_collection_name = "series_metadata"
        self.series_metadata_collection = None
        self.sample_metadata_collection_name = "sample_metadata"
        self.sample_metadata_collection = None

        try:
            self.client.admin.command('ping')
            if not(self.geo_db_name in self.client.list_database_names()):
                raise Exception("Database " + self.geo_db_name + " not found")
            
            self.geo_db = self.client.get_database(self.geo_db_name)

            if not(self.all_geo_series_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.all_geo_series_collection_name + " not found")
            
            self.all_geo_series_collection = self.geo_db.get_collection(self.all_geo_series_collection_name)

            if not(self.state_management_system_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.state_management_system_collection_name + " not found")
            
            self.state_management_system_collection = self.geo_db.get_collection(self.state_management_system_collection_name)

            if not(self.series_metadata_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.series_metadata_collection_name + " not found")
            
            self.series_metadata_collection = self.geo_db.get_collection(self.series_metadata_collection_name)


            if not(self.sample_metadata_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.sample_metadata_collection_name + " not found")
            
            self.sample_metadata_collection = self.geo_db.get_collection(self.sample_metadata_collection_name)
            
            print("You successfully connected to MongoDB!")
        except Exception as err:
            raise Exception(traceback.format_exc())
        
        
    def first_time_add_geo_series(self):
        all_geo_series_df = pd.read_csv("/Users/sabugeorge-elucidata/Personal/GEO-parsed/geo_rna_seq_data.csv")
        all_geo_series_json = json.loads(all_geo_series_df.to_json(orient = "records"))
        for each_geo_series in all_geo_series_json:
            each_geo_series["_id"] = each_geo_series["gse_id"]
            self.all_geo_series_collection.insert_one(each_geo_series)

    def first_time_add_series_metadata(self):
        series_metadata_df = pd.read_csv("/Users/sabugeorge-elucidata/Personal/GEO-parsed/gse_id_info.csv")
        series_metadata_json = json.loads(series_metadata_df.to_json(orient = "records"))
        last_gse_id = None
        for each_geo_series in series_metadata_json:
            compare_gse_id = each_geo_series["gse_id"]
            if len(each_geo_series["gse_id"].split("-")) > 1:
                compare_gse_id = each_geo_series["gse_id"].split("-")[0]

            if not (last_gse_id == compare_gse_id):
                updated_data = {}
    
            if len(each_geo_series["gse_id"].split("-")) < 2:
                for metadata in each_geo_series:
                    if metadata in ["gse_id", "Series_title", "Series_status", "Series_web_link", "Series_contact_institute"]:
                        updated_data[metadata] = each_geo_series.get(metadata)
                        if updated_data[metadata] == None:
                            updated_data[metadata] = ""
                    if metadata in ["Series_summary", "Series_overall_design"]:
                        if (not (each_geo_series[metadata] == None ) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]")):
                            updated_data[metadata] = ". ".join(filter(None,ast.literal_eval(each_geo_series[metadata]))).replace(".. ", ". ")
                        elif each_geo_series[metadata] == None:
                            updated_data[metadata] = ""
                        else:
                            updated_data[metadata] = each_geo_series[metadata]
                    if metadata in ["Series_type", "Series_pubmed_id" ,"Series_contributor", "Series_sample_id", "Series_supplementary_file", "Series_platform_id", "Series_relation", "Platform_title", "Platform_technology", "Platform_organism"]:
                        if not (each_geo_series[metadata] == None ) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]") and len(each_geo_series[metadata]) == 2:
                            updated_data[metadata] = []
                        elif (not (each_geo_series[metadata] == None ) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]")):
                            updated_data[metadata] = list(filter(None,ast.literal_eval(each_geo_series[metadata])))
                        else:
                            updated_data[metadata] = [each_geo_series[metadata]]
                updated_data["_id"] = each_geo_series["gse_id"]
                self.series_metadata_collection.insert_one(updated_data)
            else:
                print(each_geo_series["gse_id"].split("-")[0])
                for metadata in each_geo_series:
                    if metadata == "gse_id":
                         updated_data[metadata] = each_geo_series["gse_id"].split("-")[0]
                    if metadata in ["Series_title", "Series_status", "Series_web_link", "Series_contact_institute"]:
                        updated_data[metadata] = each_geo_series.get(metadata)
                        if updated_data[metadata] == None:
                            updated_data[metadata] = ""
                    if metadata == "Series_pubmed_id":
                        updated_data[metadata] = ast.literal_eval(each_geo_series[metadata])
                    if metadata in ["Series_summary", "Series_overall_design"]:
                        if (not (each_geo_series[metadata] == None ) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]")):
                            updated_data[metadata] = ". ".join(filter(None,ast.literal_eval(each_geo_series[metadata]))).replace(".. ", ". ")
                        elif each_geo_series[metadata] == None:
                            updated_data[metadata] = ""
                        else:
                            updated_data[metadata] = each_geo_series[metadata]
                    if metadata in ["Series_type", "Series_contributor", "Series_sample_id", "Series_supplementary_file", "Series_platform_id", "Series_relation", "Platform_title", "Platform_technology", "Platform_organism"]:
                        if not (each_geo_series[metadata] == None ) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]") and len(each_geo_series[metadata]) == 2:
                            updated_data[metadata] = []
                        elif (not (each_geo_series[metadata] == None ) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]")):
                            if metadata == "Series_sample_id":
                                if metadata in updated_data:
                                    updated_data[metadata] = updated_data[metadata] + list(filter(None,ast.literal_eval(each_geo_series[metadata])))
                                else:
                                    updated_data[metadata] = list(filter(None,ast.literal_eval(each_geo_series[metadata])))
                            else:
                                updated_data[metadata] = list(filter(None,ast.literal_eval(each_geo_series[metadata])))
                        else:
                            updated_data[metadata] = [each_geo_series[metadata]]

                updated_data["_id"] = each_geo_series["gse_id"].split("-")[0]
                if self.series_metadata_collection.find_one({"_id" : updated_data["_id"]}) == None:
                    self.series_metadata_collection.insert_one(updated_data)
                else:
                    self.series_metadata_collection.update_one({"_id" : updated_data["_id"]}, {"$set": {"Series_sample_id": updated_data["Series_sample_id"]}})

            last_gse_id = each_geo_series["gse_id"]
            if len(each_geo_series["gse_id"].split("-")) > 1:
                last_gse_id = each_geo_series["gse_id"].split("-")[0]
            
                    
