from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd

import traceback


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
        