from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import gridfs

import traceback


class GeoMongo():

    def __init__(self):
        #self.uri = "mongodb+srv://read_write_access:EcNPNcoW6T5Dsz6P@geocluster.f6yfva6.mongodb.net"
        self.uri = "mongodb://user:user@127.0.0.1:27017"
        self.uri = "mongodb://ec2-13-127-217-184.ap-south-1.compute.amazonaws.com:27017"
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        self.geo_db_name = "geodatasets"
        self.geo_db = None
        self.all_geo_series_collection_name = "all_geo_series"
        self.all_geo_series_collection = None
        self.series_metadata_collection_name = "series_metadata"
        self.series_metadata_collection = None
        self.sample_metadata_collection_name = "sample_metadata"
        self.sample_metadata_collection = None
        self.pubmed_metadata_collection_name = "pubmed_metadata"
        self.pubmed_metadata_collection = None
        self.pmc_metadata_collection_name = "pmc_metadata"
        self.pmc_metadata_collection = None
        
        self.fs = None

        try:
            self.client.admin.command('ping')
            if not(self.geo_db_name in self.client.list_database_names()):
                raise Exception("Database " + self.geo_db_name + " not found")
            
            self.geo_db = self.client.get_database(self.geo_db_name)

            if not(self.all_geo_series_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.all_geo_series_collection_name + " not found")
            
            self.all_geo_series_collection = self.geo_db.get_collection(self.all_geo_series_collection_name)


            if not(self.series_metadata_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.series_metadata_collection_name + " not found")
            
            self.series_metadata_collection = self.geo_db.get_collection(self.series_metadata_collection_name)


            if not(self.sample_metadata_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.sample_metadata_collection_name + " not found")
            
            self.sample_metadata_collection = self.geo_db.get_collection(self.sample_metadata_collection_name)

            if not(self.pubmed_metadata_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.pubmed_metadata_collection_name + " not found")
            
            self.pubmed_metadata_collection = self.geo_db.get_collection(self.pubmed_metadata_collection_name)

            if not(self.pmc_metadata_collection_name in self.geo_db.list_collection_names()):
                raise Exception("Collecttion " + self.pmc_metadata_collection_name + " not found")
            
            self.pmc_metadata_collection = self.geo_db.get_collection(self.pmc_metadata_collection_name)

            self.fs = gridfs.GridFS(self.geo_db)
            
            print("You successfully connected to MongoDB!")
        except Exception as err:
            raise Exception(traceback.format_exc())
        