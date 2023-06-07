from geo import geo
from geo import geo_mongo

def validate_sample_metadata(gse_id):
    number_samples_from_geo = len(geo.get_samples_ids(gse_id))
    db_inst = geo_mongo.GeoMongo()
    number_samples_from_db = db_inst.sample_metadata_collection.count_documents({"gse_id": gse_id})
    return number_samples_from_geo == number_samples_from_db

