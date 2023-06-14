#!/bin/bash
D=$(date +%A_%b_%d_%Y_%H:%M:%S)
mongoexport --uri "mongodb+srv://read_write_access:EcNPNcoW6T5Dsz6P@geocluster.f6yfva6.mongodb.net/geodatasets" --collection all_geo_series --out "/home/ubuntu/all_geo_series${D}.json"
mongoexport --uri "mongodb+srv://read_write_access:EcNPNcoW6T5Dsz6P@geocluster.f6yfva6.mongodb.net/geodatasets" --collection series_metadata --out "/home/ubuntu/series_metadata${D}.json"
mongoexport --uri "mongodb+srv://read_write_access:EcNPNcoW6T5Dsz6P@geocluster.f6yfva6.mongodb.net/geodatasets" --collection sample_metadata --out "/home/ubuntu/sample_metadata${D}.json"