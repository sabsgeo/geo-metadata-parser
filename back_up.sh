#!/bin/bash
D=$(date +%A_%b_%d_%Y_%H:%M:%S)
mongoexport --uri "mongodb://user:user@127.0.0.1:27017/geodatasets" --authenticationDatabase admin --collection all_geo_series --out "/home/ubuntu/all_geo_series${D}.json"
mongoexport --uri "mongodb://user:user@127.0.0.1:27017/geodatasets" --authenticationDatabase admin --collection series_metadata --out "/home/ubuntu/series_metadata${D}.json"
mongoexport --uri "mongodb://user:user@127.0.0.1:27017/geodatasets" --authenticationDatabase admin --collection sample_metadata --out "/home/ubuntu/sample_metadata${D}.json"
mongoexport --uri "mongodb://user:user@127.0.0.1:27017/geodatasets" --authenticationDatabase admin --collection pubmed_metadata --out "/home/ubuntu/pubmed_metadata${D}.json"