#!/bin/bash
git pull
sudo docker build -t geo-metadata-parser .
RAND=$(openssl rand -hex 6)
# sudo docker run --net=host --name=sync_status_from_geo_$RAND -d geo-metadata-parser --function sync_status_from_geo --for_n_days 20 --number_of_process 10 --min_memory 100
sudo docker run --net=host --name=add_update_metadata_$RAND -d geo-metadata-parser --function add_update_metadata --number_of_process 20 --min_memory 100 --shuffle
# sudo docker run --net=host --name=validate_sample_$RAND -d geo-metadata-parser --function validate_sample --run_interval 2880
