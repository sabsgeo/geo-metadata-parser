git pull
sudo docker build -t geo-metadata-parser .
sudo docker run --net=host -d geo-metadata-parser --function sync_status_from_geo --for_n_days 20 --number_of_process 10 --min_memory 100
sudo docker run --net=host -d geo-metadata-parser --function add_update_metadata --number_of_process 20 --min_memory 100 --shuffle
sudo docker run --net=host -d geo-metadata-parser --function validate_sample --run_interval 2880
