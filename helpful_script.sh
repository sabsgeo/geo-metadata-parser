python main.py --function add_update_metadata --number_of_process 10 --min_memory 100 --shuffle
python main.py --function validate_sample
python main.py --function sync_status_from_geo --number_of_process 10 --min_memory 100
mongoexport --uri "mongodb+srv://read_write_access:EcNPNcoW6T5Dsz6P@geocluster.f6yfva6.mongodb.net/geodatasets" --collection <> --out "/home/ubuntu/file.json"