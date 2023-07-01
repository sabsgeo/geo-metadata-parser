# GEO SYNCER

This projects gets the dataset and sample level metadata from GEO and stores it in Mongodb

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Functions](#functions)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Installation

```bash
git clone <repo>
```
and run the main.py file

## Usage

Here are some example usages of the available functions:

### `add_update_metadata`

```bash
python main.py --function add_update_metadata --number_of_process 10 --min_memory 100 --shuffle
```

This command will add or update metadata for the GEO database in the internal database. It will run with 10 parallel processes and require a minimum of 100 memory units. The `shuffle` flag is set to True by default, which means the data will be shuffled before processing.

### `validate_sample`

```bash
python main.py --function validate_sample
```

This command will validate the sample level metadata in the internal database and update the sample status accordingly. The function will run with default settings, where the validation process is performed every 30 minutes.

### `sync_status_from_geo`

```bash
python main.py --function sync_status_from_geo --number_of_process 10 --min_memory 100
```

This command will synchronize the data status of GEO to the internal database. It will run with 10 parallel processes and require a minimum of 100 memory units. The `shuffle` flag is set to False by default.

Note: Replace `python` with the appropriate command if you are using a different Python interpreter.

Make sure to provide the necessary arguments and values for each function call according to your project requirements.

## Functions

List the available functions in your project along with their descriptions and usage instructions. Include any required arguments and default values, if applicable.

### `sync_status_from_geo(number_of_process, min_memory, shuffle=False)`

Synchronizes the data status of GEO to the internal database.

- `number_of_process (int)`: The number of parallel processes to run.
- `min_memory (int)`: The minimum memory that should be conserved in the system in which the function runs.
- `shuffle (bool, optional)`: Flag indicating whether to shuffle the data. Defaults to False.

### `validate_sample(run_interval=30)`

Validates the sample level metadata in the internal database and updates the sample status accordingly.

- `run_interval (int)`: The interval in minutes for running the validation process. Default is 30 minutes.

### `add_update_metadata(number_of_process, min_memory, shuffle=False)`

Adds or updates metadata for the GEO database to the internal database.

- `number_of_process (int)`: The number of parallel processes to run.
- `min_memory (int)`: The minimum memory that should be conserved in the system in which the function runs.
- `shuffle (bool, optional)`: Flag indicating whether to shuffle the data. Defaults to False.


### Access compressed assets
```python
k = geo_mongo.GeoMongo()
id = "PMC3439153/NIHMS381381-supplement-10.xlsx"
file = k.fs.get(id).read()
file1 = tarfile.open(fileobj=io.BytesIO(file),mode='r:gz')
file1.extract(id)
```
## Contributing

Not right now

## License

Sabu George

## Acknowledgements

Sabu George
