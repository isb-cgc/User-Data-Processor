# Load data into a BigQuery table

This script is a modified version of jonparrot's [original](https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/bigquery/api/load_data_from_csv.py) BigQuery load script.

## command-line help 
```
usage: load_data_from_file.py [-h] [-p POLL_INTERVAL] [-r NUM_RETRIES]
                              [-t SOURCE_FORMAT]
                              project_id dataset_id table_name schema_file
                              data_path

Command-line application that loads data into BigQuery from a CSV file in
Google Cloud Storage.

This is a modified version of load_data_from_csv.py script from jonparrot
See other examples here: https://github.com/GoogleCloudPlatform/python-docs-samples

This sample is used on this page:

    https://cloud.google.com/bigquery/loading-data-into-bigquery#loaddatagcs

For more information, see the README.md under /bigquery.

positional arguments:
  project_id            Your Google Cloud project ID.
  dataset_id            A BigQuery dataset ID.
  table_name            Name of the table to load data into.
  schema_file           Path to a schema file describing the table schema.
  data_path             Google Cloud Storage path to the CSV data, for
                        example: gs://mybucket/in.csv

optional arguments:
  -h, --help            show this help message and exit
  -p POLL_INTERVAL, --poll_interval POLL_INTERVAL
                        How often to poll the query for completion (seconds).
  -r NUM_RETRIES, --num_retries NUM_RETRIES
                        Number of times to retry in case of 500 error.
  -t SOURCE_FORMAT, --source_format SOURCE_FORMAT
                        The source format can be NEWLINE_DELIMITED_JSON or
                        CSV. The default is CSV

```

## example run from command line:
```
* [sudo python load_data_from_file.py -t=[source-format] <project-id> <bigquery-dataset-id> <table-name> <schema-file> <path-to-file-in-bucket>]
* sudo python load_data_from_file.py -t=NEWLINE_DELIMITED_JSON abc-def tcga_data Clinical Clinical.json gs://Clinical.json
```
