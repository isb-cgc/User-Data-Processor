* [Overview](#overview)
* [Tutorial](#Tutorial)
* [Changelog](#changelog)
* [Documentation](#documentation)

<a id="overview"></a>
# Overview
--------------------------------------------------
This module provides tools for data extraction, transformation, and loading to Google BigQuery.

 * Extract
    * Connector to Google bucket - download objects (gcloud wrapper)
    * Convert data to Pandas dataframe data structure
 * Transform
    * Clean the data
        * select columns to load
        * utf-8 encoding etc
        * format columns names  etc
    * Add metadata
 * Load
    * Load data to both Google Storage and Bigquery
        * load in csv or newline json format

<a id="Tutorial"></a>
# Tutorial
--------------------------------------------------

## Example usage
-------------
### Google Storage
Example to download a file from the Google Storage, transform, and load to Google Storage and BigQuery
```python
# import the modules
from bigquery_etl.utils import gcutils
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.load import load_data_from_file

# connect to the google cloud bucket
gcs = GcsConnector(project_id, bucket_name)

# main steps: download, convert to df
data_df = gcutils.convert_blob_to_dataframe(gcs, project_id, bucket_name, filename, skiprows=1)

# clean up the dataframe for upload to BigQuery
data_df = cleanup_dataframe(data_df)

# upload the contents of the dataframe in njson format to google storage
# set metadata on the blob/object
metadata = {'info': 'etl-test'}
status = gcs.convert_df_to_njson_and_upload(data_df, outfilename, metadata=metadata)

# load the file from Google Storage to BigQuery 
load_data_from_file.run( project_id, bq_dataset, table_name, schema_file, file_location, 'NEWLINE_DELIMITED_JSON')
```

### Amazon S3
Example to download a file from the Amazon S3, transform, and load to Google Storage and BigQuery
```python
# import the modules
from bigquery_etl.utils import gcutils
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.load import load_data_from_file
import boto
import pandas

# main steps: download file(csv or tab) from S3, convert to df
s3 = boto.connect_s3()
key = s3.get_bucket('bucket_name').get_key('file.csv')
key_contents = key.key.get_contents_as_string()

# read the stringIO/file into a pandas dataframe
# load the file into a table
data_df = pandas.read_table(key_contents, sep="\t", skiprows=1, lineterminator='\n', comment='#')

# clean up the dataframe for upload to BigQuery
data_df = cleanup_dataframe(data_df)

# connect to the google cloud bucket
gcs = GcsConnector(project_id, bucket_name)

# upload the contents of the dataframe in njson format to google storage
# set metadata on the blob/object
metadata = {'info': 'etl-test'}
status = gcs.convert_df_to_njson_and_upload(data_df, outfilename, metadata=metadata)

# load the file from Google Storage to BigQuery
load_data_from_file.run( project_id, bq_dataset, table_name, schema_file, file_location, 'NEWLINE_DELIMITED_JSON')

```
<a id="changelog"></a>
# Change Log

### Version 1.0

- .0: First stable release with documentation.
- .0.1: Divided scripts into Extract, Transform, Load
 
<a id="documentation"></a>
# Documentation


## Installation
```sh
gcloud compute instances create data-etl-instance
gcloud compute ssh data-etl-instance
sudo apt-get update
sudo apt-get install -y build-essential python-dev libmysqlclient-dev python-pip git-core python-mysqldb
sudo pip install -U pip

# download the github repo
git clone https://github.com/isb-cgc/data-prototyping.git
cd data-prototyping/bigquery_etl 
pip install -r requirements.txt

#export path
export PYTHONPATH=[path to this module]
export SSL_DIR=[path to directory having SSL keys]
```
# License

please see docs/LICENSE.txt.
