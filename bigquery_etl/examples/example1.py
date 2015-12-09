#!/usr/bin/env python

# Copyright 2015, Institute for Systems Biology.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" example tutorial script
"""
from bigquery_etl.utils import gcutils
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.load import load_data_from_file

# run python demo.py

def main():
    """Example to download a file from the Google Storage, transform,
        and load to Google Storage and BigQuery
    """

    project_id = ''
    bucket_name = ''
    # example file in bucket
    filename = ''
    outfilename = ''

    # connect to the google cloud bucket
    gcs = GcsConnector(project_id, bucket_name)

     # main steps: download, convert to df
    data_df = gcutils.convert_blob_to_dataframe(gcs, project_id, bucket_name, filename, skiprows=1)

    #---------------------------------------------------------
    # get required information
    # get chromosome 1 and Genomic_Coordinate > 20000000
    #---------------------------------------------------------
    data_df = (data_df.query("Chromosome == '1' and Genomic_Coordinate > 20000000")\
                 .query("Beta_value > 0.2"))
    # we can assign this query to a new dataframe and have new data

    # upload the contents of the dataframe in njson format to google storage
    # set metadata on the blob/object
    metadata = {'info': 'etl-test'}
    status = gcs.convert_df_to_njson_and_upload(data_df, outfilename, metadata=metadata)

    print status

    # load the file from Google Storage to BigQuery
    #load_data_from_file.run(project_id, bq_dataset, table_name, schema_file,
    #                        file_location, 'NEWLINE_DELIMITED_JSON')


if __name__ == '__main__':
    main()
