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

"""
Script to parse user generated data
"""
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.transform.tools import cleanup_dataframe
from bigquery_etl.load import load_data_from_file
import sys
import pandas as pd
from metadata_updates import update_metadata_data_list
from utils.sql_connector import cloudsql_connector


def process_user_gen_files(project_id, user_project_id, study_id, bucket_name, bq_dataset, cloudsql_tables, files):

    # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name)
    data_df = pd.DataFrame()

    # Collect all columns that get passed in for generating BQ schema later
    all_columns = []

    # For each file, download, convert to df
    for idx, file in enumerate(files):
        blob_name = file['FILENAME'].split('/')[1:]
        all_columns += file['COLUMNS']

        metadata = {
            'AliquotBarcode': file.get('ALIQUOTBARCODE', ''),
            'SampleBarcode': file.get('SAMPLEBARCODE', ''),
            'ParticipantBarcode': file.get('PARTICIPANTBARCODE', ''),
            'Study': study_id,
            'SampleTypeLetterCode': file.get('SAMPLETYPE', ''),
            'Platform': file.get('PLATFORM', ''),
            'Pipeline': file.get('PIPELINE', ''),
            'DataCenterName': file.get('DATACENTER', ''),
            'Project': user_project_id,
            'Filepath': file['FILENAME'],
            'FileName': file['FILENAME'].split('/')[-1],
            'DataType': file['DATATYPE']
        }

        # download, convert to df
        filebuffer = gcs.download_blob_to_file(blob_name)

        # Get column mapping
        column_mapping = get_column_mapping(file['COLUMNS'])

        if idx == 0:
            data_df = convert_file_to_dataframe(filebuffer, skiprows=0, header=0)
            data_df = cleanup_dataframe(data_df)
            data_df.rename(columns=column_mapping, inplace=True)

            # Generate Metadata for this file
            insert_metadata(data_df, metadata, cloudsql_tables['METADATA_DATA'])

        else:
            # convert blob into dataframe
            new_df = convert_file_to_dataframe(filebuffer, skiprows=0, header=0)
            new_df = cleanup_dataframe(new_df)
            new_df.rename(columns=column_mapping, inplace=True)

            # Generate Metadata for this file
            insert_metadata(new_df, metadata, cloudsql_tables['METADATA_DATA'])

            # TODO: Write function to check for participant barcodes, for now, we assume each file contains SampleBarcode Mapping
            data_df = pd.merge(data_df, new_df, on='SampleBarcode', how='outer')

    # For complete dataframe, create metadata_samples rows
    db = cloudsql_connector()
    data_df.to_sql(cloudsql_tables['METADATA_SAMPLES'], con=db, flavor='mysql', if_exists='replace')

    # Update metadata_samples and feature_defs

    # Update and create bq table
    temp_outfile = cloudsql_tables['METADATA_SAMPLES'] + '.out'
    gcs.convert_df_to_njson_and_upload(data_df, temp_outfile, tmp_bucket='isb-cgc-dev')

    # Using temporary file location (in case we don't have write permissions on user's bucket?
    source_path = 'gs://isb-cgc-dev/' + temp_outfile

    schema = generate_bq_schema(all_columns)
    table_name = 'cgc_user_{0}_{1}'.format(user_project_id, study_id)
    load_data_from_file.run(
        project_id,
        bq_dataset,
        table_name,
        schema,
        source_path,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',
        is_schema_file=False)

    # Delete temporary files
    print 'Deleting temporary file {0}'.format(temp_outfile)
    gcs = GcsConnector(project_id, 'isb-cgc-dev')
    gcs.delete_blob(temp_outfile)


def get_column_mapping(columns):
    column_map = {}
    for column in columns:
        if 'MAP_TO' in column.keys():
            column_map[column['NAME']] = column['MAP_TO']
    return column_map


def insert_metadata(data_df, metadata, table):
    sample_barcodes = list(set([k for d, k in data_df['SampleBarcode'].iteritems()]))
    sample_metadata_list = []
    for barcode in sample_barcodes:
        metadata['SampleBarcode'] = barcode
        sample_metadata_list.append(metadata)
    update_metadata_data_list(table, sample_metadata_list)


def generate_bq_schema(columns):
    obj = []
    seen_columns = []
    print columns
    for column in columns:
        # If the column has a mapping, use that as its name
        if 'MAP_TO' in column.keys():
            column['NAME'] = column['MAP_TO']
        if column['NAME'] not in seen_columns:
            seen_columns.append(column['NAME'])
            obj.append({'name': column['NAME'], 'type': column['TYPE']})
    return obj


if __name__ == '__main__':
    project_id = sys.argv[1]
    bucket_name = sys.argv[2]
    filename = sys.argv[3]
    outfilename = sys.argv[4]
    metadata = {
        'AliquotBarcode':'AliquotBarcode',
        'SampleBarcode':'SampleBarcode',
        'ParticipantBarcode':'ParticipantBarcode',
        'Study':'Study',
        'SampleTypeLetterCode':'SampleTypeLetterCode',
        'Platform':'Platform'
    }
