#!/usr/bin/env python

# Copyright 2015-2017, Institute for Systems Biology.
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
import os
import sys

import pandas as pd
from isb_cgc_user_data.bigquery_etl.extract.gcloud_wrapper import GcsConnector
from isb_cgc_user_data.bigquery_etl.extract.utils import convert_file_to_dataframe
from isb_cgc_user_data.bigquery_etl.load import load_data_from_file
from isb_cgc_user_data.bigquery_etl.transform.tools import cleanup_dataframe

from metadata_updates import update_metadata_data_list, insert_metadata_samples, insert_feature_defs_list

def process_user_gen_files(project_id, user_project_id, study_id, bucket_name, bq_dataset, cloudsql_tables, files, config, logger=None):

    if logger:
        logger.log_text('uduprocessor: Begin processing user_gen files.', severity='INFO')

    # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name, config, logger=logger)
    data_df = pd.DataFrame()

    # Collect all columns that get passed in for generating BQ schema later
    all_columns = []

    # For each file, download, convert to df
    for idx, file in enumerate(files):
        blob_name = file['FILENAME'].split('/')[1:]
        all_columns += file['COLUMNS']

        metadata = {
            'sample_barcode': file.get('SAMPLEBARCODE', ''),
            'case_barcode': file.get('CASEBARCODE', ''),
            'project_id': study_id,
            'platform': file.get('PLATFORM', ''),
            'pipeline': file.get('PIPELINE', ''),
            'file_path': file['FILENAME'],
            'file_name': file['FILENAME'].split('/')[-1],
            'data_type': file['DATATYPE']
        }

        # download, convert to df
        filebuffer = gcs.download_blob_to_file(blob_name)

        # Get column mapping
        column_mapping = get_column_mapping(file['COLUMNS'])
        if idx == 0:
            data_df = convert_file_to_dataframe(filebuffer, skiprows=0, header=0)
            data_df = cleanup_dataframe(data_df, logger=logger)
            data_df.rename(columns=column_mapping, inplace=True)

            if metadata['case_barcode'] == '':
                # Duplicate samplebarcode with prepended 'cgc_'
                data_df['case_barcode'] = 'cgc_' + data_df['sample_barcode']
            else:
                # Make sure to fill in empty case barcodes
                data_df[metadata['case_barcode']][data_df['case_barcode']==None] = 'cgc_' + data_df['sample_barcode'][data_df['case_barcode']==None]

            # Generate Metadata for this file
            insert_metadata(data_df, metadata, cloudsql_tables['METADATA_DATA'], config)

        else:
            # convert blob into dataframe
            new_df = convert_file_to_dataframe(filebuffer, skiprows=0, header=0)
            new_df = cleanup_dataframe(new_df, logger=logger)
            new_df.rename(columns=column_mapping, inplace=True)

            # Generate Metadata for this file
            insert_metadata(new_df, metadata, cloudsql_tables['METADATA_DATA'], config)

            # TODO: Write function to check for case barcodes, for now, we assume each file contains SampleBarcode Mapping
            data_df = pd.merge(data_df, new_df, on='sample_barcode', how='outer')

    # For complete dataframe, create metadata_samples rows
    if logger:
        logger.log_text('uduprocessor: Inserting into data into {0}.'.format(cloudsql_tables['METADATA_SAMPLES'], severity='INFO'))
    data_df = cleanup_dataframe(data_df, logger=logger)
    data_df['has_mrna'] = 0
    data_df['has_mirna'] = 0
    data_df['has_protein'] = 0
    data_df['has_meth'] = 0
    insert_metadata_samples(config, data_df, cloudsql_tables['METADATA_SAMPLES'])

    # Update and create bq table file
    temp_outfile = cloudsql_tables['METADATA_SAMPLES'] + '.out'
    tmp_bucket = config['tmp_bucket']
    gcs.convert_df_to_njson_and_upload(data_df, temp_outfile, tmp_bucket=tmp_bucket)

    # Using temporary file location (in case we don't have write permissions on user's bucket?
    source_path = 'gs://' + tmp_bucket + '/' + temp_outfile

    schema = generate_bq_schema(all_columns)
    table_name = 'cgc_user_{0}_{1}'.format(user_project_id, study_id)
    load_data_from_file.run(
        config,
        project_id,
        bq_dataset,
        table_name,
        schema,
        source_path,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',
        is_schema_file=False,
        logger=logger)

    # Generate feature_defs
    feature_defs = generate_feature_defs(study_id, project_id, bq_dataset, table_name, schema)

    # Update feature_defs table
    insert_feature_defs_list(config, cloudsql_tables['FEATURE_DEFS'], feature_defs)

    # Delete temporary files
    if logger:
        logger.log_text('uduprocessor: Deleting temporary file {0}'.format(temp_outfile), severity='INFO')
    gcs = GcsConnector(project_id, tmp_bucket, config, logger=logger)
    gcs.delete_blob(temp_outfile)


def get_column_mapping(columns):
    column_map = {}
    for column in columns:
        if 'MAP_TO' in column.keys():
            # pandas automatically replaces spaces with underscores, so we will too,
            # then map them to provided column headers
            column_map[column['NAME'].replace(' ', '_')] = column['MAP_TO']
    return column_map


def insert_metadata(data_df, metadata, table, config):
    sample_barcodes = list(set([k for d, k in data_df['sample_barcode'].iteritems()]))
    sample_metadata_list = []
    for barcode in sample_barcodes:
        new_metadata = metadata.copy()
        new_metadata['sample_barcode'] = barcode
        sample_metadata_list.append(new_metadata)
    update_metadata_data_list(config, table, sample_metadata_list)

def generate_bq_schema(columns):
    obj = []
    seen_columns = []
    for column in columns:
        # If the column has a mapping, use that as its name
        if 'MAP_TO' in column.keys():
            column['NAME'] = column['MAP_TO']
        if column['NAME'] not in seen_columns:
            seen_columns.append(column['NAME'])
            if column['TYPE'].lower().startswith('varchar'):
                type = 'STRING'
            elif column['TYPE'].lower() == 'float':
                type = 'FLOAT'
            else:
                type = 'INTEGER'
            obj.append({'name': column['NAME'], 'type': type, 'shared_id': column['SHARED_ID']})
    return obj


'''
Function to generate a feature def for each feature in the dataframe except SampleBarcode
FeatureName: column name from metadata_samples
BqMapId: bq_project:bq_dataset:bq_table:column_name
'''
def generate_feature_defs(study_id, bq_project, bq_dataset, bq_table, schema):
    feature_defs = []
    for column in schema:
        if column['name'] != 'sample_barcode':
            feature_name = column['name']
            bq_map = ':'.join([bq_project, bq_dataset, bq_table, column['name']])
            if column['type'] == 'STRING':
                datatype = 0
            else:
                datatype = 1
            feature_defs.append((study_id, feature_name, bq_map, column['shared_id'], datatype))
    return feature_defs

if __name__ == '__main__':
    project_id = sys.argv[1]
    bucket_name = sys.argv[2]
    filename = sys.argv[3]
    outfilename = sys.argv[4]
    metadata = {
        'AliquotBarcode':'AliquotBarcode',
        'SampleBarcode':'SampleBarcode',
        'CaseBarcode':'CaseBarcode',
        'Study':'Study',
        'SampleTypeLetterCode':'SampleTypeLetterCode',
        'Platform':'Platform'
    }
