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

"""Script to parse Protein files
"""

import os
import sys

import pandas as pd
from isb_cgc_user_data.bigquery_etl.extract.gcloud_wrapper import GcsConnector
from isb_cgc_user_data.bigquery_etl.extract.utils import convert_file_to_dataframe
from isb_cgc_user_data.bigquery_etl.load import load_data_from_file
from isb_cgc_user_data.bigquery_etl.transform.tools import cleanup_dataframe

from bigquery_table_schemas import get_molecular_schema

from metadata_updates import update_metadata_data_list, update_molecular_metadata_samples_list, insert_feature_defs_list


def parse_file(project_id, bq_dataset, bucket_name, file_data, filename, outfilename, metadata, cloudsql_tables, config, logger):

    logger.log_text('uduprocessor: Begin processing {0}.'.format(filename), severity='INFO')

    # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name, config, logger=logger)

    #main steps: download, convert to df, cleanup, transform, add metadata
    filebuffer = gcs.download_blob_to_file(filename)

    # convert blob into dataframe
    data_df = convert_file_to_dataframe(filebuffer, skiprows=0, header=0)

    # clean-up dataframe
    data_df = cleanup_dataframe(data_df, logger=logger)
    new_df_data = []

    map_values = {}

    # Get basic column information depending on datatype
    column_map = get_column_mapping(metadata['data_type'])

    # Column headers are sample ids
    for i, j in data_df.iteritems():
        if i in column_map.keys():
            map_values[column_map[i]] = [k for d, k in j.iteritems()]

        else:
            for k, m in j.iteritems():
                new_df_obj = {}

                new_df_obj['sample_barcode'] = i # Normalized to match user_gen
                new_df_obj['project_id'] = metadata['project_id']
                new_df_obj['study_id'] = metadata['study_id']
                new_df_obj['Platform'] = metadata['platform']
                new_df_obj['Pipeline'] = metadata['pipeline']

                # Optional values
                new_df_obj['Symbol'] = map_values['Symbol'][k] if 'Symbol' in map_values.keys() else ''
                new_df_obj['ID'] = map_values['ID'][k] if 'ID' in map_values.keys() else ''
                new_df_obj['TAB'] = map_values['TAB'][k] if 'TAB' in map_values.keys() else ''

                new_df_obj['Level'] = m
                new_df_data.append(new_df_obj)
    new_df = pd.DataFrame(new_df_data)

    # Get unique barcodes and update metadata_data table
    sample_barcodes = list(set([k for d, k in new_df['sample_barcode'].iteritems()]))
    sample_metadata_list = []
    for barcode in sample_barcodes:
        new_metadata = metadata.copy()
        new_metadata['sample_barcode'] = barcode
        sample_metadata_list.append(new_metadata)
    update_metadata_data_list(config, cloudsql_tables['METADATA_DATA'], sample_metadata_list)

    # Update metadata_samples table
    update_molecular_metadata_samples_list(config, cloudsql_tables['METADATA_SAMPLES'], metadata['data_type'], sample_barcodes)

    # Generate feature names and bq_mappings
    table_name = file_data['BIGQUERY_TABLE_NAME']
    feature_defs = generate_feature_Defs(metadata['data_type'], metadata['study_id'], project_id, bq_dataset, table_name, new_df)

    # Update feature_defs table
    insert_feature_defs_list(config, cloudsql_tables['FEATURE_DEFS'], feature_defs)

    # upload the contents of the dataframe in njson format
    tmp_bucket = config['tmp_bucket']
    gcs.convert_df_to_njson_and_upload(new_df, outfilename, metadata=metadata, tmp_bucket=tmp_bucket)

    # Load into BigQuery
    # Using temporary file location (in case we don't have write permissions on user's bucket?)
    source_path = 'gs://' + tmp_bucket + '/' + outfilename
    schema = get_molecular_schema()

    load_data_from_file.run(
        config,
        project_id,
        bq_dataset,
        table_name,
        schema,
        source_path,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',
        is_schema_file=False)

    # Delete temporary files
    logger.log_text('uduprocessor: Deleting temporary file {0}'.format(outfilename), severity='INFO')
    gcs = GcsConnector(project_id, tmp_bucket, config, logger=logger)
    gcs.delete_blob(outfilename)


def get_column_mapping(datatype):
    column_map = {
        'mrna': {
            'Name': 'ID',
            'Description': 'Symbol'
        },
        'mirna': {
            'miRNA_name': 'Symbol',
            'miRNA_ID': 'ID'
        },
        'protein': {
            'Protein_Name': 'Tab',
            'Gene_Name': 'Symbol',
            'Gene_Id': 'ID',
            'Expression': 'Level'
        },
        'meth': {
            'Probe_ID': 'ID',
        }
    }
    if datatype in column_map:
        return column_map[datatype]
    else:
        return {}

'''
Function to generate a list of feature def for that datatype
FeatureName will look like this: [Datatype] [Symbol (if available)]
BqMapId will look like this: bq_project:bq_dataset:bq_table:datatype:symbol(if available):Level
'''


def generate_feature_Defs(datatype, study_id, bq_project, bq_dataset, bq_table, data_df):
    datatype_name_mapping = {
        'mrna': {
            'FeatureName': 'Gene Expression',
            'BqMapId': 'GEXP'
        },
        'mirna': {
            'FeatureName': 'MicroRNA Expression',
            'BqMapId': 'MIRN'
        },
        'protein': {
            'FeatureName': 'Protein Expression',
            'BqMapId': 'RPPA'
        },
        'meth': {
            'FeatureName': 'Methylation',
            'BqMapId': 'METH'
        }
    }

    unique_symbols = list(pd.unique(data_df.Symbol.ravel()))
    feature_defs = []
    if len(unique_symbols):
        for symbol in unique_symbols:
            feature_name = '{0} {1}'.format(datatype_name_mapping[datatype]['FeatureName'], symbol)
            bqmap = ':'.join([bq_project, bq_dataset, bq_table, datatype_name_mapping[datatype]['BqMapId'], symbol, 'Level'])
            feature_defs.append((study_id, feature_name, bqmap, None, True))
    else:
        feature_name = datatype_name_mapping[datatype]['FeatureName']
        bqmap = ':'.join([bq_project, bq_dataset, bq_table, datatype_name_mapping[datatype]['BqMapId'], 'Level'])
        feature_defs.append((study_id, feature_name, bqmap, None, True))

    return feature_defs


def process_vcf_files(project_id, user_project, user_study, bucketname, bq_dataset, cloudsql_tables, vcf_file_list, config, logger):
    logger.log_text('uduprocessor: Begin processing user_gen files.', severity='INFO')

    # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name, config, logger=logger)
    data_df = pd.DataFrame()

    # Run VCF to MAF processor

    # Upload MAF to table

    return True

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
