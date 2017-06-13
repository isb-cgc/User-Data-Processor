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

"""Script to parse Molecular data files
"""

import os
import sys

import pandas as pd
from isb_cgc_user_data.bigquery_etl.extract.gcloud_wrapper import GcsConnector
from isb_cgc_user_data.bigquery_etl.extract.utils import convert_file_to_dataframe
from isb_cgc_user_data.bigquery_etl.load import load_data_from_file
from isb_cgc_user_data.bigquery_etl.transform.tools import cleanup_dataframe

from bigquery_table_schemas import get_molecular_schema
from metadata_updates import update_metadata_data_list, update_molecular_metadata_samples_list, insert_feature_defs_list, update_metadata_cases
from isb_cgc_user_data.utils.error_handling import UduException
from isb_cgc_user_data.utils.check_dataframe_dups import reject_row_duplicate_features
from isb_cgc_user_data.utils.check_dataframe_dups import reject_col_duplicate_barcodes


def parse_file(project_id, bq_dataset, bucket_name, file_data, filename,
               outfilename, metadata, cloudsql_tables, config, logger=None):
    if logger:
        logger.log_text('uduprocessor: Begin molecular processing {0}'.format(filename), severity='INFO')

    # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name, config, logger=logger)
    if logger:
        logger.log_text('uduprocessor: GcsConnector Success', severity='INFO')

    #main steps: download, convert to df, cleanup, transform, add metadata
    # NOTE: Since we are not providing rollover=True arg, this blob is being
    # loaded into a string buffer, not a file:
    filebuffer = gcs.download_blob_to_file(filename)
    if logger:
        logger.log_text('uduprocessor: download_blob_to_file success', severity='INFO')

    # convert blob into dataframe. We may get a parsing exception out of this if e.g. a
    # row has too many fields:
    data_df = convert_file_to_dataframe(filebuffer, skiprows=0, header=0, logger=logger)
    if logger:
        logger.log_text('uduprocessor: convert_file_to_dataframe success', severity='INFO')

    # clean-up dataframe. We can get a parsing exception out of this if the table has
    # only a header, and no rows of data:
    data_df = cleanup_dataframe(data_df, logger=logger)
    if logger:
        logger.log_text('uduprocessor: cleanup_dataframe success', severity='INFO')
    new_df_data = []

    map_values = {}

    # Get basic column information depending on datatype
    column_map = get_column_mapping(metadata['data_type'])

    # Reject duplicate features:

    reject_row_duplicate_features(data_df, logger, 'feature')

    # Reject duplicate barcodes:

    reject_col_duplicate_barcodes(data_df, logger, 'barcode')

    # Column headers are sample ids. Glue on new columns with metadata here as well.
    for i, j in data_df.iteritems():

        if i in column_map.keys():
            map_values[column_map[i]] = [k for d, k in j.iteritems()]

        else:
            for k, m in j.iteritems():
                new_df_obj = {}

                new_df_obj['sample_barcode'] = i # Normalized to match user_gen
                new_df_obj['project_id'] = metadata['project_id']
                new_df_obj['Platform'] = metadata['platform']
                new_df_obj['Pipeline'] = metadata['pipeline']

                # Optional values
                new_df_obj['Symbol'] = map_values['Symbol'][k] if 'Symbol' in map_values.keys() else ''
                new_df_obj['ID'] = map_values['ID'][k] if 'ID' in map_values.keys() else ''
                new_df_obj['TAB'] = map_values['Tab'][k] if 'Tab' in map_values.keys() else ''
                new_df_obj['Level'] = m
                new_df_data.append(new_df_obj)
    new_df = pd.DataFrame(new_df_data)
    if logger:
        logger.log_text('uduprocessor: new dataframe constructed success', severity='INFO')

    # Get unique barcodes and update metadata_data table
    sample_barcodes = list(set([k for d, k in new_df['sample_barcode'].iteritems()]))
    sample_metadata_list = []
    for barcode in sample_barcodes:
        new_metadata = metadata.copy()
        new_metadata['sample_barcode'] = barcode
        sample_metadata_list.append(new_metadata)
    update_metadata_data_list(config, cloudsql_tables['METADATA_DATA'], sample_metadata_list)
    if logger:
        logger.log_text('uduprocessor: update_metadata_data_list success', severity='INFO')

    # Update metadata_samples table
    update_molecular_metadata_samples_list(config, cloudsql_tables['METADATA_SAMPLES'], metadata['data_type'], sample_barcodes)
    update_metadata_cases(config, cloudsql_tables['METADATA_SAMPLES'])
    if logger:
        logger.log_text('uduprocessor: Update metadata_samples table success', severity='INFO')

    # Generate feature names and bq_mappings
    table_name = file_data['BIGQUERY_TABLE_NAME']
    feature_defs = generate_feature_Defs(metadata['data_type'], metadata['project_id'], project_id, bq_dataset, table_name, new_df, logger=logger)
    if logger:
        logger.log_text('uduprocessor: generate_feature_Defs success', severity='INFO')

    # Update feature_defs table
    insert_feature_defs_list(config, cloudsql_tables['FEATURE_DEFS'], feature_defs)
    if logger:
        logger.log_text('uduprocessor: insert_feature_defs_list success', severity='INFO')

    # upload the contents of the dataframe in njson format
    tmp_bucket = config['tmp_bucket']
    gcs.convert_df_to_njson_and_upload(new_df, outfilename, metadata=metadata, tmp_bucket=tmp_bucket)
    if logger:
        logger.log_text('uduprocessor: convert_df_to_njson_and_upload success', severity='INFO')

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
        is_schema_file=False,
        logger=logger)
    if logger:
        logger.log_text('uduprocessor: load_data_from_file.run success', severity='INFO')

    # Delete temporary files

    gcs = GcsConnector(project_id, tmp_bucket, config, logger=logger)
    gcs.delete_blob(outfilename)
    if logger:
        logger.log_text('uduprocessor: Deleted temporary file {0}'.format(outfilename), severity='INFO')

#
# Given the expected strings coming in from the column headers, which is the ID and which is the Symbol?
#
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
def generate_feature_Defs(datatype, study_id, bq_project, bq_dataset, bq_table, data_df, logger=None):
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

    # This line right here insures that even if there are duplicate row IDs, they are flattened here.
    # But that does not keep the duplicate row from getting into the BQ dataset!

    unique_symbols = list(pd.unique(data_df.Symbol.ravel()))
    feature_defs = []
    #
    # At the moment, blank lines at end of file are apparently turned into a row of NaNs, causing
    # this block to gag on getting floats instead of Strings to glue together. This also fails here if a
    # gene name in the first column is blank, since that also shows up as a float. Also if a sample name
    # is blank, again getting a float instead of a String:
    #
    try:
        if len(unique_symbols):
            for symbol in unique_symbols:
                feature_name = '{0} {1}'.format(datatype_name_mapping[datatype]['FeatureName'], symbol)
                bqmap = ':'.join([bq_project, bq_dataset, bq_table, datatype_name_mapping[datatype]['BqMapId'], symbol, 'Level'])
                feature_defs.append((study_id, feature_name, bqmap, None, True))
        else:
            feature_name = datatype_name_mapping[datatype]['FeatureName']
            bqmap = ':'.join([bq_project, bq_dataset, bq_table, datatype_name_mapping[datatype]['BqMapId'], 'Level'])
            feature_defs.append((study_id, feature_name, bqmap, None, True))
    except Exception as exp:
        if logger:
            logger.log_text("generate_feature_defs_error: {0}".format(str(exp.message)), severity='ERROR')
        user_message = "Error parsing file: {0}. Check for empty lines at end of file, or empty sample names or feature names.".format(str(exp.message))
        raise UduException(user_message)

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
