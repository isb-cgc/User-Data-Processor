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

"""Script to parse Protein files
"""

from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.transform.tools import cleanup_dataframe
import sys
import pandas as pd
from metadata_updates import update_metadata_data_list
from utils.sql_connector import cloudsql_connector
from cloudsql_table_schemas import user_metadata, user_feature_def, user_metadata_sample

def parse_file(project_id, bucket_name, filename, outfilename, metadata, cloudsql_tables, columns):

    # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name)

    #main steps: download, convert to df, cleanup, transform, add metadata
    filebuffer = gcs.download_blob_to_file(filename)

    # convert blob into dataframe
    data_df = convert_file_to_dataframe(filebuffer, skiprows=0, header=0)

    # clean-up dataframe
    data_df = cleanup_dataframe(data_df)
    new_df_data = []

    column_map = get_column_mapping(columns)
    map_values = {}

    # Column headers are sample ids
    for i, j in data_df.iteritems():
        if i in column_map.keys():
            map_values[column_map[i]] = [k for d, k in j.iteritems()]

        else:
            for k, m in j.iteritems():
                new_df_obj = {}
                new_df_obj['ParticipantBarcode'] = ''
                new_df_obj['SampleBarcode'] = i
                new_df_obj['AliquotBarcode'] = ''
                new_df_obj['Study'] = metadata['Study']
                new_df_obj['SampleTypeLetterCode'] = ''
                new_df_obj['Platform'] = metadata['Platform']
                new_df_obj['Pipeline'] = metadata['Pipeline']

                # Datatype specific fields
                new_df_obj['Gene_Name'] = map_values['Gene_Name'][k]
                new_df_obj['Long_Gene_Name'] = map_values['Long_Gene_Name'][k]
                new_df_obj['Count'] = m
                new_df_data.append(new_df_obj)
    new_df = pd.DataFrame(new_df_data)

    # Get unique barcodes and update metadata_data table
    sample_barcodes = list(set([k for d, k in new_df['SampleBarcode'].iteritems()]))
    sample_metadata_list = []
    for barcode in sample_barcodes:
        metadata['SampleBarcode'] = barcode
        sample_metadata_list.append(metadata)
    update_metadata_data_list(cloudsql_tables['METADATA_DATA'], sample_metadata_list)

    # # upload the contents of the dataframe in njson format
    status = gcs.convert_df_to_njson_and_upload(new_df, outfilename, metadata=metadata, tmp_bucket='isb-cgc-dev')
    return status


def get_column_mapping(columns):
    column_map = {}
    for column in columns:
        column_map[column['NAME']] = column['MAP_TO']
    return column_map


def add_metadata(data_df, metadata):
    """Add metadata info to the dataframe
    """
    data_df['AliquotBarcode'] = metadata['AliquotBarcode']
    data_df['SampleBarcode'] = metadata['SampleBarcode']
    data_df['ParticipantBarcode'] = metadata['ParticipantBarcode']
    data_df['Study'] = metadata['Study'].upper()
    data_df['SampleTypeLetterCode'] = metadata['SampleTypeLetterCode']
    data_df['Platform'] = metadata['Platform']
    data_df['Pipeline'] = metadata['Pipeline']
    data_df['Center'] = metadata['DataCenterName']
    return data_df

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
