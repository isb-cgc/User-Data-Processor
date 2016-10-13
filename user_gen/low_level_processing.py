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
from bigquery_etl.load import load_data_from_file
from bigquery_etl.transform.tools import cleanup_dataframe
import sys
import pandas as pd
from metadata_updates import update_metadata_data_list, update_molecular_metadata_samples_list, insert_feature_defs_list
from bigquery_table_schemas import get_molecular_schema


def parse_file(project_id, bq_dataset, bucket_name, file_data, filename, outfilename, metadata, cloudsql_tables):

    print 'Begin processing {0}.'.format(filename)

    # # connect to the cloud bucket
    # gcs = GcsConnector(project_id, bucket_name)
    #
    # #main steps: download, convert to df, cleanup, transform, add metadata
    # filebuffer = gcs.download_blob_to_file(filename)
    #
    # # convert blob into dataframe
    # data_df = convert_file_to_dataframe(filebuffer, skiprows=0, header=0)
    #
    # # Get basic column information depending on datatype
    # column_mapping = get_column_mapping(metadata['DataType'])
    #
    # data_df = cleanup_dataframe(data_df)
    # data_df.rename(columns=column_mapping, inplace=True)

    # Get barcodes and update metadata_data table
    # Assuming second scenario where each file is a different platform/pipeline combination
    # TODO: Put in functionality for other scenario where all lists are in one file.
    # sample_barcodes = list([k for d, k in data_df['SampleBarcode'].iteritems()])
    # file_list = list([k for d, k in data_df['filenamepath'].iteritems()])
    sample_metadata_list = []
    # for idx, barcode in enumerate(sample_barcodes):
    new_metadata = metadata.copy()
    new_metadata['sample_barcode'] = 'low_level_data_barcode'
    new_metadata['file_path'] = filename
    sample_metadata_list.append(new_metadata)
    update_metadata_data_list(cloudsql_tables['METADATA_DATA'], sample_metadata_list)

def get_column_mapping(columns):
    column_map = {}
    for column in columns:
        if 'MAP_TO' in column.keys():
            # pandas automatically replaces spaces with underscores, so we will too,
            # then map them to provided column headers
            column_map[column['NAME'].replace(' ', '_')] = column['MAP_TO']
    return column_map


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
