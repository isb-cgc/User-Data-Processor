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

from isb_cgc_user_data.bigquery_etl.extract import GcsConnector

def assert_notnull_property(df, columns_list=[]):
    """
    checks if a dataframe column values are NULL/NaN;
    param columns_list accepts a list
    returns NULL valuecolumns

    Example:
        Input Dataframe
                  a   b
            0  True   2
            1     3 NaN

        Returns column that has NULL value:
                  a   b
               1  3 NaN
    """
    if not columns_list:
        columns_list = df.columns.values
    null_rows_df = df[df[columns_list].isnull().any(axis=1)]
    null_row_count = len(null_rows_df)
    
    assert (null_row_count == 0), "Selected columns cannot have NULL/NaN values"

def test_blob_exists(project_id, bucket_name, df):
    """
    Checks if the DataFileNameKey blob exists in the bucket
    """
     # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name)
    for idx, row in df.iterrows():
        records = row.to_dict()
        print records['DatafileNameKey']
        blob_status = gcs.check_blob_exists(records['DatafileNameKey'])
        assert (blob_status is True), 'Blob doesnt exist'
