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

from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.transform.tools import *
import json
from gcloud import storage
 
#------------------------------------------
# parse SDRF
# deprecate this to get info from CloudSQL
#------------------------------------------
def get_sdrf_info(project_id, bucket_name, disease_codes, header, set_index_col, search_patterns):

    client = storage.Client(project_id)
    bucket = client.get_bucket(bucket_name)

    # connect to google cloud storage
    gcs = GcsConnector(project_id, bucket_name)

    sdrf_info = pd.DataFrame()
    for disease_code in disease_codes:
        for blob in bucket.list_blobs(prefix=disease_code):
            sdrf_filename = blob.name
            if not all(x in sdrf_filename for x in search_patterns):
               continue
            print (sdrf_filename)

            filebuffer = gcs.download_blob_to_file(sdrf_filename) 
            # convert to a dataframe
            sdrf_df = convert_file_to_dataframe(filebuffer, skiprows=0)
            
            sdrf_df = cleanup_dataframe(sdrf_df)

            sdrf_df['Study'] = disease_code

            try:
               sdrf_df =sdrf_df.set_index(set_index_col)
            except:
               sdrf_df =sdrf_df.set_index("Derived_Array_Data_File")

            sdrf_info = sdrf_info.append(sdrf_df)

    print ("Done loading SDRF files.")
    return sdrf_info




