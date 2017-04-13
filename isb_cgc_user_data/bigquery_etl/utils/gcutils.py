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

"""Script to parse MAF file
"""

from isb_cgc_user_data.bigquery_etl.extract.utils import convert_file_to_dataframe
from isb_cgc_user_data.bigquery_etl.transform.tools import cleanup_dataframe


def convert_blob_to_dataframe(gcs, project_id, bucket_name, filename, skiprows=0, logger=None):
    """
    Function to connect to google cloud storage, download the file,
    and convert to a dataframe
    """

    filebuffer = gcs.download_blob_to_file(filename)

    # convert blob into dataframe
    data_df = convert_file_to_dataframe(filebuffer, skiprows=skiprows, logger=logger)

    # clean-up dataframe
    data_df = cleanup_dataframe(data_df, logger=logger)

    return data_df

