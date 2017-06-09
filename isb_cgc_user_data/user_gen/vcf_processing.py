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

"""Script to parse VCF files
"""

import sys

import pandas as pd
from isb_cgc_user_data.bigquery_etl.extract.gcloud_wrapper import GcsConnector

#
# The parse_file routine here previously was just a duplicate of the molecular stuff. Ditching it. The real
# processing, in process_vcf_files() below, has yet to be implemented!
#


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
