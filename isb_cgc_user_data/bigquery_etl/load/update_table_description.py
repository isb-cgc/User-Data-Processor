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

import argparse
import json
import time
import uuid
import sys
import os
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from isb_cgc_user_data.utils import build_config

def update_table_description(bigquery, project_id, dataset_id, table_name, table_description):

    # update table description
    # The API doesnt seem to accept while loading the job

    job_data = {
        'jobReference': {
            'projectId': project_id,
            'datasetId': dataset_id,
            'tableId' : table_name 
        },
        'tableId': table_name,
        "description": table_description
    }

    return bigquery.tables().update(
        projectId=project_id, tableId=table_name, datasetId=dataset_id,
        body=job_data).execute(num_retries=1)


def main(config, project_id, dataset_id, table_name, descriptions_file):

    current_cred = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    new_cred = config['privatekey_path']
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = new_cred
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = current_cred

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
    # [END build_service]

    with open(descriptions_file, 'r') as f:
        descriptions = json.load(f)

    table_description = descriptions[table_name]
    job = update_table_description(
         bigquery, 
         project_id,
         dataset_id,
         table_name,
         table_description)

    print 'Done'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Google Cloud project ID.')
    parser.add_argument('dataset_id', help='A BigQuery dataset ID.')
    parser.add_argument(
        'table_name', help='Name of the table to load data into.')
    parser.add_argument(
        '-d', '--descriptions_file',
        help='Path to a descriptions file describing the table.',
        type=str,
        default='table_descriptions.json'
        )

    args = parser.parse_args()

    config = build_config('config.txt')

    main(
        config,
        args.project_id,
        args.dataset_id,
        args.table_name,
        args.descriptions_file)
