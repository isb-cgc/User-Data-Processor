#!/usr/bin/env python

# Copyright 2015, Google, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Command-line application to perform an synchronous query in BigQuery.

**This is a modified version of jonparrott's script:
https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/bigquery/api/sync_query.py
**

This sample is used on this page:

    https://cloud.google.com/bigquery/querying-data#syncqueries

For more information, see the README.md under /bigquery.
"""

import argparse
import json
import pandas as pd
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import numpy as np

def sync_query(bigquery, project_id, query, timeout=10000, num_retries=5):
    print ('Running query "%s"' % (query,))
    query_data = {
        'query': query,
        'timeoutMs': timeout,
    }
    return bigquery.jobs().query(
        projectId=project_id,
        body=query_data).execute(num_retries=num_retries)


def main(project_id, query, timeout, num_retries):
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)

    query_job = sync_query(
        bigquery,
        project_id,
        query,
        timeout,
        num_retries)

    # Page through the result set and print all results.
    results = list()
    page_token = None
    print ('Paging through the results..')
    while True:
        page = bigquery.jobs().getQueryResults(
            pageToken=page_token,
            **query_job['jobReference']).execute(num_retries=2)

        page_results = process_results(page)
        results = results + page_results
        page_token = page.get('pageToken')
        if not page_token:
            break

    return results

def process_results(results):
    fields = results['schema']['fields']
    rows = results['rows']
    results = []
    for row in rows:
        row_results = {}
        for i in xrange(0, len(fields)):
            cell = row['f'][i]
            field = fields[i]
            row_results[field['name']] = cell['v']
        results.append(row_results)
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Google Cloud project ID.')
    parser.add_argument('query', help='BigQuery SQL Query.')
    parser.add_argument(
        '-t', '--timeout',
        help='Number seconds to wait for a result',
        type=int,
        default=30)
    parser.add_argument(
        '-r', '--num_retries',
        help='Number of times to retry in case of 500 error.',
        type=int,
        default=5)

    args = parser.parse_args()

    results = main(
        args.project_id,
        args.query,
        args.timeout,
        args.num_retries)

    print results
