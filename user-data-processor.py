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

import json
import os

import user_gen.mrna_transform
import user_gen.user_gen_transform
from user_gen.bigquery_table_schemas import *
from user_gen.metadata_updates import *
# from bigquery_etl.execution import process_manager
# from bigquery_etl.tests import tests
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
import argparse
from bigquery_etl.load import load_data_from_file

# extract functions


# transform functions
transform_functions = {
    'mrna': user_gen.mrna_transform.parse_file,
    'user_gen': user_gen.user_gen_transform.parse_file
}

def generate_bq_schema(columns):
    obj = []
    columns.sort(key=lambda x: x['INDEX'])
    for column in columns:
        obj.append({'name': column['NAME'], 'type': column['TYPE']})
    return obj

def main(user_data_config, etl_config_file):
    schemas_dir = os.path.join(os.getcwd(), 'schemas/')

    configs = open(user_data_config).read()
    data = json.loads(configs)

    project_id = data['GOOGLE_PROJECT']
    user_project = data['USER_PROJECT']
    user_study = data['STUDY']
    bucketname = data['BUCKET']
    bq_dataset = data['BIGQUERY_DATASET']
    cloudsql_tables = {
        'METADATA_DATA': data['USER_METADATA_TABLES']['METADATA_DATA'],
        'METADATA_SAMPLES': data['USER_METADATA_TABLES']['METADATA_SAMPLES'],
        'FEATURE_DEFS': data['USER_METADATA_TABLES']['FEATURE_DEFS']
    }
    #--------------------------------------------
    # Execution
    #------------------------------------------------------
    # pmr = process_manager.ProcessManager(max_workers=max_workers, db=db_filename, table=table_task_queue_status)
    for file in data['FILES']:
        table_name = file['BIGQUERY_TABLE_NAME']

        inputfilename = file['FILENAME']
        blob_name = inputfilename.split('/')[1:] # Path without bucket. Assuming bucket name appended to front of file path.
        outputfilename = '{0}.out'.format(inputfilename.split('/')[-1]) # Get the actual file name
        bucket_name = inputfilename.split('/')[0] # Get the bucketname

        metadata = {
            'AliquotBarcode': file.get('ALIQUOTBARCODE', ''),
            'SampleBarcode': file.get('SAMPLEBARCODE', ''),
            'ParticipantBarcode': file.get('PARTICIPANTBARCODE', ''),
            'Study': user_study,
            'SampleTypeLetterCode': file.get('SAMPLETYPE', ''),
            'Platform': file.get('PLATFORM', ''),
            'Pipeline': file.get('PIPELINE', ''),
            'DataCenterName': file.get('DATACENTER', ''),
            'Project': user_project
        }

        # Update metadata_data table in cloudSQL
        metadata['Filepath'] = inputfilename
        metadata['FileName'] = inputfilename.split('/')[-1]
        metadata['DataType'] = file['DATATYPE']
        # update_metadata_data(cloudsql_metadata_data, metadata)


        # If user_gen, Load data into CloudSQL samples table
        if file['DATATYPE'] == 'user_gen':
            # TODO: Update table and append new columns of data

            pass


        # TODO: Update user feature_defs


        status = transform_functions[file['DATATYPE']]( project_id,
                                                        bucket_name,
                                                        blob_name,
                                                        outputfilename,
                                                        metadata,
                                                        cloudsql_tables,
                                                        file['COLUMNS'],
                                                        )
        # Transform and load into BigQuery

        # source_path = 'gs://' + bucket_name + '/' + outputfilename
        # # if file['DATATYPE'] == 'user_gen':
        # is_schema_file = False
        # # schema = generate_bq_schema(file['COLUMNS'])
        # if file['DATATYPE'] != 'user_gen':
        #     schema = get_gexp_schema()
        # else:
        #     schema = generate_bq_schema(file['COLUMNS'])
        #
        # load_data_from_file.run(
        #     project_id,
        #     bq_dataset,
        #     table_name,
        #     schema,
        #     source_path,
        #     source_format='NEWLINE_DELIMITED_JSON',
        #     write_disposition='WRITE_APPEND',
        #     is_schema_file=is_schema_file)
        #
        # # Delete temporary files
        # print 'Deleting temporary file {0}'.format(outputfilename)
        # gcs = GcsConnector(project_id, 'isb-cgc-dev')
        # gcs.delete_blob(outputfilename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'config_file',
        help='Path to the config file for the job'
    )

    args = parser.parse_args()

    # log_filename = 'etl_{0}.log'.format(args.datatype)
    # log_name = 'etl_{0}'.format(args.datatype)
    # log = configure_logging(log_name, log_filename)

    # print args.config_file

    main(
        args.config_file,
        './config/data_etl_template.json',
    )
