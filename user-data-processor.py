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

import user_gen.transform
from user_gen.metadata_updates import *
# from bigquery_etl.execution import process_manager
# from bigquery_etl.tests import tests
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
import argparse

# extract functions


# transform functions
transform_functions = {
    # 'protein': protein.transform.parse_protein,
    # 'mirna_mirna':  mirna.mirna.transform.parse_mirna,
    # 'mirna_isoform': mirna.isoform.transform.parse_isoform,
    # 'methylation': methylation.transform.parse_methylation,
    # 'cnv': cnv.transform.parse_cnv,
    # 'mrna_bcgsc': mrna.bcgsc.transform.parse_bcgsc,
    # 'mrna_unc': mrna.unc.transform.parse_unc,
    'user_gen': user_gen.transform.parse_file
}

def generate_bq_schema(columns):
    obj = []
    print columns
    columns.sort(key=lambda x: x['INDEX'])
    print columns
    for column in columns:
        obj.append({'name': column['NAME'], 'type': column['TYPE']})
    return obj


def main(user_data_config, etl_config_file):
    schemas_dir = os.path.join(os.getcwd(), 'schemas/')

    configs = open(user_data_config).read()
    data = json.loads(configs)

    project_id = data['GOOGLE_PROJECT']
    user_project = data['USER_PROJECT']
    bucketname = data['BUCKET']
    bq_dataset = data['BIGQUERY_DATASET']
    cloudsql_metadata_data = data['USER_METADATA_TABLES']['METADATA_DATA']
    cloudsql_metadata_samples = data['USER_METADATA_TABLES']['METADATA_SAMPLES']
    cloudsql_metadata_featuredefs = data['USER_METADATA_TABLES']['FEATURE_DEFS']

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
            'Study': file.get('STUDY', 0),
            'SampleTypeLetterCode': file.get('SAMPLETYPE', ''),
            'Platform': file.get('PLATFORM', ''),
            'Pipeline': file.get('PIPELINE', ''),
            'DataCenterName': file.get('DATACENTER', ''),
            'Project': user_project
        }

        # Load data into CloudSQL
        if file['DATATYPE'] == 'user_gen':
            # Update metadata_data table
            metadata['Filepath'] = inputfilename
            metadata['FileName'] = inputfilename.split('/')[-1]
            metadata['DataType'] = file['DATATYPE']
            update_metadata_data(cloudsql_metadata_data, metadata)

            # Check to see if need to append to metadata_samples
            columns = check_update_metadata_samples(cloudsql_metadata_samples, file['COLUMNS'])
            if len(columns):
                # Update table and append new columns of data
                cloudsql_append_column(cloudsql_metadata_samples, columns, file['COLUMNS'], inputfilename)

            # Check to see if need to update user feature_defs
            # TODO: Update user feature_defs



        # Transform and load into BigQuery
        status = transform_functions[file['DATATYPE']]( project_id,
                                                        bucket_name,
                                                        blob_name,
                                                        outputfilename,
                                                        metadata,
                                                        bq_dataset,
                                                        table_name
                                                        )


        source_path = 'gs://' + bucket_name + '/' + outputfilename
        if file['DATATYPE'] == 'user_gen':
            is_schema_file = False
            schema = generate_bq_schema(file['COLUMNS'])

        else:
            is_schema_file = True
            schema = schemas_dir + config['methylation']['schema_file']

        # load_data_from_file.run(
        #     project_id,
        #     bq_dataset,
        #     table_name,
        #     schema,
        #     source_path,
        #     source_format='NEWLINE_DELIMITED_JSON',
        #     write_disposition='WRITE_APPEND',
        #     is_schema_file=False)





        # Delete temporary files
        # print 'Deleting temporary file {0}'.format(outputfilename)
        # gcs = GcsConnector(project_id, bucket_name)
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
