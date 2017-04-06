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
import requests
import traceback

import user_gen.user_gen_processing
import user_gen.molecular_processing
import user_gen.low_level_processing
import user_gen.vcf_processing
from google.cloud import logging
from isb_cgc_user_data.utils.build_config import read_dict

#
# Here we read the config and secret file
#

my_secrets = read_dict('../config/udu_secrets.txt')
my_config = read_dict('../config/udu_config.txt')
my_config.update(my_secrets)

STACKDRIVER_LOG = my_config['UDU_STACKDRIVER_LOG']

# STACKDRIVER LOGGING

logging_client = logging.Client()
logger = logging_client.logger(STACKDRIVER_LOG)

def generate_bq_schema(columns):
    obj = []
    columns.sort(key=lambda x: x['INDEX'])
    for column in columns:
        obj.append({'name': column['NAME'], 'type': column['TYPE']})
    return obj


def process_upload(user_data_config, success_url, failure_url):
    try:
        logger.log_text('uduprocessor handling request', severity='INFO')
        configs = open(user_data_config).read()
        logger.log_text('uduprocessor: configs: {0}'.format(user_data_config), severity='INFO')
        data = json.loads(configs)
        logger.log_text('uduprocessor: data: {0}'.format(data), severity='INFO')

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

        # Check for user_gen files and process them first
        user_gen_list = []
        mol_file_list = []
        vcf_file_list = []
        low_level_list = []
        for file in data['FILES']:
            if file['DATATYPE'] == 'user_gen':
                user_gen_list.append(file)
            elif file['DATATYPE'] == 'low_level':
                low_level_list.append(file)
            elif file['DATATYPE'] == 'vcf_file':
                vcf_file_list.append(file)
            else:
                mol_file_list.append(file)

        # TODO: Add processor for low level file listings

        logger.log_text('uduprocessor: Number of user_gen files: {0}'.format(len(user_gen_list)), severity='INFO')
        logger.log_text('uduprocessor: Number of vcf files: {0}'.format(len(vcf_file_list)), severity='INFO')
        logger.log_text('uduprocessor: Number of molecular files: {0}'.format(len(mol_file_list)), severity='INFO')
        logger.log_text('uduprocessor: Number of low level files: {0}'.format(len(low_level_list)), severity='INFO')

        # Process all user_gen files together
        if len(user_gen_list):
            logger.log_text('uduprocessor: Processing user_gen', severity='INFO')
            user_gen.user_gen_processing.process_user_gen_files(project_id,
                                                                user_project,
                                                                user_study,
                                                                bucketname,
                                                                bq_dataset,
                                                                cloudsql_tables,
                                                                user_gen_list,
                                                                my_config,
                                                                logger)
            logger.log_text('uduprocessor: Processed user_gen', severity='INFO')

        # Process all VCF Files
        if len(vcf_file_list):
            logger.log_text('uduprocessor: Processing vcf', severity='INFO')
            user_gen.vcf_processing.process_vcf_files(project_id,
                                                      user_project,
                                                      user_study,
                                                      bucketname,
                                                      bq_dataset,
                                                      cloudsql_tables,
                                                      vcf_file_list,
                                                      my_config,
                                                      logger)
            logger.log_text('uduprocessor: Processed vcf', severity='INFO')

        # Process all other datatype files
        if len(mol_file_list):
            logger.log_text('uduprocessor: Processing molecular', severity='INFO')
            for file in mol_file_list:
                table_name = file['BIGQUERY_TABLE_NAME']
                inputfilename = file['FILENAME']

                blob_name = inputfilename.split('/')[1:] # Path without bucket. Assuming bucket name appended to front of file path.
                logger.log_text('uduprocessor: Processing molecular {0}'.format(blob_name), severity='INFO')
                outputfilename = '{0}.out'.format(inputfilename.split('/')[-1]) # Get the actual file name
                bucket_name = inputfilename.split('/')[0] # Get the bucketname

                metadata = {
                    'sample_barcode': file.get('SAMPLEBARCODE', ''),
                    'participant_barcode': file.get('PARTICIPANTBARCODE', ''),
                    'project_id': user_study,
                #    'study_id': user_study,
                    'platform': file.get('PLATFORM', ''),
                    'pipeline': file.get('PIPELINE', ''),
                }

                # Update metadata_data table in cloudSQL
                metadata['file_path'] = inputfilename
                metadata['file_name'] = inputfilename.split('/')[-1]
                metadata['data_type'] = file['DATATYPE']

                # Transform and load metadata
                user_gen.molecular_processing.parse_file(project_id,
                                                         bq_dataset,
                                                         bucket_name,
                                                         file,
                                                         blob_name,
                                                         outputfilename,
                                                         metadata,
                                                         cloudsql_tables,
                                                         my_config,
                                                         logger
                                                        )
                logger.log_text('uduprocessor: Processed molecular {0}'.format(blob_name), severity='INFO')
            logger.log_text('uduprocessor: Processed molecular', severity='INFO')

        if len(low_level_list):
            logger.log_text('uduprocessor: Processing low-level', severity='INFO')
            for file in low_level_list:
                table_name = file['BIGQUERY_TABLE_NAME']

                inputfilename = file['FILENAME']
                blob_name = inputfilename.split('/')[
                            1:]  # Path without bucket. Assuming bucket name appended to front of file path.
                logger.log_text('uduprocessor: Processing low-level {0}'.format(blob_name), severity='INFO')
                outputfilename = '{0}.out'.format(inputfilename.split('/')[-1])  # Get the actual file name
                bucket_name = inputfilename.split('/')[0]  # Get the bucketname

                metadata = {
                    'sample_barcode': file.get('SAMPLEBARCODE', ''),
                    'participant_barcode': file.get('PARTICIPANTBARCODE', ''),
                    'project_id': user_study,
                 #   'study_id': user_study,
                    'platform': file.get('PLATFORM', ''),
                    'pipeline': file.get('PIPELINE', ''),
                }

                # Update metadata_data table in cloudSQL
                metadata['file_path'] = inputfilename
                metadata['file_name'] = inputfilename.split('/')[-1]
                metadata['data_type'] = file['DATATYPE']

                # Transform and load metadata
                user_gen.low_level_processing.parse_file(project_id,
                                                         bq_dataset,
                                                         bucket_name,
                                                         file,
                                                         blob_name,
                                                         outputfilename,
                                                         metadata,
                                                         cloudsql_tables,
                                                         my_config,
                                                         logger
                                                        )
                logger.log_text('uduprocessor: Processed low-level {0}'.format(blob_name), severity='INFO')
            logger.log_text('uduprocessor: Processed low-level', severity='INFO')
        logger.log_text('uduprocessor registering success', severity='INFO')
        requests.get(success_url)
    except:
        logger.log_text('uduprocessor registering failure', severity='ERROR')
        logger.log_text(traceback.format_exc(), severity='ERROR');
        requests.get(failure_url)


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

    process_upload(
        args.config_file
    )
