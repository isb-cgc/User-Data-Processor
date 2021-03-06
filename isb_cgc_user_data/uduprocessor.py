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
import os
import urllib

import user_gen.user_gen_processing
import user_gen.molecular_processing
import user_gen.low_level_processing
import user_gen.vcf_processing
from not_psq.safe_logger import Safe_Logger
from isb_cgc_user_data.utils.build_config import read_dict
from isb_cgc_user_data.utils.processed_file import processed_name
from isb_cgc_user_data.utils.error_handling import UduException

#
# Here we read the config and secret file
#

my_secrets = read_dict('../config/udu_secrets.txt')
my_config = read_dict('../config/udu_config.txt')
my_config.update(my_secrets)

STACKDRIVER_LOG = my_config['UDU_STACKDRIVER_LOG']

# STACKDRIVER LOGGING

logger = Safe_Logger(STACKDRIVER_LOG)

def generate_bq_schema(columns):
    obj = []
    columns.sort(key=lambda x: x['INDEX'])
    for column in columns:
        obj.append({'name': column['NAME'], 'type': column['TYPE']})
    return obj


def process_upload(user_data_config, success_url, failure_url):
    try:
        #
        # OK, Pub/Sub does NOT guarantee that a message gets delivered ONLY once. It may come in > 1 time.
        # It might also be possible that the WebApp sends us the same job request twice, though this should
        # never happen.
        # So we need to flag the job is being/has been handled. Right now, with ony one worker handling
        # requests, we can get away with moving the config file to a new location. Once we set up multiple
        # workers, the operation will need to be more atomic (e.g. an atomic database entry).
        #

        processed = processed_name(user_data_config)
        have_processed = os.path.isfile(processed)
        have_pending = os.path.isfile(user_data_config)

        if have_processed:
            if have_pending:
                logger.log_text('uduprocessor unexpected duplicate request from web app ignored {0}'.format(user_data_config),
                                severity='ERROR')
            else:
                logger.log_text('uduprocessor duplicate pubsub message ignored {0}'.format(user_data_config),
                                severity='WARNING')
            return

        logger.log_text('uduprocessor handling request', severity='INFO')
        # Move the processed file:
        os.rename(user_data_config, processed)
        # This construct closes the file ASAP:
        with open(processed, 'r') as f:
            configs = f.read()
        logger.log_text('uduprocessor: configs: {0}'.format(user_data_config), severity='INFO')
        data = json.loads(configs)
        logger.log_text('uduprocessor: data: {0}'.format(data), severity='INFO')

        project_id = data['GOOGLE_PROJECT']
        user_project = data['USER_PROJECT']
        # Yes, the inbound config file still uses the "STUDY" keyword.
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


        # Current UI options (5/31/17). Note VCF is NOT an option.
        # High-level files:
        #   DNA Methylation
        #   Gene Expression
        #   microRNA
        #   Protein Expression
        #   Other
        # Low-level files


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

        # Process all VCF Files. NOTE: process_vcf_files is currently an unimplemented stub!
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
                    'case_barcode': file.get('CASEBARCODE', ''),
                    'project_id': user_study,
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
                    'case_barcode': file.get('CASEBARCODE', ''),
                    'project_id': user_study,
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
        callback_url = success_url;
        job_status = 'success'
        log_severity = 'INFO'

    except UduException as uexp:
        job_status = 'failure'
        log_severity = 'ERROR'
        logger.log_text(traceback.format_exc(), severity='ERROR');
        callback_url = '{0}&errmsg={1}'.format(failure_url, urllib.quote(uexp.message))

    except Exception as ex:
        job_status = 'failure'
        log_severity = 'ERROR'
        logger.log_text(traceback.format_exc(), severity='ERROR');
        ready_msg = urllib.quote('Unexpected error loading data')
        callback_url = '{0}&errmsg={1}'.format(failure_url, ready_msg);

    # Let the WebApp know what happened to the job.
    logger.log_text('uduprocessor registering {0} to {1}'.format(job_status, callback_url), severity=log_severity)
    r = requests.get(callback_url)
    if r.status_code < 400:
        logger.log_text('uduprocessor registered {0} with return code {1}'.format(job_status, str(r.status_code)), severity='INFO')
    else:
        logger.log_text('uduprocessor callback failed with return code {0}'.format(str(r.status_code)), severity='WARNING')


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
