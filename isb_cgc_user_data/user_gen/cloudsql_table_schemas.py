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

from argparse import ArgumentParser

import MySQLdb

from isb_cgc_user_data.utils.sql_connector import cloudsql_connector
from isb_cgc_user_data.utils import build_config

def user_metadata():
    return [

        {'column_name': 'project_id',
         'type': 'INTEGER'},
        {'column_name': 'sample_barcode',
         'type': 'VARCHAR(200)'},
        {'column_name': 'file_path',
         'type': 'VARCHAR(200)'},
        {'column_name': 'file_name',
         'type': 'VARCHAR(200)'},
        {'column_name': 'data_type',
         'type': 'VARCHAR(200)'},
        {'column_name': 'pipeline',
         'type': 'VARCHAR(200)'},
        {'column_name': 'platform',
         'type': 'VARCHAR(200)'}
    ]

def user_metadata_sample():
    return [
        {'column_name': 'case_barcode',
         'type': 'VARCHAR(200)'},
        {'column_name': 'sample_barcode',
         'type': 'VARCHAR(200)'},
        {'column_name': 'has_mrna',
         'type': 'BOOLEAN'},
        {'column_name': 'has_mirna',
         'type': 'BOOLEAN'},
        {'column_name': 'has_protein',
         'type': 'BOOLEAN'},
        {'column_name': 'has_meth',
         'type': 'BOOLEAN'}
    ]

def user_feature_def():
    return [
        {'column_name': 'project_id',
         'type': 'INTEGER'},
        {'column_name': 'feature_name',
         'type': 'VARCHAR(200)'},
        {'column_name': 'bq_map_id',
         'type': 'VARCHAR(200)'},
        {'column_name': 'shared_map_id',
         'type': 'VARCHAR(200)'},
        {'column_name': 'is_numeric',
         'type': 'VARCHAR(200)'}
    ]

def create_test_tables(config, user_id=1, project_id=1):
    db = cloudsql_connector(config)

    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    create_col_template = '%s %s\n\t'

    # Create user_metdata
    user_metadata_schema = user_metadata()
    column_definitions = ','.join([create_col_template % (column['column_name'], column['type']) for column in user_metadata_schema])
    create_stmt = 'CREATE TABLE IF NOT EXISTS user_metadata_{0}_{1} ({2});'.format(user_id, project_id, column_definitions)
    # print create_stmt
    cursor.execute(create_stmt)

    # Create user_metadata_sample
    user_metadata_sample_schema = user_metadata_sample()
    column_definitions = ','.join([create_col_template % (column['column_name'], column['type']) for column in user_metadata_sample_schema])
    create_stmt = 'CREATE TABLE IF NOT EXISTS user_metadata_samples_{0}_{1} ({2});'.format(user_id, project_id, column_definitions)
    # print create_stmt
    cursor.execute(create_stmt)

    # Create user_feature_def
    user_feature_def_schema = user_feature_def()
    column_definitions = ','.join([create_col_template % (column['column_name'], column['type']) for column in user_feature_def_schema])
    create_stmt = 'CREATE TABLE IF NOT EXISTS user_feature_defs_{0}_{1} ({2});'.format(user_id, project_id, column_definitions)
    # print create_stmt
    cursor.execute(create_stmt)

    cursor.close()
    db.close()

def delete_test_tables(config, user_id=1, project_id=1):
    db = cloudsql_connector(config)
    cursor = db.cursor()

    delete_stmt = 'DROP TABLE user_metadata_{0}_{1}'.format(user_id, project_id)
    cursor.execute(delete_stmt)
    delete_stmt = 'DROP TABLE user_metadata_samples_{0}_{1}'.format(user_id, project_id)
    cursor.execute(delete_stmt)
    delete_stmt = 'DROP TABLE user_feature_defs_{0}_{1}'.format(user_id, project_id)
    cursor.execute(delete_stmt)


if __name__ == '__main__':
    config = build_config('config.txt')
    logger = None
    cmd_line_parser = ArgumentParser(description="Full sample set cohort utility")
    cmd_line_parser.add_argument('USER_ID', type=str, help="Google Cloud project ID")
    cmd_line_parser.add_argument('PROJECT_ID', type=str, help="Google Cloud project ID")
    cmd_line_parser.add_argument('-o', '--operation', type=str, choices=['create', 'delete'], default='create',
                                 help="Operation")
    args = cmd_line_parser.parse_args()

    user_id = args.USER_ID
    project_id = args.PROJECT_ID

    if args.operation == 'create':
        create_test_tables(config, user_id, project_id)
    elif args.operation == 'delete':
        delete_test_tables(config, user_id, project_id)
    else:
        print 'Operation not recognized. HOW DID THIS HAPPEN!?'


