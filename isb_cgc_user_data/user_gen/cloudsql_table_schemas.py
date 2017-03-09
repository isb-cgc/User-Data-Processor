from argparse import ArgumentParser

import MySQLdb

from isb_cgc_user_data.utils.sql_connector import cloudsql_connector


def user_metadata():
    return [

        {'column_name': 'study_id',
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
        {'column_name': 'participant_barcode',
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
        {'column_name': 'study_id',
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

def create_test_tables(user_id=1, project_id=1):
    db = cloudsql_connector()

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

def delete_test_tables(user_id=1, project_id=1):
    db = cloudsql_connector()
    cursor = db.cursor()

    delete_stmt = 'DROP TABLE user_metadata_{0}_{1}'.format(user_id, project_id)
    cursor.execute(delete_stmt)
    delete_stmt = 'DROP TABLE user_metadata_samples_{0}_{1}'.format(user_id, project_id)
    cursor.execute(delete_stmt)
    delete_stmt = 'DROP TABLE user_feature_defs_{0}_{1}'.format(user_id, project_id)
    cursor.execute(delete_stmt)


if __name__ == '__main__':

    cmd_line_parser = ArgumentParser(description="Full sample set cohort utility")
    cmd_line_parser.add_argument('USER_ID', type=str, help="Google Cloud project ID")
    cmd_line_parser.add_argument('PROJECT_ID', type=str, help="Google Cloud project ID")
    cmd_line_parser.add_argument('-o', '--operation', type=str, choices=['create', 'delete'], default='create',
                                 help="Operation")
    args = cmd_line_parser.parse_args()

    user_id = args.USER_ID
    project_id = args.PROJECT_ID

    if args.operation == 'create':
        create_test_tables(user_id, project_id)
    elif args.operation == 'delete':
        delete_test_tables(user_id, project_id)
    else:
        print 'Operation not recognized. HOW DID THIS HAPPEN!?'


