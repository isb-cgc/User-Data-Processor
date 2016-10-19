import MySQLdb
import pandas as pd
from cloudsql_table_schemas import user_metadata, user_feature_def, user_metadata_sample
from utils.sql_connector import cloudsql_connector


def cloudsql_append_column(table, missing_column_names, all_columns, inputfilename):
    columns = filter(lambda column: column['NAME'] in missing_column_names, all_columns)

    alter_stmt = 'ALTER TABLE {0} ADD '.format(table)

    for idx, column in enumerate(columns):
        if column['TYPE'] == 'STRING':
            column['TYPE'] = 'VARCHAR(200)'
        if idx == 0:
            alter_stmt += '({0} {1}'.format(column['NAME'], column['TYPE'])
        else:
            alter_stmt += ',{0} {1}'.format(column['NAME'], column['TYPE'])

    alter_stmt += ');'
    # print alter_stmt
    db = cloudsql_connector()
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(alter_stmt)
    db.commit()

    # TODO: add data from inputfile

    cursor.close()
    db.close()


'''
Function to check if the given columns are the same as the table we're trying to insert into.
Returns list of columns that are not in the the table.
'''
def check_update_metadata_samples(table, columns):
    db = cloudsql_connector()
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute('describe {0};'.format(table))
    column_names = [d['NAME'] for d in columns]

    # For each column in the table, remove it from the list of given column names
    for row in cursor.fetchall():
        if row['Field'] in column_names:
            column_names.remove(row['Field'])

    cursor.close()
    db.close()
    return column_names


'''
Function to append data to a given metadata_data table.
Takes in one row only.
'''
def update_metadata_data(table, metadata):
    metadata_schema = user_metadata()
    column_titles = [d['column_name'] for d in metadata_schema]
    value_list = []
    for idx, title in enumerate(column_titles):
        if idx == 0:
            value_list.append((metadata[title]))
        else:
            value_list.append('\'' + str(metadata[title])+ '\'')

    insert_stmt = 'INSERT INTO {0} ({1}) VALUES ({2});'.format(table, ','.join(column_titles), ','.join(value_list))
    # print insert_stmt
    # print value_list

    db = cloudsql_connector()
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(insert_stmt)
    db.commit()
    cursor.close()
    db.close()

'''
Function to append data to a given metadata_data table.
Takes in a list of metadata objects
'''
def update_metadata_data_list(table, metadata):
    metadata_schema = user_metadata()
    column_titles = [d['column_name'] for d in metadata_schema]
    insert_stmt = 'INSERT INTO {0} ({1}) VALUES ({2});'.format(table, ','.join(column_titles), ','.join(['%s' for i in range(0,len(column_titles))]))
    value_list = []

    # Generate a tuple for each row in the metadata. Only collect data from columns in the table
    for row in metadata:
        value_tuple = ()
        for idx, title in enumerate(column_titles):
            if idx == 0:
                value_tuple += ((row[title]),)
            else:
                value_tuple += ((row[title]),)
        value_list.append(value_tuple)
    print insert_stmt
    print value_list
    db = cloudsql_connector()
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.executemany(insert_stmt, value_list)
    db.commit()
    cursor.close()
    db.close()

'''
Function to insert all metadata_samples data at once from user_gen datatype
'''
def insert_metadata_samples(data_df, table):
    columns = data_df.columns.values
    data_df = data_df.where((pd.notnull(data_df)), None)
    insert_stmt = 'INSERT INTO {0} ({1}) VALUES({2})'.format(table,
                                                             ','.join(columns),
                                                             ','.join(['%s' for i in range(0, len(columns))]))
    print insert_stmt
    value_list = []
    for i, j in data_df.transpose().iteritems():
        row = data_df[i:i+1]
        value_list.append(tuple(row.values[0]))

    db = cloudsql_connector()
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.executemany(insert_stmt, value_list)
    db.commit()
    cursor.close()
    db.close()

'''
Function to update rows in metadata_samples with has_datatype information.
Create new row if doesn't exist.
'''
def update_molecular_metadata_samples_list(table, datatype, sample_barcodes):
    insert_stmt = 'INSERT INTO {0} (sample_barcode, has_{1}) VALUES (%s, %s) ON DUPLICATE KEY UPDATE has_{2}=1;'.format(table, datatype, datatype)
    value_list = []
    for barcode in sample_barcodes:
        value_list.append((barcode, 1))
    print insert_stmt
    db = cloudsql_connector()
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.executemany(insert_stmt, value_list)
    db.commit()
    cursor.close()
    db.close()

'''
Function to update empty participant barcode values
'''
def update_metadata_participants(table):
    update_stmt = 'UPDATE {0} set participant_barcode=CONCAT("cgc_", sample_barcode) where participant_barcode is NULL;'.format(table)
    db = cloudsql_connector()
    cursor = db.cursor()
    cursor.execute(update_stmt)
    cursor.close()
    db.close()


'''
Function to insert one new feature definition
'''
def insert_feature_defs(sql_table, study_id, name, bq_mapping, shared_map_id, type):
    insert_stmt = 'INSERT INTO {0} (study_id, feature_name, bq_map_id, shared_map_id, is_numeric) VALUES (%s,%s,%s,%s,%s);'
    db = cloudsql_connector()
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.executemany(insert_stmt, (study_id, name, bq_mapping, shared_map_id, type))
    db.commit()
    cursor.close()
    db.close()

'''
Function to insert list of new feature definitions
'''
def insert_feature_defs_list(sql_table, data_list):
    insert_stmt = 'INSERT INTO {0} (study_id, feature_name, bq_map_id, shared_map_id, is_numeric) VALUES (%s,%s,%s,%s,%s);'.format(sql_table)
    print insert_stmt
    db = cloudsql_connector()
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.executemany(insert_stmt, data_list)
    db.commit()
    cursor.close()
    db.close()