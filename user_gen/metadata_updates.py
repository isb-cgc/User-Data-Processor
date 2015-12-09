import MySQLdb
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
