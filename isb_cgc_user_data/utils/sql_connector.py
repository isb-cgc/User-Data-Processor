

import MySQLdb

def cloudsql_connector(config):
    if 'ssl_cert' in config:
        ssl_options = {
            'ca': config['ssl_ca'],
            'cert': config['ssl_cert'],
            'key': config['ssl_key']
        }

        db = MySQLdb.connect(
            host=config['db_host'],
            db=config['db'],
            user=config['db_user'],
            passwd=config['db_password'],
            ssl=ssl_options
        )
    else:

        db = MySQLdb.connect(
                host=config['db_host'],
                db=config['db'],
                user=config['db_user'],
                passwd=config['db_password']
        )

    return db