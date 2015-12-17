import os
from os.path import join, dirname
import MySQLdb
import dotenv

dotenv.read_dotenv(join(dirname(__file__), '../.env'))

def cloudsql_connector():
    if os.environ.has_key('ssl_cert'):
        ssl_options = {
            'ca': os.environ.get('ssl_ca'),
            'cert': os.environ.get('ssl_cert'),
            'key': os.environ.get('ssl_key')
        }

        db = MySQLdb.connect(
            host=os.environ.get('db_host', ''),
            db=os.environ.get('db', ''),
            user=os.environ.get('db_user', ''),
            passwd=os.environ.get('db_password', ''),
            ssl=ssl_options
        )
    else:

        db = MySQLdb.connect(
                host=os.environ.get('db_host', ''),
                db=os.environ.get('db', ''),
                user=os.environ.get('db_user', ''),
                passwd=os.environ.get('db_password', ''))

    return db