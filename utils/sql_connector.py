import os
from os.path import join, dirname
import MySQLdb
import dotenv

dotenv.read_dotenv(join(dirname(__file__), '../.env'))

def cloudsql_connector():
    db = MySQLdb.connect(
            host=os.environ.get('db_host', ''),
            db=os.environ.get('db', ''),
            user=os.environ.get('db_user', ''),
            passwd=os.environ.get('db_password', ''))

    return db