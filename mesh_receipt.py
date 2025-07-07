import requests
from urllib.parse import urljoin
import os
from glob import glob
from xml.etree import ElementTree
import pyodbc
from dotenv import load_dotenv
from socket import gethostname


PRIVACY_SERVICE_URL = 'http://127.0.0.1:8005'
load_dotenv()


def upload_ndoo_response(exec_id: str, filepath: str):
    url = urljoin(urljoin(PRIVACY_SERVICE_URL, 'optouts/response/nhsnumbers/'), exec_id)
    if gethostname() == 'arch-vm-02':
        response = 'true'
    else:
        response = requests.post(url, files = {'file': open(filepath, 'r')})
    return response


def get_conn_string():
    conn_string = 'UID=%s;PWD=%s;DATABASE=%s;SERVER=%s;DRIVER=%s' % (
            os.getenv('MESH_PROCESSING_USER'),
            os.getenv('MESH_PROCESSING_PASSWORD'),
            'full-dfms-medal-pcdchecker',
            os.getenv('SQL_SERVER'),
            '{ODBC Driver 18 for SQL Server}'
        )
    return conn_string


def exec_sql(conn_string: str, sql_statement: str):
    with pyodbc.connect(conn_string) as connection:
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        results = cursor.fetchall() # list of tuples
    return results


if __name__ == '__main__':
    pass
