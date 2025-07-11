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
        try:
            results = cursor.fetchall() # list of tuples
        except:
            results = []
    return results


def files_organiser(files):
    reportfiles = []
    datafiles = []
    otherfiles = []
    for file in files:
        tree = ElementTree.parse(file)
        root = tree.getroot()
        match root.find('MessageType').text:
            case 'Report':
                reportfiles.append(file)

            case 'Data':
                datafiles.append(file)

            case _:
                otherfiles.append(file)

    return (reportfiles, datafiles, otherfiles)


def reports_handler(conn_string, files):
    for file in files:
        with open(file, 'r') as xml:
            sql_statement = f"""
EXEC [mesh_processing].[new_report_entry]
      @filename = '{file}'
    , @xml = '{xml.read()}'
"""
        results = exec_sql(conn_string, sql_statement)

    return results


def data_files_handler(conn_string, files):
    for file in files:
        with open(file, 'r') as xml:
            sql_statement = f"""
EXEC [mesh_processing].[new_data_control_entry]
      @filename = '{file}'
    , @xml = '{xml.read()}'
"""
        results = exec_sql(conn_string, sql_statement)
        new_id = results[0][0]

        # Test source: NHSE (NDOO)?
        tree = ElementTree.parse(file)
        root = tree.getroot()
        match root.find('From_DTS').text:
            # NHSE?
            case 'X26HC065':
                match root.find('Subject').text:
                    # json success report?
                    case 'SUCCESS':
                        datfile = file.replace('.ctl', '.dat')
                        with open(datfile, 'r') as json:
                            sql_statement = f"""
EXEC [ndoo_processing].[new_receipts_report_entry]
      @json = '{json.read()}'
    , @data_id = '{new_id}'
"""                            
                        exec_sql(conn_string, sql_statement)

                    # else, NDOO response data
                    case _:
                        exec_id = root.find('LocalId').text # ToDo: establish exec_id is LocalId in whole or in part?
                        datfile = file.replace('.ctl', '.dat')
                        response = upload_ndoo_response(exec_id, datfile)
                        return response

            case _:
                # ToDo: move files for loading to appropriate folder
                pass


def mesh_receipt():
    load_dotenv()
    conn_string = get_conn_string()
    files = glob(os.getenv('MESH_INBOX_FOLDER') + '*.ctl')
    if len(files) > 0:
        reportfiles, datafiles, otherfiles = files_organiser(files)
        reports_handler(reportfiles)
        data_files_handler(datafiles)
        # ToDo: edge cases for `otherfiles` as necessary


if __name__ == '__main__':
    pass
