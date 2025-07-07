from mesh_receipt import *

def test_upload_ndoo_response():
    assert upload_ndoo_response('1234567890','1111111111,\n') == 'true'

def test_get_conn_string():
    os.environ['MESH_PROCESSING_USER'] = 'TEST'
    os.environ['MESH_PROCESSING_PASSWORD'] = 'TEST'
    os.environ['SQL_SERVER'] = 'TEST'
    assert get_conn_string() == 'UID=TEST;PWD=TEST;DATABASE=full-dfms-medal-pcdchecker;SERVER=TEST;DRIVER={ODBC Driver 18 for SQL Server}'

def test_exec_sql():
    assert exec_sql(get_conn_string(), "select 'true';")[0][0] == 'true'
