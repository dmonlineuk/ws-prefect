from mesh_receipt import *

def test_upload_ndoo_response():
    assert upload_ndoo_response('1234567890','1111111111,\n') == 'true'

def test_get_conn_string():
    os.environ['MESH_PROCESSING_USER'] = 'TEST'
    os.environ['MESH_PROCESSING_PASSWORD'] = 'TEST'
    os.environ['SQL_SERVER'] = 'TEST'
    assert get_conn_string() == 'UID=TEST;PWD=TEST;DATABASE=full-dfms-medal-pcdchecker;SERVER=TEST;DRIVER={ODBC Driver 18 for SQL Server}'

def test_exec_sql():
    load_dotenv(override=True)
    if gethostname() == 'arch-vm-02':
        conn_string = get_conn_string() + ';TRUSTSERVERCERTIFICATE=yes'
    else:
        conn_string = get_conn_string()    
    assert exec_sql(conn_string, "select 'true';")[0][0] == 'true'

def test_files_organiser():
    load_dotenv(override=True)
    tmp_path=os.getenv('MESH_INBOX_FOLDER')
    
    with open(os.path.join(tmp_path, 'reportfile.ctl'), 'w') as f:
        f.write('<DTSControl><MessageType>Report</MessageType></DTSControl>')
    
    with open(os.path.join(tmp_path, 'datafile.ctl'), 'w') as f:
        f.write('<DTSControl><MessageType>Data</MessageType></DTSControl>')

    with open(os.path.join(tmp_path, 'extra.ctl'), 'w') as f:
        f.write('<DTSControl><MessageType>Unknown</MessageType></DTSControl>')

    files = [
            os.path.join(tmp_path, 'reportfile.ctl'), 
            os.path.join(tmp_path, 'datafile.ctl'), 
            os.path.join(tmp_path, 'extra.ctl')
        ]
    assert files_organiser(files) == ([tmp_path + 'reportfile.ctl'], [tmp_path + 'datafile.ctl'], [tmp_path + 'extra.ctl'])
    
    for file in files:
        os.remove(file)
