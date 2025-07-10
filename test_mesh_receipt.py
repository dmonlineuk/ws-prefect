from mesh_receipt import *
from prefect import flow

def get_safe_conn_string():
    load_dotenv(override=True)
    if gethostname() == 'arch-vm-02':
        conn_string = get_conn_string() + ';TRUSTSERVERCERTIFICATE=yes'
    else:
        conn_string = get_conn_string()    
    return conn_string

def test_upload_ndoo_response():
    assert upload_ndoo_response('1234567890','1111111111,\n') == 'true'

def test_get_conn_string():
    os.environ['MESH_PROCESSING_USER'] = 'TEST'
    os.environ['MESH_PROCESSING_PASSWORD'] = 'TEST'
    os.environ['SQL_SERVER'] = 'TEST'
    assert get_conn_string() == 'UID=TEST;PWD=TEST;DATABASE=full-dfms-medal-pcdchecker;SERVER=TEST;DRIVER={ODBC Driver 18 for SQL Server}'

def test_exec_sql():
    conn_string = get_safe_conn_string()
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

def test_reports_handler():
    load_dotenv(override=True)
    tmp_path=os.getenv('MESH_INBOX_FOLDER')
    filepath = os.path.join(tmp_path, 'reportfile.ctl')
    sample_xml = """<DTSControl>
  <Version>1.0</Version>
  <AddressType>DTS</AddressType>
  <MessageType>Report</MessageType>
  <From_DTS>0ARHC002</From_DTS>
  <To_DTS>X26HC065</To_DTS>
  <Subject>0ARHC002-27022025-0047</Subject>
  <LocalId>0ARHC002-27022025-0047-TEST</LocalId>
  <DTSId>20250227121034456686_DD8A76</DTSId>
  <Compress>Y</Compress>
  <Encrypted>N</Encrypted>
  <WorkflowId>SPINE_NTT_UPHOLDING</WorkflowId>
  <IsCompressed>N</IsCompressed>
  <AllowChunking>Y</AllowChunking>
  <StatusRecord>
    <DateTime>20250227140401</DateTime>
    <Event>COLLECT</Event>
    <Status>SUCCESS</Status>
    <StatusCode>00</StatusCode>
    <Description>Data collect success confirmation.</Description>
  </StatusRecord>
</DTSControl>
"""
    with open(filepath, 'w') as f:
        f.write(sample_xml)

    assert reports_handler(get_safe_conn_string(), [filepath])
    os.remove(filepath)

def test_data_files_handler_ndoo_response():
    load_dotenv(override=True)
    tmp_path=os.getenv('MESH_INBOX_FOLDER')
    filepath = os.path.join(tmp_path, 'datafile.ctl')
    sample_xml = """<DTSControl>
  <Version>1.0</Version>
  <AddressType>DTS</AddressType>
  <MessageType>Data</MessageType>
  <From_DTS>0ARHC002</From_DTS>
  <To_DTS>X26HC065</To_DTS>
  <Subject>0ARHC002-27022025-0047</Subject>
  <LocalId>0ARHC002-27022025-0047-TEST</LocalId>
  <DTSId>20250227121034456686_DD8A76</DTSId>
  <Compress>Y</Compress>
  <Encrypted>N</Encrypted>
  <WorkflowId>SPINE_NTT_UPHOLDING</WorkflowId>
  <IsCompressed>Y</IsCompressed>
  <AllowChunking>Y</AllowChunking>
  <StatusRecord>
    <DateTime>20250227121034</DateTime>
    <Event>TRANSFER</Event>
    <Status>SUCCESS</Status>
    <StatusCode>00</StatusCode>
    <Description>Data Transfer success confirmation.</Description>
  </StatusRecord>
</DTSControl>
"""
    with open(filepath, 'w') as f:
        f.write(sample_xml)
    sample_txt = '''1111111111,
'''
    datfilepath = filepath.replace('.ctl', '.dat')
    with open(datfilepath, 'w') as f:
        f.write(sample_txt)

    assert not data_files_handler(get_safe_conn_string(), [filepath])
    os.remove(filepath)
    os.remove(datfilepath)
    
def test_data_files_handler_success_report():
    load_dotenv(override=True)
    tmp_path=os.getenv('MESH_INBOX_FOLDER')
    filepath = os.path.join(tmp_path, 'datafile.ctl')
    sample_xml = """<DTSControl>
  <Version>1.0</Version>
  <AddressType>ALL</AddressType>
  <MessageType>Data</MessageType>
  <From_DTS>X26HC065</From_DTS>
  <To_DTS>0ARHC002</To_DTS>
  <From_ESMTP>x26hc065@dts.nhs.uk</From_ESMTP>
  <To_ESMTP>0arhc002@dts.nhs.uk</To_ESMTP>
  <Subject>SUCCESS</Subject>
  <LocalId>0ARHC002-27022025-0047-TEST</LocalId>
  <DTSId>20250227141016642011_E1584E</DTSId>
  <PartnerId></PartnerId>
  <Compress>N</Compress>
  <Encrypted>N</Encrypted>
  <WorkflowId>SPINE_NTT_UPHOLDING</WorkflowId>
  <ProcessId></ProcessId>
  <DataChecksum></DataChecksum>
  <IsCompressed>N</IsCompressed>
  <StatusRecord>
    <DateTime>20250227141016</DateTime>
    <Event>TRANSFER</Event>
    <Status>SUCCESS</Status>
    <StatusCode>00</StatusCode>
    <Description>Transferred to recipient mailbox</Description>
  </StatusRecord>
</DTSControl>
"""
    with open(filepath, 'w') as f:
        f.write(sample_xml)
    sample_json = '''{"version": 10, "id": 16221413, "status": "pending", "received": "2025-02-27T14:10:15.709506", "submitted": "2025-02-27T14:10:15.457390", "finished": null, "stages": [{"stage": "validate_non_empty_file", "ts": 20250227141015717393, "passed": true, "details": null}, {"stage": "validate_is_unique_upload", "ts": 20250227141015717787, "passed": true, "details": null}, {"stage": "validate_in_order", "ts": 20250227141015722050, "passed": true, "details": null}], "runs": [], "sender_id": "0ARHC002", "agent_id": 1, "dataset_id": "mesh_to_pos", "dataset_version": null, "workspace": null, "metadata": {"submission_id": 16221413, "dataset_version": null, "sender_id": "0ARHC002", "test_submission": false, "received_timestamp": 1740665415, "submitted_timestamp": 20250227141015457390, "file_type": "csv", "request": {"dataset_id": "mesh_to_pos", "dataset_version": null, "submitted_timestamp": 20250227141015457390, "expected_file": null, "expected_failure": false, "extra": {"sending_mailbox": "0ARHC002", "landing_mailbox": "X26HC065", "transfer_id": "31579cd9617cf2c319203d74b13a0414bab46e73", "message_id": "20250227140401637568_691C4A", "filename": "0ARHC002-27022025-0050.dat", "local_id": "0ARHC002-27022025-0047-TEST", "message_type": "DATA", "subject": "0ARHC002-27022025-0047", "encrypted": false, "compressed": true}, "reporting_period_name": null, "reporting_period_start": null, "reporting_period_end": null, "submission_start_date": null, "submission_end_date": null, "submitted_by": null, "submitted_on": null, "is_test": null, "stage": null}, "filename": "0ARHC002-27022025-0050.dat", "filesize": 208, "file_hash": "mZxkTpJRvR", "dataset_id": "mesh_to_pos", "working_folder": "s3://nhsd-dspp-core-prod-raw/submissions/00000000000016221413", "agent_id": 1, "s3_url": null}, "pipeline_result": null, "test_scope": null}
'''
    datfilepath = filepath.replace('.ctl', '.dat')
    with open(datfilepath, 'w') as f:
        f.write(sample_json)

    assert not data_files_handler(get_safe_conn_string(), [filepath])
    os.remove(filepath)
    os.remove(datfilepath)

def test_make_flow_from_func():
   servable_flow = flow(mesh_receipt)

