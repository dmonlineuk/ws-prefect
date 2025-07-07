from mesh_receipt import *

def test_upload_ndoo_response():
    assert upload_ndoo_response('1234567890','1111111111,\n') == 'true'
