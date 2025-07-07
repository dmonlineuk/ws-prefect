import requests
from urllib.parse import urljoin
import os
from glob import glob
from xml.etree import ElementTree
import pyodbc
from dotenv import load_dotenv
from socket import gethostname


PRIVACY_SERVICE_URL = 'http://127.0.0.1:8005'


def upload_ndoo_response(exec_id: str, filepath: str):
    url = urljoin(urljoin(PRIVACY_SERVICE_URL, 'optouts/response/nhsnumbers/'), exec_id)
    if gethostname() == 'arch-vm-02':
        response = 'true'
    else:
        response = requests.post(url, files = {'file': open(filepath, 'r')})
    return response


if __name__ == '__main__':
    pass
