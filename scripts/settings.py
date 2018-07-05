import os
import json

INDEXD = {
  'host': 'http://localhost:8000',
  'version': 'v0',
  'auth': {'username':'test', 'password':'test'}}

PROJECT_MAP = {
       'TGCA': 'tcga-xxxx-phs000178',
       'TARGET': 'target-xxxx-phs000218',
       'FM': 'fm-xxxx-phs000179',
       'CCLE': 'ccle-xxx'
        }
GDC_TOKEN = ''
try:
    with open('/secrets/dcf_dataservice_credentials.json','r') as f:
        data = json.loads(f.read())
        PROJECT_MAP = data.get('PROJECT_MAP',{})
        INDEXD = data.get('INDEXD',{})
        GDC_TOKEN = data.get('GDC_TOKEN','')
except Exception:
    pass
