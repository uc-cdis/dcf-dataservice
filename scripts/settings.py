import os
import json

# Signpost
#SIGNPOST = {
#  'host': 'http://localhost:8000',
#  'version': 'v0',
#  'auth': None}
#
#PROJECT_MAP = {
#       'TGCA': 'tcga-xxxx-phs000178',
#       'TARGET': 'target-xxxx-phs000218',
#       'FM': 'fm-xxxx-phs000179',
#       'CCLE': 'ccle-xxx'
#        }

with open('/secrets/dcf_dataservice_credentials.json','r') as f:
    data = json.loads(f.read())
    PROJECT_MAP = data.get('PROJECT_MAP',{})
    SIGNPOST = data.get('SIGNPOST',{})
