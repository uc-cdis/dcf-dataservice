import os

PROJECT_MAP = {
        'TGCA': 'tcga-xxxx-phs000178',
        'TARGET': 'target-xxxx-phs000218',
        'FM': 'fm-xxxx-phs000179',
        'CCLE': 'ccle-xxx'
        }

SIGNPOST={'host': '',
          'version': '',
          'auth': None}

dir_path = "/secrets"
data_service_creds = os.path.join(dir_path, 'dataservice_credentials.json')

if os.path.exists(data_service_creds):
    with open(data_service_creds, 'r') as f:
        data = json.load(f)
        SIGNPOST = data.get("SIGNPOST", {})
