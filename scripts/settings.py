import json

INDEXD = {
    "host": "http://localhost:8000",
    "version": "v0",
    "auth": {"username": "test", "password": "test"},
}

GDC_TOKEN = ""
try:
    with open("/secrets/dcf_dataservice_credentials.json", "r") as f:
        data = json.loads(f.read())
        INDEXD = data.get("INDEXD", {})
        GDC_TOKEN = data.get("GDC_TOKEN", "")
except Exception as e:
    print("Can not read dcf_dataservice_credentials.json file. Detail {}".format(e))


PROJECT_ACL = {}
try:
    with open("/dcf-dataservice/GDC_project_map.json", "r") as f:
        PROJECT_ACL = json.loads(f.read())
except Exception as e:
    print("Can not read GDC_project_map.json file. Detail {}".format(e))

IGNORED_FILES = "/dcf-dataservice/ignored_files_manifest.csv"

DATA_ENDPT = "https://api.gdc.cancer.gov/data/"
