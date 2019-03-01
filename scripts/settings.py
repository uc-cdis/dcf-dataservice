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
with open("/dcf-dataservice/GDC_project_map.json", "r") as f:
    PROJECT_ACL = json.loads(f.read())

IGNORED_FILES = "./ignored_files_manifest.csv"
