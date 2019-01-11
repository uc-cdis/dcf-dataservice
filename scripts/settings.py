import json
import csv

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
    with open("/dcf-dataservice/GDC_datasets_access_control.csv", "rt") as f:
        csvReader = csv.DictReader(f, delimiter=",")
        for line in csvReader:
            PROJECT_ACL[line["project_id"]] = line
except Exception as e:
    print("Can not read GDC_datasets_access_control.csv file. Detail {}".format(e))
