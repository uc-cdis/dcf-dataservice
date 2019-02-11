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

PROJECT_ACL = {
    "TARGET": {"gs_bucket_prefix": "gdc-target-phs000218"},
    "TCGA": {"gs_bucket_prefix": "gdc-tcga-phs000178"},
    "VAREPOP": {"gs_bucket_prefix": "gdc-varepop-apollo-phs001374"},
    "FM": {"gs_bucket_prefix": "gdc-fm-phs001179"},
    "NCICCR": {"gs_bucket_prefix": "gdc-nciccr-phs001444"},
    "CTSP": {"gs_bucket_prefix": "gdc-ctsp-phs001175"},
    "CCLE": {"gs_bucket_prefix": "gdc-ccle"},
}

try:
    with open("/dcf-dataservice/GDC_datasets_access_control.csv", "rt") as f:
        csvReader = csv.DictReader(f, delimiter=",")
        for line in csvReader:
            PROJECT_ACL[line["project_id"]] = line
except Exception as e:
    print("Can not read GDC_datasets_access_control.csv file. Detail {}".format(e))


IGNORED_FILES = []
try:
    with open("/dcf-dataservice/ignored_files_manifest.csv", "rt") as f:
        csvReader = csv.DictReader(f, ",")
        for row in csvReader:
            row["gcs_object_size"] = int(row["gcs_object_size"])
            IGNORED_FILES.append(row)
except Exception as e:
    print("Can not read ignored_files_manifest.csv file. Detail {}".format(e))
