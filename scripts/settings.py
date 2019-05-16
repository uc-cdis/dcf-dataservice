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
    with open("/dcf-dataservice/GDC_project_map.json", "r") as f:
        PROJECT_ACL = json.loads(f.read())
except Exception as e:
    print("Can not read GDC_project_map.json file. Detail {}".format(e))

IGNORED_FILES = "/dcf-dataservice/ignored_files_manifest.csv"

GDC_DAC_FILE = "/dcf-dataservice/GDC_datasets_access_control.csv"

GDC_PROJECT_BUCKET_MAP = {}

try:
    with open(GDC_DAC_FILE, "rt") as f:
        csvReader = csv.DictReader(f, delimiter=',')
        for row in csvReader:
            bucket_name = ""
            if row["access control level"] == "program":
                bucket_name = "gdc_{}_{}".format(
                    row["program name"], row["program phsid"]
                )
            elif row["access control level"] == "project":
                bucket_name = "gdc_{}_{}".format(
                    row["Project ID"], row["project phsid"]
                )
            else:
                continue
            GDC_PROJECT_BUCKET_MAP[row["Project ID"]] = {
                "aws_bucket_prefix": bucket_name.lower(),
                "gs_bucket_prefix": bucket_name.lower(),
            }
except Exception as e:
    print("Can not read {} file. Detail {}".format(GDC_DAC_FILE, e))

if not GDC_PROJECT_BUCKET_MAP:
    raise Exception("Can not read GDC_datasets_access_control.csv")