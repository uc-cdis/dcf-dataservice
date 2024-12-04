import json

INDEXD = {
    "host": "http://localhost:8000",
    "version": "v0",
    "auth": {"username": "test", "password": "test"},
}

SLACK_URL = ""
GDC_TOKEN = ""
try:
    with open("/secrets/dcf_dataservice_credentials.json", "r") as f:
        data = json.loads(f.read())
        INDEXD = data.get("INDEXD", {})
        SLACK_URL = data.get("SLACK_URL", "")
        GDC_TOKEN = data.get("GDC_TOKEN", "")
except Exception as e:
    print("Can not read dcf_dataservice_credentials.json file. Detail {}".format(e))


PROJECT_ACL = {}
try:
    with open("/dcf_dataservice/GDC_project_map.json", "r") as f:
        PROJECT_ACL = json.loads(f.read())
except Exception as e:
    print("Can not read GDC_project_map.json file. Detail {}".format(e))

IGNORED_FILES = "/dcf_dataservice/ignored_files_manifest.csv"

DATA_ENDPT = "https://api.gdc.cancer.gov/data/"

# By default the postfix for open buckets are -2 and no numerical postfix for controlled
# list of buckets that have -open and -controlled postfix
POSTFIX_1_EXCEPTION = [
    "gdc-cmi-mbc-phs001709",
    "gdc-cmi-asc-phs001931",
    "gdc-cmi-mpc-phs001939",
    "gdc-rebc-thyr-phs001134",
    "gdc-trio-cru-phs001163",
]
# list of buckets that have both -2-open and -2-controlled postfix
POSTFIX_2_EXCEPTION = [
    "gdc-cgci-phs000235",
    "tcga",
    "gdc-organoid-pancreatic-phs001611",
    "gdc-beataml1-cohort-phs001657",
]
