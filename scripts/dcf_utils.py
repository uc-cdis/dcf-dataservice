import json

GDC_TOKEN = ""
DATA_ENDPT = ""
IGNORED_FILES = ""
POSTFIX_1_EXCEPTION = []
POSTFIX_2_EXCEPTION = []
PROJECT_ACL = {}

try:
    with open("/dcf-dataservice/dcf_dataservice_settings.json", "r") as f:
        data = json.loads(f.read())
        GDC_TOKEN = data.get("GDC_TOKEN", "")
        DATA_ENDPT = data.get("DATA_ENDPT", "")
        IGNORED_FILES = data.get("IGNORED_FILES", "")
        POSTFIX_1_EXCEPTION = data.get("POSTFIX_1_EXCEPTION", [])
        POSTFIX_2_EXCEPTION = data.get("POSTFIX_2_EXCEPTION", [])
        PROJECT_ACL = data.get("PROJECT_ACL", {})
except Exception as e:
    raise (f"Problem reading dcf_dataservice_settings.json file.\n{e}")


def get_dcf_aws_bucket_name(record_row: dict, reverse=False):
    """
    Return AWS bucket name
    record_row: manifest file row
    PROJECT_ACL: from json under dir dcfprod/apis_configs/dcf-dataservice/GDC_project_map.json
    and dcfprod/apis_configs/dcf-dataservice/dcf_dataservice_settings
    """
    try:
        project_info = PROJECT_ACL[record_row.get("project_id")]
    except KeyError:
        raise KeyError(
            f"ID {record_row.get('id')}\nPROJECT_ACL dict does not have {record_row.get('project_id')} key"
        )

    if reverse:
        if "target" in project_info["aws_bucket_prefix"]:
            return (
                "target-controlled"
                if record_row.get("acl") in {"[u'open']", "['open']"}
                else "gdc-target-phs000218-2-open"
            )

        if "tcga" in project_info["aws_bucket_prefix"]:
            return (
                "tcga-2-controlled"
                if record_row.get("acl") in {"[u'open']", "['open']"}
                else "tcga-2-open"
            )

        # POSTFIX_1_EXCEPTION
        if project_info["aws_bucket_prefix"] in POSTFIX_1_EXCEPTION:
            return project_info["aws_bucket_prefix"] + (
                "-controlled"
                if record_row.get("acl") in {"[u'open']", "['open']", "*"}
                else "-open"
            )

        # POSTFIX_2_EXCEPTION
        if project_info["aws_bucket_prefix"] in POSTFIX_2_EXCEPTION:
            return project_info["aws_bucket_prefix"] + (
                "-2-controlled"
                if record_row.get("acl") in {"[u'open']", "['open']", "*"}
                else "-2-open"
            )

        # Default
        return project_info["aws_bucket_prefix"] + (
            "-controlled"
            if record_row.get("acl") in {"[u'open']", "['open']", "*"}
            else "-2-open"
        )
    else:
        if "target" in project_info["aws_bucket_prefix"]:
            return (
                "gdc-target-phs000218-2-open"
                if record_row.get("acl") in {"[u'open']", "['open']"}
                else "target-controlled"
            )

        if "tcga" in project_info["aws_bucket_prefix"]:
            return (
                "tcga-2-open"
                if record_row.get("acl") in {"[u'open']", "['open']"}
                else "tcga-2-controlled"
            )

        # POSTFIX_1_EXCEPTION
        if project_info["aws_bucket_prefix"] in POSTFIX_1_EXCEPTION:
            return project_info["aws_bucket_prefix"] + (
                "-open"
                if record_row.get("acl") in {"[u'open']", "['open']", "*"}
                else "-controlled"
            )

        # POSTFIX_2_EXCEPTION
        if project_info["aws_bucket_prefix"] in POSTFIX_2_EXCEPTION:
            return project_info["aws_bucket_prefix"] + (
                "-2-open"
                if record_row.get("acl") in {"[u'open']", "['open']", "*"}
                else "-2-controlled"
            )

        # Default
        return project_info["aws_bucket_prefix"] + (
            "-2-open"
            if record_row.get("acl") in {"[u'open']", "['open']", "*"}
            else "-controlled"
        )


def get_dcf_google_bucket_name(record_row: dict, reverse=False):
    """
    Return GCP bucket name
    record_row: manifest file row
    PROJECT_ACL: dict json file
    """
    try:
        project_info = PROJECT_ACL[record_row.get("project_id")]
    except KeyError:
        raise KeyError(
            f"ID {record_row.get('id')}\nPROJECT_ACL dict does not have {record_row.get('project_id')} key"
        )
    if reverse:
        return project_info["gs_bucket_prefix"] + (
            "-controlled"
            if record_row.get("acl") in {"[u'open']", "['open']", "*"}
            else "-open"
        )
    else:
        return project_info["gs_bucket_prefix"] + (
            "-open"
            if record_row.get("acl") in {"[u'open']", "['open']", "*"}
            else "-controlled"
        )
