import boto3
import csv

from errors import UserError


def get_aws_bucket_name(fi, PROJECT_ACL):
    try:
        project_info = PROJECT_ACL[fi.get("project_id")]
    except KeyError:
        raise UserError("PROJECT_ACL does not have {} key".format(fi.get("project_id")))

    # bad hard code to support ccle buckets
    if "ccle" in project_info["aws_bucket_prefix"]:
        return (
            "ccle-open-access"
            if fi.get("acl") in {"[u'open']", "['open']"}
            else "gdc-ccle-controlled"
        )

    return project_info["aws_bucket_prefix"] + (
        "-open" if fi.get("acl") in {"[u'open']", "['open']", "*"} else "-controlled"
    )


def get_google_bucket_name(fi, PROJECT_ACL):
    try:
        project_info = PROJECT_ACL[fi.get("project_id").split("-")[0]]
    except KeyError:
        raise UserError("PROJECT_ACL does not have {} key".format(fi.get("project_id")))
    return project_info["gs_bucket_prefix"] + (
        "-open" if fi.get("acl") in {"[u'open']", "['open']", "*"} else "-controlled"
    )


def get_fileinfo_list_from_s3_manifest(url_manifest, start=None, end=None):
    """
    Get the manifest from s3
    pass to get_fileinfo_list_from_manifest to get 
    list of file info dictionary (size, md5, etc.)
    """

    s3 = boto3.resource("s3")
    from urlparse import urlparse

    out = urlparse(url_manifest)
    s3.meta.client.download_file(out.netloc, out.path[1:], "./manifest2")
    return get_fileinfo_list_from_csv_manifest("./manifest2", start, end)


def get_fileinfo_list_from_csv_manifest(manifest_file, start=None, end=None):
    """
    get file info from csv manifest
    """
    files = []
    with open(manifest_file, "rt") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter="\t")
        for row in csvReader:
            row["size"] = int(row["size"])
            files.append(row)

    start_idx = start if start else 0
    end_idx = end if end else len(files)

    return files[start_idx:end_idx]


def generate_chunk_data_list(size, data_size):
    L = []
    idx = 0
    while idx < size:
        L.append((idx, min(idx + data_size - 1, size - 1)))
        idx += data_size

    return L
