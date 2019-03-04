import os
import boto3
import csv
import random
from urlparse import urlparse
from errors import UserError


def get_aws_bucket_name(fi, PROJECT_ACL):
    try:
        project_info = PROJECT_ACL[fi.get("project_id").split("-")[0]]
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


def get_fileinfo_list_from_csv_manifest(manifest_file, start=None, end=None, dem="\t"):
    """
    get file info from csv manifest
    """
    files = []
    with open(manifest_file, "rt") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=dem)
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


def prepare_data(manifest_file, global_config):
    """
    Read data file info from manifest and organize them into groups.
    Each group contains files which should be copied to the same bucket
    The groups will be push to the queue consumed by threads
    """
    if manifest_file.startswith("s3://"):
        copying_files = get_fileinfo_list_from_s3_manifest(
            url_manifest=manifest_file,
            start=global_config.get("start"),
            end=global_config.get("end"),
        )
    else:
        copying_files = get_fileinfo_list_from_csv_manifest(
            manifest_file=manifest_file,
            start=global_config.get("start"),
            end=global_config.get("end"),
        )

    if global_config.get("file_shuffle", False):
        random.shuffle(copying_files)

    chunk_size = global_config.get("chunk_size", 1)
    tasks = []

    for idx in range(0, len(copying_files), chunk_size):
        tasks.append(copying_files[idx: idx + chunk_size])

    return tasks, len(copying_files)


def get_ignored_files(ignored_filename, delimiter=","):
    """
    get all the files in 5aa buckets and are in gdc full manifest
    """
    result = {}
    try:
        with open(ignored_filename, "rt") as f:
            csvReader = csv.DictReader(f, delimiter=delimiter)
            for row in csvReader:
                if urlparse(row["gcs_object_url"]).netloc == "5aa919de-0aa0-43ec-9ec3-288481102b6d":
                    row["gcs_object_size"] = int(row["gcs_object_size"])
                    result[row["gdc_uuid"]] = row
    except Exception as e:
        print("Can not read ignored_files_manifest.csv file. Detail {}".format(e))
    
    return result


def get_structured_object_key(uuid, ignored_dict):
    """
    Given an uuid return the url of the object in the 5aa bucket

    Args:
        uuid(str): object uuid
        ignore_dict(dict): a dictionary of 5aa bucket object
    """
    try:
        if uuid in ignored_dict:
            element = ignored_dict[uuid]
            if (
                uuid == element["gdc_uuid"]
                and urlparse(element["gcs_object_url"]).netloc == "5aa919de-0aa0-43ec-9ec3-288481102b6d"
            ):
                res = urlparse(element["gcs_object_url"])
                return os.path.join(*res.path.split("/")[2:])
    except IndexError:
        return None

    return None
