import os
import boto3
import csv
import random
from google.cloud import storage
import threading
from threading import Thread
from urllib.parse import urlparse

from scripts.errors import UserError
from indexclient.client import IndexClient
from scripts.settings import INDEXD
import asyncio
import time
from gen3.tools.indexing.download_manifest import async_download_object_manifest


def get_aws_bucket_name(fi, PROJECT_ACL):
    try:
        project_info = PROJECT_ACL[fi.get("project_id")]
    except KeyError:
        raise UserError(
            "PROJECT_ACL does not have {} key. All keys of PROJECT_ACL are {}".format(
                fi.get("project_id"), PROJECT_ACL.keys()
            )
        )

    # bad hard code to support ccle buckets
    # if "ccle" in project_info["aws_bucket_prefix"]:
    #     return (
    #         "ccle-open-access"
    #         if fi.get("acl") in {"[u'open']", "['open']"}
    #         else "gdc-ccle-controlled"
    #     )
    if "target" in project_info["aws_bucket_prefix"]:
        return (
            "gdc-target-phs000218-2-open"
            if fi.get("acl") in {"[u'open']", "['open']"}
            else "target-controlled"
        )

    if "tcga" in project_info["aws_bucket_prefix"]:
        return (
            "tcga-open"
            if fi.get("acl") in {"[u'open']", "['open']"}
            else "tcga-controlled"
        )

    return project_info["aws_bucket_prefix"] + (
        "-2-open" if fi.get("acl") in {"[u'open']", "['open']", "*"} else "-controlled"
    )


def get_google_bucket_name(fi, PROJECT_ACL):
    try:
        project_info = PROJECT_ACL[fi.get("project_id")]
    except KeyError:
        raise UserError(
            "PROJECT_ACL does not have {} key. All keys of PROJECT_ACL are {}".format(
                fi.get("project_id"), PROJECT_ACL.keys()
            )
        )
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

    out = urlparse(url_manifest)
    s3.meta.client.download_file(out.netloc, out.path[1:], "./manifest2")
    return get_fileinfo_list_from_csv_manifest("./manifest2", start, end)


def get_fileinfo_list_from_gs_manifest(url_manifest, start=None, end=None):
    """
    Get the manifest from gs
    pass to get_fileinfo_list_from_manifest to get 
    list of file info dictionary (size, md5, etc.)
    """
    import subprocess

    cmd = "gsutil cp {} ./tmp.tsv".format(url_manifest)
    subprocess.Popen(cmd, shell=True).wait()

    return get_fileinfo_list_from_csv_manifest("./tmp.tsv", start, end)


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


def prepare_data(manifest_file, global_config, copied_objects=None, project_acl=None):
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

    # Fix chunk size to 1. Do not change
    chunk_size = 1
    tasks = []
    total_copying_data = 0

    if copied_objects:
        filtered_copying_files = []
        for fi in copying_files:
            target_bucket = get_aws_bucket_name(fi, project_acl)
            key = "{}/{}/{}".format(target_bucket, fi["id"], fi["file_name"])
            if key not in copied_objects or copied_objects[key]["Size"] != fi["size"]:
                filtered_copying_files.append(fi)
                total_copying_data += fi["size"] * 1.0 / 1024 / 1024 / 1024
        copying_files = filtered_copying_files

    for idx in range(0, len(copying_files), chunk_size):
        tasks.append(copying_files[idx : idx + chunk_size])

    return tasks, len(copying_files), total_copying_data


def prepare_txt_manifest_google_dataflow(
    gs_manifest_file,
    local_manifest_txt_file,
    copied_objects=None,
    project_acl=None,
    ignored_dict=None,
):
    """
    Since Apache Beam does not support csv format, convert the csv to txt file
    """
    copying_files = get_fileinfo_list_from_gs_manifest(gs_manifest_file)
    indexd_records = async_get_indexd_records()
    if copied_objects:
        filtered_copying_files = []
        for fi in copying_files:
            gs_bucket = get_google_bucket_name(fi, project_acl)
            if fi["id"] in ignored_dict:
                object_path = "gs://{}/{}".format(
                    gs_bucket, get_structured_object_key(fi["id"], ignored_dict)
                )
            else:
                fixed_filename = fi["file_name"].replace(" ", "_")
                object_path = "gs://{}/{}/{}".format(
                    gs_bucket, fi["id"], fixed_filename
                )

            if fi["id"] in ignored_dict and object_path in indexd_records.get(
                fi.get("id"), []
            ):
                continue
            target_bucket = get_google_bucket_name(fi, project_acl)
            if (
                "{}/{}/{}".format(target_bucket, fi["id"], fi["file_name"])
                not in copied_objects
            ) or object_path not in indexd_records.get(fi.get("id"), []):
                filtered_copying_files.append(fi)
        copying_files = filtered_copying_files

    with open(local_manifest_txt_file, "w") as fw:
        fw.write("id\tfile_name\tsize\tmd5\tacl\tproject_id")
        for fi in copying_files:
            fw.write(
                "\n{}\t{}\t{}\t{}\t{}\t{}".format(
                    fi["id"],
                    fi["file_name"].replace(" ", "_"),
                    fi["size"],
                    fi["md5"],
                    fi["acl"].replace(" ", ""),
                    fi["project_id"],
                )
            )

    import subprocess

    cmd = "gsutil cp {} {}".format(
        local_manifest_txt_file, gs_manifest_file.replace(".tsv", ".txt")
    )
    subprocess.Popen(cmd, shell=True).wait()
    return gs_manifest_file.replace(".tsv", ".txt")


def get_ignored_files(ignored_filename, delimiter=","):
    """
    get all the files in 5aa buckets and are in gdc full manifest
    """
    result = {}
    try:
        with open(ignored_filename, "rt") as f:
            csvReader = csv.DictReader(f, delimiter=delimiter)
            for row in csvReader:
                if (
                    urlparse(row["gcs_object_url"]).netloc
                    == "5aa919de-0aa0-43ec-9ec3-288481102b6d"
                ):
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
                and urlparse(element["gcs_object_url"]).netloc
                == "5aa919de-0aa0-43ec-9ec3-288481102b6d"
            ):
                res = urlparse(element["gcs_object_url"])
                return os.path.join(*res.path.split("/")[2:])
    except IndexError:
        return None

    return None


def build_object_dataset_gs(PROJECT_ACL):
    mutexLock = threading.Lock()
    copied_object = {}

    def build_source_bucket_dataset(bucket_name, objects):
        """
        build source bucket dataset for lookup
        to avoid list object operations
        """
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        blobs = bucket.list_blobs()
        result = {}
        for blob in blobs:
            result[bucket_name + "/" + blob.name] = {
                "bucket": bucket_name,
                "Size": blob.size,
            }

        mutexLock.acquire()
        objects.update(result)
        mutexLock.release()

    threads = []
    target_bucket_names = set()
    for _, bucket_info in PROJECT_ACL.items():
        for label in ["open", "controlled"]:
            bucket_name = bucket_info["gs_bucket_prefix"] + "-" + label
            target_bucket_names.add(bucket_name)

    for target_bucket_name in target_bucket_names:
        threads.append(
            Thread(
                target=build_source_bucket_dataset,
                args=(target_bucket_name, copied_object),
            )
        )

    for th in threads:
        th.start()

    for th in threads:
        th.join()

    return copied_object


def write_csv(filename, files, sorted_attr=None, fieldnames=None):
    def on_key(element):
        return element[sorted_attr]

    if sorted_attr:
        sorted_files = sorted(files, key=on_key)
    else:
        sorted_files = files

    if not files:
        return
    fieldnames = fieldnames or files[0].keys()
    with open(filename, mode="w") as outfile:
        writer = csv.DictWriter(outfile, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()

        for f in sorted_files:
            writer.writerow(f)


def get_indexd_records():
    """
    Get all indexd records
    """
    results = {}
    indexd_client = IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )
    it = indexd_client.list(page_size=1000)

    progress = 0
    for doc in it:
        progress += 1
        urls = [url.replace("%20", " ") for url in doc.urls]
        results[doc.did] = urls

    return results


def async_get_indexd_records():
    loop = asyncio.get_event_loop()
    indexd = urlparse(INDEXD["host"])
    loop.run_until_complete(async_download_object_manifest(indexd.scheme + "://" + indexd.netloc))
    return _get_indexd_records_csv("object-manifest.csv")

def _get_indexd_records_csv(manifest):
    """
    structure:
    {
        guid: [location],
    }
    """
    res = {}
    with open(manifest, "rt") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=",")
        for row in csvReader:
            row["urls"] = row["urls"].split(" ") if row["urls"] else []
            res[row["guid"]] = [url.replace("%20", " ") for url in row["urls"]]
    os.remove("object-manifest.csv")
    return res
