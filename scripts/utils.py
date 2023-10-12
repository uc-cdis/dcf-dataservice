import os
import boto3
import csv
import random
from google.cloud import storage
import threading
from threading import Thread
from urllib.parse import urlparse

from indexclient.client import IndexClient


def get_aws_reversed_acl_bucket_name(target_bucket):
    """
    Get reversed acl bucket name
    """
    if "target" in target_bucket:
        if "open" in target_bucket:
            return "target-controlled"
        else:
            return "gdc-target-phs000218-2-open"

    if "tcga" in target_bucket:
        if "open" in target_bucket:
            return target_bucket[:-4] + "controlled"
        else:
            return target_bucket[:-10] + "open"

    if "controlled" in target_bucket:
        if "-2-controlled" in target_bucket:
            target_bucket = target_bucket.replace("-2-controlled", "")
        else:
            target_bucket = target_bucket.replace("-controlled", "")
        if target_bucket in POSTFIX_1_EXCEPTION:
            return target_bucket + "-open"
        else:
            return target_bucket + "-2-open"
    elif "open" in target_bucket:
        if "-2-open" in target_bucket:
            target_bucket = target_bucket.replace("-2-open", "")
        else:
            target_bucket = target_bucket.replace("-open", "")
        if target_bucket in POSTFIX_2_EXCEPTION:
            return target_bucket + "-2-controlled"
        else:
            return target_bucket + "-controlled"


def prepare_data(manifest_file, global_config, copied_objects=None, project_acl=None):
    """
    Read data file info from manifest and organize them into groups.
    Each group contains files which should be copied to the same bucket
    The groups will be push to the queue consumed by threads
    """

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
            # file does not exist or size is different
            # should check hash
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
    updated_copying_files = []
    for fi in copying_files:
        gs_bucket = get_google_bucket_name(fi, project_acl)
        if fi["id"] in ignored_dict:
            object_path = "gs://{}/{}".format(
                gs_bucket, get_structured_object_key(fi["id"], ignored_dict)
            )
        else:
            fixed_filename = fi["file_name"].replace(" ", "_")
            object_path = "gs://{}/{}/{}".format(gs_bucket, fi["id"], fixed_filename)

        target_bucket = get_google_bucket_name(fi, project_acl)
        if (
            "{}/{}/{}".format(target_bucket, fi["id"], fi["file_name"])
            not in copied_objects
        ):
            updated_copying_files.append(fi)
    copying_files = updated_copying_files

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
        results[doc.did] = doc.urls

    return results
