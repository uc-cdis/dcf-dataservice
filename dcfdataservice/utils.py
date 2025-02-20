import os
import boto3
import csv
import time
import random
from google.cloud import storage
import threading
from threading import Thread
from urllib.parse import urlparse

from dcfdataservice.errors import UserError
from indexclient.client import IndexClient
from dcfdataservice.settings import INDEXD, POSTFIX_1_EXCEPTION, POSTFIX_2_EXCEPTION


def get_aws_bucket_name(fi, PROJECT_ACL):
    try:
        project_info = PROJECT_ACL[fi.get("project_id")]
    except KeyError:
        raise UserError(
            "PROJECT_ACL does not have {} key. All keys of PROJECT_ACL are {}".format(
                fi.get("project_id"), PROJECT_ACL.keys()
            )
        )

    if "target" in project_info["aws_bucket_prefix"]:
        return (
            "gdc-target-phs000218-2-open"
            if fi.get("acl") in {"[u'open']", "['open']"}
            else "target-controlled"
        )

    if "tcga" in project_info["aws_bucket_prefix"]:
        return (
            "tcga-2-open"
            if fi.get("acl") in {"[u'open']", "['open']"}
            else "tcga-2-controlled"
        )

    # POSTFIX_1_EXCEPTION
    if project_info["aws_bucket_prefix"] in POSTFIX_1_EXCEPTION:
        return project_info["aws_bucket_prefix"] + (
            "-open"
            if fi.get("acl") in {"[u'open']", "['open']", "*"}
            else "-controlled"
        )

    # POSTFIX_2_EXCEPTION
    if project_info["aws_bucket_prefix"] in POSTFIX_2_EXCEPTION:
        return project_info["aws_bucket_prefix"] + (
            "-2-open"
            if fi.get("acl") in {"[u'open']", "['open']", "*"}
            else "-2-controlled"
        )

    # Default
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


def flip_bucket_accounts(aws_bucket_name):
    """
    flip bucket name from prod account to open account

    ex:
        aws_bucket_name: bucket-open
        return: bucket-2-open

        aws_bucket_name: bucket1-2-controlled
        return: bucket1-controlled
    """

    if "-2-controlled" in aws_bucket_name:
        return aws_bucket_name[:-12] + "controlled"
    elif "-controlled" in aws_bucket_name:
        return aws_bucket_name[:-10] + "2-controlled"
    elif "-2-open" in aws_bucket_name:
        return aws_bucket_name[:-6] + "open"
    elif "-open" in aws_bucket_name:
        return aws_bucket_name[:-4] + "2-open"


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
        results[doc.did] = doc.urls

    return results


def get_indexd_record_from_GDC_files(manifest_file, logger):
    """
    Get single indexd records for all GDC records
    Args:
        manifest_file (str): GDC manifest location
    """
    result = {}
    gdc_id_list = []
    errored_list = []

    indexd_client = IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )

    s3 = boto3.resource("s3")

    manifest_file = manifest_file.strip()
    out = urlparse(manifest_file)
    s3.meta.client.download_file(out.netloc, out.path[1:], "./manifest_read")

    def get_record_with_retry(guid, max_retries=5, base_delay=1, backoff_factor=2):
        """
        Get a record from indexd with retries and exponential backoff.
        Args:
            guid (str): The GUID to fetch.
            max_retries (int): Maximum number of retry attempts.
            base_delay (int): Initial delay between retries in seconds.
            backoff_factor (int): Multiplicative factor for exponential backoff.
        Returns:
            dict: The record from indexd if successful.
        Raises:
            Exception: If all retries fail.
        """
        attempt = 0
        while attempt < max_retries:
            try:
                return indexd_client.get(guid)
            except Exception as e:
                attempt += 1
                if attempt == max_retries:
                    raise
                wait_time = base_delay * (backoff_factor ** (attempt - 1))
                logger.warning(
                    f"Retrying {guid}: attempt {attempt}/{max_retries}, retrying in {wait_time}s. Error: {e}"
                )
                time.sleep(wait_time)

    # open GDC manifest file to extract guids
    with open("./manifest_read", "r") as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter="\t")
        for row in csv_reader:
            try:
                record = get_record_with_retry(row["id"])
                record_json = record.to_json()
                result[row["id"]] = record_json["urls"]
            except Exception as e:
                logger.error(f"Could not find record {row['id']}. Errored with {e}")
                errored_list.append(row["id"])

    if errored_list:
        logger.warning(
            f"Found {len(errored_list)} guids that weren't found in indexd. Here are all the guids: {errored_list}"
        )
    return result


def download_and_parse_map_file(file_location):
    """Downloads the map file and loads it as a readable dictionary
    Args:
        file_location (str): file location in s3 bucket
    Returns:
        dict: The contents of the map file as a dictionary.
    """
    # Initialize S3 resource
    s3 = boto3.resource("s3")

    # Strip any extra spaces
    file_location = file_location.strip()

    # Parse the S3 URL
    parsed_url = urlparse(file_location)
    bucket_name = parsed_url.netloc
    object_key = parsed_url.path.lstrip("/")

    # Download the file to a local temporary location
    local_file_path = "./map_file.json"
    s3.meta.client.download_file(bucket_name, object_key, local_file_path)

    # Load the JSON file as a dictionary
    with open(local_file_path, "r") as file:
        data = json.load(file)

    return data
