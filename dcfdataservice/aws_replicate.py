import io
from socket import error as SocketError
import errno
import math
from multiprocessing import Pool, Manager
from multiprocessing.dummy import Pool as ThreadPool
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from socket import error as SocketError
import signal
import sys
import time

from functools import partial
import subprocess
import shlex
import hashlib
import re
import urllib

import threading
from threading import Thread

from http.client import IncompleteRead
import json
import urllib.request
import boto3
import botocore
from botocore.exceptions import ClientError

from urllib.parse import urlparse

from cdislogging import get_logger
from indexclient.client import IndexClient

from dcfdataservice.settings import (
    PROJECT_ACL,
    INDEXD,
    GDC_TOKEN,
    DATA_ENDPT,
    POSTFIX_1_EXCEPTION,
    POSTFIX_2_EXCEPTION,
)
from dcfdataservice import utils
from dcfdataservice.utils import generate_chunk_data_list, prepare_data
from dcfdataservice.errors import UserError, APIError
from dcfdataservice.indexd_utils import update_url

global logger

RETRIES_NUM = 5

OPEN_ACCOUNT_PROFILE = "data-refresh-open"

# Configure retry strategy for downloads
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
)

download_adapter = HTTPAdapter(max_retries=retry_strategy)
upload_retries = 3  # Separate retry count for S3 uploads


class TransferMonitor:
    def __init__(self, total_parts):
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.completed_parts = set()
        self.total_parts = total_parts
        self.attempts = {}
        self.failed_parts = set()
        self.bytes_transferred = 0
        self.last_progress_log = 0

    def update_progress(self, part_number, bytes_transferred):
        with self.lock:
            self.completed_parts.add(part_number)
            self.bytes_transferred += bytes_transferred
            now = time.time()

            # Throttle progress logging
            if now - self.last_progress_log > 5:  # Log every 5 seconds
                elapsed = now - self.start_time
                speed = self.bytes_transferred / elapsed if elapsed > 0 else 0
                remaining = (
                    (self.total_parts - len(self.completed_parts))
                    * (elapsed / len(self.completed_parts))
                    if self.completed_parts
                    else 0
                )

                completed = f"{len(self.completed_parts)}/{self.total_parts}"
                percentage = f"{(len(self.completed_parts)/self.total_parts)*100:.1f}%"
                transferred = f"{self.bytes_transferred/(1024**2):.2f}MB"
                speed = f"{speed/(1024**2):.2f}MB/s"
                eta = f"{remaining/60:.1f}min" if remaining else "N/A"
                failed = len(self.failed_parts)
                transfer_progress = {
                    "completed": completed,
                    "percentage": percentage,
                    "transferred": transferred,
                    "speed": speed,
                    "eta": eta,
                    "failed": failed,
                }
                logger.info(json.dumps({"summary": transfer_progress}))
                self.last_progress_log = now


def calculate_total_parts(file_size_bytes, chunk_size_bytes):
    """
    Calculate total parts for predetermined chunk size

    Args:
        file_size_bytes (int): Total file size in bytes
        chunk_size_bytes (int): Fixed chunk size in bytes

    Returns:
        int: Total number of parts required

    Raises:
        ValueError: If chunk size violates AWS constraints
    """

    # AWS S3 requirements
    MIN_CHUNK = 5 * 1024**2  # 5 MiB
    MAX_PARTS = 10_000

    # Validate chunk size meets AWS minimum
    if chunk_size_bytes < MIN_CHUNK:
        raise ValueError(
            f"Chunk size {chunk_size_bytes} bytes too small. "
            f"Minimum 5 MiB ({MIN_CHUNK} bytes) required"
        )

    # Calculate total parts
    total_parts = math.ceil(file_size_bytes / chunk_size_bytes)

    # Verify AWS part limit
    if total_parts > MAX_PARTS:
        max_allowed_size = MAX_PARTS * chunk_size_bytes
        raise ValueError(
            f"File too large ({file_size_bytes} bytes) for {chunk_size_bytes} byte chunks. "
            f"Max allowed: {max_allowed_size} bytes with {MAX_PARTS} parts"
        )

    return total_parts


class ProcessingFile(object):
    def __init__(self, id, size, copy_method, original_storage):
        self.id = id
        self.size = size
        self.copy_method = copy_method
        self.original_storage = original_storage


def resume_logger(filename=None):
    global logger
    logger = get_logger("AWSReplication", filename)


def build_object_dataset_from_file(copied_objects_file, source_objects_file):
    """
    Load copied objects and source objects in local files
    """
    s3 = boto3.resource("s3")

    if copied_objects_file.startswith("s3://"):
        out = urlparse(copied_objects_file)
        s3.meta.client.download_file(
            out.netloc, out.path[1:], "./copied_objects_file.json"
        )
        copied_objects_file = "./copied_objects_file.json"

    if source_objects_file.startswith("s3://"):
        out = urlparse(source_objects_file)
        s3.meta.client.download_file(
            out.netloc, out.path[1:], "./source_objects_file.json"
        )
        source_objects_file = "./source_objects_file.json"

    with open(copied_objects_file, "r") as outfile:
        copied_objects = json.loads(outfile.read())

    with open(source_objects_file, "r") as outfile:
        source_objects = json.loads(outfile.read())

    return copied_objects, source_objects


def build_object_dataset_aws(project_acl, logger, awsbucket=None):
    """
    Load copied objects and source objects. The copied objects are obtained by
    listing the target buckets (derived from project_acl). The source objects are
    obtained by listing the objects in source bucket (GDC bucket)

    Args:
        project_acl(dict): project access control lever. It contains target bucket infos:
            - project_id(str): map with project_id in manifest
            - bucket prefix name: to derive the target bucket name (ex gdc-tcga-open|controlled)
        awsbucket(str): the source bucket

    Returns:
        copied_objects(dict): contains copied objects already
        source_objects(dict): contains source objects
    """

    mutexLock = threading.Lock()
    copied_objects = {}
    source_objects = {}
    client = boto3.client("s3")

    def list_objects(bucket_name, objects):
        """
        build object dataset for lookup with key is s3 object key and value contains
        storage class, size and md5
        to avoid list object operations
        """
        result = {}

        try:
            paginator = client.get_paginator("list_objects_v2")
            print("start to list objects in {}".format(bucket_name))
            pages = paginator.paginate(Bucket=bucket_name, RequestPayer="requester")
            for page in pages:
                for obj in page["Contents"]:
                    result[bucket_name + "/" + obj["Key"]] = {
                        "StorageClass": obj["StorageClass"],
                        "Size": obj["Size"],
                        "Bucket": bucket_name,
                    }
        except KeyError as e:
            logger.warning("{} is empty. Detail {}".format(bucket_name, e))
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 403:
                logger.error(
                    "Can not access the bucket {}. Detail {}".format(bucket_name, e)
                )
                raise
            else:
                logger.error(
                    "Can not detect the bucket {}. Detail {}".format(bucket_name, e)
                )
            raise
        except Exception as e:
            logger.error(f"Error listing objects for bucket {bucket_name}")
            logger.error(f"Erroring with message {e}")
            raise

        mutexLock.acquire()
        objects.update(result)
        mutexLock.release()

    threads = []
    target_bucket_names = set()
    for _, bucket_info in project_acl.items():
        # bad hard code to support ccle bucket name
        # if "ccle" in bucket_info["aws_bucket_prefix"]:
        #     target_bucket_names.add("ccle-open-access")
        #     target_bucket_names.add("gdc-ccle-controlled")
        #     continue
        if "target" in bucket_info["aws_bucket_prefix"]:
            target_bucket_names.add("gdc-target-phs000218-2-open")
            target_bucket_names.add("target-controlled")
            continue

        # REMINDER: if changing things here, change in get_aws_reversed_acl_bucket_name fnc and scripts/utils:get_aws_bucket_name as well.
        # Change way this is hardcoded
        for label in ["2-open", "controlled"]:
            if (
                bucket_info["aws_bucket_prefix"] in POSTFIX_1_EXCEPTION
            ) and label == "2-open":
                label = "open"
            if (
                bucket_info["aws_bucket_prefix"] in POSTFIX_2_EXCEPTION
            ) and label == "controlled":
                label = "2-controlled"

            target_bucket_names.add(bucket_info["aws_bucket_prefix"] + "-" + label)

    for target_bucket_name in target_bucket_names:
        threads.append(
            Thread(target=list_objects, args=(target_bucket_name, copied_objects))
        )

    if awsbucket:
        threads.append(Thread(target=list_objects, args=(awsbucket, source_objects)))

    logger.info("Start threads to list aws objects")
    for th in threads:
        th.start()
    logger.info("Wait for threads to finish the jobs")
    for th in threads:
        th.join()

    return copied_objects, source_objects


def bucket_exists(s3, bucket_name):
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        logger.error(
            "The bucket {} does not exist or you have no access. Detail {}".format(
                bucket_name, e
            )
        )
        return False


def object_exists(s3, bucket_name, key):
    """
    check if object exists or not. If object storage is GLACIER, the head_object will return 403
    The meaning of the function here is to check if it is possible to replicate the object with aws cli
    Args:
        s3(s3client): s3 client
        bucket_name(str): the name of the bucket
        key(str): object key
    Returns:
        bool: object exists or not
    Side effect:
        Log in case that no access provided
    """
    try:
        s3.meta.client.head_object(
            Bucket=bucket_name, Key=key, RequestPayer="requester"
        )
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code in {404, 403}:
            return False
        else:
            logger.error(
                "Something wrong with checking object {} in bucket {}. Detail {}".format(
                    key, bucket_name, e
                )
            )
            raise


def get_object_storage_class(s3, bucket_name, key):
    try:
        meta = s3.meta.client.head_object(
            Bucket=bucket_name, Key=key, RequestPayer="requester"
        )
        return meta.get("StorageClass", "STANDARD")
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            return None
        else:
            logger.error(
                "Something wrong with checking object class {} in bucket {}. Detail {}".format(
                    key, bucket_name, e
                )
            )
            raise


class JobInfo(object):
    def __init__(
        self,
        global_config,
        fi,
        total_files,
        total_copying_data,
        job_name,
        copied_objects,
        source_objects,
        manager_ns,
        release,
        bucket=None,
    ):
        """
        Class constructor

        Args:
            global_config(dict): a configuration
            {
                "multi_part_upload_threads": 10,
                "data_chunk_size": 1024*1024*5
            }
            manifest_file(str): manifest file
            thread_num(int): number of threads
            job_name(str): copying|indexing
            bucket(str): source bucket

        """
        self.bucket = bucket
        self.fi = fi
        self.total_files = total_files
        self.total_copying_data = total_copying_data
        self.global_config = global_config
        self.job_name = job_name
        self.copied_objects = copied_objects
        self.source_objects = source_objects
        self.manager_ns = manager_ns
        self.release = release

        self.indexclient = IndexClient(
            INDEXD["host"],
            INDEXD["version"],
            (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
        )


def exec_aws_copy(lock, quick_test, jobinfo):
    """
    Copy a chunk of files from the source bucket to the target buckets.
    The target buckets are infered from PROJECT_ACL and project_id in the file

    To locate the key of the file in the source bucket. Follow the below rule:
        - uuid/fname if not:
            extract key from url if not:
                uuid if not None

    There are some scenarios:
        - Object classes are "STANDARD", "REDUCED_REDUNDANCY": using aws cli
        - Object classes are not "STANDARD", "REDUCED_REDUNDANCY", using gdcapi
        - Object acl is changed, move objects to right bucket

    Intergrity check:
        - Using awscli: We rely on aws
        - Streaming:
            +) Compute local etag and match with the one provided by aws
            +) Compute md5 on the fly to check the intergrity of streaming data
                from gdcapi to local machine

    Args:
        jobinfo(JobInfo): Job info

    Returns:
        None
    """
    fi = jobinfo.fi

    try:
        target_bucket = utils.get_aws_bucket_name(fi, PROJECT_ACL)
    except UserError as e:
        logger.warning(e)
        return

    # profile_name = OPEN_ACCOUNT_PROFILE if "-2-" in target_bucket else "default"
    session = boto3.session.Session(profile_name="default")
    s3 = session.resource("s3")
    pFile = None
    try:
        if not bucket_exists(s3, target_bucket):
            logger.error(
                "There is no bucket with provided name {}\n".format(target_bucket)
            )
            return

        object_key = "{}/{}".format(fi.get("id"), fi.get("file_name"))

        # object already exists in dcf but acl is changed
        if is_changed_acl_object(fi, jobinfo.copied_objects, target_bucket):
            logger.info("acl object is changed. Delete the object in the old bucket")
            cmd = 'aws s3 mv "s3://{}/{}" "s3://{}/{}" --acl bucket-owner-full-control'.format(
                utils.get_aws_reversed_acl_bucket_name(target_bucket),
                object_key,
                target_bucket,
                object_key,
            )
            if not jobinfo.global_config.get("quiet", False):
                logger.info(cmd)
            if not quick_test:
                subprocess.Popen(shlex.split(cmd)).wait()
                try:
                    update_url(fi, jobinfo.indexclient)
                except APIError as e:
                    logger.warning(e)
            else:
                pFile = ProcessingFile(fi["id"], fi["size"], "AWS", None)

        # only copy ones not exist in target buckets
        elif "{}/{}".format(target_bucket, object_key) not in jobinfo.copied_objects:
            source_key = object_key
            object_key_object_exists = object_exists(s3, jobinfo.bucket, source_key)
            if not object_key_object_exists:
                try:
                    source_key = re.search(
                        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.*$",
                        fi["url"],
                    ).group(0)

                    url_key_object_exists = object_exists(
                        s3, jobinfo.bucket, source_key
                    )
                    if not url_key_object_exists:
                        source_key = None
                except (AttributeError, TypeError):
                    source_key = None

                id_key_object_exists = object_exists(s3, jobinfo.bucket, fi["id"])
                if source_key is None and id_key_object_exists:
                    source_key = fi["id"]

            if not source_key:
                logger.info(
                    "Object with id {} does not exist in source bucket {}. Stream from gdcapi".format(
                        fi["id"],
                        jobinfo.bucket,
                    )
                )
                if not quick_test:
                    try:
                        stream_object_from_gdc_api(
                            fi,
                            target_bucket,
                            jobinfo.global_config,
                            jobinfo,
                        )
                        update_url(fi, jobinfo.indexclient)
                    except Exception as e:
                        logger.warning(e)
                else:
                    pFile = ProcessingFile(fi["id"], fi["size"], "GDCAPI", None)
                return

            try:
                # storage_class = jobinfo.source_objects[source_key]["StorageClass"]
                storage_class = get_object_storage_class(s3, jobinfo.bucket, source_key)
            except Exception as e:
                logger.warning(e)
                return

            # If storage class is DEEP_ARCHIVE or GLACIER, stream object from gdc api
            if storage_class in {"DEEP_ARCHIVE", "GLACIER", "GLACIER_IR"}:
                if not jobinfo.global_config.get("quiet", False):
                    logger.info(
                        "Streaming: {} from GDC API. Size {} (MB). Class {}.".format(
                            object_key,
                            int(fi["size"] * 1.0 / 1024 / 1024),
                            storage_class,
                        )
                    )
                if not quick_test:
                    try:
                        stream_object_from_gdc_api(
                            fi,
                            target_bucket,
                            jobinfo.global_config,
                            jobinfo,
                        )
                    except Exception as e:
                        # catch generic exception to prevent the code from terminating
                        # in the middle of replicating process
                        logger.warning(e)
                else:
                    pFile = ProcessingFile(fi["id"], fi["size"], "GDCAPI", None)

            else:
                logger.info("start aws copying {}".format(object_key))
                cmd = 'aws s3 cp "s3://{}/{}" "s3://{}/{}" --acl bucket-owner-full-control --request-payer requester'.format(
                    jobinfo.bucket, source_key, target_bucket, object_key
                )
                if not jobinfo.global_config.get("quiet", False):
                    logger.info(cmd)
                if not quick_test:
                    # wait untill finish
                    subprocess.Popen(shlex.split(cmd)).wait()
                else:
                    pFile = ProcessingFile(fi["id"], fi["size"], "AWS", storage_class)

        else:
            logger.info(
                "object {} is already copied to {}".format(object_key, target_bucket)
            )
        if not quick_test:
            try:
                update_url(fi, jobinfo.indexclient)
            except APIError as e:
                logger.warning(e)
    except Exception as e:
        logger.error("Something wrong with {}. Detail {}".format(fi["id"], e))

    lock.acquire()
    jobinfo.manager_ns.total_processed_files += 1
    jobinfo.manager_ns.total_copied_data += fi["size"] * 1.0 / 1024 / 1024 / 1024
    if pFile:
        jobinfo.manager_ns.pFiles = jobinfo.manager_ns.pFiles + [pFile]
    if not quick_test and jobinfo.manager_ns.total_processed_files % 5 == 0:
        try:
            session.client("s3").upload_file(
                "./log.txt",
                jobinfo.global_config.get("log_bucket"),
                jobinfo.release + "/log.txt",
            )
        except Exception as e:
            logger.error(e)
    lock.release()
    logger.info(
        "{}/{} objects are processed and {}/{} (GiB) is copied".format(
            jobinfo.manager_ns.total_processed_files,
            jobinfo.total_files,
            int(jobinfo.manager_ns.total_copied_data),
            int(jobinfo.total_copying_data),
        )
    )


def stream_object_from_gdc_api(fi, target_bucket, global_config, jobinfo):
    """
    Stream object from gdc api. In order to check the integrity, we need to compute md5 during streaming data from
    gdc api and compute its local etag since aws only provides etag for multi-part uploaded object.

    Args:
        fi(dict): object info
        target_bucket(str): target bucket
        global_config(dict): a configuration
            {
                "multi_part_upload_threads": 10,
                "data_chunk_size": 1024*1024*5
            }

    Returns:
        None
    """

    class ThreadControl(object):
        """
        Class for thread synchronization
        """

        def __init__(self, total_parts):
            self.mutexLock = threading.Lock()
            self.chunk_hashes = {}  # Track hashes by part number
            self.completed_parts = set()
            self.total_parts = total_parts

    def _handler_multipart(chunk_info, monitor):
        part_number = chunk_info["part_number"]
        chunk_id = f"{fi['id']}-part-{part_number}"

        # Download with retries and streaming
        for attempt in range(RETRIES_NUM + 1):
            try:
                session = requests.Session()
                session.mount("https://", download_adapter)

                headers = {
                    "X-Auth-Token": GDC_TOKEN,
                    "Range": f"bytes={chunk_info['start']}-{chunk_info['end']}",
                }
                logger.info(
                    f"Attempting Download for file {data_endpoint} attempt #{attempt}"
                )
                start_time = time.time()

                with session.get(
                    data_endpoint,
                    headers=headers,
                    stream=True,
                    timeout=(3.05, 30),  # Connect/read timeouts
                ) as response:
                    response.raise_for_status()
                    expected_size = chunk_info["end"] - chunk_info["start"] + 1

                    # Stream directly to S3 with progress tracking
                    upload_success = False
                    for upload_attempt in range(upload_retries + 1):
                        try:
                            logger.info(
                                f"Attempting Upload for file {data_endpoint} attempt #{upload_attempt}"
                            )

                            # Create a file-like object from the response stream
                            file_obj = io.BytesIO()
                            downloaded_bytes = 0
                            for chunk in response.iter_content(chunk_size=8192):
                                file_obj.write(chunk)
                                downloaded_bytes += len(chunk)
                                monitor.update_progress(part_number, len(chunk))

                            # Verify downloaded size
                            if downloaded_bytes != expected_size:
                                raise IncompleteRead(
                                    f"Expected {expected_size}, got {downloaded_bytes}"
                                )

                            # Reset buffer position and upload
                            file_obj.seek(0)
                            res = thread_s3.upload_part(
                                Body=file_obj,
                                Bucket=target_bucket,
                                Key=object_path,
                                PartNumber=part_number,
                                UploadId=multipart_upload["UploadId"],
                            )

                            # Verify ETag matches MD5
                            md5 = hashlib.md5(file_obj.getvalue()).hexdigest()
                            if res["ETag"].strip('"') != md5:
                                raise ValueError("ETag verification failed")

                            upload_success = True
                            break

                        except (ClientError, SocketError) as e:
                            logger.warning(
                                f"Upload failed for {data_endpoint} with error {e}"
                            )
                            if upload_attempt == upload_retries:
                                raise
                            time.sleep(min(2**upload_attempt, 10))

                    if upload_success:
                        logger.info("Chunk completed successfully")
                        return res

            except (requests.RequestException, IncompleteRead) as e:
                logger.error(
                    f"Download failed for {data_endpoint} with error: {e} after {attempt} attempts"
                )
                if attempt == RETRIES_NUM:
                    with monitor.lock:
                        monitor.failed_parts.add(part_number)
                    raise
                time.sleep(min(2**attempt, 10))

        raise Exception(f"Failed after {RETRIES_NUM} download attempts")

    def _handler_single_upload():
        """
        streaming whole data from api to aws bucket without using multipart uplaod
        """
        download_tries = 0

        while download_tries < RETRIES_NUM:  # something wrong here
            try:
                req = urllib.request.Request(
                    data_endpoint, headers={"X-Auth-Token": GDC_TOKEN}
                )

                # data_stream = urllib.request.urlopen(req).read()
                with urllib.request.urlopen(req) as response:
                    logger.info(f"Downloading {fi.get('id')}: {fi.get('size')}")

                data_stream = io.BytesIO(response.read())

                data_stream.seek(0)
                # stream_size = data_stream.getbuffer().nbytes
                # logger.info(f"Data stream size: {stream_size} bytes")
                # if stream_size != fi.get("size"):
                #     raise Exception(
                #         f"Downloading {fi.get('id')}. Expecting file size: {fi.get('size')}, got: {stream_size} bytes"
                #     )
                upload_tries = 0
                while upload_tries < RETRIES_NUM:
                    try:
                        logger.info(
                            f"Attempting to upload object {fi.get('id')} to s3 with upload file object"
                        )
                        res = thread_s3.upload_fileobj(
                            Fileobj=data_stream,
                            Bucket=target_bucket,
                            Key=object_path,
                        )

                        thread_control.mutexLock.acquire()
                        thread_control.sig_update_turn += 1
                        thread_control.mutexLock.release()

                        logger.info(f"Uploaded file {fi.get('id')}")

                        return res
                    except botocore.exceptions.ClientError as e:
                        logger.warning(e)
                        time.sleep(5)
                        upload_tries += 1
                    except Exception as e:
                        logger.warning(e)
                        time.sleep(5)
                        upload_tries += 1
                    if upload_tries == RETRIES_NUM:
                        raise botocore.exceptions.ClientError(
                            "Can not upload chunk data of {} to {}".format(
                                fi["id"], target_bucket
                            )
                        )

            except urllib.error.HTTPError as e:
                logger.warning(
                    "Fail to open http connection to gdc api. Take a sleep and retry. Detail {}".format(
                        e
                    )
                )
                time.sleep(5)
                if e.code == 403:
                    break
                upload_tries += 1
            except SocketError as e:
                if e.errno != errno.ECONNRESET:
                    logger.warning(
                        "Connection reset. Take a sleep and retry. Detail {}".format(e)
                    )
                    time.sleep(60)
                    upload_tries += 1
            except Exception as e:
                logger.warning(e)
                time.sleep(5)
                upload_tries += 1

        if download_tries == RETRIES_NUM:
            raise Exception(
                "Can not open http connection to gdc api {}".format(data_endpoint)
            )

    thread_s3 = boto3.client("s3")
    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
    data_endpoint = (
        DATA_ENDPT + fi.get("id", "")
        if DATA_ENDPT
        else "https://api.gdc.cancer.gov/data/{}".format(fi.get("id"))
    )
    size = int(fi.get("size"))

    try:
        multipart_upload = thread_s3.create_multipart_upload(
            Bucket=target_bucket,
            Key=object_path,
            ACL="bucket-owner-full-control",
        )
    except botocore.exceptions.ClientError as error:
        logger.error(
            "Error when create multiple part upload for object with uuid {}. Detail {}".format(
                object_path, error
            )
        )
        return

    chunk_data_size = global_config.get("chunk_size", 256) * 1024 * 1024

    thread_control = ThreadControl(
        total_parts=calculate_total_parts(
            size,
            chunk_data_size,
        )
    )

    tasks = []
    for part_number, data_range in enumerate(
        generate_chunk_data_list(fi["size"], chunk_data_size)
    ):
        start, end = data_range
        tasks.append({"start": start, "end": end, "part_number": part_number + 1})

    if global_config.get("multi_part_upload_threads"):
        print(f"Thread Size: {global_config.get('multi_part_upload_threads')}")

    total_parts = calculate_total_parts(fi["size"], chunk_data_size)
    monitor = TransferMonitor(total_parts)

    try:
        pool = ThreadPool(global_config.get("multi_part_upload_threads", 10))
        results = pool.map(partial(_handler_multipart, monitor=monitor), tasks)
        pool.close()
        pool.join()
    except Exception as e:
        logger.error(f"Fatal error in transfer: {e}")
    finally:
        avg_speed = f"{monitor.bytes_transferred/(time.time()-monitor.start_time)/(1024**2):.2f}MB/s"
        summary_json = {
            "success": len(monitor.completed_parts),
            "failed": len(monitor.failed_parts),
            "duration": time.time() - monitor.start_time,
            "avg_speed": avg_speed,
        }
        logger.info(json.dumps({"summary": summary_json}))

    parts = []
    total_bytes_received = 0

    sorted_results = sorted(results, key=lambda x: x[1])

    for res, part_number, chunk_size in sorted_results:
        parts.append({"ETag": res["ETag"], "PartNumber": part_number})
        total_bytes_received += chunk_size

    try:
        thread_s3.complete_multipart_upload(
            Bucket=target_bucket,
            Key=object_path,
            MultipartUpload={"Parts": parts},
            UploadId=multipart_upload["UploadId"],
            RequestPayer="requester",
        )
    except botocore.exceptions.ClientError as error:
        logger.warning(
            "Error when finishing multiple part upload object with uuid {}. Detail {}".format(
                fi.get("id"), error
            )
        )
        return

    sig_check_pass = validate_uploaded_data(
        fi, thread_s3, target_bucket, total_bytes_received, thread_control
    )

    if not sig_check_pass:
        try:
            logger.warning(
                f"Object validation failed, deleting object from location: {object_path}"
            )
            thread_s3.delete_object(Bucket=target_bucket, Key=object_path)
        except botocore.exceptions.ClientError as error:
            logger.warning(error)
    else:
        logger.info(
            "successfully stream file {} to {}".format(object_path, target_bucket)
        )
    # else:
    #     _handler_single_upload()
    #     # pool = ThreadPool(global_config.get("multi_part_upload_threads", 10))
    #     # results = pool.map(
    #     #     _handler_single_upload
    #     # )
    #     # pool.close()
    #     # pool.join()
    jobinfo.manager_ns.total_processed_files += 1
    jobinfo.manager_ns.total_copied_data += fi["size"] * 1.0 / 1024 / 1024 / 1024
    logger.info(
        "{}/{} objects are processed and {}/{} (GiB) is copied".format(
            jobinfo.manager_ns.total_processed_files,
            jobinfo.total_files,
            int(jobinfo.manager_ns.total_copied_data),
            int(jobinfo.total_copying_data),
        )
    )


def validate_uploaded_data(
    fi,
    thread_s3,
    target_bucket,
    total_bytes_received,
    thread_control,
):
    """
    validate uploaded data

    Args:
        fi(dict): file info
        thread_s3(s3client): s3 client
        target_bucket(str): aws bucket
        sig(sig): md5 of downloaded data from api
        total_bytes_received(int): total data in bytes

    Returns:
       bool: pass or not
    """

    if total_bytes_received != fi.get("size"):
        logger.warning(
            "Can not stream the object {}. Size does not match".format(fi.get("id"))
        )
        return False

    ordered_hashes = [
        thread_control.chunk_hashes[part_num]
        for part_num in sorted(thread_control.chunk_hashes)
    ]

    if len(ordered_hashes) > 1:  # AWS-style multipart ETag
        combined_hash = hashlib.md5(b"".join(ordered_hashes)).hexdigest()
        final_etag = f"{combined_hash}-{len(ordered_hashes)}"
    else:  # Single part
        final_etag = hashlib.md5(ordered_hashes[0]).hexdigest()

    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))

    try:
        meta_data = thread_s3.head_object(Bucket=target_bucket, Key=object_path)
    except botocore.exceptions.ClientError as error:
        logger.warning(
            "Can not get meta data of {}. Detail {}".format(fi.get("id"), error)
        )
        return False

    size_in_bucket = meta_data.get("ContentLength")

    if size_in_bucket != fi.get("size"):
        logger.warning(
            f"Size in bucket {size_in_bucket} foes not match file size {fi.get('size')}"
        )
        return False

    if meta_data.get("ETag") is None:
        logger.warning("Can not get etag of {}".format(fi.get("id")))
        return False

    if final_etag != fi.get("md5"):
        logger.warning(
            "Can not stream the object {}. md5 check fails".format(fi.get("id"))
        )
        return False

    return True


def is_changed_acl_object(fi, copied_objects, target_bucket):
    """
    check if the object has acl changed or not
    """

    object_path = "{}/{}/{}".format(
        utils.get_aws_reversed_acl_bucket_name(target_bucket),
        fi.get("id"),
        fi.get("file_name"),
    )
    if object_path in copied_objects:
        return True

    return False


def check_and_index_the_data(lock, jobinfo):
    """
    Check if files are in manifest are copied or not.
    Index the files if they exists in target buckets and log
    """
    json_log = {}
    for fi in jobinfo.files:
        object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
        if object_path not in jobinfo.copied_objects:
            json_log[object_path] = {"copy_success": False, "index_success": False}
        else:
            try:
                json_log[object_path] = {
                    "copy_success": True,
                    "index_success": update_url(fi, jobinfo.indexclient),
                }
            except Exception as e:
                logger.error(e.message)
                json_log[object_path] = {
                    "copy_success": True,
                    "index_success": False,
                    "msg": e.message,
                }
    lock.acquire()
    jobinfo.manager_ns.total_processed_files += len(jobinfo.files)
    lock.release()
    logger.info(
        "{}/{} object are processed/indexed ".format(
            jobinfo.manager_ns.total_processed_files, jobinfo.total_files
        )
    )

    return json_log


def run(
    release,
    thread_num,
    global_config,
    job_name,
    manifest_file,
    quick_test=False,
    bucket=None,
):
    """
    start processes and log after they finish
    """

    # Add this at the beginning of run()
    def main_signal_handler(sig, frame):
        logger.warning(
            "Main thread interrupt received, initiating graceful shutdown..."
        )
        # Add any cleanup logic needed
        sys.exit(1)

    signal.signal(signal.SIGINT, main_signal_handler)
    signal.signal(signal.SIGTERM, main_signal_handler)

    resume_logger("./log.txt")
    logger.info(f"Starting GDC AWS replication. Release #:{release}")
    if not global_config.get("log_bucket"):
        raise UserError("please provide the log bucket")

    session = boto3.session.Session(profile_name="default")
    s3_sess = session.resource("s3")

    if not bucket_exists(s3_sess, global_config.get("log_bucket")):
        logger.error(f"Log bucket does not exist")
        # return # TODO: uncomment this later

    log_filename = manifest_file.split("/")[-1].replace(".tsv", ".txt")

    s3 = boto3.client("s3")
    try:
        logger.info("Downloading log file")
        s3.download_file(
            global_config.get("log_bucket"), release + "/" + log_filename, "./log.txt"
        )
    except botocore.exceptions.ClientError as e:
        print("Can not download log. Detail {}".format(e))

    copied_objects, source_objects = {}, {}

    if job_name != "indexing":
        logger.info("scan all copied objects")
        copied_objects, _ = build_object_dataset_aws(PROJECT_ACL, logger, None)

    tasks, total_files, total_copying_data = prepare_data(
        manifest_file, global_config, copied_objects, PROJECT_ACL
    )

    logger.info("Total files need to be replicated: {}".format(total_files))

    manager = Manager()
    manager_ns = manager.Namespace()
    manager_ns.total_processed_files = 0
    manager_ns.total_copied_data = 0
    manager_ns.pFiles = []
    lock = manager.Lock()

    jobInfos = []
    for task in tasks:
        job = JobInfo(
            global_config,
            task[0],
            total_files,
            total_copying_data,
            job_name,
            copied_objects,
            source_objects,
            manager_ns,
            release,
            bucket,
        )
        jobInfos.append(job)

    # Make the Pool of workers
    if global_config.get("mode") == "process":
        pool = Pool(thread_num)
    else:
        pool = ThreadPool(thread_num)

    try:
        if job_name == "copying":
            part_func = partial(exec_aws_copy, lock, quick_test)
        elif job_name == "indexing":
            part_func = partial(check_and_index_the_data, lock)
        else:
            raise UserError("Not supported!!!")

        pool.map_async(part_func, jobInfos).get(9999999)
    except KeyboardInterrupt:
        pool.terminate()

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    if not quick_test:
        try:
            s3.upload_file(
                "./log.txt",
                global_config.get("log_bucket"),
                release + "/" + log_filename,
            )
        except Exception as e:
            logger.error(e)

    logger.info("=======================SUMMARY=======================")
    n_copying_gdcapi = 0
    total_copying_gdcapi = 0
    n_copying_aws_intelligent_tiering = 0
    total_copying_aws_intelligent_tiering = 0
    n_copying_aws_non_intelligent_tiering = 0
    total_copying_aws_non_intelligent_tiering = 0
    logger.info("Total size of pFiles: {}".format(len(manager_ns.pFiles)))
    for pFile in manager_ns.pFiles:
        if pFile.copy_method == "GDCAPI":
            n_copying_gdcapi += 1
            total_copying_gdcapi += pFile.size
        elif pFile.original_storage == "INTELLIGENT_TIERING":
            n_copying_aws_intelligent_tiering += 1
            total_copying_aws_intelligent_tiering += pFile.size
        else:
            n_copying_aws_non_intelligent_tiering += 1
            total_copying_aws_non_intelligent_tiering += pFile.size

    logger.info(
        "Total files are copied by GDC API {}. Total {}(GiB)".format(
            n_copying_gdcapi, total_copying_gdcapi * 1.0 / 1024 / 1024 / 1024
        )
    )
    logger.info(
        "Total files are copied by AWS CLI {} with intelligent tiering storage classes. Total {}(GiB)".format(
            n_copying_aws_intelligent_tiering,
            total_copying_aws_intelligent_tiering * 1.0 / 1024 / 1024 / 1024,
        )
    )
    logger.info(
        "Total files are copied by AWS CLI {} with non-intelligent tiering storage classes. Total {}(GiB)".format(
            n_copying_aws_non_intelligent_tiering,
            total_copying_aws_non_intelligent_tiering * 1.0 / 1024 / 1024 / 1024,
        )
    )
