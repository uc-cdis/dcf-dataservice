from socket import error as SocketError
import errno
import multiprocessing
from multiprocessing import Pool, Manager
from multiprocessing.dummy import Pool as ThreadPool
import time
import os
import subprocess
import shlex
import hashlib
import re
import urllib2

import threading
from threading import Thread

import json
import boto3
import botocore

from cdislogging import get_logger
from indexclient.client import IndexClient

from settings import PROJECT_ACL, INDEXD, GDC_TOKEN
import utils
from utils import (
    get_fileinfo_list_from_csv_manifest,
    get_fileinfo_list_from_s3_manifest,
    generate_chunk_data_list,
)
from errors import UserError
from indexd_utils import update_url

logger = get_logger("AWSReplication")


def build_object_dataset_from_file(copied_objects_file, source_objects_file):
    """
    Load copied objects and source objects in local files
    """
    s3 = boto3.resource("s3")
    from urlparse import urlparse

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


def build_object_dataset(project_acl, awsbucket):
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

    def list_objects(bucket_name, objects):
        """
        build object dataset for lookup with key is s3 object key and value contains
        storage class, size and md5
        to avoid list object operations
        """
        client = boto3.client("s3")
        result = {}

        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, RequestPayer="requester")

        try:
            for page in pages:
                for obj in page["Contents"]:
                    result[obj["Key"]] = {
                        "StorageClass": obj["StorageClass"],
                        "Size": obj["Size"],
                        "Bucket": bucket_name,
                    }
        except KeyError:
            logger.info("There is no object in {}".format(bucket_name))
        except botocore.exceptions.ClientError as e:
            logger.error(
                "Can not detect the bucket {}. Detail {}".format(bucket_name, e)
            )
        mutexLock.acquire()
        objects.update(result)
        mutexLock.release()

    threads = []
    target_bucket_names = set()
    for _, bucket_info in project_acl.iteritems():
        # bad hard code to support ccle bucket name
        if "ccle" in bucket_info["aws_bucket_prefix"]:
            target_bucket_names.add("ccle-open-access")
            target_bucket_names.add("gdc-ccle-controlled")
            continue
        for label in ["open", "controlled"]:
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

    chunk_size = global_config.get("chunk_size", 1)
    tasks = []

    for idx in range(0, len(copying_files), chunk_size):
        tasks.append(copying_files[idx : idx + chunk_size])

    return tasks, len(copying_files)


def object_exists(bucket_name, key):
    """
    check if object exists or not
    Args:
        s3(s3client): s3 client
        bucket_name(str): the name of the bucket
        key(str): object key
    Returns:
        bool: object exists or not
    Side effect:
        Log in case that no access provided
    """
    s3 = boto3.resource("s3")
    try:
        s3.meta.client.head_object(Bucket=bucket_name, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            return False
        else:
            raise


class JobInfo(object):
    def __init__(
        self,
        global_config,
        files,
        total_files,
        job_name,
        copied_objects,
        source_objects,
        manager_ns,
        bucket=None,
    ):
        """
        Class constructor

        Args:
            global_config(dict): configuration dictionary
            manifest_file(str): manifest file
            thread_num(int): number of threads
            job_name(str): copying|indexing
            bucket(str): source bucket
        
        """
        self.bucket = bucket
        self.files = files
        self.total_files = total_files
        self.global_config = global_config
        self.job_name = job_name
        self.copied_objects = copied_objects
        self.source_objects = source_objects
        self.manager_ns = manager_ns

        self.indexclient = IndexClient(
            INDEXD["host"],
            INDEXD["version"],
            (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
        )


mutexLock = threading.Lock()

global total_processed_files
total_processed_files = 0


def exec_aws_copy(jobinfo):
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

    files = jobinfo.files

    for fi in files:
        try:
            target_bucket = utils.get_aws_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            logger.warn(e)
            continue

        object_name = "{}/{}".format(fi.get("id"), fi.get("file_name"))

        # only copy ones not exist in target buckets
        if object_name not in jobinfo.copied_objects:
            # if not object_exists(target_bucket, object_name):
            source_key = object_name
            if source_key not in jobinfo.source_objects:
                try:
                    source_key = re.search(
                        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.*$",
                        fi["url"],
                    ).group(0)
                    if source_key not in jobinfo.source_objects:
                        source_key = None
                except (AttributeError, TypeError):
                    source_key = None

                if source_key is None and fi["id"] in jobinfo.source_objects:
                    source_key = fi["id"]

            if not source_key:
                logger.warn(
                    "object with id {} does not exist in source bucket {}. Stream from gdcapi".format(
                        fi["id"], jobinfo.bucket
                    )
                )
                try:
                    stream_object_from_gdc_api(fi, target_bucket, jobinfo.global_config)
                except Exception as e:
                    logger.warn(e)
                continue

            storage_class = jobinfo.source_objects[source_key]["StorageClass"]

            # If storage class is not standard or REDUCED_REDUNDANCY, stream object from gdc api
            if storage_class not in {"STANDARD", "REDUCED_REDUNDANCY"}:
                if not jobinfo.global_config.get("quite", False):
                    logger.info(
                        "Streaming: {}. Size {} (MB). Class {}".format(
                            object_name,
                            int(fi["size"] * 1.0 / 1024 / 1024),
                            storage_class,
                        )
                    )
                try:
                    stream_object_from_gdc_api(fi, target_bucket, jobinfo.global_config)
                except Exception as e:
                    # catch generic exception to prevent the code from terminating
                    # in the middle of replicating process
                    logger.warn(e)
            else:
                cmd = "aws s3 cp s3://{}/{} s3://{}/{} --request-payer requester".format(
                    jobinfo.bucket, source_key, target_bucket, object_name
                )
                if not jobinfo.global_config.get("quite", False):
                    logger.info(cmd)
                # wait untill finish
                subprocess.Popen(shlex.split(cmd + " --quiet")).wait()

        # object already exists in dcf but acl is changed
        elif is_changed_acl_object(fi, jobinfo.copied_objects):
            logger.info("acl object is changed. Move object to the right bucket")
            cmd = "aws s3 mv s3://{}/{} s3://{}/{}".format(
                jobinfo.copied_objects[object_name],
                object_name,
                target_bucket,
                object_name,
            )
            if not jobinfo.global_config.get("quite", False):
                logger.info(cmd)
            subprocess.Popen(shlex.split(cmd + " --quiet")).wait()
        else:
            logger.info(
                "object {} is already copied to {}".format(object_name, target_bucket)
            )

    jobinfo.manager_ns.total_processed_files += len(files)
    logger.info(
        "{}/{} object are processed/copying ".format(
            jobinfo.manager_ns.total_processed_files, jobinfo.total_files
        )
    )

    return len(files)


def stream_object_from_gdc_api(fi, target_bucket, global_config, endpoint=None):
    """
    Stream object from gdc api. In order to check the integrity, we need to compute md5 during streaming data from 
    gdc api and compute its local etag since aws only provides etag for multi-part uploaded object.

    Args:
        fi(dict): object info
        target_bucket(str): target bucket
    
    Returns:
        None
    """

    class ThreadControl(object):
        def __init__(self):
            self.mutexLock = threading.Lock()
            self.sig_update_turn = 1

    def _handler(chunk_info):
        tries = 0
        request_success = False

        chunk = None
        while tries < 10 and not request_success:
            try:
                req = urllib2.Request(
                    data_endpoint,
                    headers={
                        "X-Auth-Token": GDC_TOKEN,
                        "Range": "bytes={}-{}".format(
                            chunk_info["start"], chunk_info["end"]
                        ),
                    },
                )

                chunk = urllib2.urlopen(req).read()
                if len(chunk) == chunk_info["end"] - chunk_info["start"] + 1:
                    request_success = True

            except urllib2.HTTPError as e:
                logger.warn(
                    "Fail to open http connection to gdc api. Take a sleep and retry. Detail {}".format(
                        e
                    )
                )
                time.sleep(5)
                tries += 1
            except SocketError as e:
                if e.errno != errno.ECONNRESET:
                    logger.warn(
                        "Connection reset. Take a sleep and retry. Detail {}".format(e)
                    )
                    time.sleep(60)
                    tries += 1
            except Exception as e:
                logger.warn(e)
                time.sleep(5)
                tries += 1

        if tries == 10:
            raise Exception(
                "Can not open http connection to gdc api {}".format(data_endpoint)
            )

        md5 = hashlib.md5(chunk).digest()

        tries = 0
        while tries < 3:
            try:
                res = thread_s3.upload_part(
                    Body=chunk,
                    Bucket=target_bucket,
                    Key=object_path,
                    PartNumber=chunk_info["part_number"],
                    UploadId=multipart_upload.get("UploadId"),
                )

                while thead_control.sig_update_turn != chunk_info["part_number"]:
                    time.sleep(1)

                thead_control.mutexLock.acquire()
                sig.update(chunk)
                thead_control.sig_update_turn += 1
                thead_control.mutexLock.release()

                if thead_control.sig_update_turn % 10 == 0 and not global_config.get(
                    "quite"
                ):
                    logger.info(
                        "Received {} MB".format(
                            thead_control.sig_update_turn
                            * 1.0
                            / 1024
                            / 1024
                            * chunk_data_size
                        )
                    )

                return res, chunk_info["part_number"], md5, len(chunk)

            except botocore.exceptions.ClientError as e:
                logger.warn(e)
                time.sleep(5)
                tries += 1
            except Exception as e:
                logger.warn(e)
                time.sleep(5)
                tries += 1

        if tries == 3:
            raise botocore.exceptions.ClientError(
                "Can not upload chunk data of {} to {}".format(fi["id"], target_bucket)
            )

    thead_control = ThreadControl()
    thread_s3 = boto3.client("s3")
    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
    data_endpoint = endpoint or "https://api.gdc.cancer.gov/data/{}".format(
        fi.get("id")
    )

    sig = hashlib.md5()
    # prepare to compute local etag
    md5_digests = []

    try:
        multipart_upload = thread_s3.create_multipart_upload(
            Bucket=target_bucket, Key=object_path
        )
    except botocore.exceptions.ClientError as error:
        logger.warn(
            "Error when create multiple part upload for object with uuid{}. Detail {}".format(
                object_path, error
            )
        )
        return None

    chunk_data_size = global_config.get("data_chunk_size", 1024 * 1024 * 128)

    tasks = []
    for part_number, data_range in enumerate(
        generate_chunk_data_list(fi["size"], chunk_data_size)
    ):
        start, end = data_range
        tasks.append({"start": start, "end": end, "part_number": part_number + 1})

    pool = ThreadPool(global_config.get("multi_part_upload_thread", 10))
    results = pool.map(_handler, tasks)
    pool.close()
    pool.join()

    parts = []
    total_bytes_received = 0

    sorted_results = sorted(results, key=lambda x: x[1])

    for res, part_number, md5, chunk_size in sorted_results:
        parts.append({"ETag": res["ETag"], "PartNumber": part_number})
        md5_digests.append(md5)
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
        logger.warn(
            "Error when finishing multiple part upload object with uuid {}. Detail {}".format(
                fi.get("Id"), error
            )
        )
        return

    sig_check_pass = validate_uploaded_data(
        fi, thread_s3, target_bucket, sig, sorted_results
    )

    if not sig_check_pass:
        try:
            thread_s3.delete_object(Bucket=target_bucket, Key=object_path)
        except botocore.exceptions.ClientError as error:
            logger.warn(error)
    else:
        logger.info(
            "successfully stream file {} to {}".format(object_path, target_bucket)
        )


def validate_uploaded_data(fi, thread_s3, target_bucket, sig, sorted_results):
    """
    validate uploaded data

    Args:
        fi(dict): file info
        thread_s3(s3client): s3 client
        target_bucket(str): aws bucket
        sig(sig): md5 of downloaded data from api
        sorted_results(list(object)): list of result returned by upload function
    
    Returns:
       bool: pass or not
    """

    md5_digests = []
    total_bytes_received = 0
    parts = []
    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))

    for res, part_number, md5, chunk_size in sorted_results:
        parts.append({"ETag": res["ETag"], "PartNumber": part_number})
        md5_digests.append(md5)
        total_bytes_received += chunk_size

    # compute local etag from list of md5s
    etags = hashlib.md5(b"".join(md5_digests)).hexdigest() + "-" + str(len(md5_digests))

    sig_check_pass = True

    if total_bytes_received != fi.get("size"):
        logger.warn(
            "Can not stream the object {}. Size does not match".format(
                format(fi.get("id"))
            )
        )
        sig_check_pass = False

    if sig_check_pass:
        try:
            meta_data = thread_s3.head_object(Bucket=target_bucket, Key=object_path)
        except botocore.exceptions.ClientError as error:
            logger.warn(
                "Can not get meta data of {}. Detail {}".format(fi.get("id"), error)
            )
            sig_check_pass = False

    if sig_check_pass:
        if meta_data.get("ETag") is None:
            logger.warn("Can not get etag of {}".format(fi.get("id")))
            sig_check_pass = False

    if sig_check_pass:
        if sig.hexdigest() != fi.get("md5"):
            logger.warn(
                "Can not stream the object {}. md5 check fails".format(
                    format(fi.get("id"))
                )
            )
            sig_check_pass = False

    if sig_check_pass:
        if meta_data.get("ETag", "").replace('"', "") not in {fi.get("md5"), etags}:
            logger.warn(
                "Can not stream the object {} to {}. Etag check fails".format(
                    format(fi.get("id"), target_bucket)
                )
            )
            sig_check_pass = False

    return sig_check_pass


def is_changed_acl_object(fi, copied_objects):
    """
    check if the object has acl changed or not
    """

    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
    if (
        fi.get("acl") == "[u'open']"
        and "controlled" in copied_objects.get(object_path, "")["Bucket"]
    ) or (
        fi.get("acl") != "[u'open']"
        and "open" in copied_objects.get(object_path, "")["Bucket"]
    ):
        return True
    return False


def check_and_index_the_data(jobinfo):
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

    jobinfo.manager_ns.total_processed_files += len(jobinfo.files)
    logger.info(
        "{}/{} object are processed/indexed ".format(
            jobinfo.manager_ns.total_processed_files, jobinfo.total_files
        )
    )

    return json_log


def run(thread_num, global_config, job_name, manifest_file, bucket):
    """
    start processes and log after they finish
    """
    copied_objects, source_objects = build_object_dataset_from_file(
        global_config["copied_objects"], global_config["source_objects"]
    )

    tasks, _ = prepare_data(manifest_file, global_config)

    manager = Manager()
    manager_ns = manager.Namespace()
    manager_ns.total_processed_files = 0

    jobInfos = []
    for task in tasks:
        job = JobInfo(
            global_config,
            task,
            len(tasks),
            job_name,
            copied_objects,
            source_objects,
            manager_ns,
            bucket,
        )
        jobInfos.append(job)

    # Make the Pool of workers
    pool = Pool(thread_num)

    results = []

    if job_name == "copying":
        results = pool.map(exec_aws_copy, jobInfos)
    elif job_name == "indexing":
        results = pool.map(check_and_index_the_data, jobInfos)

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    filename = global_config.get("log_file", "{}_log.json".format(job_name))

    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = timestr + "_" + filename

    if job_name == "copying":
        results = [{"data": results}]

    json_log = {}

    for result in results:
        json_log.update(result)

    s3 = boto3.client("s3")
    with open(filename, "w") as outfile:
        json.dump(json_log, outfile)
    s3.upload_file(
        filename, global_config.get("log_bucket"), os.path.basename(filename)
    )
