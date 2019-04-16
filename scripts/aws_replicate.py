from socket import error as SocketError
import errno
from multiprocessing import Pool, Manager
from multiprocessing.dummy import Pool as ThreadPool
import time
import os
from functools import partial
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
    generate_chunk_data_list,
    prepare_data
)
from errors import UserError, APIError
from indexd_utils import update_url

global logger

RETRIES_NUM = 5


def resume_logger(filename=None):
    global logger
    logger = get_logger("AWSReplication", filename)


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
                    result[bucket_name+"/"+obj["Key"]] = {
                        "StorageClass": obj["StorageClass"],
                        "Size": obj["Size"],
                        "Bucket": bucket_name,
                    }
        except KeyError as e:
            logger.error("Something wrong with listing objects in {}. Detail {}".format(bucket_name, e))
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


def bucket_exists(s3, bucket_name):
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        logger.error("The bucket {} does not exist or you have no access. Detail {}".format(bucket_name, e))
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
        s3.meta.client.head_object(Bucket=bucket_name, Key=key, RequestPayer='requester')
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code in {404, 403}:
            return False
        else:
            logger.error("Something wrong with checking object {} in bucket {}. Detail {}".format(key, bucket_name,e))
            raise


def get_object_storage_class(s3, bucket_name, key):
    try:
        meta = s3.meta.client.head_object(Bucket=bucket_name, Key=key, RequestPayer='requester')
        return meta.get("StorageClass", "STANDARD")
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            return None
        else:
            logger.error("Something wrong with checking object class {} in bucket {}. Detail {}".format(key, bucket_name, e))
            raise


class JobInfo(object):
    def __init__(
        self,
        global_config,
        files,
        total_files,
        total_copying_data,
        job_name,
        copied_objects,
        source_objects,
        manager_ns,
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
        self.files = files
        self.total_files = total_files
        self.total_copying_data = total_copying_data
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


def exec_aws_copy(lock, jobinfo):
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
    session = boto3.session.Session()
    s3 = session.resource("s3")

    for fi in files:
        try:
            target_bucket = utils.get_aws_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            logger.warn(e)
            continue

        try:
            if not bucket_exists(s3, target_bucket):
                logger.error("There is no bucket with provided name {}\n".format(target_bucket))
                continue

            object_key = "{}/{}".format(fi.get("id"), fi.get("file_name"))

            # object already exists in dcf but acl is changed
            if is_changed_acl_object(fi, jobinfo.copied_objects, target_bucket):
                logger.info("acl object is changed. Move object to the right bucket")
                cmd = "aws s3 mv s3://{}/{} s3://{}/{}".format(
                    get_reversed_acl_bucket_name(target_bucket),
                    object_key,
                    target_bucket,
                    object_key,
                )
                if not jobinfo.global_config.get("quiet", False):
                    logger.info(cmd)
                subprocess.Popen(shlex.split(cmd)).wait()
                try:
                    update_url(fi, jobinfo.indexclient)
                except APIError as e:
                    logger.warn(e)
                continue

            # only copy ones not exist in target buckets
            if "{}/{}".format(target_bucket, object_key) not in jobinfo.copied_objects:
                source_key = object_key
                if not object_exists(s3, jobinfo.bucket, source_key):
                    try:
                        source_key = re.search(
                            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.*$",
                            fi["url"],
                        ).group(0)

                        if not object_exists(s3, jobinfo.bucket, source_key):
                            source_key = None
                    except (AttributeError, TypeError):
                        source_key = None

                    if source_key is None and object_exists(s3, jobinfo.bucket, fi["id"]):
                        source_key = fi["id"]

                if not source_key:
                    logger.info(
                        "object with id {} does not exist in source bucket {}. Stream from gdcapi".format(
                            fi["id"], jobinfo.bucket
                        )
                    )
                    try:
                        stream_object_from_gdc_api(fi, target_bucket, jobinfo.global_config)
                        update_url(fi, jobinfo.indexclient)
                    except Exception as e:
                        logger.warn(e)
                    continue

                try:
                    #storage_class = jobinfo.source_objects[source_key]["StorageClass"]
                    storage_class = get_object_storage_class(s3, jobinfo.bucket, source_key)
                except Exception as e:
                    logger.warn(e)
                    continue

                # If storage class is DEEP_ARCHIVE or GLACIER, stream object from gdc api
                if storage_class in {"DEEP_ARCHIVE", "GLACIER"}:
                    if not jobinfo.global_config.get("quiet", False):
                        logger.info(
                            "Streaming: {}. Size {} (MB). Class {}".format(
                                object_key,
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
                    logger.info("start aws copying {}".format(object_key))
                    cmd = "aws s3 cp s3://{}/{} s3://{}/{} --request-payer requester".format(
                        jobinfo.bucket, source_key, target_bucket, object_key
                    )
                    if not jobinfo.global_config.get("quiet", False):
                        logger.info(cmd)
                    # wait untill finish
                    subprocess.Popen(shlex.split(cmd)).wait()
            else:
                logger.info(
                    "object {} is already copied to {}".format(object_key, target_bucket)
                )
            try:
                update_url(fi, jobinfo.indexclient)
            except APIError as e:
                logger.warn(e)
        except Exception as e:
            logger.error("Something wrong with {}. Detail {}".format(fi["id"], e))
    lock.acquire()
    jobinfo.manager_ns.total_processed_files += len(files)
    jobinfo.manager_ns.total_copied_data += fi["size"]*1.0/1024/1024/1024
    lock.release()
    logger.info(
        "{}/{} objects are processed and {}/{} (GiB) is copied".format(
            jobinfo.manager_ns.total_processed_files, jobinfo.total_files, int(jobinfo.manager_ns.total_copied_data), int(jobinfo.total_copying_data)
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
        global_config(dict): a configuration
            {
                "multi_part_upload_threads": 10,
                "data_chunk_size": 1024*1024*5
            }
        endpoint(str): gdcapi
    
    Returns:
        None
    """

    class ThreadControl(object):
        """
        Class for thread synchronization 
        """

        def __init__(self):
            self.mutexLock = threading.Lock()
            self.sig_update_turn = 1

    def _handler(chunk_info):
        """
        streamming chunk data from api to aws bucket

        Args:
            chunk_info(dict):
                {
                    "start": start,
                    "end": end,
                    "part_number": part_number
                }
        """
        tries = 0
        request_success = False

        chunk = None
        while tries < RETRIES_NUM and not request_success:
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

        if tries == RETRIES_NUM:
            raise Exception(
                "Can not open http connection to gdc api {}".format(data_endpoint)
            )

        md5 = hashlib.md5(chunk).digest()

        tries = 0
        while tries < RETRIES_NUM:
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
                    "quiet"
                ):
                    logger.info(
                        "Downloading {}. Received {} MB".format(
                            fi.get("id"),
                            thead_control.sig_update_turn
                            * 1.0
                            / 1024
                            / 1024
                            * chunk_data_size,
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

        if tries == RETRIES_NUM:
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
        logger.error(
            "Error when create multiple part upload for object with uuid{}. Detail {}".format(
                object_path, error
            )
        )
        return

    chunk_data_size = global_config.get("data_chunk_size", 1024 * 1024 * 128)

    tasks = []
    for part_number, data_range in enumerate(
        generate_chunk_data_list(fi["size"], chunk_data_size)
    ):
        start, end = data_range
        tasks.append({"start": start, "end": end, "part_number": part_number + 1})

    pool = ThreadPool(global_config.get("multi_part_upload_threads", 10))
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
        fi, thread_s3, target_bucket, sig, md5_digests, total_bytes_received
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


def validate_uploaded_data(fi, thread_s3, target_bucket, sig, md5_digests, total_bytes_received):
    """
    validate uploaded data

    Args:
        fi(dict): file info
        thread_s3(s3client): s3 client
        target_bucket(str): aws bucket
        sig(sig): md5 of downloaded data from api
        md5_digests(list(md5)): list of chunk md5
        total_bytes_received(int): total data in bytes

    Returns:
       bool: pass or not
    """

    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))

    # compute local etag from list of md5s
    etags = hashlib.md5(b"".join(md5_digests)).hexdigest() + "-" + str(len(md5_digests))

    if total_bytes_received != fi.get("size"):
        logger.warn(
            "Can not stream the object {}. Size does not match".format(fi.get("id"))
        )
        return False

    try:
        meta_data = thread_s3.head_object(Bucket=target_bucket, Key=object_path)
    except botocore.exceptions.ClientError as error:
        logger.warn(
            "Can not get meta data of {}. Detail {}".format(fi.get("id"), error)
        )
        return False

    if meta_data.get("ETag") is None:
        logger.warn("Can not get etag of {}".format(fi.get("id")))
        return False

    if sig.hexdigest() != fi.get("md5"):
        logger.warn(
            "Can not stream the object {}. md5 check fails".format(fi.get("id"))
        )
        return False

    if meta_data.get("ETag", "").replace('"', "") not in {fi.get("md5"), etags}:
        logger.warn(
            "Can not stream the object {} to {}. Etag check fails".format(
                fi.get("id"), target_bucket
            )
        )
        return False

    return True


def get_reversed_acl_bucket_name(target_bucket):
    """
    Get reversed acl bucket name
    """
    if target_bucket == "ccle-open-access":
        return "gdc-ccle-controlled"
    if target_bucket == "gdc-ccle-controlled":
        return "ccle-open-access"
    if "open" in target_bucket:
        return target_bucket[:-4] + "controlled"

    return target_bucket[:-10] + "open"


def is_changed_acl_object(fi, copied_objects, target_bucket):
    """
    check if the object has acl changed or not
    """

    object_path = "{}/{}/{}".format(get_reversed_acl_bucket_name(target_bucket), fi.get("id"), fi.get("file_name"))
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


def run(release, thread_num, global_config, job_name, manifest_file, bucket=None):
    """
    start processes and log after they finish
    """
    if not global_config.get("log_bucket"):
        raise UserError("please provide the log bucket")
    
    session = boto3.session.Session()
    s3_sess = session.resource("s3")
    
    if not bucket_exists(s3_sess, global_config.get("log_bucket")):
        return

    log_filename = manifest_file.replace.split("/")[-1](".tsv", "txt")

    s3 = boto3.client("s3")
    try:
        s3.download_file(global_config.get("log_bucket"), release + "/" + log_filename, "./log.txt")
    except botocore.exceptions.ClientError as e:
        print("Can not download log. Detail {}".format(e))

    resume_logger("./log.txt")

    copied_objects, source_objects = {}, {}

    if job_name != "indexing":
        logger.info("scan all copied objects")
        copied_objects, _ = build_object_dataset_aws(PROJECT_ACL, logger, None)

    tasks, total_files, total_copying_data = prepare_data(manifest_file, global_config, copied_objects, PROJECT_ACL)

    logger.info("Total files need to be replicated: {}".format(total_files))

    manager = Manager()
    manager_ns = manager.Namespace()
    manager_ns.total_processed_files = 0
    manager_ns.total_copied_data = 0
    lock = manager.Lock()

    jobInfos = []
    for task in tasks:
        job = JobInfo(
            global_config,
            task,
            total_files,
            total_copying_data,
            job_name,
            copied_objects,
            source_objects,
            manager_ns,
            bucket,
        )
        jobInfos.append(job)

    # Make the Pool of workers
    if global_config.get("mode") == "process":
        pool = Pool(thread_num)
    else:
        pool = ThreadPool(thread_num)

    results = []
    try:
        if job_name == "copying":
            part_func = partial(exec_aws_copy, lock)
        elif job_name == "indexing":
            part_func = partial(check_and_index_the_data, lock)
        else:
            raise UserError("Not supported!!!")

        results = pool.map_async(part_func, jobInfos).get(9999999)
    except KeyboardInterrupt:
        pool.terminate()

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

    with open(filename, "w") as outfile:
        json.dump(json_log, outfile)
    try:
        s3.upload_file(
            "./log.txt", global_config.get("log_bucket"), release + "/" + log_filename
        )
        s3.upload_file(
            filename, global_config.get("log_bucket"), release + "/" + os.path.basename(filename)
        )
    except Exception as e:
        logger.error(e)
