import os
import base64
import requests
import urllib
import time
from multiprocessing import Pool, Manager
from functools import partial
import subprocess

from google.cloud import storage
from google.cloud.storage import Blob
from scripts.google_resumable_upload import GCSObjectStreamUpload
from google.auth.transport.requests import AuthorizedSession

from google.api_core.exceptions import BadRequest, Forbidden

# from cdislogging import get_logger
import logging as logger

from indexclient.client import IndexClient

from scripts import utils
from scripts.errors import APIError, UserError, StreamError
from scripts.settings import PROJECT_ACL, INDEXD, GDC_TOKEN, IGNORED_FILES, DATA_ENDPT
from scripts import indexd_utils

logger.basicConfig(level=logger.INFO, format="%(asctime)s %(message)s")


DEFAULT_CHUNK_SIZE_DOWNLOAD = 1024 * 1024 * 32
DEFAULT_CHUNK_SIZE_UPLOAD = 1024 * 1024 * 256
NUM_TRIES = 30
NUM_STREAMING_TRIES = 5

# logger = get_logger("GoogleReplication")


class DataFlowLog(object):
    def __init__(self, copy_success=False, index_success=False, message=""):
        self.copy_success = copy_success
        self.index_success = index_success
        self.message = message


def bucket_exists(bucket_name):
    """
    check if bucket_name exists or not!
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    tries = 0
    while tries < NUM_TRIES:
        try:
            return bucket.exists()
        except BadRequest:
            return False
        except Forbidden as e:
            logger.error(
                "Bucket is not accessible {}. Detail {}".format(bucket_name, e)
            )
            return False
        except Exception:
            time.sleep(300)
            tries += 1
    logger.error(
        "Can not check the status of the bucket {} after multiple attempts".format(
            bucket_name
        )
    )
    return False


def blob_exists(bucket_name, blob_name):
    """
    check if blob/key exists or not!
    """
    tries = 0

    while tries < NUM_TRIES:
        try:
            if bucket_exists(bucket_name):
                client = storage.Client()
                bucket = client.bucket(bucket_name)
                blob = Blob(blob_name, bucket)
                return blob.exists()
            else:
                return False
        except BadRequest:
            return False
        except Exception:
            time.sleep(300)
            tries += 1

    logger.error(
        "Can not check the status of the blob {} after multiple attempts".format(
            blob_name
        )
    )
    return False


def check_blob_name_exists_and_match_md5_size(sess, bucket_name, blob_name, fi):
    """
    check if blob object exists or not
    Args:
        bucket_name(str): the name of bucket
        blob_name(str): the name of blob
        fi(dict): file info dictionary
            {
                "id": "123",
                "file_name": "abc.bam",
                "size": 5,
                ...
            }
    Returns:
        bool: indicating value if the blob is exist or not
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.parse.quote(blob_name, safe="")
    )
    res = sess.request(method="GET", url=url)
    return (
        res.status_code == 200
        and int(res.json()["size"]) == fi.get("size")
        and base64.b64decode(res.json()["md5Hash"]).hex() == fi.get("md5")
    )


def fail_resumable_copy_blob(sess, bucket_name, blob_name, fi):
    """
    Something wrong during the copy(only for simple upload and resumable upload)
    Args:
        bucket_name(str): the name of bucket
        blob_name(str): the name of blob/key
        fi(dict): file info dictionary
            {
                "id": "123",
                "file_name": "abc.bam",
                "size": 5,
                ...
            }
    Returns:
        bool: indicating value if the blob is exist or not
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.parse.quote(blob_name, safe="")
    )
    res = sess.request(method="GET", url=url)
    return res.status_code == 200 and base64.b64decode(
        res.json()["md5Hash"]
    ).hex() != fi.get("md5")


def delete_object(sess, bucket_name, blob_name):
    """
    Delete object from cloud
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.parse.quote(blob_name, safe="")
    )
    return sess.request(method="DELETE", url=url)


def google_copy_wrapper(fi, ignored_dict, global_config):
    try:
        datalog = exec_google_copy(fi, ignored_dict, global_config)
    except Exception as e:
        datalog = DataFlowLog(message="Internal error. Detail {}".format(e))

    if global_config.get("log_bucket") and global_config.get("release"):
        with open(fi["id"], "w") as f:
            f.write(
                "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s"
                % (
                    fi.get("id"),
                    fi.get("file_name").replace(" ", "_"),
                    int(fi.get("size")),
                    fi.get("md5"),
                    fi.get("acl"),
                    fi.get("project_id"),
                    datalog.copy_success,
                    datalog.index_success,
                    datalog.message,
                )
            )
        cmd = "gsutil cp {} gs://{}/{}/".format(
            fi["id"], global_config.get("log_bucket"), global_config.get("release")
        )
        subprocess.Popen(cmd, shell=True).wait()
        time.sleep(1)

    return datalog


def exec_google_copy(fi, ignored_dict, global_config):
    """
    copy a file to google bucket.
    Args:
        fi(dict): a dictionary of a copying file
        global_config(dict): a configuration
            {
                "chunk_size_download": 1024,
                "chunk_size_upload": 1024
            }
    Returns:
        DataFlowLog
    """
    if fi["size"] == 0:
        msg = "can not copy {} to GOOGLE bucket since it is empty file".format(fi["id"])
        return DataFlowLog(message=msg)

    indexd_client = IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )

    if not ignored_dict:
        raise UserError(
            "Expecting non-empty IGNORED_FILES. Please check if ignored_files_manifest.py is configured correctly!!!"
        )
    try:
        bucket_name = utils.get_google_bucket_name(fi, PROJECT_ACL)
    except UserError as e:
        msg = "can not copy {} to GOOGLE bucket. Detail {}. {}".format(
            fi["id"], e, PROJECT_ACL
        )
        logger.error(msg)
        return DataFlowLog(message=msg)

    if not bucket_exists(bucket_name):
        msg = "There is no bucket with provided name {}\n".format(bucket_name)
        logger.error(msg)
        return DataFlowLog(message=msg)

    if fi["id"] in ignored_dict:
        logger.info(
            "{} is ignored. Start to check indexd for u5aa objects".format(fi["id"])
        )
        _update_indexd_for_5aa_object(fi, bucket_name, ignored_dict, indexd_client)
        return DataFlowLog(message="{} is in the ignored list".format(fi["id"]))

    client = storage.Client()
    sess = AuthorizedSession(client._credentials)
    blob_name = fi.get("id") + "/" + fi.get("file_name")

    _check_and_handle_changed_acl_object(fi)

    if blob_exists(bucket_name, blob_name):
        logger.info("{} is already copied".format(fi["id"]))
    else:
        try:
            logger.info(
                "Start to stream {}. Size {} (MB)".format(
                    fi["id"], fi["size"] * 1.0 / 1000 / 1000
                )
            )
            tries = 0
            while tries < NUM_STREAMING_TRIES:
                try:
                    resumable_streaming_copy(
                        fi, client, bucket_name, blob_name, global_config
                    )
                    if fail_resumable_copy_blob(sess, bucket_name, blob_name, fi):
                        delete_object(sess, bucket_name, blob_name)
                    else:
                        break
                except Exception as e:
                    logger.warning(e)
                    tries += 1
            if tries == NUM_STREAMING_TRIES:
                logger.error(
                    "Can not stream {} after multiple attemps".format(fi.get("id"))
                )
            else:
                logger.info(
                    "Finish streaming {}. Size {} (MB)".format(
                        fi["id"], fi["size"] * 1.0 / 1000 / 1000
                    )
                )
        except APIError as e:
            logger.error(str(e))
            return DataFlowLog(message=str(e))
        except Exception as e:
            # Don't break (Not expected)
            logger.error(str(e))
            return DataFlowLog(message=str(e))

    # Confirm that the object was copied
    if blob_exists(bucket_name, blob_name):
        try:
            if indexd_utils.update_url(fi, indexd_client, provider="gs"):
                logger.info("Successfully update indexd for {}".format(fi["id"]))
            else:
                logger.info("Can not update indexd for {}".format(fi["id"]))
        except APIError as e:
            logger.error(e)
            return DataFlowLog(copy_success=True, message=e)
    else:
        msg = "can not copy {} to GOOGLE bucket after multiple attempts. Check the error detail in logs".format(
            blob_name
        )
        logger.error(msg)
        return DataFlowLog(message=msg)

    return DataFlowLog(
        copy_success=True,
        index_success=True,
        message="object {} successfully copied ".format(blob_name),
    )


def _update_indexd_for_5aa_object(fi, bucket_name, ignored_dict, indexclient):
    """
    update indexd for 5aa objects

    Args:
        fi(dict): file info
        bucket_name(str): bucket name
        ignored_dict(dict): dictionary of 5aa objects with key is id and value containing
        gs url hash, size, etc.
        indexclient(indexdclient): indexd client
    Returns:
        None

    """
    object_key = utils.get_structured_object_key(fi["id"], ignored_dict)
    # check if 5aa object really exists
    if blob_exists(bucket_name, object_key):
        url = "gs://" + bucket_name + "/" + object_key
        if indexd_utils.update_url(fi, indexclient, provider="gs", url=url):
            logger.info("Successfully update indexd for {}".format(fi["id"]))
        else:
            logger.info("Can not update indexd for {}".format(fi["id"]))
    else:
        logger.error(
            "{} which is 5aa bucket does not exist in dcf buckets.".format(fi["id"])
        )


def resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config):
    """
    Copy file to google bucket. Implemented using google cloud resumale API
    Args:
        fi(dict): file information
        client(google client): google client
        bucket_name(str): the target bucket
        blob_name(str): the object key
        global_config(dict): configurations
            {
                "chunk_size_download": 1024,
                "chunk_size_upload": 1024
            }
    Returns: None
    """
    chunk_size_download = global_config.get(
        "chunk_size_download", DEFAULT_CHUNK_SIZE_DOWNLOAD
    )
    chunk_size_upload = global_config.get(
        "chunk_size_upload", DEFAULT_CHUNK_SIZE_UPLOAD
    )
    data_endpt = DATA_ENDPT + fi.get("id", "")
    token = GDC_TOKEN

    tries = 0
    while tries < NUM_TRIES:
        try:
            response = requests.get(
                data_endpt,
                stream=True,
                headers={"Content-Type": "application/json", "X-Auth-Token": token},
            )
            # Too many requests
            if response.status_code == 429:
                time.sleep(60)
                tries += 1
            else:
                break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(5)
            tries += 1
        except Exception as e:
            raise APIError(
                "Can not setup connection to gdcapi for {}. Detail {}".format(
                    fi["id"], e
                )
            )

    if tries == NUM_TRIES:
        raise APIError("Can not setup connection to gdcapi for {}".format(fi["id"]))

    if response.status_code not in {200, 203}:
        raise APIError(
            "GDCAPI error {} for uuid {}".format(response.status_code, fi["id"])
        )

    try:
        streaming(
            client,
            response,
            bucket_name,
            chunk_size_download,
            chunk_size_upload,
            blob_name,
            fi["size"],
        )
    except Exception:
        raise StreamError(
            "GCSObjectStreamUpload: Can not upload {}".format(fi.get("id", ""))
        )


def streaming(
    client,
    response,
    bucket_name,
    chunk_size_download,
    chunk_size_upload,
    blob_name,
    total_size,
):
    """
    stream the file with google resumable upload
    Args:
        client(GSClient): gs storage client
        response(httpResponse): http response
        bucket_name(str): target google bucket name
        chunk_size_download(int): chunk size in bytes from downling data file
        blob_name(str): object name
        total_size(int): the file size
    Returns:
        None
    """

    # keep track the number of chunks uploaded
    with GCSObjectStreamUpload(
        client=client,
        bucket_name=bucket_name,
        blob_name=blob_name,
        chunk_size=chunk_size_upload,
    ) as s:
        progress = 0
        number_download = 0
        for chunk in response.iter_content(chunk_size=chunk_size_download):
            if chunk:  # filter out keep-alive new chunks
                progress += s.write(chunk)
                number_download += 1
                if number_download % int(1024 * 1024 * 2048 / chunk_size_download) == 0:
                    logger.info(
                        "Uploading {}. Size {} (MB). Progress {}%".format(
                            blob_name,
                            total_size * 1.0 / 1000 / 1000,
                            100.0 * progress / total_size,
                        )
                    )


def _is_completed_task(sess, task):
    for fi in task:
        try:
            target_bucket = utils.get_google_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            logger.warning(e)
            continue
        blob_name = "{}/{}".format(fi["id"], fi["file_name"])
        if not blob_exists(target_bucket, blob_name):
            return False

    return True


def _check_and_handle_changed_acl_object(fi):
    """
    if object is changed acl, move it to the right bucket
    """
    bucket_name = utils.get_google_bucket_name(fi, PROJECT_ACL)
    blob_name = "{}/{}".format(fi["id"], fi["file_name"])
    bucket_name_reverse = (
        bucket_name[:-5] + "-controlled"
        if "open" in fi["acl"]
        else bucket_name[:-11] + "-open"
    )
    if blob_exists(bucket_name_reverse, blob_name):
        cmd = "gsutil mv gs://{}/{} gs://{}/{}".format(
            bucket_name_reverse, blob_name, bucket_name, blob_name
        )
        subprocess.Popen(cmd, shell=True).wait()
        return True
    return False


class JobInfo(object):
    def __init__(
        self,
        global_config,
        files,
        total_files,
        job_name,
        copied_objects,
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
            files(list(str)): list of copying files
            total_files(int): total number of files
            job_name(str): copying|indexing
            copied_objects(dict): a dictionary of copied files with key is uuid/file_name
            manager_ns(ManagerNamespace): for synchronization
            bucket(str): source bucket

        """
        self.bucket = bucket
        self.files = files
        self.total_files = total_files
        self.global_config = global_config
        self.job_name = job_name
        self.copied_objects = copied_objects
        self.manager_ns = manager_ns

        self.indexclient = IndexClient(
            INDEXD["host"],
            INDEXD["version"],
            (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
        )
