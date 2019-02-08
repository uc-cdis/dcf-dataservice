import base64
import requests
import urllib
import time

from google.cloud import storage
from google.cloud.storage import Blob
from google_resumable_upload import GCSObjectStreamUpload
from google.auth.transport.requests import AuthorizedSession

import logging as logger

# from indexclient.client import IndexClient
import indexclient

import utils
from errors import APIError, UserError
from settings import PROJECT_ACL, INDEXD, GDC_TOKEN
import indexd_utils


DATA_ENDPT = "https://api.gdc.cancer.gov/data/"
DEFAULT_CHUNK_SIZE_DOWNLOAD = 2048000
DEFAULT_CHUNK_SIZE_UPLOAD = 1024 * 20 * 1024
NUM_TRIES = 10


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
    return bucket.exists()


def blob_exists(bucket_name, blob_name):
    """
    check if blob exists or not!
    """
    if bucket_exists(bucket_name):
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = Blob(blob_name, bucket)
        return blob.exists()
    return False


def check_blob_name_exists_and_match_md5_size(sess, bucket_name, blob_name, fi):
    """
    check if blob object exists or not
    Args:
        bucket_name(str): the name of bucket
        blob_name(str): the name of blob
        fi(dict): file info dictionary
    Returns:
        bool: indicating value if the blob is exist or not
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.quote(blob_name, safe="")
    )
    res = sess.request(method="GET", url=url)
    return (
        res.status_code == 200
        and int(res.json()["size"]) == fi.get("size")
        and base64.b64decode(res.json()["md5Hash"]).encode("hex") == fi.get("md5")
    )


def exec_google_copy(fi, global_config):
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
    indexd_client = indexclient.client.IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )
    client = storage.Client()
    sess = AuthorizedSession(client._credentials)
    blob_name = fi.get("id") + "/" + fi.get("file_name")
    try:
        bucket_name = utils.get_google_bucket_name(fi, PROJECT_ACL)
    except UserError as e:
        msg = "can not copy {} to GOOGLE bucket. Detail {}".format(blob_name, e)
        logger.error(msg)
        return DataFlowLog(message=msg)

    if not bucket_exists(bucket_name):
        msg = "There is no bucket with provided name {}\n".format(bucket_name)
        logger.error(msg)
        return DataFlowLog(message=msg)

    if not check_blob_name_exists_and_match_md5_size(sess, bucket_name, blob_name, fi):
        try:
            resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config)
        except APIError as e:
            logger.error(e.message)
            return DataFlowLog(message=e.message)
        except Exception as e:
            # Don't break (Not expected)
            logger.error(e.message)
            return DataFlowLog(message=e.message)

    if check_blob_name_exists_and_match_md5_size(sess, bucket_name, blob_name, fi):
        try:
            indexd_utils.update_url(fi, indexd_client, "gs")
        except APIError as e:
            logger.error(e)
            return DataFlowLog(copy_success=True, message=e)
    else:
        msg = "can not copy {} to GOOGLE bucket".format(blob_name)
        logger.error(msg)
        return DataFlowLog(message=msg)

    return DataFlowLog(
        copy_success=True,
        index_success=True,
        message="object {} successfully copied ".format(blob_name),
    )


def resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config):
    """
    Copy file to google bucket. Implemented using google cloud resumale API
    Args:
        fi(dict): file information
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
            raise APIError("Can not setup connection to gdcapi for {}. Detail {}".format(fi["id"]), e)

    if tries == NUM_TRIES:
        raise APIError("Can not setup connection to gdcapi for {}".format(fi["id"]))

    if response.status_code != 200:
        raise APIError("GDCPotal error {}".format(response.status_code))

    try:
        streaming(
            client,
            response,
            bucket_name,
            chunk_size_download,
            chunk_size_upload,
            blob_name,
        )
    except Exception:
        raise APIError(
            "GCSObjectStreamUpload: Can not upload {}".format(fi.get("id", ""))
        )


def streaming(
    client, response, bucket_name, chunk_size_download, chunk_size_upload, blob_name
):
    """
    stream the file with google resumable upload
    Args:
        client(GSClient): gs storage client
        response(httpResponse): http response
        bucket_name(str): target google bucket name
        chunk_size_download(int): chunk size in bytes from downling data file
        blob_name(str): object name
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

        for chunk in response.iter_content(chunk_size=chunk_size_download):
            if chunk:  # filter out keep-alive new chunks
                s.write(chunk)
