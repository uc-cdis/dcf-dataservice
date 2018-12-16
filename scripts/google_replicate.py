import base64
import requests
import urllib

from google.cloud import storage
from google.cloud.storage import Blob
from google_resumable_upload import GCSObjectStreamUpload
from google.auth.transport.requests import AuthorizedSession

import logging as logger
from indexclient.client import IndexClient

from errors import APIError
from settings import PROJECT_MAP, INDEXD, GDC_TOKEN
from utils import get_bucket_name

indexclient = IndexClient(
    INDEXD["host"],
    INDEXD["version"],
    (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
)

DATA_ENDPT = "https://api.gdc.cancer.gov/data/"
DEFAULT_CHUNK_SIZE_DOWNLOAD = 2048000
DEFAULT_CHUNK_SIZE_UPLOAD = 1024 * 20 * 1024


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
        and res.json["size"] == fi.get("size")
        and base64.b64decode(res.json()["md5Hash"]).encode("hex") == fi.get("md5")
    )


def update_indexd(fi):
    """
    update a record to indexd
    Args:
        fi(dict): file info
    Returns:
         None
    """
    gs_bucket_name = get_bucket_name(fi, PROJECT_MAP)
    gs_object_name = "{}/{}".format(fi.get("id"), fi.get("filename"))

    try:
        doc = indexclient.get(fi.get("id", ""))
        if doc is not None:
            url = "gs://{}/{}".format(gs_bucket_name, gs_object_name)
            if url not in doc.urls:
                doc.urls.append(url)
                doc.patch()
            return
    except Exception as e:
        raise APIError(
            "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                fi.get("id", ""), e.message
            )
        )

    urls = ["https://api.gdc.cancer.gov/data/{}".format(fi["fileid"])]

    if blob_exists(gs_bucket_name, gs_object_name):
        urls.append("gs://{}/{}".format(gs_bucket_name, gs_object_name))

    try:
        doc = indexclient.create(
            did=fi.get("id", ""),
            hashes={"md5": fi.get("md5", "")},
            size=fi.get("size", 0),
            urls=urls,
        )
        if doc is None:
            raise APIError(
                "INDEX_CLIENT: Fail to create a record with uuid {}".format(
                    fi.get("id", "")
                )
            )
    except Exception as e:
        raise APIError(
            "INDEX_CLIENT: Can not create the record with uuid {}. Detail {}".format(
                fi.get("id", ""), e.message
            )
        )


def exec_google_copy(fi, global_config):
    """
    copy a file to google bucket.
    Args:
        fi(dict): a dictionary of a copying file
        global_config(dict): a configuration
    Returns:
        DataFlowLog
    """
    client = storage.Client()
    sess = AuthorizedSession(client._credentials)
    blob_name = fi.get("id") + "/" + fi.get("filename")
    bucket_name = get_bucket_name(fi, PROJECT_MAP)

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

    if check_blob_name_exists_and_match_md5_size(sess, bucket_name, blob_name, fi):
        try:
            update_indexd(fi)
        except APIError as e:
            logger.error(e.message)
            return DataFlowLog(copy_success=True, message=e.message)
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

    response = requests.get(
        data_endpt,
        stream=True,
        headers={"Content-Type": "application/json", "X-Auth-Token": token},
    )

    if response.status_code != 200:
        raise APIError("GDCPotal: {}".format(response.text))

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
