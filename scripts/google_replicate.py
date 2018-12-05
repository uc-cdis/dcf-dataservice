import subprocess
import requests

from google.cloud import storage
from google.cloud.storage import Blob
from google_resumable_upload import GCSObjectStreamUpload

from cdislogging import get_logger
from indexclient.client import IndexClient

from errors import APIError
from settings import PROJECT_MAP, INDEXD, GDC_TOKEN
from utils import extract_md5_from_text, get_bucket_name

logger = get_logger("GoogleReplication")

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


def check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
    """
    check if blob object exists or not
    Args:
        bucket_name(str): the name of bucket
        blob_name(str): the name of blob
        fi(dict): file info dictionary
    Returns:
        bool: indicating value if the blob is exist or not
    """
    if blob_exists(bucket_name, blob_name):
        execstr = "gsutil hash -h gs://{}/{}".format(bucket_name, blob_name)
        result = subprocess.check_output(execstr, shell=True).strip("\n\"'")
        md5_hash = extract_md5_from_text(result)
        if md5_hash:
            return md5_hash == fi.get("hash", "").lower()
    return False


def update_indexd(fi):
    """
    update a record to indexd
    Args:
        fi(dict): file info
    Returns:
         None
    """
    gs_bucket_name = get_bucket_name(fi, PROJECT_MAP)
    gs_object_name = "{}/{}".format(fi.get("fileid"), fi.get("filename"))
    doc = indexclient.get(fi.get("fileid", ""))

    if doc is not None:
        url = "gs://{}/{}".format(gs_bucket_name, gs_object_name)
        if url not in doc.urls:
            doc.urls.append(url)
            try:
                doc.patch()
            except Exception as e:
                raise APIError(
                    message="INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                        fi.get("fileid", ""), e.message
                    ),
                )
        return

    urls = ["https://api.gdc.cancer.gov/data/{}".format(fi["fileid"])]

    if blob_exists(gs_bucket_name, gs_object_name):
        urls.append("gs://{}/{}".format(gs_bucket_name, gs_object_name))

    try:
        doc = indexclient.create(
            did=fi.get("fileid", ""),
            hashes={"md5": fi.get("hash", "")},
            size=fi.get("size", 0),
            urls=urls,
        )
        if doc is None:
            raise APIError(
                message="INDEX_CLIENT: Fail to create a record with uuid {}".format(
                    fi.get("fileid", "")
                ),
            )
    except Exception as e:
        raise APIError(
            message="INDEX_CLIENT: Can not create the record with uuid {}. Detail {}".format(
                fi.get("fileid", ""), e.message
            ),
        )


def exec_google_copy(fi, global_config):
    """
    copy a file to google bucket.
    Args:
        fi(dict): a dictionary of a copying file
        global_config(dict): a configuration
    Returns:
        True/False
    """
    client = storage.Client()
    blob_name = fi.get("fileid") + "/" + fi.get("filename")
    bucket_name = get_bucket_name(fi, PROJECT_MAP)

    if not bucket_exists(bucket_name):
        msg = "There is no bucket with provided name {}".format(bucket_name)
        logger.error(msg)
        return DataFlowLog(message=msg)

    if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
        msg = "object {} already exists. Not need to re-copy".format(blob_name)
        return DataFlowLog(copy_success=True, index_success=True, message=msg)

    try:
        resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config)
    except APIError as e:
        logger.error(e.message)
        return DataFlowLog(message=e.message)

    # check if object is copied correctly by comparing md5 hash
    if not check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
        # try to re-copy the file
        resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config)
        if not check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
            msg = "can not copy {} to GOOGLE bucket".format(blob_name)
            logger.error(msg)
            return DataFlowLog(message=msg)

    try:
        update_indexd(fi)
    except APIError as e:
        logger.error(e.message)
        return DataFlowLog(copy_success=True, index_success=False, message=e.message)

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
    data_endpt = DATA_ENDPT + fi.get("fileid", "")
    token = GDC_TOKEN

    response = requests.get(
        data_endpt,
        stream=True,
        headers={"Content-Type": "application/json", "X-Auth-Token": token},
    )

    if response.status_code != 200:
        raise APIError(
            code=response.status_code, message="GDCPotal: {}".format(response.message)
        )

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
            message="GCSObjectStreamUpload: Can not upload {}".format(
                fi.get("fileid", "")
            ),
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
