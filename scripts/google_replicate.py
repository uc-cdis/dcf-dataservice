from os import listdir
from os.path import isfile, join
import sys
import time
import timeit
import copy
import argparse
import re
import subprocess
import getopt
import requests

import google
from google.cloud import storage
from google.cloud.storage import Blob
from google_resumable_upload import GCSObjectStreamUpload
import logging as logger

from indexclient.client import IndexClient
from settings import PROJECT_MAP, INDEXD, GDC_TOKEN
from utils import (extract_md5_from_text,
                   get_bucket_name)

indexclient = IndexClient(INDEXD['host'], INDEXD['version'], (INDEXD['auth']['username'], INDEXD['auth']['password']))

DATA_ENDPT = 'https://api.gdc.cancer.gov/data/'
DEFAULT_CHUNK_SIZE_DOWNLOAD = 2048000
DEFAULT_CHUNK_SIZE_UPLOAD = 1024*20*1024

FAIL = False
SUCCESS = True

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
        execstr = 'gsutil hash -h gs://{}/{}'.format(bucket_name, blob_name)
        result = subprocess.check_output(execstr, shell=True).strip('\n"\'')
        md5_hash = extract_md5_from_text(result)
        if md5_hash:
            return md5_hash == fi.get('hash', '').lower()
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
    doc = indexclient.get(fi.get('fileid',''))

    if doc is not None:
        if gs_object_name not in doc.urls:
            doc.urls.append("gs://{}/{}".format(gs_bucket_name, gs_object_name))
            doc.patch()
        return

    urls = ['https://api.gdc.cancer.gov/data/{}'.format(fi['fileid'])]

    if blob_exists(gs_bucket_name, gs_object_name):
        urls.append("gs://{}/{}".format(gs_bucket_name, gs_object_name))

    doc = indexclient.create(did=fi.get('fileid',''),
                             hashes=fi.get('hash',''),
                             size=fi.get('size',0),
                             urls=urls)
    if doc is None:
        logger.info("successfuly create an record with uuid {}".format(fi.get('fileid','')))
    else:
        logger.info("fail to create an record with uuid {}".format(fi.get('fileid','')))

def exec_google_copy(fi, global_config):
    """
    copy a file to google bucket.
    Args:
        fi(dict): a dictionary of a copying file
        global_config(dict): a configuration
    Returns:
        None
    """
    client = storage.Client()
    blob_name = fi.get('fileid') + '/' + fi.get('filename')
    bucket_name = get_bucket_name(fi, PROJECT_MAP)

    if not bucket_exists(bucket_name):
        logger.info("There is no bucket with provided name {}".format(bucket_name))
        return FAIL

    if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
	logger.info("object {} already exists. Not need to re-copy".format(blob_name))
        return SUCCESS

    resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config)
    # check if object is copied correctly by comparing md5 hash
    if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
        logger.info("object {} successfully copied ".format(blob_name))
    else:
        # try to re-copy the file
	resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config)
	if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
	    logger.info("object {} successfully copied ".format(blob_name))
	else:
            # log the failure case
	    logger.info("can not copy {} to GOOGLE bucket".format(blob_name))
            return FAIL
    try:
        update_indexd(fi)
    except Exception as e:
        logger.info(e)
    return SUCCESS

def resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config):
    """
    Copy file to google bucket. Implemented using google cloud resumale API
    Args:
        fi(dict): file information
        global_config(dict): configurations
    Return: None
    """

    start = timeit.default_timer()
    chunk_size_download = global_config.get('chunk_size_download', DEFAULT_CHUNK_SIZE_DOWNLOAD)
    chunk_size_upload = global_config.get('chunk_size_upload', DEFAULT_CHUNK_SIZE_UPLOAD)
    data_endpt = DATA_ENDPT + fi.get('fileid', "")
    token = GDC_TOKEN

    response = requests.get(data_endpt,
                            stream=True,
                            headers={
                                "Content-Type": "application/json",
                                "X-Auth-Token": token
                            })

    if response.status_code != 200:
        logger.info('==========================\n')
        logger.info('status code {} when downloading {} from GDC API'.format(
            response.status_code, fi.get('fileid', "")))
        return
    streaming(client, response, bucket_name, chunk_size_download, chunk_size_upload, blob_name)

def streaming(client, response, bucket_name, chunk_size_download, chunk_size_upload, blob_name):
    """
    stream the file with google resumable upload
    Args:
        client(GSClient): gs storage client
        response(httpResponse): http response
        bucket_name(str): target google bucket name
        chunk_size_download(int): chunk size in bytes from downling data file from GDC
        blob_name(str): object name
    Returns:
        None
    """

    # keep track the number of chunks uploaded
    num = 0
    with GCSObjectStreamUpload(client=client, bucket_name=bucket_name, blob_name=blob_name, chunk_size=chunk_size_upload) as s:
        for chunk in response.iter_content(chunk_size=chunk_size_download):
            num = num + 1
            if num % 100 == 0:
                logger.info("Download %f GB\n" %
                            (num*1.0*chunk_size_download/1000/1000/1000))
            if chunk:  # filter out keep-alive new chunks
                s.write(chunk)

