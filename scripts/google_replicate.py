import os
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
import Queue
import threading

import google
from google.cloud import storage
from google.cloud.storage import Blob
from google_resumable_upload import GCSObjectStreamUpload

from cdislogging import get_logger
from settings import PROJECT_MAP
from utils import (extract_md5_from_text,
                   get_bucket_name,
                   get_fileinfo_list_from_manifest)

DATA_ENDPT = 'https://api.gdc.cancer.gov/data/'



global exitFlag
exitFlag = 0

global totalDownloadedBytes
totalDownloadedBytes = 0

global aliveThreads
aliveThreads = 0

logger = get_logger("ReplicationThread")

semaphoreLock = threading.Lock()
workQueue = Queue.Queue()

def check_bucket(bucket_name):
    """
    check if bucket_name is existed or not !
    """

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.exists()

def check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
    """
    require that bucket_name already existed
    check if blob object is existed or not
    Args:
        bucket_name(str): the name of bucket
        blob_name(str): the name of blob
        fi(dict): file info dictionary
    Returns:
        bool: indicating value if the blob is exist or not
    """

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = Blob(blob_name, bucket)
    if blob.exists():
        execstr = 'gsutil hash -h gs://{}/{}'.format(bucket_name, blob_name)
        result = subprocess.check_output(execstr, shell=True).strip('\n"\'')
        md5_hash = extract_md5_from_text(result)
        if md5_hash:
	    logger.info("hash {}".format(md5_hash))
            return md5_hash == fi.get('hash', '').lower()
    return False

def exec_google_copy(threadName, fi, global_config):
    """
    copy a file to google bucket.
    Args:
        threadName(str): thread name
        fi(dict): a dictionary of a copying file
        global_config(dict): a configuration
    Returns:
        None
    """

    client = storage.Client()
    blob_name = fi.get('fileid') + '/' + fi.get('filename')
    bucket_name = get_bucket_name(fi, PROJECT_MAP)

    if not check_bucket(bucket_name):
        logger.info("There is no bucket with provided name {}".format(bucket_name))
        return

    if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
	logger.info("object {} is already existed. Not need to re-copy".format(blob_name))
        return

    resumable_streaming_copy(threadName, fi, client, bucket_name, blob_name, global_config)
    # check if object is copied correctly by comparing md5 hash
    if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
        logger.info("object {} successfully copied ".format(blob_name))
    else:
        # try to re-copy the file
	resumable_streaming_copy(threadName, fi, client, bucket_name, blob_name, global_config)
	if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
	    logger.info("object {} successfully copied ".format(blob_name))
	else:
            # log the failure case
	    logger.info("can not copy {} to GOOGLE bucket".format(blob_name))


def resumable_streaming_copy(threadName, fi, client, bucket_name, blob_name, global_config):
    """
    Copy file to google bucket. Implemented using google cloud resumale API
    Args:
        threadName(str): name of the thread
        fi(dict): file information
        global_config(dict): configurations
    Return: None
    """

    start = timeit.default_timer()
    chunk_size = global_config.get('chunk_size', 2048000)
    data_endpt = DATA_ENDPT + fi.get('fileid', "")
    token = ""

    try:
        with open(global_config.get('token_path', ''), 'r') as f:
            token = str(f.read().strip())
    except IOError as e:
        logger.info("Can not find token file!!!")

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
    streaming(threadName, client, response, bucket_name, chunk_size, blob_name)

def streaming(threadName, client, response, bucket_name, chunk_size, blob_name):
    """
    stream the file with google resumable upload
    Args:
        threadName(str): thread name
        client(S3Client): S3 storage client
        response(httpResponse): http response
        bucket_name(str): target google bucket name
        chunk_size(int): chunk size in bytes
        blob_name(str): object name
    Returns:
        None
    """

    # keep tract the number of chunks uploaded
    num = 0
    with GCSObjectStreamUpload(client=client, bucket_name=bucket_name, blob_name=blob_name) as s:
        for chunk in response.iter_content(chunk_size=chunk_size):
            num = num + 1
            # just for logging
            if num % 100 == 0:
                logger.info("%s download %f GB\n" %
                            (threadName, num*1.0*chunk_size/1000/1000/1000))
                global totalDownloadedBytes
                semaphoreLock.acquire()
                totalDownloadedBytes = totalDownloadedBytes + 100*chunk_size
                semaphoreLock.release()
            if chunk:  # filter out keep-alive new chunks
                s.write(chunk)


def process_data(threadName, global_config, q):
    while not exitFlag:
        semaphoreLock.acquire()
        if not q.empty():
            fi = q.get()
            semaphoreLock.release()
            logger.info("%s processing %s" % (threadName, fi))
            exec_google_copy(threadName, fi, global_config)
        else:
            semaphoreLock.release()


class singleThread(threading.Thread):
    def __init__(self, threadID, threadName, global_config, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.global_config = global_config
        self.q = q

    def run(self):
        logger.info("Starting " + self.name)
        global aliveThreads

        semaphoreLock.acquire()
        aliveThreads = aliveThreads + 1
        semaphoreLock.release()
        process_data(self.threadName, self.global_config, self.q)
        logger.info("\nExiting " + self.name)

        semaphoreLock.acquire()
        aliveThreads = aliveThreads - 1
        semaphoreLock.release()


class GOOGLEBucketReplication(object):
    def __init__(self, global_config, manifest_file, thread_num):
        self.thread_list = []
        self.manifest_file = manifest_file
        self.global_config = global_config
        self.thread_list = []
        self.thread_num = thread_num
        for i in xrange(0, self.thread_num):
            thread = singleThread(str(i), 'thread_{}'.format(
                i), self.global_config, workQueue)
            self.thread_list.append(thread)

    def prepare(self):
        """
        get list of file info from manifest and put them to the Queue
        """
      	# un-comment this line for generating testing data.
        # This test data contains real uuids and hashes and can be used for replicating
        # GDC data to google bucket
        # from intergration_data_test import google_gen_test_data
        # submitting_files = google_gen_test_data()
	submitting_files = get_fileinfo_list_from_manifest(self.manifest_file)
        for th in self.thread_list:
            th.start()
        semaphoreLock.acquire()
        for fi in submitting_files:
            workQueue.put(fi)
        semaphoreLock.release()

    def run(self):

        global aliveThreads
        runningThreads = aliveThreads

        start = timeit.default_timer()
        # Wait for queue to empty
        while not workQueue.empty():
            current = timeit.default_timer()
            running_time = current - start
            if running_time % 10 == 0:
                logger.info("Total data transfered %f GB" %
                            (totalDownloadedBytes*1.0/1000/1000/1000))
                logger.info("Times in second {}".format(current - start))

        # Notify threads it's time to exit
        global exitFlag
        exitFlag = 1

        # Wait for all threads to complete
        for t in self.thread_list:
            t.join()
        logger.info("Done")

