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
from utils import (check_bucket_is_exists,
                 extract_md5_from_text,
                 get_fileinfo_list_from_manifest,
                 get_bucket_name,
                 get_fileinfo_list_from_manifest)

DATA_ENDPT = 'https://api.gdc.cancer.gov/data/'
MODE = 'test'

global exitFlag
exitFlag = 0

global totalDownloadedBytes
totalDownloadedBytes = 0

global aliveThreads
aliveThreads = 0

logger = get_logger("ReplicationThread")

semaphoreLock = threading.Lock()
workQueue = Queue.Queue()

def check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
    """
    require that bucket_name already existed
    check if blob object is existed or not
    Args:
        bucket_name(str): the name of bucket
        blob_name(str): the name of blob
        fi(dict): file info dictionary
    Returns:
        bool value indicating if the blob is exist or not
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = Blob(blob_name, bucket)
    if blob.exists():
        execstr = 'gsutil hash -h gs://{}/{}'.format(bucket_name, blob_name)
        result = subprocess.check_output(execstr, shell=True).strip('\n"\'')
        md5_hash = extract_md5_from_text(result)
        if md5_hash:
            return md5_hash == fi.get('hash', '').lower()
    return False


def exec_google_copy(threadName, fi, global_config):
    """
    Copy file to google bucket. Implemented using google cloud resumale API
    Args:
        threadName(str): name of the thread
        fi(dict): file information
        global_config(dict): configurations
    Return: None

    """
    if MODE == 'test':
        return
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

    client = storage.Client()

    blob_name = fi.get('fileid') + '/' + fi.get('filename')
    bucket_name = get_bucket_name(fi, PROJECT_MAP)

    if not check_bucket_is_exists(bucket_name):
        logger.info("There is no bucket with provided name")
        return

    if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
        return

    num = 0
    start = timeit.default_timer()
    chunk_size = global_config.get('chunk_size', 2048000)
    with GCSObjectStreamUpload(client=client, bucket_name=bucket_name, blob_name=blob_name) as s:
        for chunk in response.iter_content(chunk_size=chunk_size):
            num = num + 1
            if num % 10 == 0:
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
                i), self.global_config, workQueue, self.service)
            self.thread_list.append(thread)

    def prepare(self):
        """
        concurently process a set of data files.
        """
        submitting_files = get_fileinfo_list_from_manifest(self.manifest_file)
        # submitting_files = gen_mock_manifest_data()i
        #submitting_files = gen_test_data()
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

