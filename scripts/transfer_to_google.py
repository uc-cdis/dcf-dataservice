import os
from os import listdir
from os.path import isfile, join
import requests
import sys
import getopt
import argparse
import Queue
import threading
import time
import timeit

#from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests as google_req, common
from google.cloud import storage

from cdislogging import get_logger

from indexclient.client import IndexClient
from dev_settings import SIGNPOST

from google_upload2 import GCSObjectStreamUpload
THREAD_NUM = 4
DATA_ENDPT = 'https://api.gdc.cancer.gov/data/'
global exitFlag
exitFlag = 0

logger = get_logger("ReplicationThread")

indexclient = IndexClient(SIGNPOST['host'], SIGNPOST['version'], SIGNPOST['auth'])

queueLock = threading.Lock()
workQueue = Queue.Queue()

def get_fileinfo_list_from_manifest(manifest_file):
    """
    get list of dictionaries from manifest file.
    [
        {
            'did':'11443f3c-9b8b-4e47-b5b7-529468fec098',
            'filename': 'test1.bam',
            'size': 1,
            'hash': '1223344543t34mt43tb43ofh',
            'metadata': 'abcxyz'
        },
    ]
    """
    fake = {
            'fileid':'48a779c9-379f-46e0-85cc-db26b928d776',
            'filename': 'abc1.bam',
            'size': 1,
            'hash': '1223344543t34mt43tb43ofh'}
    fake2 = {
             'fileid':'22c96d28-894d-4c6c-9fab-39a6599b0451',
             'filename': 'abc2.bam',
             'size': 1,
             'hash': '1223344543t34mt43tb43ofh'}

    l = [fake, fake2]

    return l

def exec_google_copy(fi,to_bucket, token):
    data_endpt = DATA_ENDPT + fi.get('fileid',"")
    response = requests.get(data_endpt,
                         stream=True,
                         headers = {
                             "Content-Type": "application/json",
                             "X-Auth-Token": token
                             })
    if response.status_code != 200:
        print response.status_code
        return
    client = storage.Client()
    #bucket = client.get(to_bucket)
    blob_name = fi.get('fileid') + '/' + fi.get('filename')
    num = 0
    start = timeit.default_timer()
    with GCSObjectStreamUpload(client=client, bucket_name='cdistest', blob_name=blob_name) as s:
        for chunk in response.iter_content(chunk_size=102400):
            num = num + 1
            if num % 1000 == 0:
                break
            if chunk: # filter out keep-alive new chunks
                s.write(chunk)

def process_data(threadName, to_bucket, jobQueue, token):
    while not exitFlag:
        queueLock.acquire()
        if not jobQueue.empty():
            fi = jobQueue.get()
            queueLock.release()
            print "%s processing %s" % (threadName, fi)
            exec_google_copy(fi, to_bucket, token)
        else:
            queueLock.release()
            time.sleep(20)

class GoogleCopySingleThread(threading.Thread):
    def __init__(self, threadID, threadName, to_bucket, jobQueue):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.to_bucket = to_bucket
        self.jobQueue = jobQueue
        self.token = '' # fill here if want to test

    def run(self):
        print "Starting " + self.name
        process_data(self.threadName, self.to_bucket, self.jobQueue, self.token)
        print "\nExiting " + self.name


class GoogleBucketTransfer(object):

    def __init__(self, from_bucket, to_bucket, manifest_file):
        self.to_bucket = to_bucket
        self.manifest_file = manifest_file
        self.thread_list = []
        for i in xrange(0,THREAD_NUM):
            thread = GoogleCopySingleThread(str(i), 'thread_{}'.format(i), self.to_bucket, workQueue)
            self.thread_list.append(thread)

    def prepare(self):
        """
        concurently process a set of data files.
        """
        submitting_files = get_fileinfo_list_from_manifest(self.manifest_file)
        for th in self.thread_list:
            th.start()
        queueLock.acquire()
        for fi in submitting_files:
            workQueue.put(fi)
        queueLock.release()

    def run(self):

        # Wait for queue to empty
        while not workQueue.empty():
            pass

        # Notify threads it's time to exit
        global exitFlag
        exitFlag = 1

        # Wait for all threads to complete
        for t in self.thread_list:
            t.join()
        print "Done"

def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='action', dest='action')

    create_data = subparsers.add_parser('sync')
    create_data.add_argument('--from_bucket', required=True)
    create_data.add_argument('--to_bucket', required=True)
    create_data.add_argument('--manifest_file', required=True)
    return parser.parse_args()

if __name__ == "__main__":
    start = timeit.default_timer()
    #args = parse_arguments()
    aws = GoogleBucketTransfer('from','to','test')
    aws.prepare()
    aws.run()
    end = timeit.default_timer()
    print end-start
    #if args.action == 'sync':
    #    print "sync from gdc aws bucket to gen3 dcf bucket"
