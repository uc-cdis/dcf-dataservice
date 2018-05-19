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

from cdislogging import get_logger

from indexclient.client import IndexClient
from dev_settings import SIGNPOST

THREAD_NUM = 2

global exitFlag
exitFlag = 0

logger = get_logger("ReplicationThread")

indexclient = IndexClient(SIGNPOST['host'], SIGNPOST['version'], SIGNPOST['auth'])

queueLock = threading.Lock()
workQueue = Queue.Queue()

def get_file_from_uuid(uuid):
    '''
    get document from indexd with provided uuid
    '''
    doc = None
    if uuid:
        doc = indexclient.get(uuid)
    return doc

def create_index(**kwargs):
    return indexclient.create(**kwargs)

def get_fileinfo_list_from_manifest(manifest_file):
    """
    get list of dictionaries from manifest file.
    [
        {
            'did':'11111111111111111',
            'filename': 'abc.bam',
            'size': 1,
            'hash': '1223344543t34mt43tb43ofh',
            'metadata': 'abcxyz'
        },
    ]
    """
    fake = {
            'did':'11111111111111111',
            'filename': 'abc.bam',
            'size': 1,
            'hash': '1223344543t34mt43tb43ofh'}

    l = [fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake]

    #with open(manifest_file) as f:
    #    read(f)
    return l

def call_aws_copy(fi, from_bucket, to_bucket):
    #execstr = "aws s3 sync s3://{} s3://{} --exclude \"*\"".format(from_bucket, to_bucket)
    #execstr = execstr + " --include \"{}/{}\"".format(fi.get("did"), fi.get("filename"))
    #os.system(execstr)
    print("running the job on {}".format(fi.get("did")))
    time.sleep(2)

def process_data(threadName, from_bucket, to_bucket, q):
    while not exitFlag:
        queueLock.acquire()
        if not q.empty():
            fi = q.get()
            queueLock.release()
            print "%s processing %s" % (threadName, fi.get("did"))
            #call_aws_copy(fi, from_bucket, to_bucket)
        else:
            queueLock.release()
            #time.sleep(5)

class singleThread(threading.Thread):
    def __init__(self, threadID, threadName, from_bucket, to_bucket, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.from_bucket = from_bucket
        self.to_bucket = to_bucket
        self.q = q

    def run(self):
        print "Starting " + self.name
        process_data(self.threadName, self.from_bucket, self.to_bucket, self.q)
        print "Exiting " + self.name


class AWSBucketReplication(object):

    def __init__(self, from_bucket, to_bucket, manifest_file):
        self.from_bucket = from_bucket
        self.to_bucket = to_bucket
        self.manifest_file = manifest_file
        self.thread_list = []
        for i in xrange(0,THREAD_NUM):
            thread = singleThread(str(i), 'thread_{}'.format(i), self.from_bucket, self.to_bucket, workQueue)
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
    #args = parse_arguments()
    aws = AWSBucketReplication('from','to','test')
    aws.prepare()
    aws.run()
    #if args.action == 'sync':
    #    print "sync from gdc aws bucket to gen3 dcf bucket"
