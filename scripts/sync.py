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

from cdislogging import get_logger

from indexclient.client import IndexClient
from dev_settings import SIGNPOST

THREAD_NUM = 4

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

def call_aws_copy(fi, data_source, to_bucket):
    execstr = "aws s3 cp s3://{} s3://{} --recursive --exclude \"*\"".format(data_source, to_bucket)
    execstr = execstr + " --include \"{}/{}\"".format(fi.get("did"), fi.get("filename"))
    os.system(execstr)
    print("running the job on {}".format(fi.get("did")))
    print(execstr)

def exec_aws_cmd(cmd):
    os.system(cmd)

def gen_aws_cmd():
    l = []
    for i in xrange(1,20):
        cmd = "aws s3 cp s3://mybucket20018/gentoo_root{}.img s3://xssxs ".format(i)
        l.append(cmd)
    return l

def exec_google_copy(fi,data_source, to_bucket):
    pass

def process_data(threadName, data_source, to_bucket, q, service):

    while not exitFlag:

        print(" run thread here ")
        queueLock.acquire()
        print(" get lock")
        if not q.empty():
            fi = q.get()
            queueLock.release()
            print "%s processing %s" % (threadName, fi)
            #call_aws_copy(fi, data_source, to_bucket
            if service == 'aws':
                exec_aws_cmd(fi)
            elif service == 'google':
                exec_google_copy(fi, data_source, to_bucket)
            else:
                print "not supported!!!"
        else:
            queueLock.release()

class singleThread(threading.Thread):
    def __init__(self, threadID, threadName, data_source, to_bucket, q, vendor):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.data_source = data_source
        self.to_bucket = to_bucket
        self.q = q
        self.vendor = vendor

    def run(self):
        print "Starting " + self.name
        process_data(self.threadName, self.data_source, self.to_bucket, self.q, self.vendor)
        print "\nExiting " + self.name

class BucketReplication(object):
    def __init__(self, data_source, to_bucket, manifest_file, thread_num, service):
        self.thread_list = []
        self.manifest_file = manifest_file
        self.data_source = data_source
        self.to_bucket = to_bucket
        self.service = service
        self.thread_list = []
        self.thread_num = thread_num
        for i in xrange(0,self.thread_num):
            thread = singleThread(str(i), 'thread_{}'.format(i), self.data_source, self.to_bucket, workQueue, self.service)
            self.thread_list.append(thread)

    def prepare(self):
        """
        concurently process a set of data files.
        """
        #submitting_files = get_fileinfo_list_from_manifest(self.manifest_file)
        submitting_files = gen_aws_cmd()
        for th in self.thread_list:
            th.start()
        queueLock.acquire()
        for fi in submitting_files:
            workQueue.put(fi)
        queueLock.release()

    def run(self):
        print("run ...")
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

class AWSBucketReplication(BucketReplication):

    def __init__(self, data_source, to_bucket, manifest_file, thread_num):
        super(AWSBucketReplication,self).__init__(data_source, to_bucket, manifest_file,thread_num, 'aws')

class GOOGLEBucketReplication(BucketReplication):

    def __init__(self, data_source, to_bucket, manifest_file, thread_num):
        super(AWSBucketReplication,self).__init__(data_source, to_bucket, manifest_file,thread_num,'google')

def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='action', dest='action')

    create_data = subparsers.add_parser('sync')
    create_data.add_argument('--data_source', required=True)
    create_data.add_argument('--to_bucket', required=True)
    create_data.add_argument('--manifest_file', required=True)
    return parser.parse_args()

if __name__ == "__main__":
    start = timeit.default_timer()
    #args = parse_arguments()
    aws = AWSBucketReplication('from','to','test',10)
    aws.prepare()
    aws.run()
    end = timeit.default_timer()
    print end-start
    #if args.action == 'sync':
    #    print "sync from gdc aws bucket to gen3 dcf bucket"
