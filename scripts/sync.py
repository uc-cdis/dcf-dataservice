import os
from os import listdir
from os.path import isfile, join
import requests
import sys
import subprocess
import getopt
import argparse
import Queue
import threading
import time
import timeit

from cdislogging import get_logger

from google_resumable_upload import GCSObjectStreamUpload

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

def get_file_from_uuid(uuid):
    '''
    get document from indexd with provided uuid
    '''
    doc = None
    if uuid:
        doc = indexclient.get(uuid)
    return doc

def gen_mock_manifest_data():
    fake = {
            'did':'11111111111111111',
            'filename': 'abc.bam',
            'size': 1,
            'acl': '1223344543t34mt43tb43ofh'}

    l = [fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake]
    return l

def get_fileinfo_list_from_manifest(manifest_file):
    """
    get list of dictionaries from manifest file.
    [
        {
            'did':'11111111111111111',
            'filename': 'abc.bam',
            'size': 1,
            'hash': '1223344543t34mt43tb43ofh',
            'acl': 'abcxyz',
            'project': 'TCGA'
        },
    ]
    """
    l = []
    try:
        with open(manifest_file) as f:
            read(f)
    except IOError as e:
        logger.info("File {} is not existed".format(manifest_file))
    return l

def call_aws_copy(threadName, fi, data_source):
    execstr = "aws s3 cp s3://{} s3://{} --recursive --exclude \"*\"".format(data_source.get('from_source',''), data_source.get('to_bucket',''))
    execstr = execstr + " --include \"{}/{}\"".format(fi.get("did"), fi.get("filename"))
    if MODE != 'test':
        os.system(execstr)
    print("{} running the job on {}".format(threadName, fi.get("did")))
    print(execstr)

def exec_aws_cmd(cmd):
    os.system(cmd)

#for testing purpose only
def gen_aws_cmd():
    l = []
    for i in xrange(1,20):
        cmd = "aws s3 cp s3://mybucket20018/gentoo_root{}.img s3://xssxs ".format(i)
        l.append(cmd)
    return l

def check_hash_bucket_object(fi, data_source):
    bucket = data_source.get('to_bucket','')
    data = fileid
    batcmd = 'gsutil hash -h {}'.format(bucket)
    result = subprocess.check_output(batcmd, shell=True)

def get_bucket_name(fi):

    bucketname = ''
    if fi.get('acl','') == "*":
        bucketname = fi.get('project','') + "_public"
    else:
        bucketname = fi.get('project','') + "_protected"

    return bucketname

def create_bucket(bucketname):
    client = storage.Client()
    bucket = client.bucket(bucketname)
    if not bucket.exists():
        try:
            bucket = client.create_bucket(bucketname)
        except Exception as e:
            logger.info(e)
            return False
    return True

def exec_google_copy(threadName, fi, data_source):
    data_endpt = DATA_ENDPT + fi.get('fileid',"")
    token = ""
    try:
        with open(data_source.get('token_path','')) as reader:
            token = reader.read()
    except IOError as e:
        logger.info("Can not find token file!!!")
    response = requests.get(data_endpt,
                         stream=True,
                         headers = {
                             "Content-Type": "application/json",
                             "X-Auth-Token": token
                             })
    if response.status_code != 200:
        logger.info('==========================\n')
        logger.info('status code {} when downloading {} from GDC API'.format(response.status_code, fi.get('fileid',"")))
        return
    client = storage.Client()
    #bucket = client.get(to_bucket)
    project = get_project_name(fi)
    blob_name = project + "/" + fi.get('fileid') + '/' + fi.get('filename')
    bucket_name = get_bucket_name(fi)
    isCreated = create_bucket(bucket_name)

    if not isCreated:
        logger.info("There is no bucket with provided name")
        return

    num = 0
    start = timeit.default_timer()
    CHUNK_SIZE = data_source.get('chunk_size',2048000)
    with GCSObjectStreamUpload(client=client, bucket_name=bucket_name, blob_name=blob_name) as s:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            num = num + 1
            if num % 10 == 0:
                logger.info("%s download %f GB\n" %(threadName, num*1.0*CHUNK_SIZE/1000/1000/1000))
                global totalDowloadedBytes
                semaphoreLock.acquire()
                totalDowloadedBytes = totalDowloadedBytes + 100*CHUNK_SIZE
                semaphoreLock.release()
            if num % 10000 == 0:
                break
            if chunk: # filter out keep-alive new chunks
                s.write(chunk)

def process_data(threadName, data_source, q, service):
    while not exitFlag:
        semaphoreLock.acquire()
        if not q.empty():
            fi = q.get()
            semaphoreLock.release()
            logger.info("%s processing %s" % (threadName, fi))
            if service == 'aws':
                call_aws_copy(threadName, fi, data_source)
            elif service == 'google':
                exec_google_copy(threadName, fi, data_source)
            else:
                logger.info("not supported!!!")
        else:
            semaphoreLock.release()

class singleThread(threading.Thread):
    def __init__(self, threadID, threadName, data_source, q, vendor):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.data_source = data_source
        self.q = q
        self.vendor = vendor

    def run(self):
        logger.info("Starting " + self.name)
        global aliveThreads
        semaphoreLock.acquire()
        aliveThreads = aliveThreads + 1
        semaphoreLock.release()
        process_data(self.threadName, self.data_source, self.q, self.vendor)
        logger.info("\nExiting " + self.name)

        semaphoreLock.acquire()
        aliveThreads = aliveThreads - 1
        semaphoreLock.release()

class BucketReplication(object):
    def __init__(self, data_source, manifest_file, thread_num, service):
        self.thread_list = []
        self.manifest_file = manifest_file
        self.data_source = data_source
        self.service = service
        self.thread_list = []
        self.thread_num = thread_num
        for i in xrange(0,self.thread_num):
            thread = singleThread(str(i), 'thread_{}'.format(i), self.data_source, workQueue, self.service)
            self.thread_list.append(thread)

    def prepare(self):
        """
        concurently process a set of data files.
        """
        #ubmitting_files = get_fileinfo_list_from_manifest(self.manifest_file)
        submitting_files = gen_mock_manifest_data()
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
                logger.info("Total data transfered %f GB" %(totalDownloadedBytes*1.0/1000/1000/1000))
                logger.info("Times in second {}".format(current - start))

        # Notify threads it's time to exit
        global exitFlag
        exitFlag = 1

        # Wait for all threads to complete
        for t in self.thread_list:
            t.join()
        logger.info("Done")

class AWSBucketReplication(BucketReplication):

    def __init__(self, data_source, manifest_file, thread_num):
        super(AWSBucketReplication,self).__init__(data_source,  manifest_file, thread_num, 'aws')

class GOOGLEBucketReplication(BucketReplication):

    def __init__(self, data_source, manifest_file, thread_num):
        super(GOOGLEBucketReplication,self).__init__(data_source, manifest_file, thread_num, 'google')

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
    #aws = AWSBucketReplication({'from_bucket': 'from','to_bucket': 'to'},'test',10)
    #aws.prepare()
    #aws.run()
    #end = timeit.default_timer()
    google = GOOGLEBucketReplication({'token_path': '~/token','chunk_size':2048000},'test',10)
    google.prepare()
    google.run()
    #if args.action == 'sync':
    #    print "sync from gdc aws bucket to gen3 dcf bucket"
