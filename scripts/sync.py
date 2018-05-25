import os
from os import listdir
from os.path import isfile, join
import requests
import sys
import copy
import re
import subprocess
import getopt
import argparse
import Queue
import threading
import time
import timeit
import google
from cdislogging import get_logger
from google.cloud import storage
from google.cloud.storage import Blob
from google_resumable_upload import GCSObjectStreamUpload
from settings import PROJECT_MAP

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

def gen_test_data():

    fake = {
            'fileid':'989e6e5e-809e-4fb3-b5c6-6b03962e9798',
            'filename': 'abc1.bam',
            'size': 1,
            'hash': '1223344543t34mt43tb43ofh',
	    'project': 'TCGA',
	    'acl': '*'}
    fake2 = {
             'fileid':'b0fcabc2-7516-4a98-81e6-c32726e9b835',
             'filename': 'abc2.bam',
             'size': 1,
             'hash': '1223344543t34mt43tb43ofh',
	     'project': 'TCGA',
             'acl': 'tgca'}

    fake3 = {
             'fileid':'0210e2f4-d040-4fa5-ba6d-4195aa9cd0cf',
             'filename': 'abc3.bam',
             'size': 1,
             'hash': '1223344543t34mt43tb43ofh',
	     'project': 'TCGA',
             'acl': 'tgca'}

    fake4 = {
             'fileid':'d63771d1-7fec-4c0d-97b5-2f1534e31372',
             'filename': 'abc4.bam',
             'size': 1,
             'hash': '1223344543t34mt43tb43ofh',
	     'project': 'TCGA',
             'acl': 'tgca'}

    fake5 = {
             'fileid':'35cd71f3-64bc-41d6-ab8b-342c8c99dadc',
             'filename': 'abc5.bam',
             'size': 1,
             'hash': '1223344543t34mt43tb43ofh',
	     'project': 'TCGA',
             'acl': '*'}
    l = [fake,fake2,fake3,fake4,fake5]
    for i in xrange(6,21):
        tmp = copy.deepcopy(l[i%5])
        tmp['filename'] = 'abc{}.bam'.format(i)
        l.append(tmp)
    return l

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
            'acl': 'tcga',
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

def call_aws_copy(threadName, fi, global_config):
    execstr = "aws s3 cp s3://{} s3://{} --recursive --exclude \"*\"".format(global_config.get('from_source',''), global_config.get('to_bucket',''))
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

def check_hash_bucket_object(fi, global_config):
    bucket = global_config.get('to_bucket','')
    data = fileid
    batcmd = 'gsutil hash -h {}'.format(bucket)
    result = subprocess.check_output(batcmd, shell=True)


def get_bucket_name(fi):
    """
    TCGA-open -> gdc-tcga-phs000178-open
    TCGA-controled -> gdc-tcga-phs000178-controled
    TARGET-controled -> gdc-target-phs000218-controlled
    """
    bucketname = ''
    project = fi.get('project','')
    if fi.get('acl','') == "*":
        bucketname = 'gdc-' + PROJECT_MAP.get(project,'') + "-open"
    else:
        bucketname = 'gdc-' + PROJECT_MAP.get(project,'') + "-controlled"

    return bucketname

def check_bucket_is_exists(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.exists()

def extract_md5_from_text(text):
    m = re.search('[0-9,a-f]{32}', text.lower())
    if m:
        return m.group(0)
    else:
        return None

def check_blob_name_exists_and_match_md5(bucket_name,blob_name,fi):
    """
    require that bucket_name already existed
    check if blob object is existed or not

    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = Blob(blob_name, bucket)
    if blob.exists():
        execstr = 'gsutil hash -h gs://{}/{}'.format(bucket_name, blob_name)
        result = subprocess.check_output(execstr, shell=True).strip('\n"\'')
        md5_hash = extract_md5_from_text(result)
        if md5_hash:
            return md5_hash == fi.get('hash','')
    return False

def exec_google_copy(threadName, fi, global_config):
    """
    Copy file to google bucket
    Args:
        threadName(str): name of the thread
        fi(dict): file information

    """

    data_endpt = DATA_ENDPT + fi.get('fileid',"")
    token = ""
    try:
        with open(global_config.get('token_path',''), 'r') as f:
            token = str(f.read().strip())
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
    
    blob_name = fi.get('fileid') + '/' + fi.get('filename')
    bucket_name = get_bucket_name(fi)

    if not check_bucket_is_exists(bucket_name):
        logger.info("There is no bucket with provided name")
        return

    if check_blob_name_exists_and_match_md5(bucket_name, blob_name, fi):
        return

    num = 0
    start = timeit.default_timer()
    chunk_size = global_config.get('chunk_size',2048000)
    with GCSObjectStreamUpload(client=client, bucket_name=bucket_name, blob_name=blob_name) as s:
        for chunk in response.iter_content(chunk_size=chunk_size):
            num = num + 1
            if num % 10 == 0:
                logger.info("%s download %f GB\n" %(threadName, num*1.0*chunk_size/1000/1000/1000))
                global totalDownloadedBytes
                semaphoreLock.acquire()
                totalDownloadedBytes = totalDownloadedBytes + 100*chunk_size
                semaphoreLock.release()
            if num % 10000 == 0:
                break
            if chunk: # filter out keep-alive new chunks
                s.write(chunk)

def process_data(threadName, global_config, q, service):
    while not exitFlag:
        semaphoreLock.acquire()
        if not q.empty():
            fi = q.get()
            semaphoreLock.release()
            logger.info("%s processing %s" % (threadName, fi))
            if service == 'aws':
                call_aws_copy(threadName, fi, global_config)
            elif service == 'google':
                exec_google_copy(threadName, fi, global_config)
            else:
                logger.info("not supported!!!")
        else:
            semaphoreLock.release()

class singleThread(threading.Thread):
    def __init__(self, threadID, threadName, global_config, q, vendor):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.global_config = global_config
        self.q = q
        self.vendor = vendor

    def run(self):
        logger.info("Starting " + self.name)
        global aliveThreads
        semaphoreLock.acquire()
        aliveThreads = aliveThreads + 1
        semaphoreLock.release()
        process_data(self.threadName, self.global_config, self.q, self.vendor)
        logger.info("\nExiting " + self.name)

        semaphoreLock.acquire()
        aliveThreads = aliveThreads - 1
        semaphoreLock.release()

class BucketReplication(object):
    def __init__(self, global_config, manifest_file, thread_num, service):
        self.thread_list = []
        self.manifest_file = manifest_file
        self.global_config = global_config
        self.service = service
        self.thread_list = []
        self.thread_num = thread_num
        for i in xrange(0,self.thread_num):
            thread = singleThread(str(i), 'thread_{}'.format(i), self.global_config, workQueue, self.service)
            self.thread_list.append(thread)

    def prepare(self):
        """
        concurently process a set of data files.
        """
        #ubmitting_files = get_fileinfo_list_from_manifest(self.manifest_file)
        #submitting_files = gen_mock_manifest_data()i
	submitting_files = gen_test_data()
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

    def __init__(self, global_config, manifest_file, thread_num):
        super(AWSBucketReplication,self).__init__(global_config,  manifest_file, thread_num, 'aws')

class GOOGLEBucketReplication(BucketReplication):

    def __init__(self, global_config, manifest_file, thread_num):
        super(GOOGLEBucketReplication,self).__init__(global_config, manifest_file, thread_num, 'google')

def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='action', dest='action')

    create_data = subparsers.add_parser('sync')
    create_data.add_argument('--global_config', required=True)
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
    google = GOOGLEBucketReplication({'token_path': '/home/giangbui/gdc-token.txt','chunk_size':2048000},'test',1)
    google.prepare()
    google.run()
    #if args.action == 'sync':
    #    print "sync from gdc aws bucket to gen3 dcf bucket"
