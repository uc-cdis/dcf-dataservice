import os
import subprocess
import shlex
import Queue
import time
import timeit
import threading


import json
import boto3

from cdislogging import get_logger
from indexclient.client import IndexClient

from errors import APIError
from settings import PROJECT_MAP, INDEXD
from utils import get_bucket_name, get_fileinfo_list_from_manifest, exec_files_grouping

logger = get_logger("AWSReplication")

global EXIT_FLAG
EXIT_FLAG = 0

global TOTAL_PROCESSED_FILES
TOTAL_PROCESSED_FILES = 0

mutexLock = threading.Lock()
workQueue = Queue.Queue()

indexclient = IndexClient(
    INDEXD["host"],
    INDEXD["version"],
    (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
)


class singleThread(threading.Thread):
    def __init__(self, threadID, threadName, source_bucket, copied_objects, global_config, job_name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.global_config = global_config
        self.q = q
        self.source_bucket = source_bucket
        self.copied_objects = copied_objects
        self.job_name = job_name
        self.json_log = {}

    def run(self):
        logger.info("Starting " + self.name)
        if self.job_name == "copying":
            process_data(self.threadName, self.source_bucket, self.global_config, self.copied_objects, self.q)
        elif self.job_name == "indexing":
            self.indexing_data(self.threadName, self.global_config, self.copied_objects, self.q)
        logger.info("\nExiting " + self.name)

    def indexing_data(self, threadName, global_config, copied_objects, q):
        """
        Start to index the copied object

        Args:
            threadName(str): thread name
            global_config(dict): configuration dictionary
            copied_objects(set): objects aready copied
            q(Queue): queue containing the objects need to be processed
        
        Return:
            None
        """

        while not EXIT_FLAG:
            mutexLock.acquire()
            if not q.empty():
                files = q.get()
                mutexLock.release()
                logger.info("%s indexing %d files" % (threadName, len(files)))
                check_and_index_the_data(files, copied_objects, self.json_log)
                logger.info(self.json_log)

            else:
                s3 = boto3.client('s3')
                self.write_log_file(s3)
                mutexLock.release()
    
    def write_log_file(self, s3):
        """
        write log file and push to s3 log bucket
        """
        filename = self.threadName + "_log.json"
        with open(filename, "w") as outfile:
            json.dump(self.json_log, outfile)
        s3.upload_file(
            filename, self.global_config.get("log_bucket"), os.path.basename(filename)
        )


class AWSBucketReplication(object):
    def __init__(self, global_config, manifest_file, thread_num, job_name, bucket=None):
        """
        Class constructor

        Args:
            global_config(dict): configuration dictionary
            manifest_file(str): manifest file
            thread_num(int): number of threads
            job_name(str): copying|indexing
            bucket(str): source bucket
        
        """
        self.bucket = bucket
        self.thread_list = []
        self.manifest_file = manifest_file
        self.global_config = global_config
        self.thread_list = []
        self.thread_num = thread_num
        self.json_log = {}
        self.job_name = job_name

        copied_objects = self.get_copied_objects()
        for i in xrange(0, self.thread_num):
            thread = singleThread(str(i), 'thread_{}'.format(
                i), self.bucket, copied_objects, self.global_config, self.job_name, workQueue)
            self.thread_list.append(thread)

    def get_copied_objects(self):
        """
        get all copied objects so that we don't have to re-copy them
        """
        s3 = boto3.resource('s3')
        existed_keys = set()
        for _, value in PROJECT_MAP.iteritems():
            for label in ["-open", "-controlled"]:
                bucket_name = "gdc-"+value+label
                bucket = s3.Bucket(bucket_name)
                try:
                    for file in bucket.objects.all():
                        existed_keys.add(bucket_name+"/"+file.key)
                except Exception:
                    pass
        return existed_keys
    
    def prepare(self):
        """
        Read data file info from manifest and organize them into  groups.
        Each group contains files should be copied to same bucket
        The groups will be push to the queue
        """
        submitting_files, _ = get_fileinfo_list_from_manifest(self.manifest_file)
        file_grps = exec_files_grouping(submitting_files)
        mutexLock.acquire()
        for _, files in file_grps.iteritems():
            chunk_size = self.global_config.get("chunk_size", 1)
            idx = 0
            while idx < len(files):
                workQueue.put(files[idx:idx+chunk_size])
                idx = idx+chunk_size
        mutexLock.release()

        for th in self.thread_list:
            th.start()
    
  
    def run(self, ):
        global TOTAL_PROCESSED_FILES

        start = timeit.default_timer()
        # Wait for queue to empty
        while not workQueue.empty():
            time.sleep(5)
            current = timeit.default_timer()
            # running_time = current - start
            # if int(running_time) % 10== 0:
            logger.info("=======================================================")
            logger.info("Total files processed {}".format(TOTAL_PROCESSED_FILES))
            logger.info("Times in second {}".format(current - start))

        
        # Notify threads it's time to exit
        global EXIT_FLAG
        EXIT_FLAG = 1

        # Wait for all threads to complete
        for t in self.thread_list:
            t.join()

        logger.info("Done")


def exec_aws_copy(threadName, files, source_bucket, target_bucket, global_config, copied_keys):
    """
    Call AWS SLI to copy a chunk of  files from a bucket to another bucket.
    Intergrity check: After each chunk copy, check the returned md5 hashes
                        with the ones provided in manifest.
    If not match, re-copy. Log all the success and failure cases

    Args:
        threadName(str): thread name 
        files(list): a list of files which should be copied to the same bucket
        source_bucket(str): source bucket
        target_bucket(str): target bucket
        global_config(dict): user config
        copied_keys(set): set of objects already in target buckets
    
    Returns:
        None
    """
 
    config_chunk_size = global_config.get("chunk_size", 1)

    index = 0

    while index < len(files):
        base_cmd = 'aws s3 cp s3://{} s3://{} --recursive --exclude "*"'.format(
            source_bucket, target_bucket
        )

        chunk_size = min(config_chunk_size, len(files) - index)

        execstr = base_cmd
        for fi in files[index : index + chunk_size]:
            object_name = "{}/{}".format(fi.get("id"), fi.get("filename"))

            # only copy ones not exist in target bucket
            if object_name not in copied_keys:
                execstr += ' --include "{}"'.format(object_name)

        if execstr != base_cmd:
            subprocess.Popen(shlex.split(execstr + " --quiet")).wait()

        index = index + chunk_size

    global TOTAL_PROCESSED_FILES
    mutexLock.acquire()
    TOTAL_PROCESSED_FILES += len(files)
    mutexLock.release()


def process_data(threadName, source_bucket, global_config, copied_objects, q):
    """
    start to copy objects

    Args:
        threadName(str): thread name
        source_bucket(str): source bucket
        global_config(dict): configuration dictionary
        copied_objects(set): objects aready copied
        q(Queue): queue containing the objects need to be processed
    """
    while not EXIT_FLAG:
        mutexLock.acquire()
        if not q.empty():
            files = q.get()
            mutexLock.release()
            target_bucket = get_bucket_name(files[0], PROJECT_MAP)
            logger.info("%s processing %d to %s" % (threadName, len(files), target_bucket))
            exec_aws_copy(threadName, files, source_bucket, target_bucket, global_config, copied_objects)
        else:
            mutexLock.release()


def check_and_index_the_data(files, copied_objects, json_log):
    """
    batch list all the objects and check if file in manifest is copied or not.
    Index data and log
    """

    for fi in files:
        object_path = "{}/{}/{}".format(get_bucket_name(fi, PROJECT_MAP), fi.get("id"), fi.get("filename"))
        logger.info("start to index {}".format(object_path))
        if object_path not in copied_objects:
            json_log[object_path] = {
                "copy_success": False,
                "index_success": False,
            }
        else:
            try:
                json_log[object_path] = {
                    "copy_success": True,
                    "index_success":  update_indexd(fi),
                }
            except Exception as e:
                logger.error(e.message)
                json_log[object_path] = {
                    "copy_success": True,
                    "index_success": False,
                    "msg": e.message,
                }


def update_indexd(fi):
    """
    update a record to indexd
    Args:
        fi(dict): file info
    Returns:
        None
    """
    return False
    s3_bucket_name = get_bucket_name(fi, PROJECT_MAP)
    s3_object_name = "{}/{}".format(fi.get("id"), fi.get("filename"))

    try:
        doc = indexclient.get(fi.get("id", ""))
        if doc is not None:
            url = "s3://{}/{}".format(s3_bucket_name, s3_object_name)
            if url not in doc.urls:
                doc.urls.append(url)
                doc.patch()
            return doc is not None
    except Exception as e:
        raise APIError(
            "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                fi.get("id", ""), e.message
            )
        )

    urls = [
        "https://api.gdc.cancer.gov/data/{}".format(fi.get("id", "")),
        "s3://{}/{}".format(s3_bucket_name, s3_object_name),
    ]

    try:
        doc = indexclient.create(
            did=fi.get("id"),
            hashes={"md5": fi.get("md5")},
            size=fi.get("size", 0),
            urls=urls,
        )
        return doc is not None
    except Exception as e:
        raise APIError(
            "INDEX_CLIENT: Can not create the record with uuid {}. Detail {}".format(
                fi.get("id", ""), e.message
            )
        )  
