from multiprocessing.dummy import Pool as ThreadPool
import os
import subprocess
import shlex

import threading

import json
import boto3

from cdislogging import get_logger
from indexclient.client import IndexClient

from settings import PROJECT_MAP, INDEXD
from utils import (
    get_bucket_name,
    get_fileinfo_list_from_manifest,
    get_fileinfo_list_from_s3_manifest,
    exec_files_grouping,
)

from indexd_utils import update_url

logger = get_logger("AWSReplication")

global TOTAL_PROCESSED_FILES
TOTAL_PROCESSED_FILES = 0

global TOTAL_INDEXED_FILES
TOTAL_INDEXED_FILES = 0

mutexLock = threading.Lock()


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
        self.manifest_file = manifest_file
        self.global_config = global_config
        self.job_name = job_name
        self.thread_num = thread_num
        self.indexclient = IndexClient(
            INDEXD["host"],
            INDEXD["version"],
            (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
        )
        self.copied_objects = self.get_copied_objects()
        self.source_objects = self.build_source_bucket_dataset()

    def prepare_data(self):
        """
        Read data file info from manifest and organize them into  groups.
        Each group contains files should be copied to same bucket
        The groups will be push to the queue
        """
        if self.manifest_file.startswith("s3://"):
            submitting_files, _ = get_fileinfo_list_from_s3_manifest(self.manifest_file)
        else:
            submitting_files, _ = get_fileinfo_list_from_manifest(self.manifest_file)

        tasks = []
        file_grps = exec_files_grouping(submitting_files)

        for _, files in file_grps.iteritems():
            chunk_size = self.global_config.get("chunk_size", 1)
            idx = 0
            while idx < len(files):
                tasks.append(files[idx : idx + chunk_size])
                idx = idx + chunk_size

        return tasks, len(submitting_files)

    def get_copied_objects(self):
        """
        get all copied objects so that we don't have to re-copy them
        """
        s3 = boto3.resource("s3")
        existed_keys = set()
        for _, value in PROJECT_MAP.iteritems():
            for label in ["-open", "-controlled"]:
                bucket_name = "gdc-" + value + label
                bucket = s3.Bucket(bucket_name)
                try:
                    for file in bucket.objects.all():
                        existed_keys.add(bucket_name + "/" + file.key)
                except Exception:
                    pass
        return existed_keys

    def build_source_bucket_dataset(self):
        """
        build source bucket dataset for lookup
        to avoid list object operations
        """
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket)
        return set([file.key for file in bucket.objects.all()])

    def exec_aws_copy(self, files):
        """
        Call AWS SLI to copy a chunk of  files from a bucket to another bucket.
        Intergrity check: After each chunk copy, check the returned md5 hashes
                            with the ones provided in manifest.
        If not match, re-copy. Log all the success and failure cases

        Args:
            files(list): a list of files which should be copied to the same bucket
        
        Returns:
            None
        """

        config_chunk_size = self.global_config.get("chunk_size", 1)
        target_bucket = get_bucket_name(files[0], PROJECT_MAP)

        index = 0
        while index < len(files):
            base_cmd = 'aws s3 cp s3://{} s3://{} --recursive --exclude "*"'.format(
                self.bucket, target_bucket
            )
            # Some files are stored as s3://bucket/uuid
            # cmd_for_old_files = 'aws s3 cp s3://{} s3://{}

            chunk_size = min(config_chunk_size, len(files) - index)

            execstr = base_cmd
            for fi in files[index : index + chunk_size]:
                object_name = "{}/{}".format(fi.get("id"), fi.get("filename"))

                # only copy ones not exist in target bucket
                if object_name not in self.copied_objects:
                    if object_name in self.source_objects:
                        execstr += ' --include "{}"'.format(object_name)
                    elif fi.get("id") in self.source_objects:
                        # This is an old file, just single copy
                        cmd = "aws s3 cp s3://{}/{} s3://{}/{}".format(
                            self.bucket, fi.get(id), target_bucket, object_name
                        )
                        # should wait for safety
                        subprocess.Popen(shlex.split(cmd + " --quiet")).wait()

            if execstr != base_cmd:
                subprocess.Popen(shlex.split(execstr + " --quiet")).wait()

            index = index + chunk_size

        global TOTAL_PROCESSED_FILES
        mutexLock.acquire()
        TOTAL_PROCESSED_FILES += len(files)
        logger.info("{} object are processed/copying ".format(TOTAL_PROCESSED_FILES))
        mutexLock.release()

        return len(files)

    def check_and_index_the_data(self, files):
        """
        batch list all the objects and check if file in manifest is copied or not.
        Index data and log
        """
        json_log = {}
        for fi in files:
            object_path = "{}/{}/{}".format(
                get_bucket_name(fi, PROJECT_MAP), fi.get("id"), fi.get("filename")
            )

            if object_path not in self.copied_objects:
                json_log[object_path] = {"copy_success": False, "index_success": False}
            else:
                try:
                    json_log[object_path] = {
                        "copy_success": True,
                        "index_success": update_url(fi, self.indexclient),
                    }
                except Exception as e:
                    logger.error(e.message)
                    json_log[object_path] = {
                        "copy_success": True,
                        "index_success": False,
                        "msg": e.message,
                    }

        global TOTAL_INDEXED_FILES
        mutexLock.acquire()
        TOTAL_INDEXED_FILES += len(files)
        logger.info("{} object are processed/indexed ".format(TOTAL_INDEXED_FILES))
        mutexLock.release()

        return json_log

    def run(self):
        """
        start threads and log after they finish
        """

        tasks, _ = self.prepare_data()
        # Make the Pool of workers
        pool = ThreadPool(self.thread_num)

        results = []
        if self.job_name == "copying":
            results = pool.map(self.exec_aws_copy, tasks)
        elif self.job_name == "indexing":
            results = pool.map(self.check_and_index_the_data, tasks)

        # close the pool and wait for the work to finish
        pool.close()
        pool.join()

        filename = self.global_config.get(
            "log_file", "{}_log.json".format(self.job_name)
        )

        if self.job_name == "copying":
            results = [{"data": results}]

        json_log = {}
        for result in results:
            json_log.update(result)

        s3 = boto3.client("s3")
        with open(filename, "w") as outfile:
            json.dump(json_log, outfile)
        s3.upload_file(
            filename, self.global_config.get("log_bucket"), os.path.basename(filename)
        )
