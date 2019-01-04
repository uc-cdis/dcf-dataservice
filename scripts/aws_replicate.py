from multiprocessing.dummy import Pool as ThreadPool
import time
import os
import subprocess
import shlex

import threading

import json
import boto3

from cdislogging import get_logger
from indexclient.client import IndexClient

from settings import PROJECT_ACL, INDEXD
import utils
from utils import (
    get_fileinfo_list_from_csv_manifest,
    get_fileinfo_list_from_s3_manifest,
    exec_files_grouping,
)

from indexd_utils import update_url
from errors import UserError

logger = get_logger("AWSReplication")


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
        self.copied_objects = AWSBucketReplication.get_copied_objects()
        if self.bucket:
            self.source_objects = self.build_source_bucket_dataset()
        else:
            self.source_objects = []

        self.mutexLock = threading.Lock()
        self.total_processed_files = 0
        self.total_indexed_files = 0
        self.total_files = 0

    def prepare_data(self):
        """
        Read data file info from manifest and organize them into  groups.
        Each group contains files should be copied to same bucket
        The groups will be push to the queue
        """
        if self.manifest_file.startswith("s3://"):
            submitting_files = get_fileinfo_list_from_s3_manifest(self.manifest_file)
        else:
            submitting_files = get_fileinfo_list_from_csv_manifest(self.manifest_file)

        self.total_files = len(submitting_files)

        tasks = []
        file_grps = exec_files_grouping(submitting_files)

        for _, files in file_grps.iteritems():
            chunk_size = self.global_config.get("chunk_size", 1)
            idx = 0
            while idx < len(files):
                tasks.append(files[idx : idx + chunk_size])
                idx = idx + chunk_size

        return tasks, len(submitting_files)

    @staticmethod
    def get_copied_objects():
        """
        get all copied objects so that we don't have to re-copy them
        """
        s3 = boto3.resource("s3")
        existed_keys = set()

        for _, bucket_info in PROJECT_ACL.iteritems():
            for label in ["-open", "-controlled"]:
                bucket_name = bucket_info["aws_bucket_prefix"] + label
                bucket = s3.Bucket(bucket_name)
                try:
                    for file in bucket.objects.all():
                        existed_keys.add(file.key)
                except Exception as e:
                    raise Exception(
                        "Can not detect the bucket {}. Detail {}".format(bucket_name, e)
                    )

        return existed_keys

    def build_source_bucket_dataset(self):
        """
        build source bucket dataset for lookup
        to avoid list object operations
        """
        client = boto3.client("s3")

        try:
            paginator = client.get_paginator("list_objects")
            pages = paginator.paginate(Bucket=self.bucket, RequestPayer="requester")
        except Exception as e:
            raise UserError(
                "Can not detect the bucket {}. Detail {}".format(self.bucket, e)
            )
        dataset = set()
        for page in pages:
            for obj in page["Contents"]:
                dataset.add(obj["Key"])
        return dataset

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
        try:
            target_bucket = utils.get_aws_bucket_name(files[0], PROJECT_ACL)
        except UserError as e:
            # Detail error message from called function
            logger.error(e)
            return len(files)

        index = 0
        while index < len(files):
            base_cmd = 'aws s3 cp s3://{} s3://{} --request-payer requester --recursive --exclude "*"'.format(
                self.bucket, target_bucket
            )

            chunk_size = min(config_chunk_size, len(files) - index)
            execstr = base_cmd
            for fi in files[index : index + chunk_size]:
                object_name = "{}/{}".format(fi.get("id"), fi.get("file_name"))

                # only copy ones not exist in target bucket
                if object_name not in self.copied_objects:
                    if object_name in self.source_objects:
                        execstr += ' --include "{}"'.format(object_name)
                    elif fi.get("id") in self.source_objects:
                        # This is an old file, just single copy
                        cmd = "aws s3 cp s3://{}/{} s3://{}/{}".format(
                            self.bucket, fi.get("id"), target_bucket, object_name
                        )
                        # should wait for safety
                        subprocess.Popen(shlex.split(cmd + " --quiet")).wait()
                else:
                    pass
                    # logger.info("object is copied")

            if execstr != base_cmd:
                subprocess.Popen(shlex.split(execstr + " --quiet")).wait()

            index = index + chunk_size

        self.mutexLock.acquire()
        self.total_processed_files += len(files)
        logger.info(
            "{}/{} object are processed/copying ".format(
                self.total_processed_files, self.total_files
            )
        )
        self.mutexLock.release()

        return len(files)

    def check_and_index_the_data(self, files):
        """
        batch list all the objects and check if file in manifest is copied or not.
        Index data and log
        """
        json_log = {}
        for fi in files:
            object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))

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

        self.mutexLock.acquire()
        self.total_indexed_files += len(files)
        logger.info("{} object are processed/indexed ".format(self.total_indexed_files))
        self.mutexLock.release()

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

        timestr = time.strftime("%Y%m%d-%H%M%S")
        filename = timestr + "_" + filename

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
