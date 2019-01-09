from multiprocessing.dummy import Pool as ThreadPool
import time
import os
import subprocess
import shlex
import hashlib
import requests

import threading
from threading import Thread

import json
import boto3
import botocore
import timeit

from cdislogging import get_logger
from indexclient.client import IndexClient

from settings import PROJECT_ACL, INDEXD, GDC_TOKEN
import utils
from utils import (
    get_fileinfo_list_from_csv_manifest,
    get_fileinfo_list_from_s3_manifest,
    exec_files_grouping,
    get_storage_class,
    is_first_level_object,
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
        self.indexclient = (
            IndexClient(
                INDEXD["host"],
                INDEXD["version"],
                (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
            ),
        )
        self.s3 = boto3.client("s3")

        start = timeit.default_timer()

        if global_config.get("from_local"):
            self.copied_objects, self.source_objects = AWSBucketReplication.build_object_dataset_from_file(
                global_config.get("copied_objects"), global_config.get("source_objects")
            )
        else:
            self.copied_objects, self.source_objects = AWSBucketReplication.build_object_dataset(
                PROJECT_ACL, self.bucket
            )

        end = timeit.default_timer()

        logger.info("Time to build source object dataset: {}".format(end - start))

        self.mutexLock = threading.Lock()
        self.total_processed_files = 0
        self.total_indexed_files = 0
        self.total_files = 0

    def prepare_data(self):
        """
        Read data file info from manifest and organize them into groups.
        Each group contains files which should be copied to the same bucket
        The groups will be push to the queue consumed by threads
        """
        if self.manifest_file.startswith("s3://"):
            copying_files = get_fileinfo_list_from_s3_manifest(self.manifest_file)
        else:
            copying_files = get_fileinfo_list_from_csv_manifest(self.manifest_file)

        self.total_files = len(copying_files)

        tasks = []
        file_grps = exec_files_grouping(copying_files, self.source_objects, PROJECT_ACL)

        for _, files in file_grps.iteritems():
            chunk_size = self.global_config.get("chunk_size", 1)
            idx = 0
            while idx < len(files):
                tasks.append(files[idx : idx + chunk_size])
                idx = idx + chunk_size

        return tasks, len(copying_files)

    @staticmethod
    def build_object_dataset_from_file(copied_objects_file, source_objects_file):
        """
        Load copied objects and source objects in local files
        """
        with open(copied_objects_file, "r") as outfile:
            copied_objects = json.loads(outfile.read())

        with open(source_objects_file, "r") as outfile:
            source_objects = json.loads(outfile.read())

        return copied_objects, source_objects

    @staticmethod
    def build_object_dataset(project_acl, awsbucket):
        """
        Load copied objects and source objects. The copied objects are obtained by
        listing the target buckets (derived from project_acl). The source objects are
        obtained by listing the objects in source bucket (GDC bucket)
        
        Args:
            project_acl(dict): project access control lever. It contains target bucket infos:
                - project_id(str): map with project_id in manifest
                - bucket prefix name: to derive the target bucket name (ex gdc-tcga-open|controlled)
            awsbucket(str): the source bucket

        Returns:
            copied_objects(dict): contains copied objects already
            source_objects(dict): contains source objects
        """
        mutexLock = threading.Lock()
        copied_objects = {}
        source_objects = {}

        def list_objects(bucket_name, objects):
            """
            build object dataset for lookup with key is s3 object key and value contains
            storage class, size and md5
            to avoid list object operations
            """
            client = boto3.client("s3")
            result = {}

            paginator = client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_name, RequestPayer="requester")

            try:
                for page in pages:
                    for obj in page["Contents"]:
                        result[obj["Key"]] = {
                            "StorageClass": obj["StorageClass"],
                            "Size": obj["Size"],
                        }
            except KeyError:
                logger.info("There is no object in {}".format(bucket_name))
            except botocore.exceptions.ClientError as e:
                logger.error(
                    "Can not detect the bucket {}. Detail {}".format(bucket_name, e)
                )

            mutexLock.acquire()
            objects.update(result)
            mutexLock.release()

        threads = []
        target_bucket_names = set()
        for _, bucket_info in project_acl.iteritems():
            # bad hash code to support ccle bucket name
            if "ccle" in bucket_info["aws_bucket_prefix"]:
                target_bucket_names.add("ccle-open-access")
                target_bucket_names.add("gdc-ccle-controlled")
                continue
            for label in ["open", "controlled"]:
                target_bucket_names.add(bucket_info["aws_bucket_prefix"] + "-" + label)

        for target_bucket_name in target_bucket_names:
            threads.append(
                Thread(target=list_objects, args=(target_bucket_name, copied_objects))
            )

        threads.append(Thread(target=list_objects, args=(awsbucket, source_objects)))

        logger.info("Start threads to list aws objects")
        for th in threads:
            th.start()
        logger.info("Wait for threads to finish the jobs")
        for th in threads:
            th.join()

        return copied_objects, source_objects

    def exec_aws_copy(self, files):
        """
        Exec copy a chunk of  files from the source bucket to the target buckets.
        The target buckets are infered from PROJECT_ACL and project_id in the file
        
        There are some scenarios:
            - Object classes are "STANDARD", "REDUCED_REDUNDANCY": using aws cli
            - Object classes are not "STANDARD", "REDUCED_REDUNDANCY", using gdcapi
            - Object acl is changed, move objects to right bucket

        Intergrity check: 
            - Using awscli: We rely on aws 
            - Streaming: Compute local etag and match with one provided by aws, compute md5 on 
            the fly to check the intergrity of streaming data from gdcapi to local machine

        Log all the success and failure cases

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

                # only copy ones not exist in target buckets
                if object_name not in self.copied_objects:
                    storage_class = get_storage_class(fi, self.source_objects)
                    if storage_class is None:
                        logger.warn(
                            "object with id {} does not exist in source bucket {}. Stream from gdcapi".format(
                                fi["id"], self.bucket
                            )
                        )
                        self.stream_object_from_gdc_api(self.s3, fi, target_bucket)
                        continue

                    # If storage class is not standard or REDUCED_REDUNDANCY, stream object from gdc api
                    if storage_class not in {"STANDARD", "REDUCED_REDUNDANCY"}:
                        logger.info(
                            "Streaming: {}. Size {} (MB). Class {}".format(
                                object_name,
                                int(fi["size"] * 1.0 / 1024 / 1024),
                                storage_class,
                            )
                        )
                        self.stream_object_from_gdc_api(self.s3, fi, target_bucket)
                        continue

                    # If it is a first level object (does not have parent folder), just single copy
                    if is_first_level_object(fi, self.source_objects):
                        cmd = "aws s3 cp s3://{}/{} s3://{}/{} --request-payer requester".format(
                            self.bucket, fi.get("id"), target_bucket, object_name
                        )
                        # wait untill finish
                        subprocess.Popen(shlex.split(cmd + " --quiet")).wait()
                        continue

                    if object_name in self.source_objects:
                        execstr += ' --include "{}"'.format(object_name)
                # object already exists in dcf but acl is changed
                elif self.is_changed_acl_object(fi):
                    logger.info(
                        "acl object is changed. Move object to the right bucket"
                    )
                    cmd = "aws s3 mv s3://{}/{} s3://{}/{}".format(
                        self.copied_objects[object_name],
                        object_name,
                        target_bucket,
                        object_name,
                    )
                    logger.info(cmd)
                    subprocess.Popen(shlex.split(cmd + " --quiet")).wait()
                    continue

            if execstr != base_cmd:
                # just show truncated command in log since it is very long
                logger.info(execstr[:500])
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

    def stream_object_from_gdc_api(self, s3, fi, target_bucket, endpoint=None):
        """
        Stream object from gdc api. In order to check the integrity, we need to compute md5 during streaming data from 
        gdc api and compute its local etag since aws only provides etag for multi-part uploaded object.

        Args:
            s3: s3.client session
            fi(dict): object info
            target_bucket(str): target bucket
        
        Returns:
            None
        """

        data_endpoint = endpoint or "https://api.gdc.cancer.gov/data/{}".format(
            fi.get("id")
        )
        response = requests.get(
            data_endpoint,
            stream=True,
            headers={"Content-Type": "application/json", "X-Auth-Token": GDC_TOKEN},
        )
        if response.status_code != 200:
            logger.error(
                "GDCPotal: Error when streaming object with id {}. Detail {}".format(
                    fi.get("id"), response.status_code
                )
            )
            return

        object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))

        try:
            multipart_upload = s3.create_multipart_upload(
                Bucket=target_bucket, Key=object_path
            )
        except botocore.exceptions.ClientError as error:
            logger.warn(
                "Error when create multiple part upload for object with uuid{}. Detail {}".format(
                    object_path, error
                )
            )
            return

        # prepare to compute md5 in the fly
        sig = hashlib.md5()

        # prepare to compute local etag
        md5_digests = []
        parts = []

        part_number = 0
        for chunk in response.iter_content(
            chunk_size=self.global_config.get("stream_chunk_size", 1024 * 1024 * 32)
        ):
            part_number += 1
            sig.update(chunk)
            try:
                res = s3.upload_part(
                    Body=chunk,
                    Bucket=target_bucket,
                    Key=object_path,
                    PartNumber=part_number,
                    UploadId=multipart_upload.get("UploadId"),
                )
            except botocore.exceptions.ClientError as error:
                logger.warn(error)
                return

            parts.append({"ETag": res["ETag"], "PartNumber": part_number})
            md5_digests.append(hashlib.md5(chunk).digest())

        try:
            response = s3.complete_multipart_upload(
                Bucket=target_bucket,
                Key=object_path,
                MultipartUpload={"Parts": parts},
                UploadId=multipart_upload["UploadId"],
                RequestPayer="requester",
            )
        except botocore.exceptions.ClientError as error:
            logger.warn(
                "Error when finishing multiple part upload object with uuid {}. Detail {}".format(
                    fi.get("Id"), error
                )
            )
            return

        # compute local etag from list of md5s
        etags = (
            hashlib.md5(b"".join(md5_digests)).hexdigest() + "-" + str(len(md5_digests))
        )

        try:
            meta_data = s3.head_object(Bucket=target_bucket, Key=object_path)
        except botocore.exceptions.ClientError as error:
            logger.warn(
                "Can not get meta data of {}. Detail {}".format(fi.get("id"), error)
            )
            return

        if meta_data.get("ETag") is None:
            logger.warn("Can not get etag of {}".format(fi.get("id")))
            return

        if sig.hexdigest() != fi.get("md5") or meta_data.get("ETag", "").replace(
            '"', ""
        ) not in {fi.get("md5"), etags}:
            logger.warn(
                "Can not stream the object {}. Intergrity check fails".format(
                    format(fi.get("id"))
                )
            )
            try:
                s3.delete_object(Bucket=target_bucket, Key=object_path)
            except botocore.exceptions.ClientError as error:
                logger.warn(error)

    def is_changed_acl_object(self, fi):
        """
        check if the object has acl changed or not
        """

        object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
        if (
            fi.get("acl") == "[u'open']"
            and self.copied_objects.get(object_path, "").endswith("controlled")
        ) or (
            fi.get("acl") != "[u'open']"
            and self.copied_objects.get(object_path, "").endswith("open")
        ):
            return True
        return False

    def check_and_index_the_data(self, files):
        """
        Check if files are in manifest are copied or not. Index the files if they exists in target buckets and log
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
        logger.info(
            "{}/{} object are processed/indexed ".format(
                self.total_indexed_files, self.total_files
            )
        )
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
