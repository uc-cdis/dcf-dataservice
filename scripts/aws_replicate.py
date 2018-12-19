import os
import subprocess
import shlex

import json
import boto3
import botocore

from cdislogging import get_logger
from indexclient.client import IndexClient

from errors import APIError, UserError
from settings import PROJECT_MAP, INDEXD
from utils import get_bucket_name, get_fileinfo_list_from_manifest, exec_files_grouping

logger = get_logger("AWSReplication")

indexclient = IndexClient(
    INDEXD["host"],
    INDEXD["version"],
    (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
)

total_file_processed = 0

class ObjectReplicateLog(object):
    def __init__(self, url, copy_success=False, index_success=False, message=""):
        self.url = url
        self.copy_success = copy_success
        self.index_success = index_success
        self.message = message


def object_exists(s3, bucket_name, key):
    """
    check if object exists or not, this function assumes that
    the bucket_name already exists
    Args:
        s3(s3client): s3 client
        bucket_name(str): the name of the bucket
        key(str): object key
    Returns:
        bool: object exists or not
    """
    try:
        #s3.meta.client.head_object(Bucket=bucket_name, Key=key)
        s3.head_object(Bucket=bucket_name, Key=key)

    except botocore.exceptions.ClientError:
        return False

    return True


class AWSBucketReplication(object):
    def __init__(self, bucket, manifest_file, global_config):
        self.bucket = bucket
        self.manifest_file = manifest_file
        self.global_config = global_config
        self.json_log = {"success_cases": 0}

    def get_copied_keys(self):
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
        Read data file info from manifest and organize them into multiple groups.
        Each group contains files should be copied to same bucket
        Args:
            None
        Returns:
            dict: each value corresponding to a list of file info.
        """

        submitting_files, _ = get_fileinfo_list_from_manifest(self.manifest_file)
        return exec_files_grouping(submitting_files)

    def write_log_file(self, s3):
        logger.info("Store all uuids that can not be copied")
        filename = self.global_config.get("log_file", "./log_file.json")
        with open(filename, "w") as outfile:
            json.dump(self.json_log, outfile)

        s3.upload_file(
            filename, self.global_config.get("log_bucket"), os.path.basename(filename)
        )

    def run(self):
        s3 = boto3.client('s3')
        # raise exception if bucket does not exist
        try:
            s3.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            raise UserError(e.message)

        copied_keys = self.get_copied_keys()

        file_grp = self.prepare()
        for _, files in file_grp.iteritems():
            target_bucket = get_bucket_name(files[0], PROJECT_MAP)
            self.call_aws_copy(s3, files, target_bucket, copied_keys)
        self.check_and_index_the_data()

        # write result to log file
        self.write_log_file(s3)

    def update_indexd(self, fi):
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

    def call_aws_copy(self, s3, files, target_bucket, copied_keys):
        """
        Call AWS SLI to copy a chunk of  files from a bucket to another bucket.
        Intergrity check: After each chunk copy, check the returned md5 hashes
                          with the ones provided in manifest.
        If not match, re-copy. Log all the success and failure cases

        Args:
            files(list): a list of files
                         which should be copied to the same bucket
            global_config(dict): user config
            json_log(dict): log all failure cases
        Returns:
            None
        """
        config_chunk_size = self.global_config.get("chunk_size", 1)

        # Check if target bucket exists or not
        try:
            s3.head_bucket(Bucket=target_bucket)
        except botocore.exceptions.ClientError as e:
            raise UserError(e.message)

        index = 0

        global total_file_processed
        while index < len(files):
            base_cmd = 'aws s3 cp s3://{} s3://{} --recursive --exclude "*"'.format(
                self.bucket, target_bucket
            )

            chunk_size = min(config_chunk_size, len(files) - index)

            execstr = base_cmd
            for fi in files[index : index + chunk_size]:
                object_name = "{}/{}".format(fi.get("id"), fi.get("filename"))

                # only copy ones not exist in target bucket
                if object_name not in copied_keys:
                    execstr += ' --include "{}"'.format(object_name)
                total_file_processed += 1
                if total_file_processed % 10 == 0:
                    logger.info("Total number of files processed: {}".format(total_file_processed))

            if execstr != base_cmd:
                subprocess.Popen(shlex.split(execstr + " --quiet")).wait()

            index = index + chunk_size

    def check_and_index_the_data(self):
        """
        batch list all the objects and check if file in manifest is copied or not.
        Index data and log
        """
        copied_objects = self.get_copied_keys()
        submitting_files, _ = get_fileinfo_list_from_manifest(self.manifest_file)

        for fi in submitting_files:
            object_path = "{}/{}/{}".format(get_bucket_name(fi, PROJECT_MAP), fi.get("id"), fi.get("filename"))
            if object_path not in copied_objects:
                self.json_log[object_path] = {
                    "copy_success": False,
                    "index_success": False,
                }
            else:
                try:
                    self.json_log[object_path] = {
                        "copy_success": True,
                        "index_success":  self.update_indexd(fi),
                    }
                except Exception as e:
                    logger.error(e.message)
                    self.json_log[object_path] = {
                        "copy_success": True,
                        "index_success": False,
                        "msg": e.message,
                    }
