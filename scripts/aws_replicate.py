import os
import sys
import time
import timeit

import boto3, botocore

from cdislogging import get_logger
from settings import PROJECT_MAP
from utils import (get_fileinfo_list_from_manifest,
                   get_bucket_name,
                   get_fileinfo_list_from_manifest)


MODE = 'intergration_test'
if MODE == 'intergration_test':
    from intergration_test import gen_aws_test_data

logger = get_logger("ReplicationThread")

def check_bucket(s3, bucket_name):
    """
    check if the bucket is exists or not
    Args:
        s3(s3client): s3 client
        bucket_name: the name of bucket
    Returns:
        bool: indicate value
    Side effects:
        log if no access and no bucket
    """
    bucket = s3.Bucket(bucket_name)
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            logger,info("Private Bucket. Forbidden Access!")
            return True
        elif error_code == 404:
            logger.info("Bucket {} Does Not Exist!".format(bucket_name))
            return False

def check_object(s3, bucket_name, key):
    """
    check if object is existed or not
    pre-condition: bucket_name should be existed
    Args:
        s3(s3client): s3 client
        bucket_name(str): the name of the bucket
        key(str): object key
    Returns:
        bool: indicate the object is existed or not
    Side effect:
        Log in case that no access provided
    """
    try:
        s3.meta.client.head_object(Bucket=bucket_name, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
         error_code = int(e.response['Error']['Code'])
         if error_code == 403:
             logger,info("Private object. Forbidden Access!")
             return True
         elif error_code == 404:
             return False

def get_etag_aws_object(s3, bucket_name, key):
    """
    get etag of a object
    Args:
        s3(s3client): s3 client
        bucket_name(str): the name of the bucket
        key(str): object key
    Returns:
        md5hash(str): object hash
    """
    object = s3.Object(bucket_name, key)
    hash = None
    try:
        hash = object.e_tag.strip('"')[0:32]
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
             logger.info("Private Bucket. Forbidden Access!")
        return None
    return hash


class AWSBucketReplication(object):

    def __init__(self, global_config):
        self.global_config = global_config
        self.totalBytes = 0
        self.totalDownloadedBytes = 0

    def prepare(self):
        """
        Read data file info from manifest and organize them into multiple groups.
        Each group contains files should be copied to same bucket
        Args:
            None
        Returns:
            dict: each value corresponding to a list of file info. Key is only for indexing
        """
        if MODE == 'intergration_test':
            submitting_files = gen_aws_test_data()
        else:
            submitting_files = get_fileinfo_list_from_manifest(self.global_config.get('manifest_file',''))

        project_set = set()
        for fi in submitting_files:
            self.totalBytes = self.totalBytes + fi.get('size',0)
            if fi.get('project'):
                project_set.add(fi.get('project'))

        file_grp = dict()
        key = 0
        while len(project_set) > 0:
            project = project_set.pop()
            same_project_files = []
            for fi in submitting_files:
                if fi.get('project') == project:
                    same_project_files.append(fi)
            if len(same_project_files) > 0:
                if key in file_grp:
                    file_grp[key].append(same_project_files)
                else:
                    file_grp[key] = same_project_files
                key = key + 1
        return file_grp

    def run(self):
        file_grp = self.prepare()
        for _, files in file_grp.iteritems():
            self.call_aws_copy(files, self.global_config)

    def call_aws_copy(self, files, global_config):
        """
        Call AWS SLI to copy a chunk of  files from a bucket to another bucket.
        Intergrity check: After each chunk copy, check the returned md5 hashes with the ones provided in manifest.
        If not match, re-copy. Log all the success and failure cases

        Args:
            files(list): a list of files which should be copied to the same bucket
            global_config(dict): user config
        Returns:
            None
        """
        index = 0
        chunk_size = global_config.get('chunk_size',1)
        target_bucket = get_bucket_name(files[0], PROJECT_MAP)
        s3  = boto3.resource('s3')
        if not check_bucket(s3, target_bucket):
            # log and return. See the function for detail
            return

        while index < len(files):
            baseCmd = "aws s3 cp s3://{} s3://{} --recursive --exclude \"*\"".format(
                    global_config.get('from_bucket', ''), target_bucket)

            number_copying_files = min(chunk_size, len(files) - index)

            # According to AWS document, copying failure is very rare. In order to handle the failure case,
            # the system runs the below code two times, checking for the etag is performed for each time.
            # The system will log all the file with failure copy in the next step
            for turn in xrange(0,2):
                execstr = baseCmd
                for fi in files[index:index + number_copying_files]:
                    object_name = "{}/{}".format(fi.get("fileid"), fi.get("filename"))
                    if not check_object(s3, global_config.get('from_bucket', ''), object_name):
                        logger.info('object {} is not existed'.format(object_name))
                    etag = get_etag_aws_object(s3, target_bucket, object_name)
                    if etag is None or (etag.lower() != fi.get('hash','').lower()):
                        execstr = execstr + \
                            " --include \"{}\"".format(object_name)
                os.system(execstr)
                logger.info(execstr)

            # Log all failure cases here
            for fi in files[index:index + number_copying_files]:
                object_name = "{}/{}".format(fi.get("fileid"), fi.get("filename"))
                etag = get_etag_aws_object(s3, target_bucket, object_name)
                if etag is None or (etag.lower() != fi.get('hash','').lower()):
                    logger.info(" Can not copy {} to new AWS bucket. Etag {}".format(fi.get('fileid',''), etag))
                    self.totalBytes = self.totalBytes - fi.get('size',0)
                elif etag:
                    logger.info(" Finish copy file {} to new AWS bucket". format(fi.get('fileid',''), etag))
                    self.totalDownloadedBytes = self.totalDownloadedBytes + fi.get("size", 0)
            logger.info("=====================Total  %2.2f========================", self.totalDownloadedBytes/(self.totalBytes*100 + 0.001))

            index = index + number_copying_files
