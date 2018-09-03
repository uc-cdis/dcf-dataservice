import os
import sys
import time
import timeit
import subprocess
import shlex

import boto3
import botocore

from cdislogging import get_logger
from indexclient.client import IndexClient

from settings import PROJECT_MAP, INDEXD
from utils import (get_fileinfo_list_from_manifest,
                   get_bucket_name)

dir_path = os.path.dirname(os.path.realpath(__file__))

logger = get_logger("AWSReplication")

indexclient = IndexClient(INDEXD['host'], INDEXD['version'],
                          (INDEXD['auth']['username'],
                          INDEXD['auth']['password']))


def bucket_exists(s3, bucket_name):
    """
    check if the bucket exists or not
    Args:
        s3(s3client): s3 client
        bucket_name: the name of bucket
    Returns:
        bool: bucket exists or not
    Side effects:
        log if no access and no bucket
    """
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            logger.info("Private Bucket. Forbidden Access!")
            return True
        elif error_code == 404:
            logger.info("Bucket {} Does Not Exist!".format(bucket_name))
            return False


def object_exists(s3, bucket_name, key):
    """
    check if object exists or not
    Args:
        s3(s3client): s3 client
        bucket_name(str): the name of the bucket
        key(str): object key
    Returns:
        bool: object exists or not
    Side effect:
        Log in case that no access provided
    """
    try:
        s3.meta.client.head_object(Bucket=bucket_name, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            logger.info("Private object. Forbidden Access!")
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

    def __init__(self, bucket, manifest_file, global_config):
        self.bucket = bucket
        self.manifest_file = manifest_file
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
            dict: each value corresponding to a list of file info.
        """
        # un-comment those lines for generating testing data.
        # This test data contains real uuids and hashes and can be used for replicating
        # aws bucket to aws bucket
        #from intergration_data_test import gen_aws_test_data
        #submitting_files = gen_aws_test_data()
        submitting_files, _ = get_fileinfo_list_from_manifest(
            self.manifest_file)

        project_acl_set = set()
        for fi in submitting_files:
            self.totalBytes = self.totalBytes + fi.get('size', 0)
            if fi.get('project'):
                project_acl_set.add(fi.get('project') + fi.get('acl'))

        file_grp = dict()
        key = 0
        while len(project_acl_set) > 0:
            project_acl = project_acl_set.pop()
            same_project_files = []
            for fi in submitting_files:
                if fi.get('project') + fi.get('acl') == project_acl:
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

    def update_indexd(self, fi):
        """
        update a record to indexd
        Args:
            fi(dict): file info
        Returns:
            None
        """
        s3 = boto3.resource('s3')
        s3_bucket_name = get_bucket_name(fi, PROJECT_MAP)
        s3_object_name = "{}/{}".format(fi.get("fileid"), fi.get("filename"))

        doc = indexclient.get(fi.get('fileid', ''))
        if doc is not None:
            url = "s3://{}/{}".format(s3_bucket_name, s3_object_name)
            if url not in doc.urls:
                doc.urls.append(url)
                doc.patch()
                logger.info("successfuly update the record with uuid {}"
                            .format(fi.get('fileid', '')))
            return

        urls = [
            'https://api.gdc.cancer.gov/data/{}'.format(fi.get('fileid', ''))]

        if object_exists(s3, s3_bucket_name, s3_object_name):
            urls.append("s3://{}/{}".format(s3_bucket_name, s3_object_name))

        doc = indexclient.create(did=fi.get('fileid', ''),
                                 hashes={'md5': fi.get('hash', '')},
                                 size=fi.get('size', 0),
                                 urls=urls)
        if doc is not None:
            logger.info("successfuly create a record with uuid {}".format(
                fi.get('fileid', '')))
        else:
            logger.info("fail to create a record with uuid {}".format(
                fi.get('fileid', '')))

    def call_aws_copy(self, files, global_config):
        """
        Call AWS SLI to copy a chunk of  files from a bucket to another bucket.
        Intergrity check: After each chunk copy, check the returned md5 hashes
                          with the ones provided in manifest.
        If not match, re-copy. Log all the success and failure cases

        Args:
            files(list): a list of files
                         which should be copied to the same bucket
            global_config(dict): user config
        Returns:
            None
        """
        index = 0
        chunk_size = global_config.get('chunk_size', 1)
        target_bucket = get_bucket_name(files[0], PROJECT_MAP)
        s3 = boto3.resource('s3')
        if not bucket_exists(s3, target_bucket):
            # log and return. See the function for detail
            return

        failure_cases = []
        while index < len(files):
            base_cmd = "aws s3 cp s3://{} s3://{} --recursive --exclude \"*\"".format(
                self.bucket, target_bucket)

            number_copying_files = min(chunk_size, len(files) - index)

            # According to AWS document, copying failure is very rare.
            # In order to handle the failure case, the system runs the below code two times,
            # checking if the object is existed in the target bucket,
            # The key idea is AWS internally handle the copying process,
            # if the object exists in the target bucket that means success otherwise failure
            # The system will log all the file with failure copy in the next step

            for turn in xrange(0, 2):
                execstr = base_cmd
                for fi in files[index:index + number_copying_files]:
                    object_name = "{}/{}".format(fi.get("fileid"),
                                                 fi.get("filename"))
                    if not object_exists(s3, self.bucket, object_name):
                        if turn == 0:
                            logger.info(
                                'object {} does not exist'.format(object_name))
                        continue
                    if not object_exists(s3, target_bucket, object_name):
                        execstr += " --include \"{}\"".format(object_name)
                if execstr != base_cmd:
                    subprocess.Popen(shlex.split(execstr)).wait()
                    logger.info(execstr)

            # Log all failure and success cases here
            for fi in files[index:index + number_copying_files]:
                object_name = "{}/{}".format(fi.get("fileid"),
                                             fi.get("filename"))
                if not object_exists(s3, target_bucket, object_name):
                    logger.info("Can not copy {}/{} to new AWS bucket."
                                .format(fi.get('fileid', ''),
                                        fi.get('filename', '')))
                    self.totalBytes -= fi.get('size', 0)
                else:
                    logger.info("Done copying file {}/{} to new AWS bucket"
                                .format(fi.get('fileid', ''),
                                        fi.get('filename', '')))
                    try:
                        self.update_indexd(fi)
                    except Exception as e:
                        logger.info("Update indexd error")
                        logger.info(e)
                    self.totalDownloadedBytes = self.totalDownloadedBytes + \
                        fi.get("size", 0)
            logger.info("================Total  %2.2f=====================",
                        self.totalDownloadedBytes/(self.totalBytes*100 + 1.0e-6))

            index = index + number_copying_files

        logger.info("Store all uuids that can not be copied")
        filename = os.path.join(dir_path, 'fail_copy.txt')
        with open(filename, 'w') as writer:
            writer.write('fileid\tfilename\tsize\thash\tacl\tproject')
            for fi in failure_cases:
                writer.write(
                             '{}\t{}\t{}\t{}\t{}\t{}'
                             .format(
                                     fi.get('fileid', ''),
                                     fi.get('filename', ''),
                                     fi.get('size', 0),
                                     fi.get('hash', ''),
                                     fi.get('acl', '*'),
                                     fi.get('project', '')
                                     )
                             )
        # upload manifest file to s3
        log_bucket = global_config.get('log_bucket', '')
        if log_bucket != '':
            cmd = "aws s3 cp {} s3://{}".format(filename, log_bucket)
            subprocess.Popen(shlex.split(cmd)).wait()
