import os
from os import listdir
from os.path import isfile, join
import sys
import time
import timeit
import copy
import argparse
import re
import subprocess
import getopt
import requests
import Queue
import threading

from cdislogging import get_logger
from settings import PROJECT_MAP
from utils import (get_fileinfo_list_from_manifest,
                   get_bucket_name,
                   get_fileinfo_list_from_manifest)



MODE = 'test'
#if MODE == 'test':
#    from intergration_test import gen_test_data

totalDownloadedBytes = 0

logger = get_logger("ReplicationThread")

def check_bucket(s3, bucket_name):
    bucket = s3.Bucket(bucket_name)
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        print("Bucket Exists!")
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            logger,info("Private Bucket. Forbidden Access!")
            return True
        elif error_code == 404:
            logger.info("Bucket {} Does Not Exist!".format(bucket_name))
            return False

def get_etag_aws_object(s3, bucket_name, key):
    object = s3.Object(bucket_name, key)
    hash = None
    try:
        hash = object.e_tag.strip('"')[0:32]
    except botocore.exceptions.ClientError as e:
        if error_code == 403:
             logger.info("Private Bucket. Forbidden Access!")
             return True
        elif error_code == 404:
             logger("Object {} Does Not Exist!".format(key))
             return False
    return hash


class AWSBucketReplication(object):

    def __init__(self, global_config):
        self.global_config = global_config

    def prepare(self):
        """
        concurently process a set of data files.
        """
        #if MODE == 'test':
        #    submitting_files = gen_test_data()
        #else:
        #    submitting_files = get_fileinfo_list_from_manifest(self.global_config.get('manifest_file',''))
        submitting_files = get_fileinfo_list_from_manifest(self.global_config.get('manifest_file',''))
        #import pdb; pdb.set_trace()
        project_set = set()
        for fi in submitting_files:
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
                file_grp[key] = same_project_files
                key = key + 1

        return file_grp

    def run(self):
        file_grp = self.prepare()
        for _, files in file_grp.iteritems():
            self.call_aws_copy(files, self.global_config)

    def call_aws_copy(self, files, global_config):
        """
        """
        if MODE == 'test':
            return
        index = 0
        chunk_size = global_config.get('chunk_size',1)
        target_bucket = get_bucket_name(files[0].get('project'),'')
        s3  = boto3.resource('s3')
        if not check_bucket(s3, target_bucket):
            return

        while index < len(files):
            baseCmd = "aws s3 cp s3://{} s3://{} --recursive --exclude \"*\"".format(
                    global_config.get('from_source', ''), target_bucket)

            number_copying_files = min(chunk_size, len(files) - index)

            # According to AWS document, copying failure is very rare. In order to handle the failure case,
            # the system runs the below code two times, checking for the etag is performed for each time.
            # The system will log all the file with failure copy in the next step
            for turn in xrange(0,2):
                execstr = baseCmd
                for fi in files[index:index + number_copying_files]:
                    object_name = "{}/{}".format(fi.get("did"), fi.get("filename"))
                    etag = get_etag_aws_object(s3, bucket_name, object_name)
                    if etag is None or (etag.lower() != fi.get('hash','').lower()):
                        execstr = execstr + \
                            " --include \"{}\"".format(object_name)
                os.system(execstr)
                logger.info(execstr)

            # Log all failure case here
            for fi in files[index:index + number_copying_files]:
                object_name = "{}/{}".format(fi.get("did"), fi.get("filename"))
                etag = get_etag_aws_object(s3, bucket_name, object_name)
                if etag is None or (etag.lower() != fi.get('hash','').lower()):
                    logger.info(" Can not copy {} to new bucket".format(fi.get('fileid','')))
                elif etag:
                     totalDownloadedBytes = totalDownloadedBytes + fi.get("size", 0)

            index = index + number_copying_files

