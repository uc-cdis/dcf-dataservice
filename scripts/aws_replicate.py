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
        s3.meta.client.head_object(Bucket=bucket_name, Key=key)
    except botocore.exceptions.ClientError:
        return False

    return True


class AWSBucketReplication(object):
    def __init__(self, bucket, manifest_file, global_config):
        self.bucket = bucket
        self.manifest_file = manifest_file
        self.global_config = global_config
        self.json_log = {"success_case": 0}

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
        # from intergration_data_test import gen_aws_test_data
        # submitting_files = gen_aws_test_data()
        submitting_files, _ = get_fileinfo_list_from_manifest(self.manifest_file)
        return exec_files_grouping(submitting_files)

    def write_log_file(self, filname):
        logger.info("Store all uuids that can not be copied")
        with open(filname, "w") as outfile:
            json.dump(self.json_log, outfile)

    def run(self):
        s3 = boto3.resource("s3")
        # raise exception if bucket does not exist
        try:
            s3.meta.client.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            raise UserError(e.message, int(e.response["Error"]["Code"]))

        file_grp = self.prepare()
        for _, files in file_grp.iteritems():
            target_bucket = get_bucket_name(files[0], PROJECT_MAP)
            self.call_aws_copy(s3, files, target_bucket)

        # write result to log file
        self.write_log_file()

    def update_indexd(self, fi):
        """
        update a record to indexd
        Args:
            fi(dict): file info
        Returns:
            None
        """
        s3 = boto3.resource("s3")
        s3_bucket_name = get_bucket_name(fi, PROJECT_MAP)
        s3_object_name = "{}/{}".format(fi.get("fileid"), fi.get("filename"))

        doc = indexclient.get(fi.get("fileid", ""))
        if doc is not None:
            url = "s3://{}/{}".format(s3_bucket_name, s3_object_name)
            if url not in doc.urls:
                doc.urls.append(url)
                try:
                    doc.patch()
                except Exception as e:
                    raise APIError(
                        message="INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                            fi.get("fileid", ""), e.message
                        ),
                    )
            return

        urls = ["https://api.gdc.cancer.gov/data/{}".format(fi.get("fileid", ""))]

        if object_exists(s3, s3_bucket_name, s3_object_name):
            urls.append("s3://{}/{}".format(s3_bucket_name, s3_object_name))

        try:
            doc = indexclient.create(
                did=fi.get("fileid", ""),
                hashes={"md5": fi.get("hash", "")},
                size=fi.get("size", 0),
                urls=urls,
            )
        except Exception as e:
            raise APIError(
                message="INDEX_CLIENT: Can not create the record with uuid {}. Detail {}".format(
                    fi.get("fileid", ""), e.message
                ),
            )

    def call_aws_copy(self, s3, files, target_bucket):
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
            s3.meta.client.head_bucket(Bucket=target_bucket)
        except botocore.exceptions.ClientError as e:
            raise UserError(e.message, int(e.response["Error"]["Code"]))

        index = 0
        while index < len(files):
            base_cmd = 'aws s3 cp s3://{} s3://{} --recursive --exclude "*"'.format(
                self.bucket, target_bucket
            )

            chunk_size = min(config_chunk_size, len(files) - index)

            # According to AWS document, copying failure is very rare.
            # The key idea is AWS internally handle the copying process,
            # if the object exists in the target bucket that means the copying is success otherwise failure

            execstr = base_cmd
            for fi in files[index : index + chunk_size]:
                object_name = "{}/{}".format(fi.get("fileid"), fi.get("filename"))

                if not object_exists(s3, self.bucket, object_name):
                    self.json_log[object_name] = {
                        "msg": "object does not exists in {}".format(self.bucket),
                        "module": "Service"
                    }
                    continue

                # only copy ones not exist in target bucket
                if not object_exists(s3, target_bucket, object_name):
                    execstr += ' --include "{}"'.format(object_name)

            if execstr != base_cmd:
                subprocess.Popen(shlex.split(execstr)).wait()
                logger.info(execstr)

            # Log all failures
            for fi in files[index : index + chunk_size]:
                object_name = "{}/{}".format(fi.get("fileid"), fi.get("filename"))
                if not object_exists(s3, target_bucket, object_name):
                    self.json_log[object_name] = {
                        "msg": "Can not copy the {} from {} to {} due to AWS CLI error.".format(
                            object_name, self.bucket, target_bucket
                        ),
                        "module": "AWSCLI"
                    }
                else:
                    try:
                        self.update_indexd(fi)
                        self.json_log["success_case"] += 1
                    except Exception as e:
                        logger.error(e.message)
                        self.json_log[object_name] = {
                            "msg": e.message,
                            "module": "Indexd"
                        }

                    if self.json_log["success_case"] % 1000 == 0:
                        self.write_log_file()

            index = index + chunk_size
