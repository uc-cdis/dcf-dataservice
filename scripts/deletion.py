from os.path import join, basename
import time

import json
import boto3
from google.cloud import storage

from cdislogging import get_logger
from indexclient.client import IndexClient

from settings import INDEXD, PROJECT_ACL
from utils import (
    get_aws_bucket_name,
    get_google_bucket_name,
    get_fileinfo_list_from_csv_manifest,
    get_fileinfo_list_from_s3_manifest,
)
from indexd_utils import remove_url_from_indexd_record
from errors import APIError, UserError

logger = get_logger("DCFRedacts")


class DeletionLog(object):
    def __init__(self, url, deleted=False, indexdUpdated=False, message=None):
        """
        Construction

        Args:
            url(string): cloud storage object
            deleted(bool): object is removed from cloud
            indexdUpdated(bool): indexd is updated
            message(str): message

        Returns:
            None
        """
        self.url, self.deleted = url, deleted
        self.indexdUpdated, self.message = indexdUpdated, message

    def to_dict(self):
        return {
            "url": self.url,
            "deleted": self.deleted,
            "indexdUpdated": self.indexdUpdated,
            "message": self.message,
        }


def delete_objects_from_cloud_resources(manifest, log_bucket):
    """
    delete object from S3 and GS
    for safety use filename instead of file_name in manifest file
    to avoid accident deletion.

    Args:
        manifest(str): manifest file
        log_filename(str): the name of log file
    """
    indexclient = IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )

    if manifest.startswith("s3://"):
        file_infos = get_fileinfo_list_from_s3_manifest(manifest)
    else:
        file_infos = get_fileinfo_list_from_csv_manifest(manifest)

    s3 = boto3.resource("s3")
    gs_client = storage.Client()

    deletion_logs = []
    for fi in file_infos:
        try:
            aws_target_bucket = get_aws_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            deletion_logs.append(
                DeletionLog(
                    url=fi.get("id") + "/" + fi.get("filename"), message=e.message
                )
            )
            aws_target_bucket = None

        if aws_target_bucket:
            deletion_logs.append(
                _remove_object_from_s3(s3, indexclient, fi, aws_target_bucket)
            )

        try:
            google_target_bucket = get_google_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            deletion_logs.append(
                DeletionLog(
                    url=fi.get("id") + "/" + fi.get("filename"), message=e.message
                )
            )
            continue

        deletion_logs.append(
            _remove_object_from_gs(gs_client, indexclient, fi, google_target_bucket)
        )

    log_list = []
    for log in deletion_logs:
        log_list.append(log.to_dict())
    log_json = {}
    log_json["data"] = log_list

    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = timestr + "_deletion_log.json"

    s3 = boto3.client("s3")
    with open(filename, "w") as outfile:
        json.dump(log_json, outfile)
    s3.upload_file(filename, log_bucket, basename(filename))


def _remove_object_from_s3(s3, indexclient, f, target_bucket):
    """
    remove object from s3

    Args:
        s3(resource): session resource
        f(dict): file info
        target_bucket(str): aws bucket

    Returns:
        list(DeletionLog): list of deletion logs
    """
    bucket = s3.Bucket(target_bucket)

    key = join(f.get("id"), f.get("filename"))
    full_path = join("s3://" + target_bucket, key)
    deleting_object = {"Key": key}
    deletion_log = DeletionLog(url=full_path)

    try:
        res = bucket.delete_objects(Delete={"Objects": [deleting_object]})
    except Exception as e:
        deletion_log.message = str(e)
        return deletion_log

    if res["Deleted"]:
        try:
            remove_url_from_indexd_record(f.get("id"), [full_path], indexclient)
            deletion_log.indexdUpdated = True
        except Exception as e:
            deletion_log.message = str(e)
    else:
        deletion_log.message = str(res.Errors)

    return deletion_log


def _remove_object_from_gs(client, indexclient, f, target_bucket):
    """
    remove object from gs

    Args:
        client(storage.client): Google storage client session
        f(dict): file info
        target_bucket(str): google bucket name
        log_json(dict): json log

    Returns:
        list(DeletionLog)

    """

    key = join(f.get("id"), f.get("filename"))
    full_path = join("gs://" + target_bucket, key)
    deletion_log = DeletionLog(url=full_path)
    bucket = client.get_bucket(target_bucket)

    try:
        blob = bucket.blob(key)
        blob.delete()
    except Exception as e:
        deletion_log.message = str(e)
        return deletion_log
    try:
        remove_url_from_indexd_record(f.get("id"), [full_path], indexclient)
    except Exception as e:
        deletion_log.deleted = True
        deletion_log.message = str(e)

    return deletion_log
