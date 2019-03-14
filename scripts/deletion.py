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
    get_ignored_files,
    get_structured_object_key,
)
from indexd_utils import remove_url_from_indexd_record
from errors import UserError
from settings import IGNORED_FILES

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

    ignored_dict = get_ignored_files(IGNORED_FILES)

    deletion_logs = []
    num = 0
    for fi in file_infos:
        num = num + 1
        logger.info("Start to process file {}".format(num))
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
            logger.warn(e)
            deletion_logs.append(
                DeletionLog(
                    url=fi.get("id") + "/" + fi.get("filename"), message=e.message
                )
            )
            continue
        deletion_logs.append(
            _remove_object_from_gs(gs_client, indexclient, fi, google_target_bucket, ignored_dict)
        )

    log_list = []
    for log in deletion_logs:
        log_list.append(log.to_dict())
    log_json = {}
    log_json["data"] = log_list

    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = timestr + "_deletion_log.json"

    try:
        s3 = boto3.client("s3")
        with open(filename, "w") as outfile:
            json.dump(log_json, outfile)
        s3.upload_file(filename, log_bucket, basename(filename))
    except Exception as e:
        logger.error(e)


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
    logger.info("Start to remove {} from AWS".format(f["id"]))
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

    if res.get("Deleted"):
        try:
            deletion_log.deleted = True
            remove_url_from_indexd_record(f.get("id"), [full_path], indexclient)
            deletion_log.indexdUpdated = True
        except Exception as e:
            deletion_log.message = str(e)
            logger.warn("Can not remove aws indexd url of {}. Detail {}".format(f["id"], e))
    else:
        logger.warn("Can not delete {} from AWS".format(f["id"]))
        deletion_log.message = str(res.Errors)

    return deletion_log


def _remove_gs_5aa_object(client, url, f):
    """
    """
    logger.info("Ignore 5aa object with uuid {}".format(f["id"]))
    return DeletionLog(url=url)


def _remove_object_from_gs(client, indexclient, f, target_bucket, ignored_dict):
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
    object_key = get_structured_object_key(f["id"], ignored_dict)
    if object_key:
        url = "gs://gdc-tcga-phs000178-controlled/{}".format(object_key)
        return _remove_gs_5aa_object(client, url, f)

    logger.info("Start to remove {} from GS".format(f["id"]))
    key = join(f.get("id"), f.get("filename"))
    full_path = join("gs://" + target_bucket, key)
    deletion_log = DeletionLog(url=full_path)
    bucket = client.get_bucket(target_bucket)

    try:
        blob = bucket.blob(key)
        blob.delete()
        deletion_log.deleted = True
    except Exception as e:
        logger.warn("Can not delete {} from GS. Detail {}".format(f["id"], e))
        deletion_log.message = str(e)
        return deletion_log

    try:
        logger.info("Start to update indexd for {}".format(f["id"]))
        remove_url_from_indexd_record(f.get("id"), [full_path], indexclient)
        deletion_log.indexdUpdated = True
    except Exception as e:
        logger.warn("Can not remove gs indexd url of {}. Detail {}".format(f["id"], e))
        deletion_log.message = str(e)

    return deletion_log
