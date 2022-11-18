from os.path import join, basename
import time

import json
import boto3
import botocore
from retry import retry
from google.cloud import storage
from cdislogging import get_logger

from indexclient.client import IndexClient

from scripts.aws_replicate import object_exists
from scripts.settings import INDEXD, PROJECT_ACL
from scripts.utils import (
    get_aws_bucket_name,
    get_google_bucket_name,
    get_fileinfo_list_from_csv_manifest,
    get_fileinfo_list_from_s3_manifest,
    get_ignored_files,
    get_structured_object_key,
)
from scripts.indexd_utils import (
    remove_url_from_indexd_record,
    delete_record_from_indexd,
)
from scripts.errors import UserError, APIError
from scripts.settings import IGNORED_FILES

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


def delete_objects_from_cloud_resources(manifest, log_bucket, release, dry_run=True):
    """
    delete object from S3 and GS
    for safety use filename instead of file_name in manifest file
    to avoid accident deletion.

    Args:
        manifest(str): manifest file
        log_filename(str): the name of log file
        release(str): data release
        dry_run(bool): True the program does not really delete the file (for report purpose)
    """
    session = boto3.session.Session()
    s3_sess = session.resource("s3")

    try:
        s3_sess.meta.client.head_bucket(Bucket=log_bucket)
    except botocore.exceptions.ClientError as e:
        logger.error(
            "The bucket {} does not exist or you have no access. Detail {}".format(
                log_bucket, e
            )
        )
        return

    indexclient = IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )

    logger.info("DRY RUN: {}".format(dry_run))

    if manifest.startswith("s3://"):
        file_infos = get_fileinfo_list_from_s3_manifest(manifest)
    else:
        file_infos = get_fileinfo_list_from_csv_manifest(manifest)

    s3 = boto3.resource("s3")
    gs_client = storage.Client()

    ignored_dict = get_ignored_files(IGNORED_FILES, "\t")

    aws_deletion_logs = []
    gs_deletion_logs = []
    num = 0
    for fi in file_infos:
        num = num + 1
        logger.info("Start to process file {}".format(num))
        try:
            aws_target_bucket = get_aws_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            aws_deletion_logs.append(
                DeletionLog(
                    url=fi.get("id") + "/" + fi.get("filename"), message=e.message
                )
            )
            aws_target_bucket = None

        if not dry_run:
            if aws_target_bucket:
                aws_deletion_logs.append(
                    _remove_object_from_s3(
                        s3, indexclient, fi, aws_target_bucket, dry_run
                    )
                )
            try:
                google_target_bucket = get_google_bucket_name(fi, PROJECT_ACL)
            except UserError as e:
                logger.warning(e)
                gs_deletion_logs.append(
                    DeletionLog(
                        url=fi.get("id") + "/" + fi.get("filename"), message=e.message
                    )
                )
                continue
            gs_deletion_logs.append(
                _remove_object_from_gs(
                    gs_client, indexclient, fi, google_target_bucket, ignored_dict
                )
            )
            delete_record_from_indexd(fi.get("id"), indexclient)
        else:
            # only the _remove_object_from_s3 function has a dry run option that will not delete object when in the func
            if aws_target_bucket:
                aws_deletion_logs.append(
                    _remove_object_from_s3(
                        s3, indexclient, fi, aws_target_bucket, dry_run
                    )
                )

    aws_log_list = []
    for log in aws_deletion_logs:
        aws_log_list.append(log.to_dict())
    aws_log_json = {}
    aws_log_json["data"] = aws_log_list

    gs_log_list = []
    for log in gs_deletion_logs:
        gs_log_list.append(log.to_dict())
    gs_log_json = {}
    gs_log_json["data"] = gs_log_list

    timestr = time.strftime("%Y%m%d-%H%M%S")
    gs_filename = timestr + "gs_deletion_log.json"
    aws_filename = timestr + "aws_deletion_log.json"

    if not dry_run:
        try:
            s3 = boto3.client("s3")
            with open(aws_filename, "w") as outfile:
                json.dump(aws_log_json, outfile)
            s3.upload_file(
                aws_filename, log_bucket, release + "/" + basename(aws_filename)
            )

            with open(gs_filename, "w") as outfile:
                json.dump(gs_log_json, outfile)
            s3.upload_file(
                gs_filename, log_bucket, release + "/" + basename(gs_filename)
            )
        except Exception as e:
            logger.error(e)
    else:
        logger.info(
            "All following files are for redaction.\nIf there is nothing below that means there is nothing to redact!!!\n\n"
        )
        logger.info("url\n")
        for log in aws_log_list:
            if log["deleted"]:
                logger.info(log["url"])


@retry(APIError, tries=10, delay=2)
def _remove_object_from_s3(s3, indexclient, f, target_bucket, dry_run=False):
    """
    remove object from s3

    Args:
        s3(resource): session resource
        f(dict): file info
        target_bucket(str): aws bucket

    Returns:
        list(DeletionLog): list of deletion logs
    """
    logger.info("Start to check if {} needs to be removed from AWS".format(f["id"]))
    bucket = s3.Bucket(target_bucket)

    key = join(f.get("id"), f.get("filename"))
    full_path = join("s3://" + target_bucket, key)
    deleting_object = {"Key": key}
    deletion_log = DeletionLog(url=full_path)

    if not object_exists(s3, target_bucket, key):
        return deletion_log

    # if we really want to delete the file
    if not dry_run:
        try:
            res = bucket.delete_objects(Delete={"Object": [deleting_object]})
        except Exception as e:
            deletion_log.message = str(e)
            return deletion_log

        if res.get("Deleted"):
            try:
                deletion_log.deleted = True
                remove_url_from_indexd_record(f.get("id"), [full_path], indexclient)
                deletion_log.indexdUpdated = True
                logger.info("Deleted {} from AWS".format(f.get("id")))
            except Exception as e:
                deletion_log.message = str(e)
                logger.warning(
                    "Can not remove aws indexd url of {}. Detail {}".format(f["id"], e)
                )
        else:
            logger.warning("Can not delete {} from AWS".format(f["id"]))
            deletion_log.message = str(res.Error)
    else:
        # Just log it as deleted for pre-report purpose
        deletion_log.deleted = True

    return deletion_log


@retry(APIError, tries=10, delay=2)
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
    logger.info("Start to check if {} needs to be removed from GS".format(f["id"]))
    key = get_structured_object_key(f["id"], ignored_dict)
    if not key:
        key = join(f.get("id"), f.get("filename"))
    full_path = join("gs://" + target_bucket, key)
    deletion_log = DeletionLog(url=full_path)
    bucket = client.get_bucket(target_bucket)

    try:
        blob = bucket.blob(key)
        blob.delete()
        deletion_log.deleted = True
        logger.info("Deleted {} from GS".format(f["id"]))
    except Exception as e:
        logger.warning("Can not delete {} from GS. Detail {}".format(f["id"], e))
        deletion_log.message = str(e)
        return deletion_log

    try:
        logger.info("Start to update indexd for {}".format(f["id"]))
        remove_url_from_indexd_record(f.get("id"), [full_path], indexclient)
        deletion_log.indexdUpdated = True
    except Exception as e:
        logger.warning(
            "Can not remove gs indexd url of {}. Detail {}".format(f["id"], e)
        )
        deletion_log.message = str(e)

    return deletion_log
