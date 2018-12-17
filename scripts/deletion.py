from os.path import join

import json
import boto3
from google.cloud import storage

from cdislogging import get_logger
from indexclient.client import IndexClient

from errors import APIError
from settings import PROJECT_MAP, INDEXD
from utils import get_bucket_name, get_fileinfo_list_from_manifest, exec_files_grouping

logger = get_logger("DCFRedacts")

indexclient = IndexClient(
    INDEXD["host"],
    INDEXD["version"],
    (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
)


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
            "message": self.message
        }


def _remove_url_from_indexd_record(uuid, urls):
    """
    remove url from indexd record

    Args:
        uuid(str): did
        urls(list): list of urls

    """
    doc = indexclient.get(uuid)
    if doc is not None:
        for url in urls:
            doc.urls.remove(url)
            if url in doc.urls_metadata:
                del doc.urls_metadata[url]
        try:
            doc.patch()
        except Exception as e:
            raise APIError(
                "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                    uuid, e.message
                )
            )


def delete_objects_from_cloud_resources(manifest, log_filename):
    """
    delete object from S3 and GS

    Args:
        manifest(str): manifest file
        log_filename(str): the name of log file
    """
    file_info = get_fileinfo_list_from_manifest(manifest)
    file_grp = exec_files_grouping(file_info)

    s3 = boto3.resource("s3")
    gs_client = storage.Client()

    deletion_logs = []
    for _, files in file_grp.iteritems():
        target_bucket = get_bucket_name(files[0], PROJECT_MAP)
        deletion_logs.append(_remove_object_from_s3(s3, files, target_bucket))
        deletion_logs.append(_remove_object_from_gs(gs_client, files, target_bucket))

    log_json = {}
    log_json["data"] = deletion_logs
    with open(log_filename, "w") as outfile:
        json.dump(log_json, outfile)


def _remove_object_from_s3(s3, files, target_bucket):
    """
    remove object from s3

    Args:
        s3(resource): session resource
        files(list): the list of files
        target_bucket(str): aws bucket

    Returns:
        list(DeletionLog): list of deletion logs
    """
    bucket = s3.Bucket(target_bucket)
    deletion_logs = []
    for f in files:
        key = join(f.get("id"), f.get("filename"))
        full_path = join("s3://" + target_bucket, key)
        deleting_object = {"Key": key}
        deletion_log = DeletionLog(url=full_path)

        try:
            res = bucket.delete_objects(Delete={"Objects": [deleting_object]})
        except Exception as e:
            deletion_log.message = e.message
            deletion_logs.append(deletion_log)
            continue

        if res["Deleted"]:
            try:
                _remove_url_from_indexd_record(f.get("id"), [full_path])
                deletion_log.indexdUpdated = True
            except Exception as e:
                deletion_log.message = e.message
        else:
            deletion_log.message = res.Errors

        deletion_logs.append(deletion_log)

    return deletion_logs


def _remove_object_from_gs(client, files, target_bucket):
    """
    remove object from gs

    Args:
        client(storage.client): Google storage client session
        files(list): list of files
        target_bucket(str): google bucket name
        log_json(dict): json log

    Returns:
        list(DeletionLog)

    """
    deletion_logs = []
    for f in files:
        key = join(f.get("id"), f.get("filename"))
        full_path = join("gs://" + target_bucket, key)
        deletion_log = DeletionLog(url=full_path)
        bucket = client.get_bucket(target_bucket)

        try:
            blob = bucket.blob(key)
            blob.delete()
        except Exception as e:
            deletion_log.message = e.message

        try:
            _remove_url_from_indexd_record(f.get("id"), [full_path])
        except Exception as e:
            deletion_log.deleted = True
            deletion_log.message = e.message

        deletion_logs.append(deletion_log)

    return deletion_logs
