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

    log_json = {}
    for _, files in file_grp.iteritems():
        target_bucket = get_bucket_name(files[0], PROJECT_MAP)
        _remove_object_from_s3(s3, files, target_bucket, log_json)
        _remove_object_from_gs(gs_client, files, target_bucket, log_json)

    with open(log_filename, "w") as outfile:
        json.dump(log_json, outfile)


def _remove_object_from_s3(s3, files, target_bucket, log_json):
    """
    remove object froms3

    Args:
        s3(resource): session resource
        files(list): the list of files
        target_bucket(str): aws bucket
        json_log(dict): json log
    
    """
    bucket = s3.Bucket(target_bucket)
    for f in files:
        key = join(f.get("id"), f.get("filename"))
        full_path = join("s3://" + target_bucket, key)
        deleting_object = {"Key": key}

        try:
            res = bucket.delete_objects(Delete={"Objects": [deleting_object]})
        except Exception as e:
            log_json[full_path] = {"deleted": False, "error": {"msg": e.message}}
            continue

        if res["Deleted"]:
            log_json[full_path] = {"deleted": True, "error": None}
            try:
                _remove_url_from_indexd_record(f.get("id"), [full_path])
                log_json[full_path]["remove_from_indexd"] = True
            except Exception as e:
                log_json[full_path]["remove_from_indexd"] = False
        else:
            log_json[full_path] = {"deleted": False, "error": res.Errors}


def _remove_object_from_gs(client, files, target_bucket, log_json):
    """
    remove object from gs

    Args:
        client(storage.client): Google storage client session
        files(list): list of files
        target_bucket(str): google bucket name
        log_json(dict): json log
    
    """
    for f in files:
        key = join(f.get("id"), f.get("filename"))
        full_path = join("gs://" + target_bucket, key)
        try:
            bucket = client.get_bucket(target_bucket)
            blob = bucket.blob(key)
            blob.delete()
            _remove_url_from_indexd_record(f.get("id"), [full_path])
        except Exception as e:
            log_json[full_path] = {"deleted": False, "error": {"msg": e.message}}
