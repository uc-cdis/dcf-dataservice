import os
import time
import json
import boto3
import subprocess
import logging as logger

from indexclient.client import IndexClient

import utils
from errors import UserError
from scripts.aws_replicate import build_object_dataset
from scripts.utils import build_object_dataset_gs
from settings import PROJECT_ACL, INDEXD, IGNORED_FILES

logger.basicConfig(level=logger.INFO, format='%(asctime)s %(message)s')


def get_indexd_records():
    """
    """
    results = {}
    indexd_client = IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )
    it = indexd_client.list(page_size=1000)

    progress = 0
    for doc in it:
        progress += 1
        if progress % 1000 == 0:
            logger.info("INDEXD: get {} records".format(progress))
        results[doc.did] = it.urls
    return results


def run(global_config):
    """
    run validation
    Args:
        global_config(dict): a configuration

    Returns:
        None

    """
    ignored_dict = utils.get_ignored_files(IGNORED_FILES)

    if not ignored_dict:
        raise UserError(
            "Expecting non-empty IGNORED_FILES. Please check if ignored_files_manifest.csv is configured correctly!!!"
        )

    manifest_files = global_config.get("manifest_files", "").split(",")
    out_manifests = global_config.get("out_manifests", "").split(",")

    if len(manifest_files) != len(out_manifests):
        raise UserError("number of output manifests and number of manifest_files are not the same")
    logger.info("scan all copied objects")

    indexd_records = get_indexd_records()
    aws_copied_objects, _ = build_object_dataset(PROJECT_ACL)
    gs_copied_objects = build_object_dataset_gs(PROJECT_ACL)

    if global_config("save_copied_objects"):
        with open("./indexd_records.json", "w") as outfile:
            json.dump(indexd_records, outfile)
        with open("./gs_copied_objects.json", "w") as outfile:
            json.dump(gs_copied_objects, outfile)
        
        s3 = boto3.client("s3")
        try:
            s3.upload_file(
                'indexd_records.json', global_config.get("log_bucket"), 'indexd_records.json'
            )
            s3.upload_file(
                'gs_copied_objects.json', global_config.get("log_bucket"), 'gs_copied_objects.json'
            )
        except Exception as e:
            logger.error(e)
    
    for idx, manifest_file in enumerate(manifest_files):
        files = utils.get_fileinfo_list_from_s3_manifest(manifest_file)
        for fi in files:
            del fi["url"]
            if fi["id"] in ignored_dict:
                fi['gs_url'], fi['indexd_url'] = None, None
                object_key = utils.get_structured_object_key(fi["id"], ignored_dict)
            else:
                bucket = utils.get_google_bucket_name(fi, PROJECT_ACL)
                object_key = "{}/{}/{}".format(bucket, fi["id"], fi["file_name"])
            
            if object_key in gs_copied_objects:
                logger.warn("{} is not copied yet".format(object_key))
                fi['gs_url'] = "gs://" + object_key
            fi['indexd_url'] = indexd_records.get(indexd_records[fi["id"]], None)
            if not fi['indexd_url']:
                logger.warn("There is no indexd record for {}".format(fi["id"]))
            elif fi['gs_url'] not in fi['indexd_url']:
                logger.warn("indexd does not have gs url of {}".format(fi["id"]))

        utils.write_csv("./tmp.csv", files)
        try:
            s3.upload_file(
                'tmp.csv', global_config.get("log_bucket"), out_manifests[idx]
            )
        except Exception as e:
            logger.error(e)
