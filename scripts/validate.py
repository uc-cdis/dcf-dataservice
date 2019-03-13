import json
import boto3
from cdislogging import get_logger

from indexclient.client import IndexClient

import utils
from errors import UserError
from aws_replicate import build_object_dataset
from settings import PROJECT_ACL, INDEXD, IGNORED_FILES

logger = get_logger("Validation")


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
        results[doc.did] = doc.urls
    return results


def run(global_config):
    """
    run validation
    Args:
        global_config(dict): a configuration

    Returns:
        None

    """
    if not global_config.get("log_bucket"):
        raise UserError("please provide the log bucket")

    ignored_dict = utils.get_ignored_files(IGNORED_FILES, "\t")
    if not ignored_dict:
        raise UserError(
            "Expecting non-empty IGNORED_FILES. Please check if ignored_files_manifest.csv is configured correctly!!!"
        )

    logger.info("List of the manifests")
    logger.info(global_config.get("manifest_files"))
    logger.info(global_config.get("out_manifests"))

    manifest_files = global_config.get("manifest_files", "").split(",")
    out_manifests = global_config.get("out_manifests", "").split(",")

    if len(manifest_files) != len(out_manifests):
        raise UserError("number of output manifests and number of manifest_files are not the same")
    logger.info("scan all copied objects")

    indexd_records = get_indexd_records()
    aws_copied_objects, _ = build_object_dataset(PROJECT_ACL)
    gs_copied_objects = utils.build_object_dataset_gs(PROJECT_ACL)

    if global_config.get("save_copied_objects"):
        with open("./indexd_records.json", "w") as outfile:
            json.dump(indexd_records, outfile)
        with open("./aws_copied_objects.json", "w") as outfile:
            json.dump(aws_copied_objects, outfile)
        with open("./gs_copied_objects.json", "w") as outfile:
            json.dump(gs_copied_objects, outfile)
        
        s3 = boto3.client("s3")
        try:
            s3.upload_file(
                'indexd_records.json', global_config.get("log_bucket"), 'indexd_records.json'
            )
            s3.upload_file(
                'aws_copied_objects.json', global_config.get("log_bucket"), 'aws_copied_objects.json'
            )
            s3.upload_file(
                'gs_copied_objects.json', global_config.get("log_bucket"), 'gs_copied_objects.json'
            )
        except Exception as e:
            logger.error(e)

    for idx, manifest_file in enumerate(manifest_files):
        manifest_file = manifest_file.strip()
        files = utils.get_fileinfo_list_from_s3_manifest(manifest_file)
        for fi in files:
            del fi["url"]
            fi['aws_url'], fi['gs_url'], fi['indexd_url'] = None, None, None

            fi['indexd_url'] = indexd_records.get(fi.get("id"))
            if not fi['indexd_url']:
                logger.error("There is no indexd record for {}".format(fi["id"]))

            # validate aws 
            aws_bucket = utils.get_aws_bucket_name(fi, PROJECT_ACL)
            object_path = "{}/{}/{}".format(aws_bucket, fi["id"], fi["file_name"])
            if object_path not in aws_copied_objects and fi["size"] != 0:
                logger.error("{} is not copied yet to aws buckets".format(object_path))
            elif fi["size"] != 0:
                fi['aws_url'] = "s3://" + object_path
                if fi['aws_url'] not in fi['indexd_url']:
                    logger.error("indexd does not have aws url of {}".format(fi["id"]))

            # validate google
            gs_bucket = utils.get_google_bucket_name(fi, PROJECT_ACL)
            if fi["id"] in ignored_dict:
                object_path = "{}/{}".format(gs_bucket, utils.get_structured_object_key(fi["id"], ignored_dict))
            else:
                object_path = "{}/{}/{}".format(gs_bucket, fi["id"], fi["file_name"])

            if object_path not in gs_copied_objects and fi["size"] != 0:
                logger.error("{} is not copied yet to google buckes".format(object_path))
            elif fi["size"] != 0:
                fi['gs_url'] = "gs://" + object_path
                if fi['gs_url'] not in fi['indexd_url']:
                    logger.error("indexd does not have gs url of {}".format(fi["id"]))

        utils.write_csv("./tmp.csv", files)
        try:
            s3 = boto3.client("s3")
            s3.upload_file(
                'tmp.csv', global_config.get("log_bucket"), out_manifests[idx].strip()
            )
        except Exception as e:
            logger.error(e)
