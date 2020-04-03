import json
import boto3
from cdislogging import get_logger

from indexclient.client import IndexClient

from scripts import utils
from scripts.errors import UserError
from scripts.aws_replicate import bucket_exists, build_object_dataset_aws
from scripts.settings import PROJECT_ACL, INDEXD, IGNORED_FILES

import time

global logger


def resume_logger(filename=None):
    global logger
    logger = get_logger("Validation", filename)


def run(global_config):
    total_time_start = time.time()
    """
    Given manifests run validation process to check if all the objects exist and are indexed correctly
    Args:
        global_config(dict): a configuration
        {
            'manifest_files': 's3://input/active_manifest.tsv, s3://input/legacy_manifest.tsv'
            'out_manifests': 'active_manifest_aug.tsv, legacy_manifest_aug.tsv'
        }
    Returns:
        bool
    """
    resume_logger("./log.txt")
    if not global_config.get("log_bucket"):
        raise UserError("please provide the log bucket")

    s3 = boto3.client("s3")

    session = boto3.session.Session()
    s3_sess = session.resource("s3")

    if not bucket_exists(s3_sess, global_config.get("log_bucket")):
        return

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
        raise UserError(
            "number of output manifests and number of manifest_files are not the same"
        )
    logger.info("scan all copied objects")

    # should check if these are running 
    aws_copied_objects, _ = build_object_dataset_aws(PROJECT_ACL, logger) #here
    gs_copied_objects = utils.build_object_dataset_gs(PROJECT_ACL) #here

    print("COPIED OBJECTS CHECK-------------------------------------------")
    print(aws_copied_objects)
    print(gs_copied_objects)
    print("---------------------------------------------------------------")

    indexd_records = utils.get_indexd_records() # here. checking if files have been indexd?
    #wont be needing to upload any file hopefully
    if global_config.get("save_copied_objects") and False:
        print("YOOOOOO")
        with open("./indexd_records.json", "w") as outfile:
            json.dump(indexd_records, outfile)
        with open("./aws_copied_objects.json", "w") as outfile:
            json.dump(aws_copied_objects, outfile)
        with open("./gs_copied_objects.json", "w") as outfile:
            json.dump(gs_copied_objects, outfile)

        try:
            s3.upload_file(
                "indexd_records.json",
                global_config.get("log_bucket"),
                "indexd_records.json",
            )
            s3.upload_file(
                "aws_copied_objects.json",
                global_config.get("log_bucket"),
                "aws_copied_objects.json",
            )
            s3.upload_file(
                "gs_copied_objects.json",
                global_config.get("log_bucket"),
                "gs_copied_objects.json",
            )
        except Exception as e:
            logger.error(e)

    pass_validation = True
    # Could possibly async this since this does take a while and would be waiting on the first manifest to be done
    for idx, manifest_file in enumerate(manifest_files): # Just the two manifest_files so no need to worry about this too much.
        total_aws_copy_failures = 0
        total_gs_copy_failures = 0
        total_aws_index_failures = 0
        total_gs_index_failures = 0
        manifest_file = manifest_file.strip()
        # files = utils.get_fileinfo_list_from_s3_manifest(manifest_file) # lists all the gz zipped files
        files = manifest_file
        fail_list = []
        
        for fi in files: # HERE
            del fi["url"]
            fi["aws_url"], fi["gs_url"], fi["indexd_url"] = None, None, None
            # could possibly do some parts of aws and gs validation at the same time. 

            fi["indexd_url"] = indexd_records.get(fi.get("id"), []) #adds indexd_url to the fi by chekcing if its in the indexd_records. Not super sure if this is gonna work for testing
            if not fi["indexd_url"]: # [KEY]
                total_aws_index_failures += 1
                total_gs_index_failures += 1
                fail_list.append(fi)
                logger.error("There is no indexd record for {}".format(fi["id"]))

            # could possibly do some parts of aws and gs validation at the same time. 

            # Use asyncio to parallelize validate aws and validate google. Seperate those to two different functions. 
            aws_time_start = time.time()
            # validate aws
            aws_bucket = utils.get_aws_bucket_name(fi, PROJECT_ACL) # get aws_bucket name according to fi
            print("AWS BUCKET NAME: {0}".format(aws_bucket))
            object_path = "{}/{}/{}".format(aws_bucket, fi["id"], fi["file_name"]) # this should be the object path
            print("OBJECT PATH: {}".format(object_path))
            if object_path not in aws_copied_objects and fi["size"] != 0: # HERE [KEY] check if object path in aws_copied_object list
                total_aws_copy_failures += 1
                fail_list.append(fi)
                logger.error("{} is not copied yet to aws buckets".format(object_path))
            elif fi["size"] != 0: # if file in aws_copied_object list and in indexd_record 
                fi["aws_url"] = "s3://" + object_path
                total_aws_index_failures += 1
                fail_list.append(fi)
                logger.error("indexd does not have aws url of {}".format(fi["id"]))
            aws_time_end = time.time()

            google_time_start = time.time()
            # validate google
            gs_bucket = utils.get_google_bucket_name(fi, PROJECT_ACL)
            # just to include different paths for files in ignored manifest vs regular 
            if fi["id"] in ignored_dict:
                object_path = "{}/{}".format(
                    gs_bucket, utils.get_structured_object_key(fi["id"], ignored_dict)
                )
            else:
                fixed_filename = fi["file_name"].replace(" ", "_")
                object_path = "{}/{}/{}".format(gs_bucket, fi["id"], fixed_filename)

            if object_path not in gs_copied_objects and fi["size"] != 0: # same thing as aws 
                total_gs_copy_failures += 1
                fail_list.append(fi)
                logger.error(
                    "{} is not copied yet to google buckets".format(object_path)
                )
            elif fi["size"] != 0:
                fi["gs_url"] = "gs://" + object_path
                total_gs_index_failures += 1
                fail_list.append(fi)
                logger.error("indexd does not have gs url of {}".format(fi["id"]))
            google_time_end = time.time()

        if total_gs_index_failures + total_gs_copy_failures == 0:
            logger.info(
                "All the objects in {} are replicated to GS and indexed correctly!!!".format(manifest_file)
            )
        else:
            if total_gs_index_failures > 0:
                logger.info(
                    "TOTAL GS INDEX FAILURE CASES {} in {}".format(
                        total_gs_index_failures, manifest_file
                    )
                )
            if total_gs_copy_failures > 0:
                logger.info(
                    "TOTAL GS COPY FAILURE CASES {} in {}".format(
                        total_gs_copy_failures, manifest_file
                    )
                )

        if total_aws_index_failures + total_aws_copy_failures == 0:
            logger.info(
                "All the objects in {} are replicated to AWS and indexed correctly!!!".format(manifest_file)
            )
        else:
            if total_aws_index_failures > 0:
                logger.info(
                    "TOTAL AWS INDEX FAILURE CASES {} in {}".format(
                        total_aws_index_failures, manifest_file
                    )
                )
            if total_aws_copy_failures > 0:
                logger.info(
                    "TOTAL AWS COPY FAILURE CASES {} in {}".format(
                        total_aws_copy_failures, manifest_file
                    )
                )

        _pass = (
            total_aws_copy_failures
            + total_gs_copy_failures
            + total_aws_index_failures
            + total_gs_index_failures
            == 0
        )

        if _pass:
            HEADERS = ["id", "file_name", "md5", "size", "state", "project_id", "baseid", "version", "release", "acl", "type", "deletereason", "gs_url", "indexd_url"]
            isb_files = []
            for fi in files:
                del fi["aws_url"]
                if fi["size"] != 0:
                    isb_files.append(fi)

            utils.write_csv("./tmp.csv", isb_files, fieldnames=HEADERS)
        else:
            utils.write_csv("./tmp.csv", fail_list)
            logger.info("Can not generate the augmented manifest for {}. Please fix all the errors".format(manifest_file))

        if pass_validation:
            pass_validation = _pass

        out_filename = out_manifests[idx].strip() if _pass else "FAIL_" + out_manifests[idx].strip()

        try:
            s3.upload_file(
                "tmp.csv", global_config.get("log_bucket"), out_filename
            )
        except Exception as e:
            logger.error(e)
        
        try:
            s3.upload_file(
                "./log.txt",
                global_config.get("log_bucket"),
                global_config.get("release") + "/validation.log"
            )
        except Exception as e:
            logger.error(e)
    total_time_end = time.time()

    print("////////////////////////////////////////////////////////////////////////////////////////")
    print("AWS_TIME: {0}".format(aws_time_end - aws_time_start))
    print("GOOGLE_TIME: {0}".format(google_time_end - google_time_start))
    print("TOTAL_TIME: {0}".format(total_time_end - total_time_start))
    print("////////////////////////////////////////////////////////////////////////////////////////")

    return pass_validation