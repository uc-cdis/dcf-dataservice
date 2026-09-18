import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import botocore
from cdislogging import get_logger
from urllib.parse import urlparse

from indexclient.client import IndexClient

from dcfdataservice import utils
from dcfdataservice.errors import UserError
from dcfdataservice.aws_replicate import (
    bucket_exists,
    build_object_dataset_aws,
    object_exists,
)
from dcfdataservice.settings import PROJECT_ACL, INDEXD, IGNORED_FILES

global logger

MAX_WORKERS = 20


def resume_logger(filename=None):
    global logger
    logger = get_logger("Validation", filename)


PROJECT_ACL = {
    "CHARLIE": {
        "aws_bucket_prefix": "test-gdc-abc-phs000222",
        "gs_bucket_prefix": "test-gdc-abc-phs000222",
    },
    "TCGA-ACC": {
        "aws_bucket_prefix": "test-gdc-def-phs000333",
        "gs_bucket_prefix": "test-gdc-def-phs000333",
    },
    "TCGA-BLCA": {
        "aws_bucket_prefix": "test-gdc-xyz-phs000111",
        "gs_bucket_prefix": "test-gdc-xyz-phs000111",
    },
}


def _validate_single_file(
    fi,
    release,
    indexd_records,
    VALIDATE_PLATFORM,
    PROJECT_ACL,
    ignored_dict,
    gs_copied_objects,
):
    aws_copy_fail = 0
    gs_copy_fail = 0
    aws_index_fail = 0
    gs_index_fail = 0
    fails = []
    processed = False

    if float(fi["release"]) != float(release):
        logger.info(
            f"Skipping validation of record. File {fi['id']} is from release {fi['release']}, only processing release {release}"
        )
        return (
            fi,
            aws_copy_fail,
            gs_copy_fail,
            aws_index_fail,
            gs_index_fail,
            fails,
            processed,
        )

    del fi["url"]
    fi["aws_url"], fi["gs_url"], fi["indexd_url"] = None, None, None
    fi["indexd_url"] = indexd_records.get(fi.get("id"), [])

    if not fi["indexd_url"]:
        aws_index_fail += 1
        gs_index_fail += 1
        fails.append(fi)
        logger.error("There is no indexd record for {}".format(fi["id"]))

    if _validate_aws(VALIDATE_PLATFORM):
        # Each thread needs its own session — boto3 sessions are not thread-safe.
        session = boto3.session.Session()
        s3_sess = session.resource("s3")
        aws_bucket = utils.get_aws_bucket_name(fi, PROJECT_ACL)
        object_path = "{}/{}/{}".format(aws_bucket, fi["id"], fi["file_name"])
        s3_exists = object_exists(s3_sess, aws_bucket, object_path)

        if not s3_exists and fi["size"] != 0:
            aws_copy_fail += 1
            fails.append(fi)
            logger.error(
                f"File {fi['id']} does not exist on s3. s3_exists? {s3_exists}. Searched s3 location s3://{aws_bucket}/{object_path}"
            )
        elif fi["size"] != 0:
            fi["aws_url"] = "s3://" + object_path
            if fi["aws_url"] not in fi["indexd_url"]:
                aws_index_fail += 1
                fails.append(fi)
                logger.error(
                    "indexd does not have aws url of {}. aws_url: {}, indexd_url: {}".format(
                        fi["id"], fi["aws_url"], fi["indexd_url"]
                    )
                )

    if _validate_gs(VALIDATE_PLATFORM):
        gs_bucket = utils.get_google_bucket_name(fi, PROJECT_ACL)
        if fi["id"] in ignored_dict:
            object_path = "{}/{}".format(
                gs_bucket,
                utils.get_structured_object_key(fi["id"], ignored_dict),
            )
        else:
            fixed_filename = fi["file_name"].replace(" ", "_")
            object_path = "{}/{}/{}".format(gs_bucket, fi["id"], fixed_filename)

        if object_path not in gs_copied_objects and fi["size"] != 0:
            gs_copy_fail += 1
            fails.append(fi)
            logger.error("{} is not copied yet to google buckets".format(object_path))
        elif fi["size"] != 0:
            fi["gs_url"] = "gs://" + object_path
            if fi["gs_url"] not in fi["indexd_url"]:
                gs_index_fail += 1
                fails.append(fi)
                logger.error(
                    "indexd does not have gs url of {}. gs_url: {}, indexd_url: {}".format(
                        fi["id"], fi["gs_url"], fi["indexd_url"]
                    )
                )

    processed = True
    return (
        fi,
        aws_copy_fail,
        gs_copy_fail,
        aws_index_fail,
        gs_index_fail,
        fails,
        processed,
    )


def run(global_config):
    """
    Given manifests run validation process to check if all the objects exist and are indexed correctly
    Args:
        global_config(dict): a configuration
        {
            'manifest_files': 's3://input/active_manifest.tsv, s3://input/legacy_manifest.tsv'
            'out_manifests': 'active_manifest_aug.tsv, legacy_manifest_aug.tsv'
            'FORCE_CREATE_MANIFEST': 'True' 'False'
            'map_file': 's3://location/to/map_file.json'
            'validate_platform': 'AWS'
        }

    Returns:
        bool

    """
    pass_validation = True
    resume_logger("./log.txt")
    if not global_config.get("log_bucket"):
        raise UserError("please provide the log bucket")

    s3 = boto3.client("s3")

    release = global_config.get("release")

    session = boto3.session.Session()
    s3_sess = session.resource("s3")

    if not bucket_exists(s3_sess, global_config.get("log_bucket")):
        return

    ignored_dict = utils.get_ignored_files(IGNORED_FILES, "\t")
    if not ignored_dict:
        raise UserError(
            "Expecting non-empty IGNORED_FILES. Please check if ignored_files_manifest.csv is configured correctly!!!"
        )

    FORCE_CREATE_MANIFEST = global_config.get("FORCE_CREATE_MANIFEST", False)
    if FORCE_CREATE_MANIFEST:
        logger.info(
            "If validation job is run with FORCE_CREATE_MANIFEST True: errors from missing objects are to be expected due to redaction of records in the data release following the current run"
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

    if not _pass_preliminary_check(FORCE_CREATE_MANIFEST, manifest_files):
        raise UserError("The input does not pass the preliminary check")

    logger.info("scan all copied objects")

    indexd_records = {}

    if global_config.get("map_file"):
        logger.info("Using Map file")
        indexd_records = utils.download_and_parse_map_file(
            global_config.get("map_file")
        )

    else:
        for manifest_file in manifest_files:
            records = utils.get_bulk_indexd_record_from_GDC_files(manifest_file, logger)
            indexd_records.update(records)

    if global_config.get("save_copied_objects"):
        with open("./indexd_records.json", "w") as outfile:
            json.dump(indexd_records, outfile)
        try:
            s3.upload_file(
                "indexd_records.json",
                global_config.get("log_bucket"),
                "indexd_records.json",
            )
        except Exception as e:
            logger.error(e)

    VALIDATE_PLATFORM = global_config.get("validate_platform", "AWS")

    logger.info(f"Validating Cloud Storage: {VALIDATE_PLATFORM}")
    logger.info(global_config)

    if _validate_aws(VALIDATE_PLATFORM):
        logger.info("Not going to build aws dataset anymore")
        # logger.info("Building aws dataset")
        # aws_copied_objects, _ = build_object_dataset_aws(PROJECT_ACL, logger)
        # logger.info("Done building object datasets")
        # if global_config.get("save_copied_objects"):
        #     with open("./aws_copied_objects.json", "w") as outfile:
        #         json.dump(aws_copied_objects, outfile)
        #     try:
        #         s3.upload_file(
        #             "aws_copied_objects.json",
        #             global_config.get("log_bucket"),
        #             "aws_copied_objects.json",
        #         )
        #     except Exception as e:
        #         logger.error(e)

    gs_copied_objects = {}
    if _validate_gs(VALIDATE_PLATFORM):
        logger.info("Validating data on Google Cloud Platform..")
        logger.info("Building gs dataset")
        gs_copied_objects = utils.build_object_dataset_gs(PROJECT_ACL)
        if global_config.get("save_copied_objects"):
            with open("./gs_copied_objects.json", "w") as outfile:
                json.dump(gs_copied_objects, outfile)
            try:
                s3.upload_file(
                    "gs_copied_objects.json",
                    global_config.get("log_bucket"),
                    "gs_copied_objects.json",
                )
            except Exception as e:
                logger.error(e)

    for idx, manifest_file in enumerate(manifest_files):
        total_aws_copy_failures = 0
        total_gs_copy_failures = 0
        total_aws_index_failures = 0
        total_gs_index_failures = 0
        total_processed_files = 0
        manifest_file = manifest_file.strip()
        files = utils.get_fileinfo_list_from_s3_manifest(manifest_file)
        fail_list = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    _validate_single_file,
                    fi,
                    release,
                    indexd_records,
                    VALIDATE_PLATFORM,
                    PROJECT_ACL,
                    ignored_dict,
                    gs_copied_objects,
                ): fi
                for fi in files
            }
            for future in as_completed(futures):
                _, aws_cf, gs_cf, aws_if, gs_if, fi_fails, processed = future.result()
                total_aws_copy_failures += aws_cf
                total_gs_copy_failures += gs_cf
                total_aws_index_failures += aws_if
                total_gs_index_failures += gs_if
                fail_list.extend(fi_fails)
                if processed:
                    total_processed_files += 1

        if _validate_gs(VALIDATE_PLATFORM):
            if total_gs_index_failures + total_gs_copy_failures == 0:
                logger.info(
                    "All the objects in {} are replicated to GS and indexed correctly!!!".format(
                        manifest_file
                    )
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

        if _validate_aws(VALIDATE_PLATFORM):
            if total_aws_index_failures + total_aws_copy_failures == 0:
                logger.info(
                    "All the objects in {} are replicated to AWS and indexed correctly!!!".format(
                        manifest_file
                    )
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
        logger.info(f"Total files processed: {total_processed_files}")

        _pass = (
            total_aws_copy_failures
            + total_gs_copy_failures
            + total_aws_index_failures
            + total_gs_index_failures
            == 0
        )

        out_filename = out_manifests[idx].strip()

        if _pass or FORCE_CREATE_MANIFEST:

            if not _pass and FORCE_CREATE_MANIFEST:
                logger.warning(
                    "Validation failed but creating final manifest anyway..."
                )

            HEADERS = [
                "id",
                "file_name",
                "md5",
                "size",
                "state",
                "project_id",
                "baseid",
                "version",
                "release",
                "acl",
                "type",
                "deletereason",
                "gs_url",
                "indexd_url",
                "case_submitter_ids",
            ]
            isb_files = []
            for fi in files:
                del fi["aws_url"]
                if fi["size"] != 0:
                    isb_files.append(fi)

            utils.write_csv("./tmp.csv", isb_files, fieldnames=HEADERS)
        else:
            utils.write_csv("./tmp.csv", fail_list)
            out_filename = "FAIL_" + out_filename
            logger.info(
                "Can not generate the augmented manifest for {}. Please fix all the errors".format(
                    manifest_file
                )
            )

        if pass_validation:
            pass_validation = _pass

        try:
            s3.upload_file("tmp.csv", global_config.get("log_bucket"), out_filename)
        except Exception as e:
            logger.error(e)

        try:
            s3.upload_file(
                "./log.txt",
                global_config.get("log_bucket"),
                global_config.get("release") + "/validation.log",
            )
        except Exception as e:
            logger.error(e)

    return pass_validation


def _pass_preliminary_check(FORCE_CREATE_MANIFEST, manifest_files):
    """
    Check if manifests are in the manifest bucket

    'FORCE_CREATE_MANIFEST': True, False command arg parameter
    'manifest_files': 's3://input/active_manifest.tsv, s3://input/legacy_manifest.tsv'
    """

    session = boto3.session.Session()
    s3 = session.resource("s3")

    for url in manifest_files:
        try:
            parsed = urlparse(url)
            bucket_name = parsed.netloc
            key = parsed.path.strip("/")
            s3.meta.client.head_object(Bucket=bucket_name, Key=key)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404 and FORCE_CREATE_MANIFEST:
                logger.error(
                    "Missing object {} in bucket {}. Detail {}".format(
                        key, bucket_name, e
                    )
                )
            elif error_code == 404:
                return False
            else:
                logger.error(
                    "Something wrong with checking object {} in bucket {}. Detail {}".format(
                        key, bucket_name, e
                    )
                )
                raise
    return True


def _validate_aws(VALIDATE_PLATFORM):
    if VALIDATE_PLATFORM == "AWS" or VALIDATE_PLATFORM == "ALL":
        return True
    return False


def _validate_gs(VALIDATE_PLATFORM):
    if VALIDATE_PLATFORM == "GS" or VALIDATE_PLATFORM == "ALL":
        return True
    return False
