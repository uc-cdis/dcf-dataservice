import os
import json

from datetime import datetime
from cdislogging import get_logger

import file_utils
import cloud_utils
import dcf_utils
import general_utils


global logger


S3_AUTH_SETTINGS = {}
GS_AUTH_SETTINGS = {}
MANIFESTS_SETTINGS = {}
LOGS_SETTINGS = {}
SLACK_URL = None

try:
    with open("/dataservice_settings.json", "r") as f:
        data = json.loads(f.read())
        S3_AUTH_SETTINGS = data.get("S3_AUTH_SETTINGS", {})
        GS_AUTH_SETTINGS = data.get("GS_AUTH_SETTINGS", {})
        MANIFESTS_SETTINGS = data.get("MANIFESTS_SETTINGS", {})
        LOGS_SETTINGS = data.get("LOGS_SETTINGS", {})
        SLACK_URL = data.get("SLACK_URL", None)
except Exception as e:
    raise (f"Problem reading dataservice_settings.json file.\n{e}")

AUTH_SETTINGS = {"s3": S3_AUTH_SETTINGS, "gs": GS_AUTH_SETTINGS}


def resume_logger(filename=None):
    global logger
    logger = get_logger("Validation", filename)


# Manifest


def proposed_manifest(waiting_location: dict):
    """
    Create manifest to validate records against
    """

    def create_output_record(record_row):
        aws_bucket_name = dcf_utils.get_dcf_aws_bucket_name(record_row)
        gcp_bucket_name = dcf_utils.get_dcf_google_bucket_name(record_row)
        object_key = "{}/{}".format(record_row.get("id"), record_row.get("file_name"))
        aws_url = "{}://{}/{}".format("s3", aws_bucket_name, object_key)
        gcp_url = "{}://{}/{}".format("gs", gcp_bucket_name, object_key)

        missing_acl = []
        if record_row.get("acl") in {"[u'open']", "['open']"}:
            acl = ["*"]
        else:
            acl = [
                ace.strip().replace("u'", "").replace("'", "")
                for ace in record_row.get("acl", "").strip()[1:-1].split(",")
            ]

            for ace in acl:
                if not ace.startswith("phs"):
                    if ace == "":
                        missing_acl.append([record_row.get("id")])
                    else:
                        raise Exception(
                            f'Only "open" and "phs..." ACLs are allowed. Got ACL "{ace}" for id {record_row.get("id")}'
                        )
        if len(missing_acl) > 0:
            logger.info(f"The following records are missing acls: {missing_acl}")

        urls = [
            f"https://api.gdc.cancer.gov/data/{record_row.get('id', '')}",
            aws_url,
            gcp_url,
        ]
        record = [
            record_row.get("id"),
            record_row.get("file_name"),
            record_row.get("md5"),
            record_row.get("size", 0),
            record_row.get("state"),
            record_row.get("project_id"),
            record_row.get("baseid"),
            record_row.get("version"),
            record_row.get("release"),
            record_row.get("acl"),
            record_row.get("type"),
            record_row.get("deletereason"),
            gcp_url,
            urls,
        ]
        return record

    cwd = os.getcwd()

    location_type = waiting_location.get("location_type")
    location = waiting_location.get("location")

    session = None
    if location_type == "cloud":
        cloud = cloud_utils.parse_cloud_path(f"{location}").get("cloud")
        session = cloud_utils.start_session(cloud, AUTH_SETTINGS[cloud])

    manifest_list = general_utils.get_resource_list(location_type, location, session)
    manifest_list_len = len(manifest_list)
    manifest_list_pos = 0

    done_location = f"{cloud_utils.parse_cloud_path(location).get('id')}/cloud_waiting"
    done_list = general_utils.get_resource_list(location_type, done_location, session)

    failed_manifests = 0

    for manifest in manifest_list:
        if manifest not in done_list:
            try:
                manifest_list_pos += 1
                logger.info(
                    f"Creating proposed manifest for manifest {manifest_list_pos}/{manifest_list_len}"
                )

                if location_type == "cloud":
                    file_name = cloud_utils.parse_cloud_path(
                        f"{location}/{manifest}"
                    ).get("file_name")
                else:
                    file_name = manifest

                output_path = f"{cwd}/{file_name}"
                content = general_utils.get_resource_object(
                    location_type, f"{location}/{manifest}", output_path, session
                )

                records = [
                    [
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
                    ]
                ]
                headers = content.pop(0)
                for manifest_record in content:
                    dict_manifest_record = {
                        "id": manifest_record[0],
                        "file_name": manifest_record[1],
                        "md5": manifest_record[2],
                        "size": manifest_record[3],
                        "state": manifest_record[4],
                        "project_id": manifest_record[5],
                        "baseid": manifest_record[6],
                        "version": manifest_record[7],
                        "release": manifest_record[8],
                        "acl": manifest_record[9],
                        "type": manifest_record[10],
                        "deletereason": manifest_record[11],
                        "url": manifest_record[12],
                    }

                    records.append(create_output_record(dict_manifest_record))

                resource_path = f"{cwd}/{file_name}_proposed.tsv"
                file_utils.write(records, resource_path)
                if location_type == "cloud":
                    cloud_utils.upload_cloud(location, resource_path, session)
                    # TODO: remove from local dir after upload
            except Exception as e:
                failed_manifests += 1
                logger.error(
                    f"Manifest, {file_name}, failed during the creation of proposed manifest\n{e}"
                )

    return failed_manifests


# Check Cloud


def check_objects_cloud(
    waiting_location: str, validated_location: str, failed_location: str
):
    """
    Create failed and validated manifests, after checking for presence of resources in buckets
    """
    cwd = os.getcwd()

    waiting_location_type = waiting_location.get("location_type")
    waiting_location_path = waiting_location.get("location")

    validated_location_type = validated_location.get("location_type")
    validated_location_path = validated_location.get("location")

    failed_location_type = failed_location.get("location_type")
    failed_location_path = failed_location.get("location")

    waiting_session = None
    if waiting_location_type == "cloud":
        waiting_cloud = cloud_utils.parse_cloud_path(f"{waiting_location_path}").get(
            "cloud"
        )
        waiting_session = cloud_utils.start_session(
            waiting_cloud, AUTH_SETTINGS[waiting_cloud]
        )

    validated_session = None
    if validated_location_type == "cloud":
        validated_cloud = cloud_utils.parse_cloud_path(
            f"{validated_location_path}"
        ).get("cloud")
        validated_session = cloud_utils.start_session(
            validated_cloud, AUTH_SETTINGS[validated_cloud]
        )

    failed_session = None
    if failed_location_type == "cloud":
        failed_cloud = cloud_utils.parse_cloud_path(f"{failed_location_path}").get(
            "cloud"
        )
        failed_session = cloud_utils.start_session(
            failed_cloud, AUTH_SETTINGS[failed_cloud]
        )

    manifest_list = general_utils.get_resource_list(
        waiting_location_type, waiting_location_path, waiting_session
    )
    manifest_list_len = len(manifest_list)
    manifest_list_pos = 0

    done_list = general_utils.get_resource_list(
        validated_location_type, validated_location_path, validated_session
    )

    failed_manifests = 0

    for manifest in manifest_list:
        if manifest not in done_list:
            try:
                manifest_list_pos += 1
                logger.info(
                    f"Validating cloud for manifest {manifest_list_pos}/{manifest_list_len}"
                )

                if waiting_location_type == "cloud":
                    file_name = cloud_utils.parse_cloud_path(
                        f"{waiting_location_path}/{manifest}"
                    ).get("file_name")
                else:
                    file_name = manifest

                output_path = f"{cwd}/{file_name}"
                content = general_utils.get_resource_object(
                    waiting_location_type,
                    f"{waiting_location_path}/{manifest}",
                    output_path,
                    waiting_session,
                )
                proposed_urls = _list_proposed_urls(content)
                bucket_urls = _list_bucket_urls(content)
                validated_records, failed_records = _check_urls_in_list(
                    proposed_urls, bucket_urls
                )

                if validated_location_type == "cloud":
                    validated_file_path = (
                        f"{cwd}/{file_name[:-4]}_validated{file_name[-4:]}"
                    )
                    file_utils.write(validated_records, validated_file_path)
                    cloud_utils.upload_cloud(
                        validated_location_path, validated_file_path, validated_session
                    )
                else:
                    validated_file_path = f"{validated_location_path}/{file_name[:-4]}_validated{file_name[-4:]}"
                    file_utils.write(validated_records, validated_file_path)

                if failed_location_type == "cloud":
                    failed_file_path = f"{cwd}/{file_name[:-4]}_failed{file_name[-4:]}"
                    file_utils.write(failed_records, failed_file_path)
                    cloud_utils.upload_cloud(
                        failed_location_path, failed_file_path, failed_session
                    )
                else:
                    failed_file_path = f"{failed_location_path}/{file_name[:-4]}_failed{file_name[-4:]}"
                    file_utils.write(failed_records, failed_file_path)

            except Exception as e:
                failed_manifests += 1
                logger.error(
                    f"Manifest, {file_name}, failed during the validation of objects in clouds\n{e}"
                )

    return failed_manifests


def _list_proposed_urls():
    "separate aws gs"


def _list_bucket_urls():
    ""


def _check_urls_in_list():
    ""


# Check objects in IndexD


def check_objects_indexd(
    waiting_location: str, validated_location: str, failed_location: str
):
    """
    Create failed and validated manifests, after checking records in IndexD
    """

    cwd = os.getcwd()

    waiting_location_type = waiting_location.get("location_type")
    waiting_location_path = waiting_location.get("location")

    validated_location_type = validated_location.get("location_type")
    validated_location_path = validated_location.get("location")

    failed_location_type = failed_location.get("location_type")
    failed_location_path = failed_location.get("location")

    waiting_session = None
    if waiting_location_type == "cloud":
        waiting_cloud = cloud_utils.parse_cloud_path(f"{waiting_location_path}").get(
            "cloud"
        )
        waiting_session = cloud_utils.start_session(
            waiting_cloud, AUTH_SETTINGS[waiting_cloud]
        )

    validated_session = None
    if validated_location_type == "cloud":
        validated_cloud = cloud_utils.parse_cloud_path(
            f"{validated_location_path}"
        ).get("cloud")
        validated_session = cloud_utils.start_session(
            validated_cloud, AUTH_SETTINGS[validated_cloud]
        )

    failed_session = None
    if failed_location_type == "cloud":
        failed_cloud = cloud_utils.parse_cloud_path(f"{failed_location_path}").get(
            "cloud"
        )
        failed_session = cloud_utils.start_session(
            failed_cloud, AUTH_SETTINGS[failed_cloud]
        )

    manifest_list = general_utils.get_resource_list(
        waiting_location_type, waiting_location_path, waiting_session
    )
    manifest_list_len = len(manifest_list)
    manifest_list_pos = 0

    done_list = general_utils.get_resource_list(
        validated_location_type, validated_location_path, validated_session
    )

    failed_manifests = 0

    for manifest in manifest_list:
        if manifest not in done_list:
            try:
                manifest_list_pos += 1
                logger.info(
                    f"Validating cloud for manifest {manifest_list_pos}/{manifest_list_len}"
                )

                if waiting_location_type == "cloud":
                    file_name = cloud_utils.parse_cloud_path(
                        f"{waiting_location_path}/{manifest}"
                    ).get("file_name")
                else:
                    file_name = manifest

                output_path = f"{cwd}/{file_name}"
                content = general_utils.get_resource_object(
                    waiting_location_type,
                    f"{waiting_location_path}/{manifest}",
                    output_path,
                    waiting_session,
                )
                proposed_records = _list_proposed_records(content)
                indexd_records = _list_indexd_records(content)
                validated_records, failed_records = compare_records(
                    proposed_records, indexd_records
                )

                if validated_location_type == "cloud":
                    validated_file_path = (
                        f"{cwd}/{file_name[:-4]}_validated{file_name[-4:]}"
                    )
                    file_utils.write(validated_records, validated_file_path)
                    cloud_utils.upload_cloud(
                        validated_location_path, validated_file_path, validated_session
                    )
                else:
                    validated_file_path = f"{validated_location_path}/{file_name[:-4]}_validated{file_name[-4:]}"
                    file_utils.write(validated_records, validated_file_path)

                if failed_location_type == "cloud":
                    failed_file_path = f"{cwd}/{file_name[:-4]}_failed{file_name[-4:]}"
                    file_utils.write(failed_records, failed_file_path)
                    cloud_utils.upload_cloud(
                        failed_location_path, failed_file_path, failed_session
                    )
                else:
                    failed_file_path = f"{failed_location_path}/{file_name[:-4]}_failed{file_name[-4:]}"
                    file_utils.write(failed_records, failed_file_path)

            except Exception as e:
                failed_manifests += 1
                logger.error(
                    f"Manifest, {file_name}, failed during the validation of records in IndexD\n{e}"
                )

    return failed_manifests


def _list_proposed_records():
    "separate aws gs"


def _list_indexd_records():
    ""


def compare_records():
    ""


# Final Manifest


def create_final_manifest():
    """"""
    final_manifest_created = True

    return final_manifest_created


# Check Access


def run(global_config):
    """
    Main controller of process
    Run command: gen3 runjob replicate-validation RELEASE $RELEASE_NUMBER SKIP_TO $STEP
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_path = f"./validation_{timestamp}.txt"
    resume_logger(log_path)
    if not global_config.get("RELEASE"):
        raise Exception("Must provide a release number")
        # raise UserError("Must provide a release number")

    RELEASE = global_config.get("RELEASE", None)
    SKIP_TO = global_config.get("SKIP_TO", None)
    logger.info(f"Starting validation process for release {RELEASE}...")

    proposed_settings = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/GDC",
    }

    cloud_check_waiting = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/cloud_waiting",
    }
    cloud_check_validated = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/cloud_validated",
    }
    cloud_check_failed = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/cloud_failed",
    }

    indexd_check_waiting = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/cloud_validated",
    }
    indexd_check_validated = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/index_validated",
    }
    indexd_check_failed = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/index_failed",
    }

    final_manifest_settings = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/index_validated",
    }

    if SKIP_TO is None:
        logger.info(f"Starting step 1 of 4, Create Manifests to Validate Against")
        proposed_failed = proposed_manifest(proposed_settings)
        logger.info(f"Finished step 1 of 4, {proposed_failed} failed manifests")

        logger.info(f"Starting step 2 of 4, Check Objects in Clouds")
        cloud_failed = check_objects_cloud(
            cloud_check_waiting, cloud_check_validated, cloud_check_failed
        )
        logger.info(f"Finished step 2 of 4, {cloud_failed} failed manifests")

        logger.info(f"Starting step 3 of 4, Check Records in IndexD")
        indexd_failed = check_objects_indexd(
            indexd_check_waiting, indexd_check_validated, indexd_check_failed
        )
        logger.info(f"Finished step 3 of 4, {indexd_failed} failed manifests")

        logger.info(f"Starting step 4 of 4, Create final Manifest")
        final_manifest_failed = create_final_manifest(final_manifest_settings)
        logger.info(
            f"Finished step 4 of 4, final manifest created: {final_manifest_failed}"
        )

    elif SKIP_TO.lower() == "cloud":
        logger.info(f"Starting step 2 of 4, Check Objects in Clouds")
        cloud_failed = check_objects_cloud(
            cloud_check_waiting, cloud_check_validated, cloud_check_failed
        )
        logger.info(f"Finished step 2 of 4, {cloud_failed} failed manifests")

        logger.info(f"Starting step 3 of 4, Check Records in IndexD")
        indexd_failed = check_objects_indexd(
            indexd_check_waiting, indexd_check_validated, indexd_check_failed
        )
        logger.info(f"Finished step 3 of 4, {indexd_failed} failed manifests")

        logger.info(f"Starting step 4 of 4, Create final Manifest")
        final_manifest_failed = create_final_manifest(final_manifest_settings)
        logger.info(
            f"Finished step 4 of 4, final manifest created: {final_manifest_failed}"
        )

    elif SKIP_TO.lower() == "indexd":
        logger.info(f"Starting step 3 of 4, Check Records in IndexD")
        indexd_failed = check_objects_indexd(
            indexd_check_waiting, indexd_check_validated, indexd_check_failed
        )
        logger.info(f"Finished step 3 of 4, {indexd_failed} failed manifests")

        logger.info(f"Starting step 4 of 4, Create final Manifest")
        final_manifest_failed = create_final_manifest(final_manifest_settings)
        logger.info(
            f"Finished step 4 of 4, final manifest created: {final_manifest_failed}"
        )

    elif SKIP_TO.lower() == "final":
        logger.info(f"Starting step 4 of 4, Create final Manifest")
        final_manifest_failed = create_final_manifest(final_manifest_settings)
        logger.info(
            f"Finished step 4 of 4, final manifest created: {final_manifest_failed}"
        )

    log_settings = {
        "location_type": LOGS_SETTINGS["location_type"],
        "location": f"{LOGS_SETTINGS['location']}/{RELEASE}",
    }
    if log_settings["location_type"] == "cloud":
        log_location = log_settings["location"]
        log_cloud = cloud_utils.parse_cloud_path(f"{log_location}").get("cloud")
        log_session = cloud_utils.start_session(log_cloud, AUTH_SETTINGS[log_cloud])
        cloud_utils.upload_cloud(log_location, log_path, log_session)
