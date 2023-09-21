import json

from scripts import validateII
from scripts import general_utils
from scripts import file_utils
from scripts import cloud_utils

from cdislogging import get_logger

INDEXD_SETTINGS = {}
S3_AUTH_SETTINGS = {}
GS_AUTH_SETTINGS = {}
MANIFESTS_SETTINGS = {}
LOG_SETTINGS = {}
SLACK_URL = None

try:
    with open("/dataservice_settings.json", "r") as f:
        data = json.loads(f.read())
        SLACK_URL = data.get("SLACK_URL", None)
        INDEXD_SETTINGS = data.get("INDEXD", {})
        S3_AUTH_SETTINGS = data.get("S3_AUTH", {})
        GS_AUTH_SETTINGS = data.get("GS_AUTH", {})
        MANIFESTS_SETTINGS = data.get("MANIFESTS", {})
        LOG_SETTINGS = data.get("LOGS", {})
except Exception as e:
    raise (f"Problem reading dataservice_settings.json file.\n{e}")

AUTH_SETTINGS = {
    "s3": S3_AUTH_SETTINGS,
    "gs": GS_AUTH_SETTINGS,
    "indexd": INDEXD_SETTINGS,
}


def resume_logger(filename=None):
    global logger
    logger = get_logger("Validation", filename)


def run(global_config):
    """
    Main controller of process
    Run command: gen3 runjob replicate-validation RELEASE $RELEASE_NUMBER SKIP_TO $STEP
    """

    if not global_config.get("RELEASE"):
        raise Exception("Must provide a release number")
        # raise UserError("Must provide a release number")

    RELEASE = global_config.get("RELEASE")
    SKIP_TO = global_config.get("SKIP_TO", None)

    LOG_SETTINGS["location"] = f"{LOG_SETTINGS['location']}/DR{RELEASE}_validation.txt"

    log_path = f"./DR{RELEASE}_validation.txt"
    log_session = None
    if LOG_SETTINGS["location_type"] == "cloud":
        log_cloud = cloud_utils.parse_cloud_path(f"{LOG_SETTINGS['location']}").get(
            "cloud"
        )
        log_session = cloud_utils.start_session(log_cloud, AUTH_SETTINGS[log_cloud])

    try:
        general_utils.get_resource_object(
            LOG_SETTINGS["location_type"],
            LOG_SETTINGS["location"],
            log_path,
            log_session,
        )
    except Exception as e:
        print(
            f"Could not find logs for validation under {LOG_SETTINGS['location']}, writing new file"
        )
        file_utils.write([""], log_path)

    resume_logger(log_path)
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

    final_manifest_waiting = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}/index_validated",
    }
    final_manifest_destination = {
        "location_type": MANIFESTS_SETTINGS["location_type"],
        "location": f"{MANIFESTS_SETTINGS['location']}/{RELEASE}",
    }

    try:
        final_manifest_passed = False
        if SKIP_TO is None:
            logger.info(f"Starting step 1 of 4, Create Manifests to Validate Against")
            proposed_failed = validateII.proposed_manifest(
                proposed_settings, cloud_check_waiting, AUTH_SETTINGS
            )
            logger.info(f"Finished step 1 of 4, {proposed_failed} failed manifests")

            logger.info(f"Starting step 2 of 4, Check Objects in Clouds")
            cloud_failed = validateII.check_objects_cloud(
                cloud_check_waiting,
                cloud_check_validated,
                cloud_check_failed,
                AUTH_SETTINGS,
            )
            logger.info(f"Finished step 2 of 4, {cloud_failed} failed manifests")

            logger.info(f"Starting step 3 of 4, Check Records in IndexD")
            indexd_failed = validateII.check_objects_indexd(
                indexd_check_waiting,
                indexd_check_validated,
                indexd_check_failed,
                AUTH_SETTINGS,
            )
            logger.info(f"Finished step 3 of 4, {indexd_failed} failed manifests")

            if proposed_failed + cloud_failed + indexd_failed == 0:
                logger.info(f"Starting step 4 of 4, Create final Manifest")
                final_manifest_passed = validateII.create_final_manifest(
                    final_manifest_waiting, final_manifest_destination, AUTH_SETTINGS
                )
                logger.info(
                    f"Finished step 4 of 4, final manifest created: {final_manifest_passed}"
                )
            else:
                logger.warning(
                    f"Cannot create final manifest, 1 one more manifests have failed\nPrep failed: {proposed_failed}, Cloud failed: {cloud_check_failed}, IndexD failed: {indexd_check_failed}"
                )

        elif SKIP_TO.lower() == "cloud":
            logger.info(f"Starting step 2 of 4, Check Objects in Clouds")
            cloud_failed = validateII.check_objects_cloud(
                cloud_check_waiting,
                cloud_check_validated,
                cloud_check_failed,
                AUTH_SETTINGS,
            )
            logger.info(f"Finished step 2 of 4, {cloud_failed} failed manifests")

            logger.info(f"Starting step 3 of 4, Check Records in IndexD")
            indexd_failed = validateII.check_objects_indexd(
                indexd_check_waiting,
                indexd_check_validated,
                indexd_check_failed,
                AUTH_SETTINGS,
            )
            logger.info(f"Finished step 3 of 4, {indexd_failed} failed manifests")

            if cloud_failed + indexd_failed == 0:
                logger.info(f"Starting step 4 of 4, Create final Manifest")
                final_manifest_passed = validateII.create_final_manifest(
                    final_manifest_waiting, final_manifest_destination, AUTH_SETTINGS
                )
                logger.info(
                    f"Finished step 4 of 4, final manifest created: {final_manifest_passed}"
                )
            else:
                logger.warning(
                    f"Cannot create final manifest, 1 one more manifests have failed\nCloud failed: {cloud_check_failed}, IndexD failed: {indexd_check_failed}"
                )

        elif SKIP_TO.lower() == "indexd":
            logger.info(f"Starting step 3 of 4, Check Records in IndexD")
            indexd_failed = validateII.check_objects_indexd(
                indexd_check_waiting,
                indexd_check_validated,
                indexd_check_failed,
                AUTH_SETTINGS,
            )
            logger.info(f"Finished step 3 of 4, {indexd_failed} failed manifests")

            if indexd_failed == 0:
                logger.info(f"Starting step 4 of 4, Create final Manifest")
                final_manifest_passed = validateII.create_final_manifest(
                    final_manifest_waiting, final_manifest_destination, AUTH_SETTINGS
                )
                logger.info(
                    f"Finished step 4 of 4, final manifest created: {final_manifest_passed}"
                )
            else:
                logger.warning(
                    f"Cannot create final manifest, 1 one more manifests have failed\nIndexD failed: {indexd_check_failed}"
                )

        elif SKIP_TO.lower() == "final":
            logger.info(f"Starting step 4 of 4, Create final Manifest")
            final_manifest_passed = validateII.create_final_manifest(
                final_manifest_waiting, final_manifest_destination, AUTH_SETTINGS
            )
            logger.info(
                f"Finished step 4 of 4, final manifest created: {final_manifest_passed}"
            )

    except Exception as e:
        logger.error(f"The following error occurred during validation: {e}")
        if LOG_SETTINGS["location_type"] == "cloud":
            cloud_utils.upload_cloud(
                LOG_SETTINGS["location"], log_path, log_session, True
            )
        else:
            general_utils.move_resource(
                "local", log_path, LOG_SETTINGS["location"], log_session
            )
