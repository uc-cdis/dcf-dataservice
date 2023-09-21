import os

import file_utils
import cloud_utils
import general_utils
import dcf_utils

from indexclient.client import IndexClient
from cdislogging import get_logger

logger = get_logger("Validation")

# Manifest


def proposed_manifest(
    waiting_location: dict, ready_location: dict, AUTH_SETTINGS: dict
):
    """
    Sort manifest by project and segment into 10 smaller manifests, while also processing to create proposed urls with info in manifest
    """

    def _create_output_record(record_row: dict):
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

    waiting_location_type = waiting_location.get("location_type")
    waiting_location_path = waiting_location.get("location")

    ready_location_type = ready_location.get("location_type")
    ready_location_path = ready_location.get("location")

    waiting_session = None
    if waiting_location_type == "cloud":
        waiting_cloud = cloud_utils.parse_cloud_path(f"{waiting_location_path}").get(
            "cloud"
        )
        waiting_session = cloud_utils.start_session(
            waiting_cloud, AUTH_SETTINGS[waiting_cloud]
        )

    ready_session = None
    if ready_location_type == "cloud":
        ready_cloud = cloud_utils.parse_cloud_path(f"{ready_location_path}").get(
            "cloud"
        )
        ready_session = cloud_utils.start_session(
            ready_cloud, AUTH_SETTINGS[ready_cloud]
        )

    manifest_list = general_utils.get_resource_list(
        waiting_location_type, waiting_location_path, waiting_session
    )

    manifest_list_len = len(manifest_list)
    manifest_list_pos = 0

    failed_manifests = 0

    for manifest in manifest_list:
        try:
            manifest_list_pos += 1
            logger.info(f"Sorting manifest {manifest_list_pos}/{manifest_list_len}")

            waiting_file_path = f"{waiting_location_path}/{manifest}"
            output_path = f"{cwd}/{manifest}"

            content = general_utils.get_resource_object(
                waiting_location_type,
                waiting_file_path,
                output_path,
                waiting_session,
            )

            record_list = []
            # headers = file_utils.get_headers(output_path)
            headers = [
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
            for record_dict in content:
                record_list.append(_create_output_record(record_dict))

            project_header_pos = headers.index("project_id")
            record_list.sort(key=lambda x: x[project_header_pos])
            record_list.insert(0, headers)

            resource_path = f"{cwd}/{manifest}_DCFCREATED{manifest_list_pos}.tsv"
            file_utils.write(record_list, resource_path)
            dest_path = f"{ready_location}/{manifest}_DCFCREATED{manifest_list_pos}.tsv"
            if ready_location_type == "cloud":
                cloud_utils.upload_cloud(dest_path, resource_path, ready_session, True)
            else:
                general_utils.move_resource("local", resource_path, dest_path)

            general_utils.remove_resource(
                waiting_location_type, waiting_file_path, waiting_session
            )
        except Exception as e:
            failed_manifests += 1
            logger.error(
                f"Manifest, {manifest}, failed during the creation of proposed manifest\n{e}"
            )

    return failed_manifests


# Check Cloud


def check_objects_cloud(
    waiting_location: str,
    validated_location: str,
    failed_location: str,
    AUTH_SETTINGS: dict,
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

    failed_manifests = 0

    cloud_sessions = {
        "gs": cloud_utils.start_session("gs", AUTH_SETTINGS["gs"]),
        "s3": cloud_utils.start_session("s3", AUTH_SETTINGS["s3"]),
    }

    for manifest in manifest_list:
        try:
            manifest_list_pos += 1
            logger.info(
                f"Validating cloud for manifest {manifest_list_pos}/{manifest_list_len}"
            )

            waiting_file_path = f"{waiting_location_path}/{manifest}"
            output_path = f"{cwd}/{manifest}"

            content = general_utils.get_resource_object(
                waiting_location_type,
                waiting_file_path,
                output_path,
                waiting_session,
            )

            proposed_urls = general_utils.record_to_url_dict(content)
            proposed_bucket_list = []
            proposed_url_list = []
            for src_dict in proposed_urls.values():
                gs_bucket = cloud_utils.parse_cloud_path(src_dict["gs"]).get("bucket")
                s3_bucket = cloud_utils.parse_cloud_path(src_dict["s3"]).get("bucket")
                proposed_bucket_list.append(f"gs://{gs_bucket}")
                proposed_bucket_list.append(f"s3://{s3_bucket}")
                proposed_url_list.append(src_dict["s3"])
                proposed_url_list.append(src_dict["gs"])

            unique_buckets = set(proposed_bucket_list)
            bucket_list = list(unique_buckets)
            bucket_urls = _list_bucket_urls(bucket_list, cloud_sessions)

            validated_subset, failed_subset = general_utils.check_subset_in_list(
                proposed_url_list, bucket_urls
            )

            id_content = {}
            for record_dict in content:
                id_content[record_dict["id"]] = list(record_dict.values())

            validated_records = []
            failed_records = []
            for id, url_dict in proposed_urls.items():
                all_present = True
                for url in url_dict.values():
                    if url in failed_subset:
                        all_present = False
                        logger.warning(f"{url} not copied to cloud")
                if all_present:
                    validated_records.append(id_content[id])
                else:
                    failed_records.append(id_content[id])

            validated_file_path = (
                f"{cwd}/{manifest[:-4]}_cloud_validated{manifest[-4:]}"
            )
            file_utils.write(validated_records, validated_file_path)
            dest_validated_file_path = f"{validated_location_path}/{manifest[:-4]}_cloud_validated{manifest[-4:]}"
            if validated_location_type == "cloud":
                cloud_utils.upload_cloud(
                    dest_validated_file_path,
                    validated_file_path,
                    validated_session,
                    True,
                )
            else:
                general_utils.move_resource(
                    "local", validated_file_path, dest_validated_file_path
                )

            general_utils.remove_resource(
                waiting_location_type, waiting_file_path, waiting_session
            )

            if len(failed_subset) > 0:
                failed_list_path = (
                    f"{cwd}/{manifest[:-4]}_cloud_failed_list{manifest[-4:]}"
                )
                failed_file_path = f"{cwd}/{manifest[:-4]}_cloud_failed{manifest[-4:]}"
                file_utils.write(failed_subset, failed_list_path)
                file_utils.write(failed_records, failed_file_path)
                dest_failed_list_path = f"{failed_location_path}/{manifest[:-4]}_cloud_failed_list{manifest[-4:]}"
                dest_failed_file_path = f"{failed_location_path}/{manifest[:-4]}_cloud_failed{manifest[-4:]}"
                if failed_location_type == "cloud":
                    cloud_utils.upload_cloud(
                        dest_failed_list_path, failed_list_path, failed_session, True
                    )
                    cloud_utils.upload_cloud(
                        dest_failed_file_path, failed_file_path, failed_session, True
                    )
                else:
                    general_utils.move_resource(
                        "local", failed_list_path, dest_failed_list_path
                    )
                    general_utils.move_resource(
                        "local", failed_file_path, dest_failed_file_path
                    )

        except Exception as e:
            failed_manifests += 1
            logger.error(
                f"Manifest, {manifest}, encountered an error during the validation of objects in clouds\n{e}"
            )

    return failed_manifests


def _list_bucket_urls(bucket_list: list, cloud_session):
    """
    Returns list of urls in bucket
    """
    bucket_urls = []
    for bucket in bucket_list:
        try:
            if "gs" in bucket:
                bucket_contents = general_utils.get_resource_list(
                    "cloud", bucket, cloud_session["gs"]
                )
            elif "s3" in bucket:
                bucket_contents = general_utils.get_resource_list(
                    "cloud", bucket, cloud_session["s3"]
                )
            else:
                raise ValueError(
                    f"{bucket} does not reside in supported cloud environment, i.e. gs, s3"
                )
            bucket_urls += bucket_contents
        except Exception as e:
            logger.error(f"Could not list resources in bucket {bucket}: {e}")
    return bucket_urls


# Check objects in IndexD


def check_objects_indexd(
    waiting_location: str,
    validated_location: str,
    failed_location: str,
    AUTH_SETTINGS: dict,
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

    failed_manifests = 0

    for manifest in manifest_list:
        try:
            manifest_list_pos += 1
            logger.info(
                f"Validating IndexD for manifest {manifest_list_pos}/{manifest_list_len}"
            )

            waiting_file_path = f"{waiting_location_path}/{manifest}"
            output_path = f"{cwd}/{manifest}"

            content = general_utils.get_resource_object(
                waiting_location_type,
                waiting_file_path,
                output_path,
                waiting_session,
            )

            proposed_urls = general_utils.record_to_url_dict(content)
            proposed_url_list = []
            for src_dict in proposed_urls.values():
                proposed_url_list = proposed_url_list + list(src_dict.values())

            indexd_records = _list_indexd_urls(
                list(proposed_urls.keys()), AUTH_SETTINGS["indexd"]
            )

            (
                validated_subset,
                failed_subset,
                extras,
            ) = general_utils.check_subset_in_list(
                proposed_url_list, indexd_records, True
            )

            id_content = {}
            for record_dict in content:
                id_content[record_dict["id"]] = list(record_dict.values())

            validated_records = []
            failed_records = []
            for id, url_dict in proposed_urls.items():
                all_present = True
                for url in url_dict.values():
                    if url in failed_subset:
                        all_present = False
                        logger.warning(f"{url} not indexed")
                if all_present:
                    validated_records.append(id_content[id])
                else:
                    failed_records.append(id_content[id])

            validated_file_path = (
                f"{cwd}/{manifest[:-4]}_index_validated{manifest[-4:]}"
            )
            file_utils.write(validated_records, validated_file_path)
            dest_validated_file_path = f"{validated_location_path}/{manifest[:-4]}_index_validated{manifest[-4:]}"
            if validated_location_type == "cloud":
                cloud_utils.upload_cloud(
                    dest_validated_file_path,
                    validated_file_path,
                    validated_session,
                    True,
                )
            else:
                general_utils.move_resource(
                    "local", validated_file_path, dest_validated_file_path
                )

            general_utils.remove_resource(
                waiting_location_type, waiting_file_path, waiting_session
            )

            if len(failed_subset) > 0:
                failed_list_path = (
                    f"{cwd}/{manifest[:-4]}_index_failed_list{manifest[-4:]}"
                )
                failed_file_path = f"{cwd}/{manifest[:-4]}_index_failed{manifest[-4:]}"
                file_utils.write(failed_subset, failed_list_path)
                file_utils.write(failed_records, failed_file_path)
                dest_failed_list_path = f"{failed_location_path}/{manifest[:-4]}_index_failed_list{manifest[-4:]}"
                dest_failed_file_path = f"{failed_location_path}/{manifest[:-4]}_index_failed{manifest[-4:]}"
                if failed_location_type == "cloud":
                    cloud_utils.upload_cloud(
                        dest_failed_list_path, failed_list_path, failed_session, True
                    )
                    cloud_utils.upload_cloud(
                        dest_failed_file_path, failed_file_path, failed_session, True
                    )
                else:

                    general_utils.move_resource(
                        "local", failed_list_path, dest_failed_list_path
                    )
                    general_utils.move_resource(
                        "local", failed_file_path, dest_failed_file_path
                    )

        except Exception as e:
            failed_manifests += 1
            logger.error(
                f"Manifest, {manifest}, failed during the validation of records in IndexD\n{e}"
            )

    return failed_manifests


def _list_indexd_urls(ids: list, indexd_settings: dict):
    """
    Returns list of urls in indexd
    """
    indexd_client = IndexClient(
        indexd_settings["host"],
        indexd_settings["version"],
        (indexd_settings["auth"]["username"], indexd_settings["auth"]["password"]),
    )
    urls = []
    for id in ids:
        try:
            doc = indexd_client.get(id)
            if doc is not None:
                urls = urls + doc.urls
            else:
                logger.warning(
                    f"Could not get urls from Indexd for {id}, empty results"
                )
        except Exception as e:
            raise (f"Could not get record from Indexd for {id}: {e}")
    return urls


# Final Manifest


def create_final_manifest(
    waiting_location: dict, destination_location, AUTH_SETTINGS: dict
):
    """
    Create final output manifest file
    """
    final_manifest_created = False

    cwd = os.getcwd()

    waiting_location_type = waiting_location.get("location_type")
    waiting_location_path = waiting_location.get("location")

    destination_location_type = destination_location.get("location_type")
    destination_location_path = destination_location.get("location")

    waiting_session = None
    if waiting_location_type == "cloud":
        waiting_cloud = cloud_utils.parse_cloud_path(f"{waiting_location_path}").get(
            "cloud"
        )
        waiting_session = cloud_utils.start_session(
            waiting_cloud, AUTH_SETTINGS[waiting_cloud]
        )

    destination_session = None
    if destination_location_type == "cloud":
        destination_cloud = cloud_utils.parse_cloud_path(
            f"{destination_location_path}"
        ).get("cloud")
        destination_session = cloud_utils.start_session(
            destination_cloud, AUTH_SETTINGS[destination_cloud]
        )

    manifest_list = general_utils.get_resource_list(
        waiting_location_type, waiting_location_path, waiting_session
    )
    manifest_list_len = len(manifest_list)
    manifest_list_pos = 0

    all_content = []
    for manifest in manifest_list:
        manifest_list_pos += 1
        logger.info(
            f"Creating final manifest, processing manifest {manifest_list_pos}/{manifest_list_len}"
        )

        waiting_file_path = f"{waiting_location_path}/{manifest}"
        output_path = f"{cwd}/{manifest}"

        content = general_utils.get_resource_object(
            waiting_location_type,
            waiting_file_path,
            output_path,
            waiting_session,
        )
        all_content = all_content + content

    final = []
    for dict_ in all_content:
        record = list(dict_.values())
        final.append(record)

    final_file_path = f"{cwd}/DCF_final.tsv"
    file_utils.write(final, final_file_path)
    dest_path = f"{destination_location_path}/DCF_final.tsv"
    if destination_location_type == "cloud":
        cloud_utils.upload_cloud(dest_path, final_file_path, destination_session, True)
    else:
        general_utils.move_resource("local", final_file_path, dest_path)

    return final_manifest_created


# Check Access


def check_access():
    """
    Check access by polling a project from each project (bucket) and checking for permissions
    """
