import os
import shutil

import utils_file
import utils_cloud


def _get_cloud(session, resource_type: str, cloud_path: str, output_path: str):
    """
    Access cloud resource
    session: cloud session
    resource_type: list, object
    cloud_path: cloud://PROJECT_BUCKET/ID/FILE_NAME
    output_path: only req for object, path to output resource to
    """
    cloud_name = cloud_utils.parse_cloud_path(cloud_path).get("cloud")
    bucket_name = cloud_utils.parse_cloud_path(cloud_path).get("bucket")

    cloud = cloud_name.lower()
    if cloud_utils.bucket_exists(cloud, session, bucket_name):
        if cloud == "s3":
            return cloud_utils.access_aws(
                session, resource_type, cloud_path, output_path
            )
        elif cloud == "gs":
            return cloud_utils.access_gcp(
                session, resource_type, cloud_path, output_path
            )
        else:
            raise ValueError(
                f"Cloud, {cloud}, is not supported, only s3 and gs accepted, cannot get {resource_type}."
            )
    else:
        raise Exception(
            f"Bucket {bucket_name} does not exist in {cloud}, cannot get {resource_type}."
        )


def get_resource_list(location_type: str, path: str, session):
    """
    Retrieve resources from location
    location_type: cloud, local
    path: path to resource
    session: req for cloud access
    """
    location_type = location_type.lower()
    if location_type == "cloud":
        return _get_cloud(session, "list", path)
    elif location_type == "local":
        if os.path.isdir(path):
            return os.listdir(path)
        else:
            raise ValueError(f"Path {path} is not a directory, cannot get list.")
    else:
        raise ValueError(
            f"{location_type} is not supported, only cloud and local accepted, cannot get list."
        )


def get_resource_object(location_type: str, path: str, output_path: str, session):
    """
    Retrieve resources from location
    location_type: cloud, local
    path: path to resource
    output_path: path to output resource to
    session: req for cloud access
    """
    try:
        location_type = location_type.lower()
        if location_type == "cloud":
            _get_cloud(session, "object", path, output_path)
        else:
            if location_type != "local":
                raise ValueError(
                    f"{location_type} is not supported, only cloud and local accepted, cannot get object"
                )

        return file_utils.file_to_listdict(output_path)
    except Exception as e:
        raise e


def move_resource(location_type: str, src_path: str, dest_path: str, session):
    """
    Retrieve resources from location
    location_type: cloud, local
    path: path to resource
    output_path: path to output resource to
    session: req for cloud access
    """
    location_type = location_type.lower()
    if location_type == "cloud":
        cloud_utils.move_object_cloud(src_path, dest_path, session)

    elif location_type == "local":
        shutil.move(src_path, dest_path)

    else:
        raise ValueError(
            f"{location_type} is not supported, only cloud and local accepted, cannot move object."
        )


def remove_resource(location_type: str, path: str, session):
    """
    Retrieve resources from location
    location_type: cloud, local
    path: path to resource
    session: req for cloud access
    """
    location_type = location_type.lower()
    if location_type == "cloud":
        cloud_utils.remove_object_cloud(path, session)
    elif location_type == "local":
        os.remove(path)
    else:
        raise ValueError(
            f"{location_type} is not supported, only cloud and local accepted, cannot remove object."
        )


def record_to_url_dict(manifest_content: list):
    """
    Return nested dict, id:src:url
    manifest_content: list of dictionaries
    """
    id_src_dict = {}
    for record in manifest_content:
        id_src_dict[record["id"]] = {}
        for url in record["indexd_url"]:
            if "gs://" in url:
                src = "gs"
            elif "s3://" in url:
                src = "s3"
            elif "http" in url:
                src = "indexd"
            else:
                src = "unsupported"
            id_src_dict[record["id"]][src] = url

    return id_src_dict


def check_subset_in_list(subset_list: list, main_list: list, check_reverse=False):
    """
    Returns two lists, items found and not found in list
    """
    found = []
    missing = []
    extra = []
    for item in subset_list:
        if item in main_list:
            found.append(item)
        else:
            missing.append(item)

    if check_reverse:
        for item in main_list:
            if item not in subset_list:
                extra.append(item)

    return found, missing, extra
