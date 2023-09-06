import os
import file_utils
import cloud_utils


def _get_cloud(location_type: str, cloud_path: str, session, output_path: str):
    """
    Access cloud resource
    location_type: list, object
    cloud_path: cloud://PROJECT_BUCKET/ID/FILE_NAME
    session: cloud session
    output_path: only req for object, path to output resource to
    """
    cloud_name = cloud_utils.parse_cloud_path(cloud_path).get("cloud")
    bucket_name = cloud_utils.parse_cloud_path(cloud_path).get("bucket")

    cloud = cloud_name.lower()
    if cloud_utils.bucket_exists(cloud, session, bucket_name):
        if cloud == "s3":
            return cloud_utils.access_aws(
                session, location_type, cloud_path, output_path
            )
        elif cloud == "gs":
            return cloud_utils.access_gcp(
                session, location_type, cloud_path, output_path
            )
        else:
            raise ValueError(
                f"Cloud, {cloud}, is not supported, only s3 and gs accepted."
            )
    else:
        raise Exception(
            f"Bucket {bucket_name} does not exist in {cloud}, cannot get resource from cloud"
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
        return _get_cloud("list", path, session)
    elif location_type == "local":
        if os.path.isdir(path):
            return os.listdir(path)
        else:
            raise ValueError(
                f"Path {path} is not a directory, cannot list objects inside directory"
            )
    else:
        raise ValueError(
            f"Location, {location_type}, is not supported, only cloud and local accepted."
        )


def get_resource_object(location_type: str, path: str, output_path: str, session):
    """
    Retrieve resources from location
    location_type: cloud, local
    path: path to resource
    output_path: path to output resource to
    session: req for cloud access
    """
    location_type = location_type.lower()
    if location_type == "cloud":
        object_path = _get_cloud("object", path, session, output_path)
    elif location_type == "local":
        object_path = path
    else:
        raise ValueError(
            f"Location, {location_type}, is not supported, only cloud and local accepted."
        )

    #  TODO: make sure proper error handling in file_utils
    return file_utils.file_to_listdict(object_path)


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
        # move resource
        ""
    else:
        raise ValueError(
            f"Location, {location_type}, is not supported, only cloud and local accepted."
        )


def remove_resources(location_type: str, path: str, session):
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
        # remove local
        ""
    else:
        raise ValueError(
            f"Location, {location_type}, is not supported, only cloud and local accepted."
        )


def proposed_urls(manifest_content: list):
    """
    Collect urls from manifest and return nested dict of urls
    manifest_content: list of dictionaries
    """
    urls = {}
    for record in manifest_content:
        urls[record["id"]] = {}
        for url in record["indexd_url"]:
            if "gs://" in url:
                src = "gs"
            elif "s3://" in url:
                src = "s3"
            elif "http" in url:
                src = "indexd"
            urls[record["id"]][src] = url

    return urls


def check_subset_in_list(subset_list, main_list):
    """
    Returns two lists, items found and not found in list
    """
