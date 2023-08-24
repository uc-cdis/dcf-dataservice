import file_utils
import cloud_utils


def local_list(resource_path: str):
    """
    Return local folder list
    resource_path: local path resource is located
    """
    return []


def local_file(resource_path: str):
    """
    Return local file
    resource_path: local path resource is located
    """


def _get_local(
    location_type: str,
    resource_path: str,
):
    """
    Access resource of local path
    location_type: local_folder, local_file
    resource_path: local path resource is located
    """
    if location_type == "list":
        return local_list(resource_path)
    elif location_type == "object":
        return local_file(resource_path)


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
        return _get_local("list", path)
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
        path = _get_cloud("object", path, session, output_path)
    elif location_type == "local":
        path = _get_local("object", path)
    else:
        raise ValueError(
            f"Location, {location_type}, is not supported, only cloud and local accepted."
        )

    #  TODO: make sure proper error handling in file_utils
    return file_utils.file_to_list(path)


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


def move_resource(k):
    """"""
