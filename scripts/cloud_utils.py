import boto3
import botocore

from cdislogging import get_logger
from google.cloud import storage

global logger


def resume_logger(filename=None):
    global logger
    logger = get_logger("Cloud Utils", filename)


def start_session(cloud: str, auth_settings: dict):
    """
    Start session for given cloud provider
    cloud: gs, s3
    auth_settings: dict with auth information
    s3: profile, access_key
    gs: using auth via service account https://cloud.google.com/sdk/docs/authorizing
    gcloud config set account '$ACCOUNT'
    """
    if cloud == "s3":
        auth_type = auth_settings.get("auth_type")
        if auth_type == "profile":
            profile = auth_settings.get("value")
            s3_session = boto3.session.Session(profile_name=profile)
        elif auth_type == "access_key":
            access_key_id = auth_settings.get("value").get("access_key_id")
            access_key = auth_settings.get("value").get("access_key")
            s3_session = boto3.session.Session(
                aws_access_key_id=access_key_id, aws_secret_access_key=access_key
            )
        else:
            s3_session = boto3.session.Session()

        return s3_session

    elif cloud == "gs":
        gs_session = storage.Client()
        return gs_session

    else:
        raise ValueError(f"Cloud, {cloud}, is not supported, only s3 and gs accepted.")


def parse_cloud_path(path: str):
    """
    Return dict of path
    path: cloud://PROJECT_BUCKET/ID/FILE_NAME
    """
    path_dict = {}
    try:
        cloud = path.split("://")[0]
        bucket = path.split("://")[1].split("/")[0]
        path_dict["cloud"] = cloud.lower()
        path_dict["bucket"] = bucket.lower()
    except:
        raise ValueError(
            f"Path must contain at least a cloud provider and bucket, i.e. s3://test-bucket"
        )
    try:
        id = path.split("://")[1].split("/")[1].split("/")[0]
        path_dict["id"] = id
    except:
        path_dict["id"] = None
    try:
        file_name = path.split("://")[1].split("/")[1].split("/")[1]
        path_dict["file_name"] = file_name
    except:
        path_dict["file_name"] = None

    return path_dict


def bucket_exists(cloud: str, cloud_session, bucket_name: str):
    """
    Check if bucket exists in cloud
    cloud: gs, s3
    cloud_session: session
    bucket_name: str bucket name
    """
    if cloud == "s3":
        try:
            resource = cloud_session.resource("s3")
            resource.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 403:
                logger.warning(f"{bucket_name} Forbidden Access")
                return True
            elif error_code == 404:
                logger.info(f"{bucket_name} Bucket Does Not Exist!")
                return False
            else:
                logger.error(f"Unexpected error: {error_code}")
                return False

    elif cloud == "gs":
        bucket = storage.Bucket(cloud_session, bucket_name)
        return bucket.exists()

    else:
        raise Exception(f"{cloud} not supported, cannot check for bucket")


def aws_bucket_list(s3_session, cloud_path: str):
    """
    Return AWS bucket content list
    s3_session: session
    cloud_path: s3://PROJECT_BUCKET/ID/FILE_NAME
    """
    try:
        bucket_name = parse_cloud_path(cloud_path).get("bucket")
        id = parse_cloud_path(cloud_path).get("id")

        resource = s3_session.resource("s3")
        bucket = resource.Bucket(bucket_name)

        bucket_list = []
        if id is not None:
            for bucket_object in bucket.objects.filter(Prefix=id):
                bucket_list.append(bucket_object)
        else:
            for bucket_object in bucket.objects.all():
                bucket_list.append(bucket_object)

        return bucket_list
    except Exception as e:
        raise (f"Unable to return list of objects in s3 bucket {bucket_name}, {e}")


def gcp_bucket_list(gs_session, cloud_path: str):
    """
    Return GCP bucket content list
    gs_session: session
    cloud_path: gs://PROJECT_BUCKET/ID/FILE_NAME
    """
    try:
        bucket_name = parse_cloud_path(cloud_path).get("bucket")
        id = parse_cloud_path(cloud_path).get("id")

        if id is not None:
            blobs = gs_session.list_blobs(bucket_name, prefix=id)
        else:
            blobs = gs_session.list_blobs(bucket_name)

        bucket_list = []
        for blob in blobs:
            bucket_list.append(blob.name)

        return bucket_list
    except Exception as e:
        raise (f"Unable to return list of objects in gs bucket {bucket_name}, {e}")


def aws_bucket_object(s3_session, resource_path: str, output_path: str):
    """
    Return downloaded AWS bucket object location
    s3_session: session
    resource_path: s3://PROJECT_BUCKET/ID/FILE_NAME
    output_path: path to output resource to
    """
    bucket_name = parse_cloud_path(resource_path).get("bucket")
    file_name = parse_cloud_path(resource_path).get("file_name")
    key = f"{parse_cloud_path(resource_path).get('id')}/{file_name}"

    try:
        resource = s3_session.resource("s3")
        resource.meta.client.download_file(bucket_name, key, output_path)
        return output_path
    except Exception as e:
        raise (f"Unable to return object {file_name} in s3 bucket {bucket_name}, {e}")


def gcp_bucket_object(gs_session, resource_path: str, output_path: str):
    """
    Return downloaded GCP bucket object location
    gs_session: session
    resource_path: gs://PROJECT_BUCKET/ID/FILE_NAME
    output_path: path to output resource to
    """
    try:
        bucket_name = parse_cloud_path(resource_path).get("bucket")
        file_name = parse_cloud_path(resource_path).get("file_name")
        key = f"{parse_cloud_path(resource_path).get('id')}/{file_name}"

        return output_path
    except Exception as e:
        raise (f"Unable to return object {file_name} in g3 bucket {bucket_name}, {e}")


def access_aws(s3_session, location_type: str, cloud_path: str, output_path: str):
    """
    Access AWS resources
    s3_session: session
    location_type: list, object
    cloud_path: s3://PROJECT_BUCKET/ID/FILE_NAME
    output_path: only req for object, path to output resource to
    """
    if location_type == "list":
        return aws_bucket_list(s3_session, cloud_path)
    elif location_type == "object":
        return aws_bucket_object(s3_session, cloud_path, output_path)
    else:
        raise (
            ValueError(
                f"Only list and object location type accepted, received {location_type}"
            )
        )


def access_gcp(gs_session, location_type: str, cloud_path: str, output_path: str):
    """
    Access GCP resources
    gs_session: session
    location_type: list, object
    cloud_path: gs://PROJECT_BUCKET/ID/FILE_NAME
    output_path: only req for object, path to output resource to
    """
    if location_type == "list":
        return gcp_bucket_list(gs_session, cloud_path)
    elif location_type == "object":
        return gcp_bucket_object(gs_session, cloud_path, output_path)
    else:
        raise (
            ValueError(
                f"Only list and object location type accepted, received {location_type}"
            )
        )


def upload_aws(s3_session, cloud_path: str, resource_path: str):
    """
    Upload AWS resources
    s3_session: session
    cloud_path: s3://PROJECT_BUCKET/ID/FILE_NAME
    resource_path: local path resource is located
    """
    bucket = parse_cloud_path(cloud_path).get("bucket")
    file_name = parse_cloud_path(cloud_path).get("file_name")
    key = f"{parse_cloud_path(cloud_path).get('id')}/{file_name}"

    try:
        s3_session.meta.client.upload_file(
            Filename=resource_path, Bucket=bucket, Key=key
        )
    except Exception as e:
        raise (f"Failed to upload {file_name} to {bucket}: {e}")


def upload_gcp(gs_session, cloud_path: str, resource_path: str):
    """
    Upload GCP resources
    gs_session: session
    cloud_path: gs://PROJECT_BUCKET/ID/FILE_NAME
    resource_path: local path resource is located
    """
    bucket = parse_cloud_path(cloud_path).get("bucket")
    file_name = parse_cloud_path(cloud_path).get("file_name")
    key = f"{parse_cloud_path(cloud_path).get('id')}/{file_name}"

    try:
        bucket = gs_session.get_bucket(bucket)
        blob = bucket.blob(key)
        blob.upload_from_filename(resource_path)
    except Exception as e:
        raise (f"Failed to upload {file_name} to {bucket}: {e}")


def upload_cloud(cloud_path: str, resource_path: str, session):
    """
    Upload to cloud resource
    cloud_path: cloud://PROJECT_BUCKET/ID/FILE_NAME
    resource_path: local path resource is located
    session: cloud session
    """
    cloud_name = parse_cloud_path(cloud_path).get("cloud")
    bucket_name = parse_cloud_path(cloud_path).get("bucket")

    cloud = cloud_name.lower()
    if bucket_exists(cloud, session, bucket_name):
        if cloud == "s3":
            return upload_aws(session, cloud_path, resource_path)
        elif cloud == "gs":
            return upload_gcp(session, cloud_path, resource_path)
        else:
            raise ValueError(
                f"Cloud, {cloud}, is not supported, only s3 and gs accepted."
            )
    else:
        raise Exception(
            f"Bucket {bucket_name} does not exist in {cloud}, cannot upload resource to cloud"
        )


def move_object_cloud():
    """"""
