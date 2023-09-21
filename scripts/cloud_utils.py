import os
import boto3
import botocore

from cdislogging import get_logger
from google.cloud import storage

logger = get_logger("CloudUtils")


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
            s3_session = boto3.session.Session(profile_name=profile).resource("s3")
        elif auth_type == "access_key":
            access_key_id = auth_settings.get("value").get("access_key_id")
            access_key = auth_settings.get("value").get("access_key")
            s3_session = boto3.session.Session(
                aws_access_key_id=access_key_id, aws_secret_access_key=access_key
            ).resource("s3")
        else:
            s3_session = boto3.session.Session().resource("s3")

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
            cloud_session.meta.client.head_bucket(Bucket=bucket_name)
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


def access_aws(s3_session, resource_type: str, cloud_path: str, output_path: str):
    """
    Access AWS resources
    s3_session: session
    resource_type: list, object
    cloud_path: s3://PROJECT_BUCKET/ID/path
    output_path: only req for object, path to output resource to
    """
    bucket_name = parse_cloud_path(cloud_path).get("bucket")

    if resource_type == "list":
        try:
            id = parse_cloud_path(cloud_path).get("id")
            bucket = s3_session.Bucket(bucket_name)

            bucket_list = []
            if id:
                for bucket_object in bucket.objects.filter(Prefix=id):
                    bucket_list.append(bucket_object)
            else:
                for bucket_object in bucket.objects.all():
                    bucket_list.append(bucket_object)

            return bucket_list
        except Exception as e:
            raise (f"Unable to return list of objects in s3 bucket {bucket_name}, {e}")
    elif resource_type == "object":
        key = f"{cloud_path.split(f'{bucket_name}/')[1]}"

        try:
            s3_session.meta.client.download_file(bucket_name, key, output_path)
        except Exception as e:
            raise (f"Unable to return object at {key} in s3 bucket {bucket_name}, {e}")
    else:
        raise (
            ValueError(
                f"Only list and object resource type accepted, received {resource_type}"
            )
        )


def access_gcp(gs_session, resource_type: str, cloud_path: str, output_path: str):
    """
    Access GCP resources
    gs_session: session
    resource_type: list, object
    cloud_path: gs://PROJECT_BUCKET/ID/path
    output_path: only req for object, path to output resource to
    """
    bucket_name = parse_cloud_path(cloud_path).get("bucket")

    if resource_type == "list":
        try:
            id = parse_cloud_path(cloud_path).get("id")

            if id:
                blobs = gs_session.list_blobs(bucket_name, prefix=id)
            else:
                blobs = gs_session.list_blobs(bucket_name)

            bucket_list = []
            for blob in blobs:
                bucket_list.append(blob.name)

            return bucket_list
        except Exception as e:
            raise (f"Unable to return list of objects in gs bucket {bucket_name}, {e}")
    elif resource_type == "object":
        key = f"{cloud_path.split(f'{bucket_name}/')[1]}"

        try:
            bucket = gs_session.bucket(bucket_name)
            blob = bucket.blob(key)
            blob.download_to_filename(output_path)

        except Exception as e:
            raise (f"Unable to return object at {key} in gs bucket {bucket_name}, {e}")
    else:
        raise (
            ValueError(
                f"Only list and object resource type accepted, received {resource_type}"
            )
        )


def upload_cloud(cloud_path: str, resource_path: str, session, delete=False):
    """
    Upload to cloud resource
    cloud_path: cloud://PROJECT_BUCKET/path/path
    resource_path: local path resource is located
    session: cloud session
    delete: remove file from local dir after uploading
    """
    cloud_name = parse_cloud_path(cloud_path).get("cloud")
    bucket_name = parse_cloud_path(cloud_path).get("bucket")
    key = f"{cloud_path.split(f'{bucket_name}/')[1]}"

    cloud = cloud_name.lower()
    if os.path.isfile(resource_path):
        if bucket_exists(cloud, session, bucket_name):
            if cloud == "s3":
                try:
                    session.meta.client.upload_file(
                        Filename=resource_path, Bucket=bucket_name, Key=key
                    )
                    return cloud_path

                except Exception as e:
                    raise (f"Failed to upload {key} to {bucket_name}: {e}")
            elif cloud == "gs":
                try:
                    bucket = session.get_bucket(bucket_name)
                    blob = bucket.blob(key)
                    blob.upload_from_filename(resource_path)

                    if delete:
                        os.remove(resource_path)

                    return cloud_path
                except Exception as e:
                    raise (f"Failed to upload {key} to {bucket_name}: {e}")
            else:
                raise ValueError(
                    f"Cloud, {cloud}, is not supported, only s3 and gs accepted."
                )
        else:
            raise Exception(
                f"Bucket {bucket_name} does not exist in {cloud}, cannot upload resource to cloud"
            )
    else:
        raise ValueError(f"{resource_path} is not a file, cannot continue upload")


def move_object_cloud(source_path: str, destination_path: str, session):
    """
    Move resource from one bucket to another
    source_path: cloud://PROJECT_BUCKET/path/path
    destination_path: cloud://PROJECT_BUCKET_2/path/path
    session: cloud session
    """
    source_cloud_name = parse_cloud_path(source_path).get("cloud")
    dest_cloud_name = parse_cloud_path(destination_path).get("cloud")
    source_bucket_name = parse_cloud_path(source_path).get("bucket")
    dest_bucket_name = parse_cloud_path(destination_path).get("bucket")
    source_key = f"{source_path.split(f'{source_bucket_name}/')[1]}"
    dest_key = f"{destination_path.split(f'{dest_bucket_name}/')[1]}"

    src_bucket_exists = bucket_exists(source_cloud_name, session, source_bucket_name)
    dest_bucket_exists = bucket_exists(source_cloud_name, session, dest_bucket_name)
    if source_cloud_name != dest_cloud_name:
        raise Exception(
            f"Moving objects from different clouds is not supported, object at {source_path} was not moved to {destination_path}"
        )

    if src_bucket_exists and dest_bucket_exists:
        if source_cloud_name == "s3":
            copy_source = {"Bucket": source_bucket_name, "Key": source_key}
            session.meta.client.copy(copy_source, dest_bucket_name, dest_key)
            session.meta.client.delete_object(Bucket=source_bucket_name, Key=source_key)

            return destination_path
        elif source_cloud_name == "gs":
            source_bucket = session.bucket(source_bucket_name)
            source_blob = source_bucket.blob(source_key)
            destination_bucket = session.bucket(dest_bucket_name)

            source_bucket.copy_blob(source_blob, destination_bucket, dest_key)
            source_bucket.delete_blob(source_key)

            return destination_path
        else:
            raise ValueError(
                f"Cloud, {source_cloud_name}, is not supported, only s3 and gs accepted."
            )
    else:
        raise Exception(
            f"Bucket {source_bucket_name} found: {src_bucket_exists} or {dest_bucket_name} found: {dest_bucket_exists} does not exist in {source_cloud_name}, cannot move resource"
        )


def remove_object_cloud(path: str, session):
    """
    Remove resource from bucket
    path: cloud://PROJECT_BUCKET/path/path
    session: cloud session
    """
    cloud_name = parse_cloud_path(path).get("cloud")
    bucket_name = parse_cloud_path(path).get("bucket")
    key = f"{path.split(f'{bucket_name}/')[1]}"

    if bucket_exists(cloud_name, session, bucket_name):
        if cloud_name == "s3":
            session.meta.client.delete_object(Bucket=bucket_name, Key=key)

            return True
        elif cloud_name == "gs":
            bucket = session.bucket(bucket_name)
            bucket.delete_blob(key)

            return True
        else:
            raise ValueError(
                f"Cloud, {cloud_name}, is not supported, only s3 and gs accepted."
            )
    else:
        raise Exception(
            f"Bucket {bucket_name} does not exist in {cloud_name}, cannot remove resource"
        )
