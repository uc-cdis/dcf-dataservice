import subprocess

# Python 2 and 3 compatible
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch

import pytest
import google
import scripts.aws_replicate
from scripts.aws_replicate import AWSBucketReplication
from scripts.google_replicate import exec_google_copy, resumable_streaming_copy

from scripts.errors import APIError
from scripts import utils

TEST_UUID = "11111111111111111111111111111111"
TEST_FILENAME = "test"
TEST_URL = ["test_url1", "test_url2"]


def gen_mock_manifest_data():
    fake = {
        "id": "11111111111111111",
        "file_name": "abc.bam",
        "size": 1,
        "md5": "1223344543t34mt43tb43ofh",
        "acl": "tgca",
        "project_id": "TGCA",
    }
    return 16 * [fake]


@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.check_blob_name_exists_and_match_md5_size")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_called(
    mock_bucket_exists, mock_check_blob, mock_client
):
    """
    test that resumable_streaming_function is called
    """

    mock_bucket_exists.return_value = True
    mock_check_blob.return_value = False
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.utils.get_google_bucket_name = MagicMock()
    scripts.utils.get_google_bucket_name.side_effect = ["test", "test"]
    exec_google_copy(
        {"id": "test_file_id", "file_name": "test file name", "project_id": "TCGA"}, {}
    )
    assert scripts.google_replicate.resumable_streaming_copy.called == True


@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.check_blob_name_exists_and_match_md5_size")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_not_called_due_to_existed_blob(
    mock_bucket_exists, mock_check_blob, mock_client
):
    """
    test that streaming function is not called due to the existed object
    """
    mock_bucket_exists.return_value = True
    mock_check_blob.return_value = True
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.utils.get_google_bucket_name = MagicMock()
    scripts.utils.get_google_bucket_name.return_value = "test"
    exec_google_copy(
        {"id": "test_file_id", "file_name": "test file name", "project_id": "TCGA"}, {}
    )
    assert scripts.google_replicate.resumable_streaming_copy.called == False


@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_not_called_due_to_not_existed_bucket(
    mock_bucket_exists
):
    """
    test that streaming function is not called due to the not-existed bucket
    """
    mock_bucket_exists.return_value = False
    google.cloud.storage.Client = MagicMock()
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.utils.get_google_bucket_name = MagicMock()
    scripts.utils.get_google_bucket_name.return_value = "test"
    exec_google_copy(
        {"id": "test_file_id", "file_name": "test file name", "project_id": "TCGA"}, {}
    )
    assert scripts.google_replicate.resumable_streaming_copy.called == False


@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.check_blob_name_exists_and_match_md5_size")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_called_one_time(
    mock_bucket_exists, mock_check_blob, mock_client
):
    """
    test that the streaming called only one time
    The object is successfully copied
    """
    mock_bucket_exists.return_value = True
    mock_check_blob.side_effect = [False, True]
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.google_replicate.update_url = MagicMock()
    scripts.utils.get_google_bucket_name = MagicMock()
    scripts.utils.get_google_bucket_name.return_value = "test"
    exec_google_copy(
        {"id": "test_file_id", "file_name": "test file name", "project_id": "TCGA"}, {}
    )
    assert scripts.google_replicate.resumable_streaming_copy.call_count == 1
    assert scripts.google_replicate.update_url.call_count == 1


@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.check_blob_name_exists_and_match_md5_size")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_called_two_time(
    mock_bucket_exists, mock_check_blob, mock_client
):
    """
    Test that the streamming called upto 2 times (the first time fails)
    """
    mock_bucket_exists.return_value = True
    mock_check_blob.side_effect = [False, False, True]
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.utils.get_google_bucket_name = MagicMock()
    scripts.utils.get_google_bucket_name.return_value = "test"
    exec_google_copy(
        {"id": "test_file_id", "file_name": "test file name", "project_id": "TCGA"}, {}
    )
    assert scripts.google_replicate.resumable_streaming_copy.call_count == 1


@patch("google.cloud.storage.Client")
@patch("requests.get")
def test_streamUpload_called(mock_requests_get, mock_client):
    """
    Test that the uploading object is called
    """

    class Mock_Requests(object):
        def __init__(self, _status_code):
            self.status_code = _status_code

    mock_value = Mock_Requests(200)
    mock_requests_get.return_value = mock_value
    scripts.google_replicate.streaming = MagicMock()
    resumable_streaming_copy(
        {"fileid": "test_file_id", "file_name": "test file name"},
        mock_client,
        "bucket_test",
        "blob_test",
        {},
    )
    assert scripts.google_replicate.streaming.called


@patch("google.cloud.storage.Client")
@patch("requests.get")
def test_streamUpload_not_called(mock_requests_get, mock_client):
    """
    Test that the uploading method is not called due to wrong url
    """

    class Mock_Requests(object):
        def __init__(self, status_code, message):
            self.status_code = status_code
            self.message = message

    mock_requests_get.return_value = Mock_Requests(status_code=400, message="error")
    scripts.google_replicate.streaming = MagicMock()
    with pytest.raises(APIError):
        resumable_streaming_copy(
            {"id": "test_file_id", "file_name": "test file name"},
            mock_client,
            "bucket_test",
            "blob_test",
            {},
        )
    assert not scripts.google_replicate.streaming.called


def test_call_aws_copy_cli_called():
    """
    Test that the aws cli called
    """

    subprocess.Popen = MagicMock()
    utils.get_aws_bucket_name = MagicMock()
    utils.get_aws_bucket_name.return_value = "TCGA-open"
    AWSBucketReplication.build_source_bucket_dataset = MagicMock()
    AWSBucketReplication.get_copied_objects = MagicMock()
    AWSBucketReplication.build_source_bucket_dataset.return_value = {
        "11111111111111111/abc.bam": "STANDARD"
    }
    instance = AWSBucketReplication(
        bucket="test_bucket",
        manifest_file="test_manifest",
        thread_num=1,
        global_config={"chunk_size": 1},
        job_name="copying",
    )
    instance.exec_aws_copy(gen_mock_manifest_data()[0:1])
    assert subprocess.Popen.call_count == 1


def test_call_aws_copy_cli_no_called():
    """
    Test that aws cli is not called due to object already exists 
    """

    AWSBucketReplication.get_copied_objects = MagicMock()
    AWSBucketReplication.build_source_bucket_dataset = MagicMock()
    AWSBucketReplication.get_copied_objects.return_value = {
        "11111111111111111/abc.bam": "gdc-tcga-open"
    }
    AWSBucketReplication.is_changed_acl_object = MagicMock()
    AWSBucketReplication.is_changed_acl_object.return_value = False

    subprocess.Popen = MagicMock()
    instance = AWSBucketReplication(
        bucket="test_bucket",
        manifest_file="test_manifest",
        thread_num=1,
        global_config={"chunk_size": 1},
        job_name="copying",
    )
    instance.exec_aws_copy(gen_mock_manifest_data()[0:1])
    assert subprocess.Popen.call_count == 0


def test_call_aws_copy_cli_no_called2():
    """
    test that the aws cli is not called due to object already exists 
    """

    AWSBucketReplication.get_copied_objects = MagicMock()
    AWSBucketReplication.build_source_bucket_dataset = MagicMock()
    AWSBucketReplication.build_source_bucket_dataset.return_value = {
        "11111111111111111/abc.bam": "STANDARD",
        "22222222222222222/abc2.bam": "STANDARD",
    }
    AWSBucketReplication.get_copied_objects.return_value = {
        "11111111111111111/abc.bam": "gdc-tcga-open"
    }
    AWSBucketReplication.is_changed_acl_object = MagicMock()
    AWSBucketReplication.is_changed_acl_object.return_value = False

    subprocess.Popen = MagicMock()
    instance = AWSBucketReplication(
        bucket="test_bucket",
        manifest_file="test_manifest",
        thread_num=1,
        global_config={"chunk_size": 1},
        job_name="copying",
    )
    instance.exec_aws_copy(gen_mock_manifest_data()[0:1])
    assert subprocess.Popen.call_count == 0
