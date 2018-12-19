import subprocess

# Python 2 and 3 compatible
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch

import boto3
import pytest
import google
import scripts.aws_replicate
from scripts.aws_replicate import AWSBucketReplication, exec_aws_copy
from scripts.google_replicate import exec_google_copy, resumable_streaming_copy

from scripts.errors import APIError

TEST_UUID = "11111111111111111111111111111111"
TEST_FILENAME = "test"
TEST_URL = ["test_url1", "test_url2"]


def gen_mock_manifest_data():
    fake = {
        "id": "11111111111111111",
        "filename": "abc.bam",
        "size": 1,
        "md5": "1223344543t34mt43tb43ofh",
        "acl": "tgca",
        "project_id": "TGCA",
    }

    return [
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
        fake,
    ]


@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.check_blob_name_exists_and_match_md5_size")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_called(
    mock_bucket_exists, mock_check_blob, mock_client
):
    mock_bucket_exists.return_value = True
    mock_check_blob.return_value = False
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    exec_google_copy({"id": "test_file_id", "filename": "test file name"}, {})
    assert scripts.google_replicate.resumable_streaming_copy.called == True


@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.check_blob_name_exists_and_match_md5_size")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_not_called_due_to_existed_blob(
    mock_bucket_exists, mock_check_blob, mock_client
):
    mock_bucket_exists.return_value = True
    mock_check_blob.return_value = True
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    exec_google_copy({"id": "test_file_id", "filename": "test file name"}, {})
    assert scripts.google_replicate.resumable_streaming_copy.called == False


@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_not_called_due_to_not_existed_bucket(
    mock_bucket_exists
):
    mock_bucket_exists.return_value = False
    google.cloud.storage.Client = MagicMock()
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    exec_google_copy({"id": "test_file_id", "filename": "test file name"}, {})
    assert scripts.google_replicate.resumable_streaming_copy.called == False


@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.check_blob_name_exists_and_match_md5_size")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_called_one_time(
    mock_bucket_exists, mock_check_blob, mock_client
):
    mock_bucket_exists.return_value = True
    mock_check_blob.side_effect = [False, True]
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.google_replicate.update_indexd = MagicMock()
    exec_google_copy({"id": "test_file_id", "filename": "test file name"}, {})
    assert scripts.google_replicate.resumable_streaming_copy.call_count == 1
    assert scripts.google_replicate.update_indexd.call_count == 1


@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.check_blob_name_exists_and_match_md5_size")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_called_two_time(
    mock_bucket_exists, mock_check_blob, mock_client
):
    mock_bucket_exists.return_value = True
    mock_check_blob.side_effect = [False, False, True]
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.google_replicate.update_indexd = MagicMock()
    exec_google_copy({"id": "test_file_id", "filename": "test file name"}, {})
    assert scripts.google_replicate.resumable_streaming_copy.call_count == 1


@patch("google.cloud.storage.Client")
@patch("requests.get")
def test_streamUpload_called(mock_requests_get, mock_client):
    class Mock_Requests(object):
        def __init__(self, _status_code):
            self.status_code = _status_code

    mock_value = Mock_Requests(200)
    mock_requests_get.return_value = mock_value
    scripts.google_replicate.streaming = MagicMock()
    resumable_streaming_copy(
        {"fileid": "test_file_id", "filename": "test file name"},
        mock_client,
        "bucket_test",
        "blob_test",
        {},
    )
    assert scripts.google_replicate.streaming.called


@patch("google.cloud.storage.Client")
@patch("requests.get")
def test_streamUpload_not_called(mock_requests_get, mock_client):
    class Mock_Requests(object):
        def __init__(self, status_code, message):
            self.status_code = status_code
            self.message = message

    mock_requests_get.return_value = Mock_Requests(status_code=400, message="error")
    scripts.google_replicate.streaming = MagicMock()
    with pytest.raises(APIError):
        resumable_streaming_copy(
            {"id": "test_file_id", "filename": "test file name"},
            mock_client,
            "bucket_test",
            "blob_test",
            {},
        )
    assert not scripts.google_replicate.streaming.called


def test_call_aws_copy():

    subprocess.Popen = MagicMock()
    exec_aws_copy(
        "test",
        gen_mock_manifest_data()[0:1],
        "source_bucket",
        "target_bucket",
        {"chunk_size": 1},
        {},
    )
    assert subprocess.Popen.call_count == 1


def test_call_aws_copy_no_called():

    subprocess.Popen = MagicMock()
    exec_aws_copy(
        "test",
        gen_mock_manifest_data()[0:1],
        "source_bucket",
        "target_bucket",
        {"chunk_size": 1},
        set(["11111111111111111/abc.bam"]),
    )
    assert subprocess.Popen.call_count == 0
