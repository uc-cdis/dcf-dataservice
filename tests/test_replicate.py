import os
import subprocess
import mock
from mock import MagicMock
from mock import patch
import pytest
import google
import scripts.replicate
from scripts.aws_replicate import AWSBucketReplication
from scripts.google_replicate import GOOGLEBucketReplication, exec_google_copy, resumable_streaming_copy
from scripts.google_resumable_upload import GCSObjectStreamUpload

TEST_UUID = '11111111111111111111111111111111'
TEST_FILENAME = 'test'
TEST_URL = ['test_url1', 'test_url2']


def gen_mock_manifest_data():
    fake = {
        'fileid': '11111111111111111',
        'filename': 'abc.bam',
        'size': 1,
        'hash': '1223344543t34mt43tb43ofh',
        'acl': 'tgca',
        'project': 'TGCA'
        }

    l = [fake, fake, fake, fake, fake, fake, fake, fake,
         fake, fake, fake, fake, fake, fake, fake, fake]
    return l

@pytest.fixture
def initialization():
    scripts.google_replicate.exitFlag = 0


@patch('scripts.google_replicate.get_fileinfo_list_from_manifest')
def test_multithread_google(get_file_from_manifest, initialization):
    get_file_from_manifest.return_value = gen_mock_manifest_data()
    number_of_threads = 10
    scripts.google_replicate.exec_google_copy = MagicMock()
    google = GOOGLEBucketReplication(
         {'token_path': '/test/token.txt', 'chunk_size': 2048000}, 'test', number_of_threads)

    google.prepare()
    google.run()
    assert scripts.google_replicate.exec_google_copy.call_count == 9

@patch('google.cloud.storage.Client')
@patch('scripts.google_replicate.check_blob_name_exists_and_match_md5')
@patch('scripts.google_replicate.bucket_exists')
def test_resumable_streaming_copy_called(mock_bucket_exists, mock_check_blob,  mock_client, initialization):
    mock_bucket_exists.return_value = True
    mock_check_blob.return_value = False
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    exec_google_copy("test",{"fileid":"test_file_id","filename":"test file name"},{})
    assert scripts.google_replicate.resumable_streaming_copy.called == True

@patch('google.cloud.storage.Client')
@patch('scripts.google_replicate.check_blob_name_exists_and_match_md5')
@patch('scripts.google_replicate.bucket_exists')
def test_resumable_streaming_copy_not_called_due_to_existed_blob(mock_bucket_exists, mock_check_blob,  mock_client, initialization):
    mock_bucket_exists.return_value = True
    mock_check_blob.return_value = True
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    exec_google_copy("test",{"fileid":"test_file_id","filename":"test file name"},{})
    assert scripts.google_replicate.resumable_streaming_copy.called == False

@patch('scripts.google_replicate.bucket_exists')
def test_resumable_streaming_copy_not_called_due_to_not_existed_bucket(mock_bucket_exists,  initialization):
    mock_bucket_exists.return_value = False
    google.cloud.storage.Client = MagicMock()
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    exec_google_copy("test",{"fileid":"test_file_id","filename":"test file name"},{})
    assert scripts.google_replicate.resumable_streaming_copy.called == False

@patch('google.cloud.storage.Client')
@patch('scripts.google_replicate.check_blob_name_exists_and_match_md5')
@patch('scripts.google_replicate.bucket_exists')
def test_resumable_streaming_copy_called_one_time(mock_bucket_exists, mock_check_blob,  mock_client, initialization):
    mock_bucket_exists.return_value = True
    mock_check_blob.side_effect = [False, True]
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    exec_google_copy("test",{"fileid":"test_file_id","filename":"test file name"},{})
    assert scripts.google_replicate.resumable_streaming_copy.call_count == 1

@patch('google.cloud.storage.Client')
@patch('scripts.google_replicate.check_blob_name_exists_and_match_md5')
@patch('scripts.google_replicate.bucket_exists')
def test_resumable_streaming_copy_called_two_time(mock_bucket_exists, mock_check_blob,  mock_client, initialization):
    mock_bucket_exists.return_value = True
    mock_check_blob.side_effect = [False, False, True]
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    exec_google_copy("test",{"fileid":"test_file_id","filename":"test file name"},{})
    assert scripts.google_replicate.resumable_streaming_copy.call_count == 2

@patch('google.cloud.storage.Client')
@patch('requests.get')
def test_treamUpload_called(mock_requests_get, mock_client, initialization):
    class Mock_Requests(object):
        def __init__(self, _status_code):
            self.status_code = _status_code
    mock_value = Mock_Requests(200)
    mock_requests_get.return_value = mock_value
    scripts.google_replicate.streaming = MagicMock()
    resumable_streaming_copy("test",{"fileid":"test_file_id","filename":"test file name"},mock_client, 'bucket_test', 'blob_test', {})
    scripts.google_replicate.streaming.called == True


@patch('google.cloud.storage.Client')
@patch('requests.get')
def test_streamUpload_not_called(mock_requests_get, mock_client, initialization):
    class Mock_Requests(object):
        def __init__(self, _status_code):
            self.status_code = _status_code
    mock_value = Mock_Requests(400)
    mock_requests_get.return_value = mock_value
    scripts.google_replicate.streaming = MagicMock()
    resumable_streaming_copy("test",{"fileid":"test_file_id","filename":"test file name"},mock_client, 'bucket_test', 'blob_test', {})
    scripts.google_replicate.streaming.called == False

@patch('scripts.aws_replicate.bucket_exists')
def test_call_aws_copy(mock_aws_bucket_exists):
    instance = AWSBucketReplication(bucket='test_bucket', manifest_file = 'test_manifest', global_config={})
    mock_aws_bucket_exists.return_value = False
    instance.get_etag_aws_object = MagicMock()
    instance.call_aws_copy(gen_mock_manifest_data(), {})
    assert instance.get_etag_aws_object.called == False

@patch('scripts.aws_replicate.object_exists')
@patch('scripts.aws_replicate.bucket_exists')
def test_call_aws_copy_with_no_object_in_source_bucket(mock_aws_bucket_exists, mock_aws_object_exists):
    instance = AWSBucketReplication(bucket='test_bucket', manifest_file = 'test_manifest', global_config={'chunk_size': 1})
    mock_aws_bucket_exists.return_value = True
    mock_aws_object_exists.side_effect = [False, False]

    subprocess.Popen = MagicMock()
    scripts.aws_replicate.get_etag_aws_object = MagicMock()
    instance.call_aws_copy(gen_mock_manifest_data()[0:1], {})
    assert subprocess.Popen.call_count == 0
    assert scripts.aws_replicate.object_exists.call_count == 2
    assert scripts.aws_replicate.get_etag_aws_object.call_count == 3


@patch('scripts.aws_replicate.object_exists')
@patch('scripts.aws_replicate.bucket_exists')
def test_call_aws_copy_with_success_upload_first_try(mock_aws_bucket_exists, mock_aws_object_exists):
    instance = AWSBucketReplication(bucket='test_bucket', manifest_file = 'test_manifest', global_config={'chunk_size': 1})
    mock_aws_bucket_exists.return_value = True
    mock_aws_object_exists.side_effect = [True, True]

    subprocess.Popen = MagicMock()
    scripts.aws_replicate.get_etag_aws_object= MagicMock()
    scripts.aws_replicate.get_etag_aws_object.side_effect = ['original etag', None, 'original etag', 'original etag', 'original etag']
    instance.call_aws_copy(gen_mock_manifest_data()[0:1], {})
    assert subprocess.Popen.call_count == 1
    assert scripts.aws_replicate.object_exists.call_count == 2
    assert scripts.aws_replicate.get_etag_aws_object.call_count == 5

@patch('scripts.aws_replicate.object_exists')
@patch('scripts.aws_replicate.bucket_exists')
def test_call_aws_copy_with_success_upload_second_try(mock_aws_bucket_exists, mock_aws_object_exists):
    instance = AWSBucketReplication(bucket='test_bucket', manifest_file = 'test_manifest', global_config={'chunk_size': 1})
    mock_aws_bucket_exists.return_value = True
    mock_aws_object_exists.side_effect = [True, True]
    subprocess.Popen = MagicMock()
    scripts.aws_replicate.get_etag_aws_object= MagicMock()
    scripts.aws_replicate.get_etag_aws_object.side_effect = ['original etag', None, 'original etag', 'wrong hash', 'original etag']
    instance.call_aws_copy(gen_mock_manifest_data()[0:1], {})
    assert subprocess.Popen.call_count == 2
    assert scripts.aws_replicate.object_exists.call_count == 2
    assert scripts.aws_replicate.get_etag_aws_object.call_count == 5

@patch('scripts.aws_replicate.object_exists')
@patch('scripts.aws_replicate.bucket_exists')
def test_call_aws_copy_with_fail_upload_second_try(mock_aws_bucket_exists, mock_aws_object_exists):
    instance = AWSBucketReplication(bucket='test_bucket', manifest_file = 'test_manifest', global_config={'chunk_size': 1})
    mock_aws_bucket_exists.return_value = True
    mock_aws_object_exists.side_effect = [True, True]
    subprocess.Popen = MagicMock()
    scripts.aws_replicate.get_etag_aws_object= MagicMock()
    scripts.aws_replicate.get_etag_aws_object.side_effect = ['original etag', None, 'original etag', 'wrong hash', 'wrong hash', 'wrong hash']
    instance.call_aws_copy(gen_mock_manifest_data()[0:1], {})
    assert subprocess.Popen.call_count == 2
    assert scripts.aws_replicate.object_exists.call_count == 2
    assert scripts.aws_replicate.get_etag_aws_object.call_count == 5

