import subprocess

# Python 2 and 3 compatible
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch

from multiprocessing import Manager

import copy
import pytest
import google
import scripts.aws_replicate
from scripts.google_replicate import exec_google_copy, resumable_streaming_copy

from scripts.errors import APIError
from scripts import utils, indexd_utils

TEST_UUID = "11111111111111111111111111111111"
TEST_FILENAME = "test"
TEST_URL = ["test_url1", "test_url2"]


PROJECT_ACL = {
    "TCGA": {
        "aws_bucket_prefix": "tcga",
        "gs_bucket_prefix": "gdc-tcga-phs000178",
    }
}


class MockIndexDRecord(object):
    def __init__(self, uuid="", urls=[], urls_metadata={}, acl=[], authz=[]):
        self.uuid = uuid
        self.urls = urls
        self.urls_metadata = urls_metadata
        self.acl = acl
        self.authz = authz

    def patch(self):
        pass


DEFAULT_RECORDS = {
            "uuid1": MockIndexDRecord(
                uuid="uuid1",
                urls=["s3://tcga-open/uuid1/filename1"],
                urls_metadata={"s3://tcga-open/uuid1/filename1": {}},
                acl=["*"],
                authz=["/open"],
            ),
            "uuid2": MockIndexDRecord(
                uuid="uuid2",
                urls=["s3://tcga-controlled/uuid2/filename2"],
                urls_metadata={"s3://tcga-controlled/uuid2/filename2": {}},
                acl=["phs000128"],
                authz=["/phs000128"],
            ),
        }

RECORDS = {}

@pytest.fixture(scope="function")
def reset_records():
    global RECORDS
    RECORDS = copy.deepcopy(DEFAULT_RECORDS)

class MockIndexdClient(object):

    def __init__(self):
        pass

    def get(self, uuid):
        return RECORDS.get(uuid)

    def create(self, did, acl, authz, urls):
        RECORDS[did] = MockIndexDRecord(uuid=did, urls=urls, acl=acl, authz=authz)

    def patch(self):
        pass


def gen_mock_manifest_data():
    fake = {
        "id": "11111111111111111",
        "file_name": "abc.bam",
        "size": 1,
        "md5": "1223344543t34mt43tb43ofh",
        "url": "s3://s3-external-1.amazonaws.com/gdcbackup/2127dca2-e6b7-4e8a-859d-0d7093bd91e6/abc",
        "acl": "tgca",
        "project_id": "TGCA",
    }
    return 16 * [fake]


@patch("scripts.utils.get_google_bucket_name")
@patch("google.cloud.storage.Client")
@patch("scripts.google_replicate.blob_exists")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_called(
    mock_bucket_exists, mock_blob_exist, mock_client, mock_get_google_bucket_name
):
    """
    test that resumable_streaming_function is called
    """

    mock_bucket_exists.return_value = True
    mock_blob_exist.side_effect = [False, False]
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.google_replicate._check_and_handle_changed_acl_object = MagicMock()
    mock_get_google_bucket_name.side_effect = ["test", "test"]
    exec_google_copy(
        {
            "id": "test_file_id",
            "file_name": "test file name",
            "size": 1,
            "project_id": "TCGA",
            "acl": "[u'open']",
        },
        {"11111111": "test"},
        {},
    )
    assert scripts.google_replicate.resumable_streaming_copy.called == True

@patch("scripts.indexd_utils.update_url")
@patch("scripts.utils.get_google_bucket_name")
@patch("scripts.google_replicate.blob_exists")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_not_called_due_to_existed_blob(
    mock_bucket_exists, mock_blob_exist, mock_get_google_bucket_name, mock_update_url
):
    """
    test that streaming function is not called due to the existed object
    """
    mock_bucket_exists.return_value = True
    mock_blob_exist.return_value = True
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    mock_get_google_bucket_name.return_value = "test"
    exec_google_copy(
        {
            "id": "test_file_id",
            "file_name": "test file name",
            "size": 1,
            "project_id": "TCGA",
            "acl": "[u'open']",
        },
        {"11111111": "test"},
        {},
    )
    assert scripts.google_replicate.resumable_streaming_copy.called == False
    assert mock_update_url.called == True


@patch("scripts.utils.get_google_bucket_name")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_not_called_due_to_not_existed_bucket(
    mock_bucket_exists, mock_get_google_bucket_name
):
    """
    test that streaming function is not called due to the not-existed bucket
    """
    mock_bucket_exists.return_value = False
    google.cloud.storage.Client = MagicMock()
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    mock_get_google_bucket_name.return_value = "test"
    exec_google_copy(
        {
            "id": "test_file_id",
            "file_name": "test file name",
            "size": 1,
            "project_id": "TCGA",
            "acl": "[u'open']",
        },
        {"11111111": "test"},
        {},
    )
    assert scripts.google_replicate.resumable_streaming_copy.called == False


@patch("scripts.utils.get_google_bucket_name")
@patch("scripts.indexd_utils.update_url")
@patch("scripts.google_replicate.blob_exists")
@patch("scripts.google_replicate.bucket_exists")
def test_resumable_streaming_copy_called_one_time(
    mock_bucket_exists, mock_blob_exist, mock_update_url, mock_get_google_bucket_name
):
    """
    test that the streaming called only one time
    The object is successfully copied
    """
    mock_bucket_exists.return_value = True
    mock_blob_exist.side_effect = [False, True]
    scripts.google_replicate.resumable_streaming_copy = MagicMock()
    scripts.google_replicate.fail_resumable_copy_blob = MagicMock()
    scripts.google_replicate._check_and_handle_changed_acl_object = MagicMock()
    mock_get_google_bucket_name.return_value = "test"
    exec_google_copy(
        {
            "id": "test_file_id",
            "file_name": "test file name",
            "size": 1,
            "project_id": "TCGA",
            "acl": "[u'open']",
        },
        {"11111111": "test"},
        {},
    )
    assert scripts.google_replicate.resumable_streaming_copy.call_count == 1
    assert mock_update_url.call_count == 1


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
        {"fileid": "test_file_id", "file_name": "test file name", "size": 1},
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
            {"id": "test_file_id", "file_name": "test file name", "size": 1},
            mock_client,
            "bucket_test",
            "blob_test",
            {},
        )
    assert not scripts.google_replicate.streaming.called


@patch("scripts.utils.get_aws_bucket_name")
def test_call_aws_cli_called(mock_aws):
    """
    Test that the aws cli is called since the object storage class is standard
    """
    scripts.aws_replicate.logger = MagicMock()
    subprocess.Popen = MagicMock()
    utils.get_aws_bucket_name = MagicMock()

    mock_aws.return_value = "tcga-open"

    scripts.aws_replicate.bucket_exists = MagicMock()
    scripts.aws_replicate.bucket_exists.return_value = True
    scripts.aws_replicate.object_exists = MagicMock()
    scripts.aws_replicate.object_exists.return_value = True
    scripts.aws_replicate.get_object_storage_class = MagicMock()
    scripts.aws_replicate.get_object_storage_class.return_value = "STANDARD"

    manager = Manager()
    manager_ns = manager.Namespace()
    manager_ns.total_processed_files = 0
    manager_ns.total_copied_data = 0
    lock = manager.Lock()

    job_info = scripts.aws_replicate.JobInfo(
        {}, gen_mock_manifest_data()[0], 1, 1, "", {}, {}, manager_ns, "bucket"
    )

    scripts.aws_replicate.exec_aws_copy(lock, False, job_info)
    assert subprocess.Popen.call_count == 1


@patch("scripts.utils.get_aws_bucket_name")
def test_call_streamming_method_called(mock_aws):
    """
    Test that the streamming method is called since the object is Glacier
    """
    scripts.aws_replicate.logger = MagicMock()
    subprocess.Popen = MagicMock()
    scripts.aws_replicate.stream_object_from_gdc_api = MagicMock()
    mock_aws.return_value = "tcga-open"

    scripts.aws_replicate.bucket_exists = MagicMock()
    scripts.aws_replicate.bucket_exists.return_value = True
    scripts.aws_replicate.object_exists = MagicMock()
    scripts.aws_replicate.object_exists.return_value = False

    source_objects = {"11111111111111111/abc.bam": {"StorageClass": "GLACIER"}}
    copied_objects = {}
    manager = Manager()
    manager_ns = manager.Namespace()
    manager_ns.total_processed_files = 0
    manager_ns.total_copied_data = 0
    lock = manager.Lock()

    job_info = scripts.aws_replicate.JobInfo(
        {},
        gen_mock_manifest_data()[0],
        1,
        1,
        "",
        copied_objects,
        source_objects,
        manager_ns,
        "bucket",
    )
    scripts.aws_replicate.exec_aws_copy(lock, False, job_info)
    assert subprocess.Popen.call_count == 0
    assert scripts.aws_replicate.stream_object_from_gdc_api.call_count == 1


def test_remove_changed_url():
    """
    Test removing changed url for indexd
    """
    urls = ["s3://tcga-open/uuid1/filename1", "gs://gdc-tcga-open/uuid1/filename1"]
    urls_metadata = {
        "s3://tcga-open/uuid1/filename1": {},
        "gs://gdc-tcga-open/uuid1/filename1": {},
    }
    url = "s3://tcga-controlled/uuid1/filename1"

    doc = MockIndexDRecord(urls=urls, urls_metadata=urls_metadata)
    doc, modified = indexd_utils._remove_changed_url(doc, url)
    assert modified == True
    assert doc.urls == ["gs://gdc-tcga-open/uuid1/filename1"]
    assert doc.urls_metadata == {"gs://gdc-tcga-open/uuid1/filename1": {}}

    urls = ["s3://ccle-open-access/uuid1/filename1", "gs://gdc-ccle-open/uuid1/filename1"]
    urls_metadata = {
        "s3://ccle-open-access/uuid1/filename1": {},
        "gs://gdc-ccle-open/uuid1/filename1": {},
    }
    url = "s3://gdc-ccle-controlled/uuid1/filename1"

    doc = MockIndexDRecord(urls=urls, urls_metadata=urls_metadata)
    doc, modified = indexd_utils._remove_changed_url(doc, url)
    assert modified
    assert doc.urls == ["gs://gdc-ccle-open/uuid1/filename1"]
    assert doc.urls_metadata == {"gs://gdc-ccle-open/uuid1/filename1": {}}


def test_is_changed_acl_object():
    """
    Test that acl object is changed
    :return:
    """
    fi = {"id": "test_id", "file_name": "test_file_name"}
    copied_objects = {
        "tcga-controlled/test_id/test_file_name": {
            "id": "test_id",
            "file_name": "test_file_name",
        }
    }
    assert scripts.aws_replicate.is_changed_acl_object(fi, copied_objects, "tcga-open")

    copied_objects = {
        "ccle-open-access/test_id/test_file_name": {
            "id": "test_id",
            "file_name": "test_file_name",
        }
    }
    assert scripts.aws_replicate.is_changed_acl_object(
        fi, copied_objects, "gdc-ccle-controlled"
    )


@patch('scripts.indexd_utils.PROJECT_ACL', PROJECT_ACL)
def test_update_url_with_new_acl(reset_records):
    """
    Test that updates indexd with new acl
    """
    mock_client = MockIndexdClient()
    fi = {
        "project_id": "TCGA-PNQS",
        "id": "uuid1",
        "file_name": "filename1",
        "acl": "['phs000218',  'phs000219']",
    }
    indexd_utils.update_url(fi, mock_client)
    doc = mock_client.get("uuid1")
    assert doc.acl == ['phs000218', 'phs000219']


@patch('scripts.indexd_utils.PROJECT_ACL', PROJECT_ACL)
def test_update_url_with_new_url(reset_records):
    """
    Test that update indexd with new s3 url
    """
    mock_client = MockIndexdClient()
    fi = {
        "project_id": "TCGA-PNQS",
        "id": "uuid1",
        "file_name": "filename1",
        "acl": "['phs000218',  'phs000219']",
        "url": "tcga-controlled/uuid1/filename1"
    }
    indexd_utils.update_url(fi, mock_client)
    doc = mock_client.get("uuid1")
    assert doc.urls == ['s3://tcga-controlled/uuid1/filename1']


@patch('scripts.indexd_utils.PROJECT_ACL', PROJECT_ACL)
def test_update_url_with_new_url2(reset_records):
    """
    Test that update indexd with new gs url
    """
    mock_client = MockIndexdClient()
    fi = {
        "project_id": "TCGA-PNQS",
        "id": "uuid1",
        "file_name": "filename1",
        "acl": "['open']",
        "url": "tcga-open/uuid1/filename1"
    }
    indexd_utils.update_url(fi, mock_client, provider="gs")
    doc = mock_client.get("uuid1")
    assert doc.urls == ['s3://tcga-open/uuid1/filename1', 'gs://gdc-tcga-phs000178-open/uuid1/filename1']
