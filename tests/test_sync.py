import mock
from mock import MagicMock
from mock import patch

from scripts.sync import AMZBucketReplication

TEST_UUID = '11111111111111111111111111111111'
TEST_FILENAME = 'test'
TEST_URL = ['test_url1', 'test_url2']

@patch('scripts.sync.AMZBucketReplication.get_submitting_indexd_files')
@patch('scripts.sync.AMZBucketReplication.get_file_from_uuid')
def test_uuid_not_existed(get_index_uuid, get_submitting_files):
    """
    in case that uuid is not existed, copy object from backup bucket to dcf bucket and update indexd
    """
    document = MagicMock()
    document.did = TEST_UUID
    document.urls = TEST_URL
    document.filename = TEST_FILENAME
    get_index_uuid.return_value = document
    get_submitting_files.return_value = [{'did':TEST_UUID, 'filename': TEST_FILENAME}]

    instance = AMZBucketReplication('test1','test2','manifest')
    instance.create_index = MagicMock()
    instance.call_aws_copy = MagicMock()

    instance.run()
    assert instance.create_index.called == True
    assert instance.call_aws_copy.called == True

@patch('scripts.sync.AMZBucketReplication.get_fileinfo_list_from_manifest')
@patch('scripts.sync.AMZBucketReplication.get_file_from_uuid')
def test_uuid_is_existed(get_index_uuid, get_fileinfo_list, monkeypatch):

    document = MagicMock()
    document.did = TEST_UUID
    document.urls = TEST_URL
    document.filename = TEST_FILENAME
    get_index_uuid.return_value = document
    get_fileinfo_list.return_value = [{'did':TEST_UUID, 'filename': TEST_FILENAME}]
    instance = AMZBucketReplication('test1','test2','manifest')
    submitting_files = instance.get_submitting_indexd_files()
    assert len(submitting_files) == 0
