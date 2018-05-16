import mock
from mock import MagicMock
from mock import patch

from scripts.sync import AMZBucketReplication

TEST_UUID = '11111111111111111111111111111111'
TEST_URL = ['test_url1', 'test_url2']

@patch('scripts.sync.AMZBucketReplication.get_fileinfo_list_from_manifest')
@patch('scripts.sync.AMZBucketReplication.get_file_from_uuid')
def test_file_already_existed(get_index_uuid, get_fileinfo_list, monkeypatch):

    document = MagicMock()
    document.did = TEST_UUID
    document.urls = TEST_URL
    get_index_uuid.return_value = document
    get_fileinfo_list.return_value = []

    instance = AMZBucketReplication('test1','test2','manifest')
    instance.create_index = MagicMock()
    instance.call_aws_copy = MagicMock()
    instance.run()
    assert instance.create_index.called == False
    assert instance.call_aws_copy.called == False

@patch('scripts.sync.AMZBucketReplication.get_fileinfo_list_from_manifest')
@patch('scripts.sync.AMZBucketReplication.get_file_from_uuid')
def test_updat4e_indexd_with_new_file(get_index_uuid, get_fileinfo_list, monkeypatch):

    get_index_uuid.return_value = None
    get_fileinfo_list.return_value = [{'did':TEST_UUID}]

    instance = AMZBucketReplication('test1','test2','manifest')
    instance.create_index = MagicMock()
    instance.call_aws_copy = MagicMock()
    instance.run()
    assert instance.create_index.called == True
    assert instance.call_aws_copy.called == True
