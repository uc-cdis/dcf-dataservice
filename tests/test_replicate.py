import mock
from mock import MagicMock
from mock import patch
import pytest

import scripts.replicate
from scripts.replicate import AWSBucketReplication
from scripts.replicate import GOOGLEBucketReplication

TEST_UUID = '11111111111111111111111111111111'
TEST_FILENAME = 'test'
TEST_URL = ['test_url1', 'test_url2']


def gen_mock_manifest_data():
    fake = {
        'did': '11111111111111111',
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
    scripts.replicate.exitFlag = 0

@patch('scripts.replicate.get_fileinfo_list_from_manifest')
def test_multithread_google(get_file_from_manifest,monkeypatch):
    monkeypatch.setattr(
         'scripts.replicate.MODE', 'test')
    get_file_from_manifest.return_value = gen_mock_manifest_data()
    number_of_threads = 2
    scripts.replicate.exec_google_copy = MagicMock()
    google = GOOGLEBucketReplication(
         {'token_path': '/test/token.txt', 'chunk_size': 2048000}, 'test', number_of_threads)
    google.prepare()
    google.run()
    assert scripts.replicate.exec_google_copy.called == True

@patch('scripts.replicate.get_fileinfo_list_from_manifest')
def test_multithread_aws(get_file_from_manifest,monkeypatch,initialization):
    monkeypatch.setattr(
            'scripts.replicate.MODE', 'test')
    get_file_from_manifest.return_value = gen_mock_manifest_data()
    number_of_threads = 4
    scripts.replicate.call_aws_copy = MagicMock()
    aws = AWSBucketReplication({'from_bucket': 'from','to_bucket': 'to'},'test',number_of_threads)
    aws.prepare()
    aws.run()
    assert scripts.replicate.call_aws_copy.called == True




#patch('scripts.replicate.AMZBucketReplication.get_submitting_indexd_files')
#patch('scripts.sync.AMZBucketReplication.get_file_from_uuid')
#ef test_uuid_not_existed(get_index_uuid, get_submitting_files):
#   """
#   in case that uuid is not existed, copy object from backup bucket to dcf bucket and update indexd
#   """
#   document = MagicMock()
#   document.did = TEST_UUID
#   document.urls = TEST_URL
#   document.filename = TEST_FILENAME
#   get_index_uuid.return_value = document
#   get_submitting_files.return_value = [{'did':TEST_UUID, 'filename': TEST_FILENAME}]
#
#   instance = AMZBucketReplication('test1','test2','manifest')
#   instance.create_index = MagicMock()
#   instance.call_aws_copy = MagicMock()
#
#   instance.run()
#   assert instance.create_index.called == True
#   assert instance.call_aws_copy.called == True
#
#patch('scripts.sync.AMZBucketReplication.get_fileinfo_list_from_manifest')
#patch('scripts.sync.AMZBucketReplication.get_file_from_uuid')
#ef test_uuid_is_existed(get_index_uuid, get_fileinfo_list, monkeypatch):
#
#   document = MagicMock()
#   document.did = TEST_UUID
#   document.urls = TEST_URL
#   document.filename = TEST_FILENAME
#   get_index_uuid.return_value = document
#   get_fileinfo_list.return_value = [{'did':TEST_UUID, 'filename': TEST_FILENAME}]
#   instance = AMZBucketReplication('test1','test2','manifest')
#   submitting_files = instance.get_submitting_indexd_files()
#   assert len(submitting_files) == 0
