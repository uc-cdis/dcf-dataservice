import os
from os import listdir
from os.path import isfile, join
import requests
import sys
import getopt
import argparse

from cdislogging import get_logger

from indexclient.client import IndexClient
from dev_settings import SIGNPOST

logger = get_logger("ReplicationThread")

indexclient = IndexClient(SIGNPOST['host'], SIGNPOST['version'], SIGNPOST['auth'])

class AMZBucketReplication(object):

    def __init__(self, from_bucket, to_bucket, manifest_file):
        self.from_bucket = from_bucket
        self.to_bucket = to_bucket
        self.manifest_file = manifest_file

    def get_file_from_uuid(self, uuid):
        doc = None

        if uuid:
            doc = self.indexclient.get(uuid)
        return doc

    def create_index(self, **kwargs):
        return indexclient.create(**kwargs)

    def get_fileinfo_list_from_manifest(self):
        l = []
        with open(self.manifest_file) as f:
            read(f)
        return l

    def call_aws_copy(self, fileinfo):
        execstr = ("aws s3 cp {}/{}/{} {}", self.from_bucket, fileinfo['uuid'], fileinfo.get('filename',''), self.to_bucket)
        os.system(execstr)

    def run(self, mode="test"):
        logger.info("starting ....")
        file_list = self.get_fileinfo_list_from_manifest()
        for fileinfo in file_list:
            document = self.get_file_from_uuid(fileinfo.get("did"), None)
            if document is None:
                logger.info('copy file location in from_bucket to to-bucket')
                self.call_aws_copy(fileinfo)

                logger.info("update indexd with new uuid {}".format(fileinfo["did"]))
                # IndexClient
                self.create_index(did=fileinfo.get("did"),
                                   hashes=fileinfo.get('hashes',''),
                                   size=fileinfo.get('size',0),
                                   urls=fileinfo.get('urls',[]),
                                   metadata=fileinfo.get('metadata',''))

def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='action', dest='action')

    create_data = subparsers.add_parser('sync')
    create_data.add_argument('--from_bucket', required=True)
    create_data.add_argument('--to_bucket', required=True)
    create_data.add_argument('--manifest_file', required=True)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()

    if args.action == 'sync':
        print "sync from gdc aws bucket to gen3 dcf bucket"
