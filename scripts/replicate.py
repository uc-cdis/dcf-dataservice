import os
from os import listdir
from os.path import isfile, join
import sys
import time
import timeit
import argparse

from aws_replicate import AWSBucketReplication
from google_replicate import GOOGLEBucketReplication

def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='action', dest='action')

    create_data = subparsers.add_parser('sync')
    create_data.add_argument('--global_config', required=True)
    create_data.add_argument('--to_bucket', required=True)
    create_data.add_argument('--manifest_file', required=True)
    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()
    #aws = AWSBucketReplication({'from_bucket': 'mybucket20018','manifest_file':'./test','chunk_size': 4})
    #aws.run()
    #end = timeit.default_timer()
    google = GOOGLEBucketReplication(
        {'token_path': './gdc-token.txt', 'chunk_size': 2048000}, 'test', 4)
    google.prepare()
    google.run()
    # if args.action == 'sync':
    #    print "sync from gdc aws bucket to gen3 dcf bucket"
