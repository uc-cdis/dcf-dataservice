import os
from os import listdir
from os.path import isfile, join
import sys
import time
import timeit
import argparse
import json

from aws_replicate import AWSBucketReplication
from google_replicate import GOOGLEBucketReplication
from indexd_update_service import update_indexd_from_manifest

def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='action', dest='action')

    aws_replicate_cmd = subparsers.add_parser('aws_replicate')
    aws_replicate_cmd.add_argument('--global_config', required=True)
    aws_replicate_cmd.add_argument('--bucket', required=True)
    aws_replicate_cmd.add_argument('--manifest_file', required=True)

    google_replicate_cmd = subparsers.add_parser('google_replicate')
    google_replicate_cmd.add_argument('--global_config', required=True)
    google_replicate_cmd.add_argument('--manifest_file', required=True)
    google_replicate_cmd.add_argument('--thread_num', required=True)

    indexd_update_cmd = subparsers.add_parser('indexd_update')
    indexd_update_cmd.add_argument('--manifest_file', required=True)

    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()
    args = parse_arguments()
    if args.action == 'aws_replicate':
        # eg. python replicate.py aws_replicate --bucket mybucket20018 --manifest_file ./test --global_config '{"chunk_size": 4}'
        aws = AWSBucketReplication(bucket=args.bucket, manifest_file=args.manifest_file, global_config=json.loads(args.global_config))
        aws.run()
    elif args.action == 'google_replicate':
        # python replicate.py google_replicate --manifest_file ./test --thread_num 4 --global_config '{"token_path": "./gdc-token.txt", "chunk_size_download": 2048000, "chunk_size_upload": 20*1024*1024}'
        google = GOOGLEBucketReplication( global_config=json.loads(args.global_config), manifest_file=args.manifest_file, thread_num=int(args.thread_num))
        google.prepare()
        google.run()
    elif args.action == 'indexd_update_service':
        update_indexd_from_manifest(args.manifest_file)

    end = timeit.default_timer()
    print('Total time: {} seconds'.format(end-start))

    #aws = AWSBucketReplication(bucket='mybucket20018', manifest_file='./test', global_config={'chunk_size': 4})
    #aws.run()
    #end = timeit.default_timer()
    #google = GOOGLEBucketReplication(
    #    global_config={'token_path': './gdc-token.txt', 'chunk_size': 2048000}, manifest_file='test', thread_num=4)
    #google.prepare()
    #google.run()
    # if args.action == 'sync':
    #    print "sync from gdc aws bucket to gen3 dcf bucket"
