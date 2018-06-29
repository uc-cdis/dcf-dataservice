import os
from os import listdir
from os.path import isfile, join
import sys
import time
import timeit
import argparse
import json

from aws_replicate import AWSBucketReplication

def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='action', dest='action')

    aws_replicate_cmd = subparsers.add_parser('aws_replicate')
    aws_replicate_cmd.add_argument('--global_config', required=True)
    aws_replicate_cmd.add_argument('--bucket', required=True)
    aws_replicate_cmd.add_argument('--manifest_file', required=True)

    return parser.parse_args()
if __name__ == "__main__":
    start = timeit.default_timer()
    args = parse_arguments()
    if args.action == 'aws_replicate':
        # eg. python replicate.py aws_replicate --bucket mybucket20018 --manifest_file ./manifest --global_config '{"chunk_size": 4}'
        aws = AWSBucketReplication(bucket=args.bucket, manifest_file=args.manifest_file, global_config=json.loads(args.global_config))
        aws.run()

    end = timeit.default_timer()
    print('Total time: {} seconds'.format(end-start))

