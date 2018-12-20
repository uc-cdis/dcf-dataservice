import timeit
import argparse
import json

from aws_replicate import AWSBucketReplication
from deletion import delete_objects_from_cloud_resources


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    aws_replicate_cmd = subparsers.add_parser("aws_replicate")
    aws_replicate_cmd.add_argument("--global_config", required=True)
    aws_replicate_cmd.add_argument("--bucket", required=True)
    aws_replicate_cmd.add_argument("--manifest_file", required=True)
    aws_replicate_cmd.add_argument("--thread_num", required=True)

    aws_indexing_cmd = subparsers.add_parser("indexing")
    aws_indexing_cmd.add_argument("--global_config", required=True)
    aws_indexing_cmd.add_argument("--manifest_file", required=True)
    aws_indexing_cmd.add_argument("--thread_num", required=True)

    redact_cmd = subparsers.add_parser("readact")
    redact_cmd.add_argument("--manifest_file", required=True)
    redact_cmd.add_argument("--log_file", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()

    args = parse_arguments()
    if args.action == "aws_replicate":
        # bucket, global_config, manifest_file, thread_num
        # eg. python replicate.py aws_replicate --bucket mybucket20018 --manifest_file ./manifest --global_config '{"chunk_size": 100, "log_bucket": "xssxs"}' --thread_num 4
        aws = AWSBucketReplication(
            bucket=args.bucket,
            manifest_file=args.manifest_file,
            global_config=json.loads(args.global_config),
            thread_num=int(args.thread_num),
            job_name="copying",
        )
        aws.run()

    elif args.action == "indexing":
        aws = AWSBucketReplication(
            manifest_file=args.manifest_file,
            global_config=json.loads(args.global_config),
            thread_num=int(args.thread_num),
            job_name="indexing",
        )
        aws.prepare()
        aws.run()

    elif args.action == "readact":
        delete_objects_from_cloud_resources(args.manifest, args.log_file)

    end = timeit.default_timer()
    print("Total time: {} seconds".format(end - start))
