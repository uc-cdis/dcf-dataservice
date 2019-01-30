import timeit
import argparse
import json

import aws_replicate
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

    # set config in dictionary. Only for AWS replicate:
    #   {
    #       "chunk_size": 100,
    #       "log_bucket": "bucketname".
    #       "mode": "process|thread", # multiple process or multiple thread. Default: thread
    #       ""
    #   }
    #
    aws_indexing_cmd.add_argument("--global_config", required=True)
    aws_indexing_cmd.add_argument("--manifest_file", required=True)
    aws_indexing_cmd.add_argument("--thread_num", required=True)

    redact_cmd = subparsers.add_parser("readact")
    redact_cmd.add_argument("--redact_file", required=True)
    redact_cmd.add_argument("--log_bucket", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()

    args = parse_arguments()
    if args.action == "aws_replicate" or args.action == "indexing":
        job_name = "copying" if args.action == "aws_replicate" else "indexing"
        source_bucket = args.bucket if job_name == "copying" else None
        aws_replicate.run(
            int(args.thread_num),
            json.loads(args.global_config),
            job_name,
            args.manifest_file,
            source_bucket,
        )

    elif args.action == "readact":
        delete_objects_from_cloud_resources(args.redact_file, args.log_bucket)

    end = timeit.default_timer()
    print("Total time: {} seconds".format(end - start))
