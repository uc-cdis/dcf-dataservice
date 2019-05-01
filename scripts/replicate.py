import timeit
import argparse
import json

import aws_replicate
import google_replicate
import validate
from deletion import delete_objects_from_cloud_resources


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    aws_replicate_cmd = subparsers.add_parser("aws_replicate")
    aws_replicate_cmd.add_argument("--release", required=True)
    aws_replicate_cmd.add_argument("--global_config", required=True)
    aws_replicate_cmd.add_argument("--bucket", required=True)
    aws_replicate_cmd.add_argument("--manifest_file", required=True)
    aws_replicate_cmd.add_argument("--thread_num", required=True)

    google_replicate_cmd = subparsers.add_parser("google_replicate")
    google_replicate_cmd.add_argument("--global_config", required=True)
    google_replicate_cmd.add_argument("--manifest_file", required=True)
    google_replicate_cmd.add_argument("--thread_num", required=True)

    google_validate_cmd = subparsers.add_parser("validate")
    google_validate_cmd.add_argument("--global_config", required=True)

    aws_indexing_cmd = subparsers.add_parser("indexing")

    # set config in dictionary. Only for AWS replicate:
    # {
    #     "chunk_size": 100, # number of objects will be processed in single process/thread
    #     "log_bucket": "bucketname".
    #     "mode": "process|thread", # multiple process or multiple thread. Default: thread
    #     "quite": 1|0, # specify if we want to print all the logs or not. Default: 0
    #     "from_local": 1|0, # specify how we want to check if object exist or not (*). On the fly or from json dictionary. Deault 0
    #     "copied_objects": "path_to_the_file", # specify json file containing all copied objects
    #     "source_objects": "path_to_the_file", # specify json file containing all source objects (gdcbackup),
    #     "data_chunk_size": 1024 * 1024 * 128, # chunk size with multipart download and upload. Default 1024 * 1024 * 128
    #     "multi_part_upload_threads": 10, # Number of threads for multiple download and upload. Default 10
    # }
    aws_indexing_cmd.add_argument("--global_config", required=True)
    aws_indexing_cmd.add_argument("--manifest_file", required=True)
    aws_indexing_cmd.add_argument("--thread_num", required=True)

    redact_cmd = subparsers.add_parser("readact")
    redact_cmd.add_argument("--redact_file", required=True)
    redact_cmd.add_argument("--log_bucket", required=True)
    redact_cmd.add_argument("--release", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()

    args = parse_arguments()
    if args.action == "aws_replicate" or args.action == "indexing":
        job_name = "copying" if args.action == "aws_replicate" else "indexing"
        source_bucket = args.bucket if job_name == "copying" else None
        aws_replicate.run(
            args.release,
            int(args.thread_num),
            json.loads(args.global_config),
            job_name,
            args.manifest_file,
            source_bucket,
        )
    elif args.action == "google_replicate":
        job_name = "copying"

        google_replicate.run(
            int(args.thread_num),
            json.loads(args.global_config),
            job_name,
            args.manifest_file,
            None,
        )
    elif args.action == "validate":
        validate.run(
            json.loads(args.global_config)
        )

    elif args.action == "readact":
        delete_objects_from_cloud_resources(args.redact_file, args.log_bucket, args.release)

    end = timeit.default_timer()
    print("Total time: {} seconds".format(end - start))
