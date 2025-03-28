import timeit
import argparse
import json

import dcfdataservice.aws_replicate as aws_replicate
import dcfdataservice.google_replicate as google_replicate
import dcfdataservice.validate as validate
from dcfdataservice.deletion import delete_objects_from_cloud_resources
from slack_sdk.webhook import WebhookClient
from cdislogging import get_logger

try:
    from dcfdataservice.settings import SLACK_URL
except ImportError:
    SLACK_URL = None


logger = get_logger("DCFReplicate")


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    aws_replicate_cmd = subparsers.add_parser("aws_replicate")
    aws_replicate_cmd.add_argument("--release", required=True)
    aws_replicate_cmd.add_argument("--global_config", required=True)
    aws_replicate_cmd.add_argument("--quick_test", required=True)
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

    redact_cmd = subparsers.add_parser("redact")
    redact_cmd.add_argument("--dry_run", required=False)
    redact_cmd.add_argument("--redact_file", required=True)
    redact_cmd.add_argument("--log_bucket", required=True)
    redact_cmd.add_argument("--release", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()
    args = parse_arguments()
    if SLACK_URL:
        try:
            webhook = WebhookClient(SLACK_URL)
            slack_call = webhook.send(text=f"Starting {args.action}")
            assert slack_call.status_code == 200
        except AssertionError as e:
            logger.error("The slack hook has encountered an error: Detail {}".format(e))
    else:
        logger.warning("SLACK_URL not configured")

    if args.action == "aws_replicate" or args.action == "indexing":
        job_name = "copying" if args.action == "aws_replicate" else "indexing"
        source_bucket = args.bucket if job_name == "copying" else None
        quick_test = True if args.quick_test == "True" else False
        aws_replicate.run(
            args.release,
            int(args.thread_num),
            json.loads(args.global_config),
            job_name,
            args.manifest_file,
            quick_test,
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
        validate.run(json.loads(args.global_config))

    elif args.action == "redact":
        dry_run = False if args.dry_run == "False" else True
        delete_objects_from_cloud_resources(
            args.redact_file, args.log_bucket, args.release, dry_run
        )

    end = timeit.default_timer()
    print("Total time: {} seconds".format(end - start))

    if SLACK_URL:
        try:
            slack_call = webhook.send(text=f"Completed {args.action}")
            assert slack_call.status_code == 200
        except AssertionError as e:
            logger.error("The slack hook has encountered an error: Detail {}".format(e))
