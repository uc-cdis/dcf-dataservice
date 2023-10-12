import timeit
import argparse
import json

from cdislogging import get_logger

import scripts.aws_replicate as aws_replicate
import scripts.google_replicate as google_replicate
import scripts.index as index
import scripts.deletion as redact
import scripts.validate as validate

from scripts.settings import SLACK_URL

logger = get_logger("DataReplication")


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog="DataReplication",
        description="Manage data replication process; replication to cloud resources, indexing, redaction, and validation",
    )
    subparsers = parser.add_subparsers(title="action", dest="action")

    aws_replicate_cmd = subparsers.add_parser("aws_replicate")
    aws_replicate_cmd.add_argument("--global_config", required=True)
    aws_replicate_cmd.add_argument("--release", type=int, required=True)
    aws_replicate_cmd.add_argument("--thread_num", required=False)
    aws_replicate_cmd.add_argument(
        "--skip_to", required=False, help="Available options: "
    )
    aws_replicate_cmd.add_argument("--dry_run", required=False)

    google_replicate_cmd = subparsers.add_parser("google_replicate")
    google_replicate_cmd.add_argument("--global_config", required=True)
    google_replicate_cmd.add_argument("--release", type=int, required=True)
    google_replicate_cmd.add_argument("--thread_num", required=False)
    google_replicate_cmd.add_argument(
        "--skip_to", required=False, help="Available options: "
    )
    google_replicate_cmd.add_argument("--dry_run", required=False)

    index_cmd = subparsers.add_parser("index")
    index_cmd.add_argument("--global_config", required=True)
    index_cmd.add_argument("--release", type=int, required=True)
    index_cmd.add_argument("--thread_num", required=False)
    index_cmd.add_argument("--skip_to", required=False, help="Available options: ")
    index_cmd.add_argument("--dry_run", required=False)

    redact_cmd = subparsers.add_parser("redact")
    redact_cmd.add_argument("--global_config", required=True)
    redact_cmd.add_argument("--release", type=int, required=True)
    redact_cmd.add_argument("--thread_num", required=False)
    redact_cmd.add_argument("--skip_to", required=False, help="Available options: ")
    redact_cmd.add_argument("--dry_run", required=False)

    validate_cmd = subparsers.add_parser("validate")
    validate_cmd.add_argument("--global_config", required=True)
    validate_cmd.add_argument("--release", type=int, required=True)
    validate_cmd.add_argument(
        "--skip_to", required=False, help="Available options: cloud, IndexD, final"
    )
    validate_cmd.add_argument("--dry_run", required=False)

    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()
    webhook = WebhookClient(SLACK_URL)

    args = parse_arguments()
    try:
        slack_call = webhook.send(text=f"Starting {args.action}")
        assert slack_call.status_code == 200
    except AssertionError as e:
        logger.error(f"The slack hook has encountered an error: Detail {e}")

    if args.action == "aws_replicate":
        dry_run = True if args.dry_run == "True" else False
        aws_replicate.run(json.loads(args.global_config), dry_run)

    elif args.action == "google_replicate":
        dry_run = True if args.dry_run == "True" else False
        google_replicate.run(json.loads(args.global_config), dry_run)

    elif args.action == "index":
        dry_run = True if args.dry_run == "True" else False
        index.run(json.loads(args.global_config), dry_run)

    elif args.action == "redact":
        dry_run = False if args.dry_run == "False" else True
        redact.run(json.loads(args.global_config), dry_run)

    elif args.action == "validate":
        dry_run = True if args.dry_run == "True" else False
        validate.run(json.loads(args.global_config), dry_run)

    end = timeit.default_timer()
    logger.info(f"Total time: {end - start} seconds")

    try:
        slack_call = webhook.send(text=f"Completed {args.action}")
        assert slack_call.status_code == 200
    except AssertionError as e:
        logger.error(f"The slack hook has encountered an error: Detail {e}")
