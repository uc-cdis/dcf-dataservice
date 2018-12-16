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

    redact_cmd = subparsers.add_parser("readact")
    redact_cmd.add_argument("--manifest_file", required=True)
    redact_cmd.add_argument("--log_file", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()

    # from google_replicate import exec_google_copy

    # fi = {
    #     "id": "000ed0fb-d1f4-4b80-8d77-0d134bb4c0d6",
    #     "filename": "test.py",
    #     "md5": "dd0022192f447a21f44cf1058e3371de",
    #     "size": 5880133619,
    #     "state": "released",
    #     "acl": "controlled",
    #     "project_id": "TARGET-CNC",
    # }
    # import pdb

    # pdb.set_trace()
    # exec_google_copy(fi, {})

    args = parse_arguments()
    if args.action == "aws_replicate":
        # eg. python replicate.py aws_replicate --bucket mybucket20018 --manifest_file ./manifest --global_config '{"chunk_size": 4}'
        aws = AWSBucketReplication(
            bucket=args.bucket,
            manifest_file=args.manifest_file,
            global_config=json.loads(args.global_config),
        )
        aws.run()
    elif args.action == "readact":
        delete_objects_from_cloud_resources(args.manifest, args.log_file)

    end = timeit.default_timer()
    print("Total time: {} seconds".format(end - start))
