import os
import time
import timeit
import boto3, botocore

from cdislogging import get_logger
from indexclient.client import IndexClient

from utils import get_fileinfo_list_from_manifest, get_bucket_name
from aws_replicate import check_bucket as aws_check_bucket, check_object as aws_check_object
from google_replicate import check_bucket as gs_check_bucket, check_blob_name_exists_and_match_md5 as gs_check_object
from settings import SIGNPOST, PROJECT_MAP


logger = get_logger("Indexd Update Service")

indexclient = IndexClient(SIGNPOST['host'], SIGNPOST['version'], SIGNPOST['auth'])

def get_file_from_uuid(uuid):
    '''
    get document from indexd with provided uuid
    '''
    doc = None
    if uuid:
        doc = indexclient.get(uuid)
    return doc

def create_index(**kwargs):
    return indexclient.create(**kwargs)

def update_indexd_from_manifest(manifest_file, aws_bucket_name)
    submitting_files = get_fileinfo_list_from_manifest(manifest_file)
    s3 = boto3.resource('s3')
    for fi in submitting_files:
        doc = get_file_from_uuid(fi.get('fileid',''))
        if doc is not None:
            logger.info("document with uuid {} already existed")
            continue
        urls = ['https://gov', ]
        gonna_add_s3_loc = True
        gonna_add_gs_loc = True

        s3_bucket_name = get_bucket_name(fi, PROJECT_MAP)
        s3_object_name = "{}/{}".format(fi.get("fileid"), fi.get("filename"))

        if not aws_check_bucket(s3, s3_bucket_name):
            logger.info("aws bucket {} is not existed".format(s3_bucket_name))
            gonna_add_s3_loc = False
        if not aws_check_object(s3, s3_bucket_name, s3_object_name):
            logger.info("aws object {} is not existed".format(s3_object_name))
            gonna_add_s3_loc = False

        gs_bucket_name = s3_bucket_name
        gs_object_name = s3_object_name

        if not gs_check_bucket(gs_bucket_name):
            logger.info("gcs bucket {} is not existed".format(gs_bucket_name))
            gonna_add_gs_loc = False
        if not gs_check_object(gs_bucket_name, gs_object_name, fi):
            logger.info("gcs object {} is not existed".format(gs_object_name))
            gonna_add_gs_loc = False

        if gonna_add_aws_loc:
            urls.append("s3://{}/{}".format(s3_bucket_name, s3_object_name))
        if gonna_add_gs_loc:
            urls.append("gs://{}/{}".format(gs_bucket_name, gs_object_name))

        doc = create_index(did=fi.get('fileid',''),
                           hashes=fi.get('hash',''),
                           size=fi.get('size',0),
                           urls=urls)
        if doc is None:
            logger.info("successfuly create an record with uuid {}".format(fi.get('fileid','')))
        else:
            logger.info("fail to create an record with uuid {}".format(fi.get('fileid','')))

if __name__ == "__main__":
    start = timeit.default_timer()
    #args = parse_arguments()
    aws = AWSBucketReplication('from','to','test')
    aws.prepare()
    aws.run()
    end = timeit.default_timer()
    print end-start
    #if args.action == 'sync':
    #    print "sync from gdc aws bucket to gen3 dcf bucket"
