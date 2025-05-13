# dcf-dataservice
 Jobs to replicate data from GDC data center to AWS and Google buckets. The jobs need to be run inside a kubernetes cluster.

 GDC has two different data sources: Data center and AWS backup bucket which are synchronized continuously with each other. Every one or two months, GDC releases new/updated/deleted (active/legacy/obsolete) data in manifest files which serve as input of the dcf-dataservice to replicate the data from GDC data sources to DCF AWS buckets and DCF GOOGLE buckets respectively.

## Replication Job in AWS and GCP
- Use AWS API to sync in-AWS buckets. To boost the performance, we deploy multiple-thread aws s3 cli.

- Deploy a google data flow job that copies data from GDC to GCP buckets. The `copy` transform of the data-flow pipeline streams objects from GDC data center to GOOGLE buckets.

## Adding new Projects (Buckets)
- Also, put the `GDC_project_map.json` into `dcf_dataservice` folder. This file provides a mapping between a project and an aws and google bucket prefix. It contains two fields `aws_bucket_prefix` and `google_bucket_prefix` to help determining which bucket an object need to be copied to. Please note that there are some buckets already created before we have the bucket name convention so this file is edited to support the old name buckets accordingly.
```
{
    "TARGET": {
        "aws_bucket_prefix": "target"},
        "gs_bucket_prefix": "gdc-target-phs000218"},
    "TCGA": {
        "aws_bucket_prefix": "tcga"},
        "gs_bucket_prefix": "gdc-tcga-phs000178"},
    "VAREPOP": {
        "aws_bucket_prefix": "gdc-varepop-apollo-phs001374"},
        "gs_bucket_prefix": "gdc-varepop-apollo-phs001374"},
}
```

## Manifest Files
### Manifest Formats
Both the `sync manifest` and `redaction manifest` have the same format as described below:
```
id	file_name	md5	size	state	project_id	baseid	version	release	acl	type	deletereason	url
ada53c3d-16ff-4e2a-8646-7cf78baf7aff	ada53c3d-16ff-4e2a-8646-7cf78baf7aff.vcf.gz	ff53a02d67fd28fcf5b8cd609cf51b06	137476	released	TCGA-LGG	6230cd6d-8610-4db4-94f4-2f87592c949b	1	12.0	[u'phs000xxx']	active		s3://xxxx/ada53c3d-16ff-4e2a-8646-7cf78baf7aff
2127dca2-e6b7-4e8a-859d-0d7093bd91e6	2127dca2-e6b7-4e8a-859d-0d7093bd91e6.vep.vcf.gz	c4deccb43f5682cffe8f56c97d602d08	136553	released	TCGA-LGG	6a84714f-2523-4424-b9cb-c21e8177cd0f	1	12.0	[u'phs000xxx']	active		s3://xxxx/2127dca2-e6b7-4e8a-859d-0d7093bd91e6/2127dca2-e6b7-4e8a-859d-0d7093bd91e6.vep.vcf.gz
```

The `ignored_files_manifest.csv`

```
gcs_object_size	gcs_object_time_stamp	md5sum	acl	gcs_object_url	gdc_uuid
19490957	2016-03-28T21:51:54Z	bd7b6da28d89c922ea26d145aef5387e	phs000xxx	gs://5aaxxxxxxxx/tcga/PAAD/DNA/AMPLICON/BI/ILLUMINA/C1748.TCGA-LB-A9Q5-01A-11D-A397-08.1.bam	e5c9f17d-8f66-4089-ac70-2a5cde55450f
```

### Copy Manifests
- Download released manifest files from box (Downloading and saving the files under named folder `DCF` makes copying files to DCF prod vm easier)

- Remove/replace old folder (if present) from DCF prod vm
`rm -r DCF`

- Copy manifest folder from local to DCF prod
`scp -r <local folder path (from) i.e. Downloads/DCF> <hostname>:<vm folder path (to) i.e. /home/dcfprod>`

- Copy manifest files from DCF folder to cloud buckets
(From /home/dcfprod/DCF directory)
**AWS bucket (data-refresh-manifest)**
`aws s3 cp --profile data_refresh ./<file> s3://data-refresh-manifest/<file>/`
**Google bucket (name of bucket)**
`<command goes here>`
Will also need to have `LOG_BUCKET` to store the log and the output. Please see the yaml job file for more detail.
- *Put the file `ignored_files_manifest.csv` provided by the sponsor under `gs://replication-input/<file>`. This file provides a list of structured or un-flattened objects already existing in DCF. ISB does not want DCF to flatten them since they are in use for existing projects but the DCF refresh service needs to ignore them during the replication process.*

- Run `jobs/kube-script-setup.sh`.

## Manage Tokens
Cloud tokens generally only need to be setup once, the NIH token expires (~30 days) so will most likely need to be replaced with every run

### Cloud Tokens
- Create a directory under `$vpc_name/apis_configs` and name it `dcf_dataservice`

### GDC Token
- Download token
Login to NIH (https://portal.gdc.cancer.gov/) and download token under user profile options
*From root directory*
- Replace token in config files if they exist already
`cd dcfprod/apis_configs/dcf_dataservice`
`vim dcf_dataservice_settings`
`vim creds.json`

#### Under the `dcf_dataservice` folder
- Create file named `creds.json`, this file contains GDCAPI token and indexd account
```
{
"GDC_TOKEN": "TOKEN",
"INDEXD": {
    "host": "https://nci-crdc.datacommons.io/index/index",
    "version": "v0",
    "auth": {"username": "gdcapi", "password": "xxxxx"}
}
}
```
- Create `dcf_dataservice_settings` file containing GDCAPI token, indexd account, and project bucket prefixes
```
GDC_TOKEN = xxxxxxxx

INDEXD = {
  "host": "https://nci-crdc.datacommons.io/index/index",
  "version": "v0",
  "auth": {
    "username": "gdcapi",
    "password": xxxxxxxx
  }
}

DATA_ENDPT = "https://api.gdc.cancer.gov/data/"

PROJECT_ACL = {
    "TEST-PROJECT": {
        "aws_bucket_prefix": "gdc-test-project-phs00xxxx",
        "gs_bucket_prefix": "gdc-test-project-phs00xxxx"
    },
    ...
}

IGNORED_FILES = "/dcf-dataservice/ignored_files_manifest.csv"
```

*For AWS*
- Create `aws_creds_secret` file containing AWS key
```
aws_access_key_id=xxxxxxxxxx
aws_secret_access_key=xxxxxxxx
```
- Create `aws_fence_bot_secret` file. For qa environments it should be identical to `aws_creds_secret` above. For production environment, generate it by terraform module. https://github.com/uc-cdis/gen3-terraform/tree/master/tf_files/aws/modules/fence-bot-user

*For GCP*
- Create `gcloud-creds-secret` file containing GCP key
```
{
  "type": "service_account",
  "project_id": "dcf-prod",
  "private_key_id": xxxxxxxx,
  "private_key": xxxxxxxx,
  "client_email": "data-replication-dcf-prod@dcf-prod.iam.gserviceaccount.com",
  "client_id": xxxxxxxx,
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": xxxxxxxx,
  "client_x509_cert_url": xxxxxxxx
}
```

### Final Setup
Lastly to finish setting up tokens, we need to run `gen3 kube-setup-data-replicate` to refresh the secrets.
Remember to pull from `cloud-automation` repo to ensure latest script is used.
https://github.com/uc-cdis/cloud-automation/blob/master/gen3/bin/kube-setup-data-replicate.sh

## Run Replication Job
### Dry Run
*AWS*
`gen3 runjob aws-bucket-replicate-job RELEASE DR<release number> QUICK_TEST True GDC_BUCKET_NAME gdcbackup MANIFEST_S3 s3://data-refresh-manifest/<active manifest file name> THREAD_NUM 20 LOG_BUCKET data-refresh-output CHUNK_SIZE 1`

*GCP*
`gen3 runjob google-bucket-replicate-job PROJECT dcf-prod MAX_WORKERS 80 RELEASE DR<release number>  MANIFEST_FILE gs://replication-input/<active manifest file name> IGNORED_FILE gs://replication-input/ignored_files_manifest.csv LOG_BUCKET datarefresh-log`

### If pass
Replace True with False in previous cmd

## Redaction
Upload redaction manifest to s3 and run job

### Dry Run
`gen3 runjob remove-objects-from-clouds-job DRY_RUN True RELEASE DR<release number> MANIFEST_S3 s3://data-refresh-manifest/<obsolete manifest file name> LOG_BUCKET data-refresh-output IGNORED_FILE_S3 s3://data-refresh-manifest/ignored_files_manifest.csv`

### If pass
Nothing needs to be redacted

## Validation
`gen3 runjob replicate-validation RELEASE DR<release number> IGNORED_FILE gs://replication-input/ignored_files_manifest.csv MANIFEST_FILES 's3://data-refresh-manifest/<active file>,s3://data-refresh-manifest/<legacy file>' OUT_FILES '<active file>,GDC_full_sync_legacy_manifest_20210423_post_DR29_DCF.tsv' LOG_BUCKET 'data-refresh-output'`

## Configuration
### AWS bucket replication
At this version, the script is supporting replicating the data using both aws cli and streaming from gdcapi. If object storage classes are Glacier or Infrequent access standard they are replicated by streaming from gdc api; otherwise they are directly replicated using aws cli.
There are two modes for running the replicating process: multiple processes and multiple threads. Each process/thread will handle one file at a time. Multiple processes, recommended for multiple-core VMs, can utilize much better bandwidth than multiple thread.

```
{
    "chunk_size": 100, # number of objects will be processed in single process/thread
    "log_bucket": "bucketname".
    "mode": "process|thread", # multiple process or multiple thread. Default: thread
    "quiet": 1|0, # specify if we want to print all the logs or not. Default: 0
    "from_local": 1|0, # specify how we want to check if object exist or not (*). On the fly or from json dictionary. Default 0
    "copied_objects": "path_to_the_file", # specify json file containing all copied objects
    "source_objects": "path_to_the_file", # specify json file containing all source objects (gdcbackup),
    "data_chunk_size": 1024 * 1024 * 128, # chunk size with multipart download and upload. Default 1024 * 1024 * 128
    "multi_part_upload_threads": 10, # Number of threads for multiple download and upload. Default 10
}

```

(*) It is quite costly to check if the object exist or not by asking aws bucket, user can build dictionaries for both DCF bucket and GDC bucket for look up purpose and save as json files. The trade-off is the memory cost.

While aws cli runs on server side, users have to deal with throughput issues when streaming data from gdc api. GDC currently configures each VM can not get more than 250 concurrently. The user should pay attention to make sure that `number of process/thread * multi_part_upload_threads < 250`

### GS bucket replication

The followings is an example of configuration for running google replication with google dataflow.

```
{
    "scan_copied_objects": 1, #
    "chunk_size_download": 1024 * 1024 * 32, # chunk size with download from gdcapi. Default 1024 * 1024 * 32
    "chunk_size_upload": 1024 * 1024 * 256, # chunk size with download from gdcapi. Default 1024 * 1024 * 128
    "multi_part_upload_threads": 10, # Number of threads for multiple download and upload. Default 10
}

```
