# dcf-dataservice
 Jobs to replicate data from GDC data center to AWS and Google buckets. The jobs need to be run inside a kubernetes cluster.

 GDC has two different data sources: Data center and AWS backup bucket which are synchronized continuously with each other. Every one or two months, GDC releases new/updated/deleted data in manifest files which serve as input of the dcf-dataservice to replicate the data from GDC data sources to DCF AWS buckets and DCF GOOGLE buckets respectively.

## GDC AWS bucket to DCF AWS buckets
 Simply use AWS API to sync in-AWS buckets. To boost up the performance, we deploy multiple-thread aws s3 cli.

## GDC data center to DCF GOOGLE buckets
 Deploy a google data flow job that copies data from GDC to GCP buckets. The `copy` transform of the data-flow pipeline streams objects from GDC data center to GOOGLE buckets.

## How to run
Create a directory in `$vpc_name/apis_configs` and name it as `dcf_dataservice` and follow the below steps to setup to run as k8 jobs

### AWS sync
- Put the `creds.json`, `aws_creds_secret` into the `dcf_dataservice` folder. While `aws_creds_secret` contains AWS key `creds.json` contains GDCAPI token and indexd account.

`aws_creds_secret`
```
aws_access_key_id=xxxxxxxxxx
aws_secret_access_key=xxxxxxxx
```
`creds.json`
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
- Also put the `GDC_project_map.json` into `dcf_dataservice` folder. This file provides a mapping between a project and an aws and google bucket prefix. It contains two fields `aws_bucket_prefix` and `google_bucket_prefix` to help determining which bucket an object need to be copied to. Please note that there are some buckets already created before we have the bucket name convention so this file is edited to support the old name buckets accordingly.
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
- Upload the `manifest` to S3 bucket `s3://INPUT_BUCKET/input/`.
- Run `jobs/kube-script-setup.sh`.
An example command to run the aws sync job with 100 threads and each thread handles 50 files.
```
gen3 runjob jobs/aws-bucket-replicate-job.yaml GDC_BUCKET_NAME gdcbackup MANIFEST_S3 s3://tests/manifest THREAD_NUM 100 LOG_BUCKET log_bucket CHUNK_SIZE 50
```

### GOOGLE sync
- Put the `gcloud-creds-secret` and `dcf_dataservice_settings` into the `dcf_dataservice` folder. 
`dcf_dataservice_settings`
```
GDC_TOKEN = "TOKEN"
INDEXD = {
    "host": "https://nci-crdc.datacommons.io/index/index",
    "version": "v0",
    "auth": {"username": "gdcapi", "password": "xxxxx"}
}
```
- Also put the `GDC_project_map.csv` into `dcf_dataservice` folder.
- Upload the `manifest` to a GS bucket `gs://INPUT_BUCKET/input/`. User also need to have `LOG_BUCKET` to store the log and the output. Please see the yaml job file for more detail.
- Put the file `ignored_files_manifest.csv` provided by the sponsor to `gs://INPUT_BUCKET/5aa/`. This file provides a list of structured or un-flatten objects already existed in DCF. ISB does not want DCF to flatten them since they are in use for their existed projects. The dcf refresh service need to ignore them during the replication process.
- Run `jobs/kube-script-setup.sh`.

An example command to run the google sync job.
```
gen3 runjob dcf-dataservice/jobs/google-bucket-replicate-job.yaml PROJECT cdis-test MAX_WORKERS 100 MANIFEST_FILE gs://INPUT_BUCKET/input/manifest IGNORED_FILE gs://INPUT_BUCKET/5aa/ignored_files_manifest.csv LOG_BUCKET log_bucket
```

### Redaction
- Put a redaction manifest to a S3 and run the following command
```
gen3 runjob jobs/remove-objects-from-clouds-job.yaml MANIFEST_S3 s3://bucket/manifest_redact LOG_BUCKET s3://log_bucket
```

### Manifest formats

Both the `sync manifest` and `redaction manifest` have the same format as described below.
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