# dcf-dataservice
 Copy data buckets from GDC to AWS and Google buckets. The jobs need to be run inside a setup kubeneste cluster.

 GDC has two different data storages: Data center and AWS backup bucket which are synchronized continuously with each other. Every one or two months, GDC releases new/updated/deleted data in manifest files, the goal is to deploy two separated jobs which copy the data to AWS, GOOGLE cloud respectively.

## GDC AWS bucket to DCF AWS buckets
 Simply using AWS API to sync in-AWS buckets. To boost up the performance, we deploy multiple-thread aws s3 copying.

## GDC data center to DCF GOOGLE buckets
 Deploy a data-flow job that copies data from GDC to GCP buckets. The `copy` transform of the data-flow pipeline streams objects from GDC data center to GOOGLE buckets.

## How to run
Following the below steps to setup to run as k8 jobs
- Create a directory in `$vpc_name/apis_configs` and named as `dcf_dataservice`.
- Put the `creds.json`, `aws_creds_secret` and `gcloud-creds-secret` into `dcf_dataservice` folder. While `aws_creds_secret` contains AWS key `gcloud-creds-secret` contains google cloud service account.
- Also put the `GDC_datasets_access_control.csv` into `dcf_dataservice` folder. This file contains GDC project access control level. The file is little bit different to 
the one GDC provides. It has two more column to map the project id to dcf bucket: `aws_bucket_prefix` and `google_bucket_prefix`. Let say if the file has project_id `TCGA-CLM`, the google_bucket_prefix and aws_bucket_prefix should be `gdc-tcga-phs000178`. Please note that there are some dcf buckets already created before we have the bucket name convention so the this file is edited to support the old name buckets accordingly.
- Upload the `manifest` in S3 for AWS sync. For GOOLE sync, put the file into `gs://INPUT_BUCKET//input/`. User also need to have `LOG_BUCKET` to store the log and the output outcome. Please see the yaml job file for more detail.
- Run `jobs/kube-script-setup.sh`.
- See jobs/*.yaml file for more details how to run the jobs.

## Configuration
### AWS bucket replication
At this version, the script is supporting replicating the data using both aws cli and streaming from gdcapi. If object storage classes are Glacier or Infrequent access standard they are replicated by streaming from gdc api; otherwise they are directly replicated using aws cli.
There are two modes for running the replicating process: multiple processes and multiple threads. Each process/thread will handle one file at a time. Mutiple processes, recommended for multiple-core VMs, can utilize much better bandwidth than multiple thread.

```
{
    "chunk_size": 100, # number of objects will be processed in single process/thread
    "log_bucket": "bucketname".
    "mode": "process|thread", # multiple process or multiple thread. Default: thread
    "quite": 1|0, # specify if we want to print all the logs or not. Default: 0
    "from_local": 1|0, # specify how we want to check if object exist or not (*). On the fly or from json dictionary. Deault 0
    "copied_objects": "path_to_the_file", # specify json file containing all copied objects
    "source_objects": "path_to_the_file", # specify json file containing all source objects (gdcbackup),
    "data_chunk_size": 1024 * 1024 * 128, # chunk size with multipart download and upload. Default 1024 * 1024 * 128
    "multi_part_upload_threads": 10, # Number of threads for multiple download and upload. Default 10
}

```

(*) It is quite costly to check if the object exist or not by asking aws bucket, user can build dictionaries for both DCF bucket and GDC bucket for look up purpose and save as json files. The trade-off is the memory cost.

While aws cli runs on server side, user have to deal with throughput issues when streaming data from gdc api. GDC currently configure each VM can not get more than 250 concurency. The user should pay attention to make sure that `number of process/thread * multi_part_upload_threads < 250`

### GS bucket replication

