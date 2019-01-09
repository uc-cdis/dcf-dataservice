# dcf-dataservice
 Copy data buckets from GDC to AWS and Google buckets. The jobs need to be run inside a setup kubeneste cluster.

 GDC has two different data storages: Data center and AWS backup bucket which are synchronized continuously with each other. Every one or two months, GDC releases new/updated/deleted data in manifest files, the goal is to deploy two separated jobs which copy the data to AWS, GOOGLE cloud respectively.

## GDC AWS bucket to DCF AWS buckets
 Simply using AWS API to sync in-AWS buckets. To boost up the performance, we deploy multiple-thread aws s3 copying.

## GDC data center to DCF GOOGLE buckets
 Deploy a data-flow job that copies data from GDC to GCP buckets. The `copy` transform of the data-flow pipeline streams objects from GDC data center to GOOGLE buckets.

## How to run
Following the below steps to setup
- Create a directory in `$vpc_name/apis_configs` and named as `dcf_dataservice`.
- Put the `creds.json`, `aws_creds_secret` and `gcloud-creds-secret` into `dcf_dataservice` folder. While `aws_creds_secret` contains AWS key `gcloud-creds-secret` contains google cloud service account.
- Also put the `GDC_datasets_access_control.csv` into `dcf_dataservice` folder. This file contains GDC project access control level. The file is little bit different to 
the one GDC provides. It has two more column to map the project id to dcf bucket: `aws_bucket_prefix` and `google_bucket_prefix`. Let say if the file has project_id `TCGA-CLM`, the google_bucket_prefix and aws_bucket_prefix should be `gdc-tcga-phs000178`. Please note that there are some dcf buckets already created before we have the bucket name convention so the this file is edited to support the old name buckets accordingly.
- Upload the `manifest` in S3 for AWS sync. For GOOLE sync, put the file into `gs://INPUT_BUCKET//input/`. User also need to have `LOG_BUCKET` to store the log and the output outcome. Please see the yaml job file for more detail.
- Run `jobs/kube-script-setup.sh`.
- See jobs/*.yaml file for more details how to run the jobs.

## Some other notes
