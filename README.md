# dcf-dataservice
Copy data buckets from GDC to AWS and Google buckets. The jobs need to be run inside a setup kubeneste cluster.

Following the below steps to setup
- Create a directory in `$vpc_name/apis_configs` and named as `dcf_dataservice`.
- Put the `manifest` file, `aws_creds_secret` and `gcloud-creds-secret` into `dcf_dataservice` folder. While `aws_creds_secret` contains AWS key `gcloud-creds-secret` contains google cloud service account.
- Run `jobs/kube-script-setup.sh`.
- See jobs/*.yaml file for more details how to run the jobs.
