#!/bin/bash
#
# Initializes the Gen3 k8s secrets and services.
#
set -e

source "${GEN3_HOME}/gen3/lib/utils.sh"
gen3_load "gen3/lib/kube-setup-init"

if [[ -d ${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice ]]; then
  if ! g3kubectl get secret aws-creds-secret > /dev/null 2>&1; then
     g3kubectl create secret generic aws-creds-secret --from-file=credentials=${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/aws_creds_secret
  fi

  if ! g3kubectl get secret google-creds-secret > /dev/null 2>&1; then
     g3kubectl create secret generic google-creds-secret --from-file=google_service_account_creds=${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/gcloud-creds-secret
  fi

  if ! g3kubectl get secrets/dcf-dataservice-json-secret > /dev/null 2>&1; then
     if [[ ! -f "${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/creds.json" ]]; then
         touch "${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/creds.json"
     fi
     echo "create dcf-dataservice-json-secret using current creds file apis_configs/dcf_dataservice/creds.json"
     g3kubectl create secret generic dcf-dataservice-json-secret --from-file=dcf_dataservice_credentials.json=${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/creds.json
  fi

  if ! g3kubectl get secrets/dcf-dataservice-settings-secrets > /dev/null 2>&1; then
      if [[ ! -f "${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice_settings" ]]; then
          touch "${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice_settings"
      fi
      g3kubectl create secret generic dcf-dataservice-settings-secrets --from-file=dcf_dataservice_settings=${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/dcf_dataservice_settings
   fi

  if ! g3kubectl get configmaps/project-acl-manifest > /dev/null 2>&1; then
     if [[ ! -f "${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/GDC_datasets_access_control.csv" ]]; then
       touch "${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/GDC_datasets_access_control.csv"
     fi
     g3kubectl create configmap project-acl-manifest --from-file=GDC_datasets_access_control.csv=${WORKSPACE}/${vpc_name}/apis_configs/dcf_dataservice/GDC_datasets_access_control.csv
  fi
fi
