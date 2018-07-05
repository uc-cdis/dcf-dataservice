 #!/bin/bash
 #
 # Initializes the Gen3 k8s secrets and services.
 #
 set -e

 source "${GEN3_HOME}/gen3/lib/utils.sh"
 gen3_load "gen3/lib/kube-setup-init"

 if [[ -d apis_configs/dcf_dataservice ]]; then
   if ! g3kubectl get secret aws-creds-secret > /dev/null 2>&1; then
      g3kubectl create secret generic aws-creds-secret --from-file=credentials=./apis_configs/dcf_dataservice/aws_creds_secret
   fi

   if ! g3kubectl get secret google-creds-secret > /dev/null 2>&1; then
      g3kubectl create secret generic google-creds-secret --from-file=google_service_account_creds=./apis_configs/dcf_dataservice/gcloud-creds-secret
   fi

   if ! g3kubectl get secrets/dcf-dataservice-json-secret > /dev/null 2>&1; then
      if [[ ! -f "./apis_configs/dcf_dataservice/creds.json" ]]; then
          touch "./apis_configs/dcf_dataservice/creds.json"
      fi
      echo "create dcf-dataservice-json-secret using current creds file apis_configs/dcf_dataservice/creds.json"
      g3kubectl create secret generic dcf-dataservice-json-secret --from-file=dcf_dataservice_credentials.json=./apis_configs/dcf_dataservice/creds.json
   fi

   if ! g3kubectl get configmaps/data-service-manifest > /dev/null 2>&1; then
      if [[ ! -f "./apis_configs/dcf_dataservice/manifest" ]]; then
        touch "./apis_configs/dcf_dataservice/manifest"
      fi
      g3kubectl create configmap data-service-manifest --from-file=manifest=./apis_configs/dcf_dataservice/manifest
   fi
fi
