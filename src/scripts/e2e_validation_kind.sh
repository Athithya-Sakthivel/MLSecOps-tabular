# Optionally delete bucket for fresh run. make delete-temp-s3

export S3_BUCKET=$(aws sts get-caller-identity --query Account --output text | sed 's/^/s3-temp-bucket-mlsecops-/')
export MLFLOW_S3_BUCKET=$S3_BUCKET
export PG_BACKUPS_S3_BUCKET=$S3_BUCKET
export MODEL_ARTIFACTS_S3_BUCKET=$S3_BUCKET

make temp-s3

make core                                  # create fresh kind Kubernetes cluster + default storage class

export K8S_CLUSTER=kind                    # target Kubernetes platform (kind)
export PG_BACKUPS_S3_BUCKET=$S3_BUCKET     # S3 bucket storing Postgres backups
export PG_CLUSTER_ID=cnpg-cluster-kind     # stable S3 namespace for this environment
export PG_SERVER_NAME=mlsecops             # stable backup lineage identifier
make pg-cluster                            # deploy fresh Postgres cluster (no restore, no initial backup)

make elt                                   # deploy Iceberg + Spark + Flyte and run ELT pipeline

make prune-elt                             # cleanup Spark operator / ELT-related resources

export K8S_CLUSTER=kind                    # re-export (ensures env consistency)
export PG_BACKUPS_S3_BUCKET=$S3_BUCKET   # same S3 bucket (must match lineage)
export PG_CLUSTER_ID=cnpg-cluster-kind     # same namespace (must NOT change)
export PG_SERVER_NAME=mlsecops             # same lineage (must NOT change)
make pg-backup                             # create base backup + archive WAL to S3

make core                                  # destroy and recreate Kubernetes cluster (stateless reset)

export K8S_CLUSTER=kind                    # reconfigure environment after cluster reset
export PG_BACKUPS_S3_BUCKET=$S3_BUCKET   # same backup bucket
export PG_CLUSTER_ID=cnpg-cluster-kind     # same backup namespace
export PG_SERVER_NAME=mlsecops             # same lineage name

# Restore iceberg tables so train workflow can read persisted data (Iceberg metadata lives in Postgres)
make pg-restore-latest                     # restore latest base backup + WAL from s3 into fresh k8s cluster

make train                                 # run Flyte training workflow (consumes Gold Iceberg tables)

aws s3 ls s3://$S3_BUCKET/model-artifacts/ --recursive

make core && bash src/infra/deploy/kuberay_operator.sh --rollout

export MODEL_URI=s3://e2e-mlops-data-681802563986/model-artifacts/trip_eta_lgbm_v1/bceb2eb9-e373-4c44-91e5-abde147fec8b/2025-01-06
export MODEL_VERSION=v1
export MODEL_SHA256=29505278adb825a2f79812221b5d3a245145e140973d0354b74e278b50811976
export MODEL_INPUT_NAME=input
export MODEL_OUTPUT_NAMES=variable
export FEATURE_ORDER=pickup_hour,pickup_dow,pickup_month,pickup_is_weekend,pickup_borough_id,pickup_zone_id,pickup_service_zone_id,dropoff_borough_id,dropoff_zone_id,dropoff_service_zone_id,route_pair_id,avg_duration_7d_zone_hour,avg_fare_30d_zone,trip_count_90d_zone_hour
export ALLOW_EXTRA_FEATURES=false
export MODEL_CACHE_DIR=/mlsecops/model-cache
export LOG_LEVEL=WARNING
export RAY_IMAGE=ghcr.io/athithya-sakthivel/tabular-inference-service:2026-04-12-18-01--cb879d3@sha256:745919dfe5ffec5474c0f07d28ed067e30ea232ccf1d49b7d177b0096e8a6bdf
export USE_IAM=false


python3 src/infra/deploy/inference_service.py --delete
python3 src/infra/deploy/inference_service.py --rollout
sleep 120
kubectl get pods -A

python3 src/infra/deploy/inference_service.py --delete


kubectl port-forward -n inference svc/tabular-inference-t9m5h-head-svc 8000:8000