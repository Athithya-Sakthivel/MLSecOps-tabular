#!/usr/bin/env bash
# Bootstraps a full Flyte deployment with spark plugin on Kubernetes using Helm with deterministic configuration.
# Discovers CNPG Postgres credentials, ensures required databases exist, and wires Flyte services to them.
# Creates Kubernetes Secrets for DB password and, when USE_IAM=false, AWS credentials for task pods in execution namespaces.
# Dynamically renders a storage-aware Helm values file (S3/GCS/Azure) and applies it via `helm upgrade --install`.
# Waits for all deployments to become ready and outputs access details along with cluster diagnostics on failure.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT_DIR}"

TARGET_NS="${TARGET_NS:-flyte}"
POSTGRES_NS="${POSTGRES_NS:-default}"
CNPG_CLUSTER="${CNPG_CLUSTER:-postgres-cluster}"
POOLER_SERVICE="${POOLER_SERVICE:-postgres-pooler}"
DB_ACCESS_MODE="${DB_ACCESS_MODE:-rw}"
DB_HOST="${DB_HOST:-}"
POOLER_PORT="${POOLER_PORT:-5432}"

DB_SECRET_NAME="${DB_SECRET_NAME:-db-pass}"
AUTH_SECRET_NAME="${AUTH_SECRET_NAME:-flyte-secret-auth}"

FLYTE_ADMIN_DB="${FLYTE_ADMIN_DB:-flyteadmin}"
FLYTE_DATACATALOG_DB="${FLYTE_DATACATALOG_DB:-datacatalog}"

FLYTE_OAUTH_CLIENT_ID="${FLYTE_OAUTH_CLIENT_ID:-flytepropeller}"
FLYTE_OAUTH_CLIENT_SECRET="${FLYTE_OAUTH_CLIENT_SECRET:-flytepropeller-secret}"

STORAGE_PROVIDER="${STORAGE_PROVIDER:-auto}"
USE_IAM="${USE_IAM:-false}"
AWS_AUTH_MODE="${AWS_AUTH_MODE:-iam}"

AWS_REGION="${AWS_REGION:-ap-south-1}"
S3_BUCKET="${S3_BUCKET:-e2e-mlops-data-681802563986}"
S3_PREFIX="${S3_PREFIX:-}"
S3_ENDPOINT="${S3_ENDPOINT:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
AWS_ROLE_ARN="${AWS_ROLE_ARN:-}"

GCP_PROJECT="${GCP_PROJECT:-}"
GCP_BUCKET="${GCP_BUCKET:-mlops_iceberg_warehouse}"
GCP_SA_EMAIL="${GCP_SA_EMAIL:-}"

AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-}"
AZURE_CONTAINER="${AZURE_CONTAINER:-iceberg}"
AZURE_CLIENT_ID="${AZURE_CLIENT_ID:-}"
AZURE_TENANT_ID="${AZURE_TENANT_ID:-}"
FLYTE_STORAGE_CUSTOM_YAML="${FLYTE_STORAGE_CUSTOM_YAML:-}"

FLYTE_INGRESS_ENABLED="${FLYTE_INGRESS_ENABLED:-false}"
FLYTE_INGRESS_CLASS_NAME="${FLYTE_INGRESS_CLASS_NAME:-}"
FLYTE_SEPARATE_GRPC_INGRESS="${FLYTE_SEPARATE_GRPC_INGRESS:-false}"

FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED="${FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED:-true}"
FLYTE_WORKFLOW_SCHEDULER_ENABLED="${FLYTE_WORKFLOW_SCHEDULER_ENABLED:-false}"
FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED="${FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED:-false}"
FLYTE_EXTERNAL_EVENTS_ENABLED="${FLYTE_EXTERNAL_EVENTS_ENABLED:-false}"
FLYTE_CLOUD_EVENTS_ENABLED="${FLYTE_CLOUD_EVENTS_ENABLED:-false}"
FLYTE_CONNECTOR_ENABLED="${FLYTE_CONNECTOR_ENABLED:-false}"
FLYTE_SPARK_OPERATOR_ENABLED="${FLYTE_SPARK_OPERATOR_ENABLED:-true}"

CHART_REPO_NAME="${CHART_REPO_NAME:-flyte}"
CHART_REPO_URL="${CHART_REPO_URL:-https://flyteorg.github.io/flyte}"
CHART_NAME="${CHART_NAME:-flyte-core}"
CHART_VERSION="${CHART_VERSION:-1.16.4}"
RELEASE_NAME="${RELEASE_NAME:-flyte}"

MANIFEST_DIR="${MANIFEST_DIR:-src/manifests/flyte}"
VALUES_FILE="${VALUES_FILE:-${MANIFEST_DIR}/values.yaml}"
READY_TIMEOUT="${READY_TIMEOUT:-1200}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-1200s}"
FLYTE_ATOMIC="${FLYTE_ATOMIC:-false}"

# Namespaces where Flyte task pods will run and where the task AWS secret must exist.
# Default matches the namespace used in the current ELT workflow.
FLYTE_TASK_NAMESPACES="${FLYTE_TASK_NAMESPACES:-flytesnacks-development}"
TASK_AWS_SECRET_NAME="${TASK_AWS_SECRET_NAME:-flyte-aws-credentials}"

mkdir -p "${MANIFEST_DIR}"

FLYTE_DASK_OPERATOR_ENABLED="${FLYTE_DASK_OPERATOR_ENABLED:-false}"
FLYTE_DATABRICKS_ENABLED="${FLYTE_DATABRICKS_ENABLED:-false}"

log() { printf '[%s] [flyte] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
fatal() { printf '[%s] [flyte][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; exit 1; }
require_bin() { command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"; }

yaml_bool() {
  case "$(printf '%s' "${1:-false}" | tr '[:upper:]' '[:lower:]')" in
    true|1|yes|y|on) printf 'true' ;;
    *) printf 'false' ;;
  esac
}

detect_storage_provider() {
  case "${STORAGE_PROVIDER}" in
    aws|gcs|azure)
      printf '%s' "${STORAGE_PROVIDER}"
      ;;
    auto)
      if [[ -n "${AZURE_STORAGE_ACCOUNT}" || -n "${AZURE_CLIENT_ID}" || -n "${AZURE_TENANT_ID}" ]]; then
        printf 'azure'
      elif [[ -n "${GCP_PROJECT}" || -n "${GCP_SA_EMAIL}" ]]; then
        printf 'gcs'
      else
        printf 'aws'
      fi
      ;;
    *)
      fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
      ;;
  esac
}

split_namespaces() {
  printf '%s' "${FLYTE_TASK_NAMESPACES}" | tr ',; ' '\n\n\n' | awk 'NF'
}

ensure_namespace() {
  local namespace="$1"
  kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl label namespace "${namespace}" app.kubernetes.io/part-of=flyte --overwrite >/dev/null 2>&1 || true
}

ensure_task_aws_secret() {
  local namespace="$1"
  ensure_namespace "${namespace}"

  local -a cmd=(kubectl -n "${namespace}" create secret generic "${TASK_AWS_SECRET_NAME}")

  cmd+=(--from-literal="AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}")
  cmd+=(--from-literal="AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}")
  cmd+=(--from-literal="AWS_DEFAULT_REGION=${AWS_REGION}")
  cmd+=(--from-literal="AWS_REGION=${AWS_REGION}")
  cmd+=(--from-literal="FLYTE_AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}")
  cmd+=(--from-literal="FLYTE_AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}")
  cmd+=(--from-literal="FLYTE_AWS_ENDPOINT=${S3_ENDPOINT}")
  cmd+=(--dry-run=client -o yaml)

  if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
    cmd=(kubectl -n "${namespace}" create secret generic "${TASK_AWS_SECRET_NAME}" \
      --from-literal="AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
      --from-literal="AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
      --from-literal="AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}" \
      --from-literal="AWS_DEFAULT_REGION=${AWS_REGION}" \
      --from-literal="AWS_REGION=${AWS_REGION}" \
      --from-literal="FLYTE_AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
      --from-literal="FLYTE_AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
      --from-literal="FLYTE_AWS_ENDPOINT=${S3_ENDPOINT}" \
      --dry-run=client -o yaml)
  fi

  if [[ -z "${S3_ENDPOINT}" ]]; then
    cmd=(kubectl -n "${namespace}" create secret generic "${TASK_AWS_SECRET_NAME}" \
      --from-literal="AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
      --from-literal="AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
      --from-literal="AWS_DEFAULT_REGION=${AWS_REGION}" \
      --from-literal="AWS_REGION=${AWS_REGION}" \
      --from-literal="FLYTE_AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
      --from-literal="FLYTE_AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
      --dry-run=client -o yaml)
    if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
      cmd=(kubectl -n "${namespace}" create secret generic "${TASK_AWS_SECRET_NAME}" \
        --from-literal="AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
        --from-literal="AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
        --from-literal="AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}" \
        --from-literal="AWS_DEFAULT_REGION=${AWS_REGION}" \
        --from-literal="AWS_REGION=${AWS_REGION}" \
        --from-literal="FLYTE_AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
        --from-literal="FLYTE_AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
        --dry-run=client -o yaml)
    fi
  fi

  "${cmd[@]}" | kubectl apply -f - >/dev/null
}

find_app_secret_name() {
  local s
  s="$(kubectl -n "${POSTGRES_NS}" get secret -l "cnpg.io/cluster=${CNPG_CLUSTER},cnpg.io/userType=app" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${s}" ]]; then
    printf '%s' "${s}"
    return 0
  fi
  if kubectl -n "${POSTGRES_NS}" get secret "${CNPG_CLUSTER}-app" >/dev/null 2>&1; then
    printf '%s' "${CNPG_CLUSTER}-app"
    return 0
  fi
  printf ''
}

secret_value() {
  local secret_name="$1"
  local key="$2"
  kubectl -n "${POSTGRES_NS}" get secret "${secret_name}" -o "jsonpath={.data.${key}}" 2>/dev/null | base64 -d || true
}

get_primary_pod() {
  kubectl -n "${POSTGRES_NS}" get pods -l "cnpg.io/cluster=${CNPG_CLUSTER},cnpg.io/instanceRole=primary" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

resolve_db_host() {
  if [[ -n "${DB_HOST}" ]]; then
    return 0
  fi
  case "${DB_ACCESS_MODE}" in
    rw)
      DB_HOST="${CNPG_CLUSTER}-rw.${POSTGRES_NS}.svc.cluster.local"
      ;;
    pooler)
      DB_HOST="${POOLER_SERVICE}.${POSTGRES_NS}.svc.cluster.local"
      ;;
    *)
      fatal "unsupported DB_ACCESS_MODE=${DB_ACCESS_MODE}"
      ;;
  esac
}

ensure_database() {
  local db_name="$1"
  local primary exists
  primary="$(get_primary_pod)"
  [[ -n "${primary}" ]] || fatal "CNPG primary pod not found for ${CNPG_CLUSTER}"
  exists="$(kubectl -n "${POSTGRES_NS}" exec "${primary}" -- sh -lc "psql -U postgres -tAc \"SELECT 1 FROM pg_database WHERE datname='${db_name}';\"" 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "${exists}" != "1" ]]; then
    log "creating database ${db_name}"
    kubectl -n "${POSTGRES_NS}" exec "${primary}" -- sh -lc "psql -U postgres -v ON_ERROR_STOP=1 -c \"CREATE DATABASE ${db_name} OWNER \\\"${APP_DB_USER}\\\";\"" >/dev/null
  else
    log "database ${db_name} already exists"
  fi
  kubectl -n "${POSTGRES_NS}" exec "${primary}" -- sh -lc "psql -U postgres -v ON_ERROR_STOP=1 -c \"ALTER DATABASE ${db_name} OWNER TO \\\"${APP_DB_USER}\\\";\"" >/dev/null 2>&1 || true
  kubectl -n "${POSTGRES_NS}" exec "${primary}" -- sh -lc "psql -U postgres -d \"${db_name}\" -v ON_ERROR_STOP=1 -c \"ALTER SCHEMA public OWNER TO \\\"${APP_DB_USER}\\\";\"" >/dev/null 2>&1 || true
}

ensure_secret_literal() {
  local namespace="$1"
  local name="$2"
  local key="$3"
  local value="$4"
  [[ -n "${value}" ]] || fatal "empty value for secret ${namespace}/${name}:${key}"
  kubectl -n "${namespace}" create secret generic "${name}" --from-literal="${key}=${value}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

render_sa_block() {
  local key="$1"
  local value="$2"
  if [[ -n "${key}" && -n "${value}" ]]; then
    cat <<EOF
  serviceAccount:
    create: true
    annotations:
      ${key}: "${value}"
EOF
  else
    cat <<EOF
  serviceAccount:
    create: true
    annotations: {}
EOF
  fi
}

render_storage_block() {
  local provider="$1"
  case "${provider}" in
    aws)
      if [[ "${USE_IAM}" == "true" ]]; then
        AWS_AUTH_MODE="iam"
      else
        AWS_AUTH_MODE="accesskey"
      fi
      cat <<EOF
storage:
  secretName: ""
  type: s3
  bucketName: "${S3_BUCKET}"
  s3:
    endpoint: "${S3_ENDPOINT}"
    region: "${AWS_REGION}"
    authType: "${AWS_AUTH_MODE}"
    accessKey: "${AWS_ACCESS_KEY_ID}"
    secretKey: "${AWS_SECRET_ACCESS_KEY}"
  gcs:
    projectId: ""
  custom: {}
  enableMultiContainer: false
  limits:
    maxDownloadMBs: 10
  cache:
    maxSizeMBs: 0
    targetGCPercent: 70
EOF
      ;;
    gcs)
      cat <<EOF
storage:
  secretName: ""
  type: gcs
  bucketName: "${GCP_BUCKET}"
  s3:
    endpoint: ""
    region: "${AWS_REGION}"
    authType: iam
    accessKey: ""
    secretKey: ""
  gcs:
    projectId: "${GCP_PROJECT}"
  custom: {}
  enableMultiContainer: false
  limits:
    maxDownloadMBs: 10
  cache:
    maxSizeMBs: 0
    targetGCPercent: 70
EOF
      ;;
    azure)
      [[ -n "${FLYTE_STORAGE_CUSTOM_YAML}" ]] || fatal "FLYTE_STORAGE_CUSTOM_YAML is required for azure"
      cat <<EOF
storage:
  secretName: ""
  type: custom
  bucketName: "${AZURE_CONTAINER}"
  s3:
    endpoint: ""
    region: "${AWS_REGION}"
    authType: iam
    accessKey: ""
    secretKey: ""
  gcs:
    projectId: ""
  custom:
$(printf '%s\n' "${FLYTE_STORAGE_CUSTOM_YAML}" | sed 's/^/    /')
  enableMultiContainer: false
  limits:
    maxDownloadMBs: 10
  cache:
    maxSizeMBs: 0
    targetGCPercent: 70
EOF
      ;;
    *)
      fatal "unsupported storage provider ${provider}"
      ;;
  esac
}

render_k8s_block() {
  local provider="$1"
  if [[ "${provider}" == "aws" && "${USE_IAM}" == "false" ]]; then
    cat <<EOF
k8s:
  plugins:
    k8s:
      default-cpus: "100m"
      default-memory: "200Mi"
      default-env-from-configmaps: []
      default-env-from-secrets:
        - ${TASK_AWS_SECRET_NAME}
      default-env-vars: []
EOF
  else
    cat <<EOF
k8s:
  plugins:
    k8s:
      default-cpus: "100m"
      default-memory: "200Mi"
      default-env-from-configmaps: []
      default-env-from-secrets: []
      default-env-vars: []
EOF
  fi
}

render_sparkoperator_block() {
  local provider="$1"
  local enabled
  enabled="$(yaml_bool "${FLYTE_SPARK_OPERATOR_ENABLED:-false}")"

  cat <<EOF
sparkoperator:
  enabled: ${enabled}
EOF
}

render_spark_runtime_block() {
  local provider="$1"

  case "${provider}" in
    aws)
      cat <<EOF
plugins:
  spark:
    spark-config-default:
      - spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      - spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version: "2"
      - spark.kubernetes.allocation.batch.size: "50"
      - spark.hadoop.fs.s3a.acl.default: "BucketOwnerFullControl"
      - spark.hadoop.fs.s3n.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
      - spark.hadoop.fs.AbstractFileSystem.s3n.impl: "org.apache.hadoop.fs.s3a.S3A"
      - spark.hadoop.fs.s3.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
      - spark.hadoop.fs.AbstractFileSystem.s3.impl: "org.apache.hadoop.fs.s3a.S3A"
      - spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
      - spark.hadoop.fs.AbstractFileSystem.s3a.impl: "org.apache.hadoop.fs.s3a.S3A"
      - spark.hadoop.fs.s3a.multipart.threshold: "536870912"
      - spark.blacklist.enabled: "true"
      - spark.blacklist.timeout: "5m"
      - spark.task.maxfailures: "8"
EOF
      ;;
    gcs)
      cat <<EOF
plugins:
  spark:
    spark-config-default: []
EOF
      ;;
    azure)
      cat <<EOF
plugins:
  spark:
    spark-config-default: []
EOF
      ;;
    *)
      fatal "unsupported storage provider ${provider}"
      ;;
  esac
}
render_values_file() {
  local provider="$1"
  local admin_sa scheduler_sa datacatalog_sa propeller_sa console_sa webhook_sa raw_prefix remote_scheme

  admin_sa="$(render_sa_block "${SERVICE_ANNOTATION_KEY}" "${SERVICE_ANNOTATION_VALUE}")"
  scheduler_sa="$(render_sa_block "${SERVICE_ANNOTATION_KEY}" "${SERVICE_ANNOTATION_VALUE}")"
  datacatalog_sa="$(render_sa_block "${SERVICE_ANNOTATION_KEY}" "${SERVICE_ANNOTATION_VALUE}")"
  propeller_sa="$(render_sa_block "${SERVICE_ANNOTATION_KEY}" "${SERVICE_ANNOTATION_VALUE}")"
  console_sa="$(render_sa_block "" "")"
  webhook_sa="$(render_sa_block "" "")"

  case "${provider}" in
    aws)
      raw_prefix="${FLYTE_RAWOUTPUT_PREFIX:-s3://${S3_BUCKET}/}"
      remote_scheme="s3"
      ;;
    gcs)
      raw_prefix="${FLYTE_RAWOUTPUT_PREFIX:-gs://${GCP_BUCKET}/}"
      remote_scheme="gs"
      ;;
    azure)
      raw_prefix="${FLYTE_RAWOUTPUT_PREFIX:-abfs://${AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/}"
      remote_scheme="abfs"
      ;;
    *)
      fatal "unsupported storage provider ${provider}"
      ;;
  esac

  cat > "${VALUES_FILE}" <<EOF
deployRedoc: false

flyteadmin:
  enabled: true
  replicaCount: 1
${admin_sa}
  service:
    type: ClusterIP

flytescheduler:
  runPrecheck: true
${scheduler_sa}
  service:
    enabled: false

datacatalog:
  enabled: true
  replicaCount: 1
${datacatalog_sa}
  service:
    type: ClusterIP

flyteconnector:
  enabled: $(yaml_bool "${FLYTE_CONNECTOR_ENABLED:-false}")

flytepropeller:
  enabled: true
  manager: false
  createCRDs: true
  replicaCount: 1
${propeller_sa}
  service:
    enabled: false

flyteconsole:
  enabled: true
  replicaCount: 1
${console_sa}
  service:
    type: ClusterIP

webhook:
  enabled: true
${webhook_sa}
  service:
    type: ClusterIP

common:
  databaseSecret:
    name: ${DB_SECRET_NAME}
    secretManifest: {}
  ingress:
    ingressClassName: "${FLYTE_INGRESS_CLASS_NAME}"
    enabled: $(yaml_bool "${FLYTE_INGRESS_ENABLED:-false}")
    webpackHMR: false
    separateGrpcIngress: $(yaml_bool "${FLYTE_SEPARATE_GRPC_INGRESS:-false}")
    separateGrpcIngressAnnotations: {}
    annotations: {}
    albSSLRedirect: false
    tls:
      enabled: false
  flyteNamespaceTemplate:
    enabled: false

$(render_storage_block "${provider}")

db:
  datacatalog:
    database:
      port: ${POOLER_PORT}
      username: ${APP_DB_USER}
      host: ${DB_HOST}
      dbname: "${FLYTE_DATACATALOG_DB}"
      passwordPath: /etc/db/pass.txt
  admin:
    database:
      port: ${POOLER_PORT}
      username: ${APP_DB_USER}
      host: ${DB_HOST}
      dbname: "${FLYTE_ADMIN_DB}"
      passwordPath: /etc/db/pass.txt

secrets:
  adminOauthClientCredentials:
    enabled: true
    clientSecret: "${FLYTE_OAUTH_CLIENT_SECRET}"
    clientId: "${FLYTE_OAUTH_CLIENT_ID}"
    secretName: flyte-secret-auth

configmap:
  core:
    propeller:
      rawoutput-prefix: "${raw_prefix}"
      metadata-prefix: metadata/propeller
      workers: 4
      max-workflow-retries: 30
      workflow-reeval-duration: 30s
      downstream-eval-duration: 30s
      limit-namespace: "all"
      leader-election:
        enabled: true
    remoteData:
      remoteData:
        region: "${AWS_REGION}"
        scheme: "${remote_scheme}"
        signedUrls:
          durationMinutes: 3
    enabled_plugins:
      tasks:
        task-plugins:
          enabled-plugins:
            - container
            - sidecar
            - k8s-array
            - connector-service
            - echo
            - spark
          default-for-task-types:
            container: container
            sidecar: sidecar
            container_array: k8s-array
            spark: spark
    task_logs:
      plugins:
        logs:
          kubernetes-enabled: true
          cloudwatch-enabled: false
$(render_k8s_block "${provider}" | sed 's/^/    /')
$(render_spark_runtime_block "${provider}" | sed 's/^/    /')

workflow_scheduler:
  enabled: $(yaml_bool "${FLYTE_WORKFLOW_SCHEDULER_ENABLED:-false}")

workflow_notifications:
  enabled: $(yaml_bool "${FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED:-false}")

external_events:
  enable: $(yaml_bool "${FLYTE_EXTERNAL_EVENTS_ENABLED:-false}")

cloud_events:
  enable: $(yaml_bool "${FLYTE_CLOUD_EVENTS_ENABLED:-false}")

cluster_resource_manager:
  enabled: $(yaml_bool "${FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED:-true}")

$(render_sparkoperator_block "${provider}")

daskoperator:
  enabled: $(yaml_bool "${FLYTE_DASK_OPERATOR_ENABLED:-false}")

databricks:
  enabled: $(yaml_bool "${FLYTE_DATABRICKS_ENABLED:-false}")
EOF
}

require_prereqs() {
  require_bin kubectl
  require_bin helm
  require_bin sed
  require_bin awk
  require_bin base64
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach cluster"
}

ensure_release_namespace() {
  if ! kubectl get namespace "${TARGET_NS}" >/dev/null 2>&1; then
    kubectl create namespace "${TARGET_NS}" >/dev/null
  fi

  kubectl label namespace "${TARGET_NS}" \
    app.kubernetes.io/part-of=flyte \
    --overwrite >/dev/null 2>&1 || true
}

wait_for_rollouts() {
  local dep
  for dep in $(kubectl -n "${TARGET_NS}" get deploy -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true); do
    log "waiting for deployment ${dep}"
    kubectl -n "${TARGET_NS}" rollout status deployment/"${dep}" --timeout="${ROLLOUT_TIMEOUT}" >/dev/null
  done
}

dump_diagnostics() {
  log "diagnostics namespace=${TARGET_NS}"
  kubectl -n "${TARGET_NS}" get pods -o wide || true
  kubectl -n "${TARGET_NS}" get svc -o wide || true
  kubectl -n "${TARGET_NS}" get events --sort-by=.lastTimestamp || true
  local dep
  for dep in flyteadmin datacatalog flytepropeller flyteconsole flyte-pod-webhook syncresources; do
    if kubectl -n "${TARGET_NS}" get deployment "${dep}" >/dev/null 2>&1; then
      log "logs deployment/${dep}"
      kubectl -n "${TARGET_NS}" logs deployment/"${dep}" --all-containers --tail=200 || true
    fi
  done
}

print_summary() {
  log "helm release status"
  helm status "${RELEASE_NAME}" -n "${TARGET_NS}" || true
  log "workloads"
  kubectl -n "${TARGET_NS}" get deploy -o wide || true
  kubectl -n "${TARGET_NS}" get svc -o wide || true
  printf '\n'
  printf 'Local access:\n'
  printf 'kubectl -n %s port-forward svc/flyteadmin 30080:80\n' "${TARGET_NS}"
  printf '\n'
  printf 'Flyte namespace: %s\n' "${TARGET_NS}"
  printf 'Postgres namespace: %s\n' "${POSTGRES_NS}"
  printf 'Flyte admin DB: %s\n' "${FLYTE_ADMIN_DB}"
  printf 'Datacatalog DB: %s\n' "${FLYTE_DATACATALOG_DB}"
  printf 'DB host: %s\n' "${DB_HOST}"
  printf 'DB access mode: %s\n' "${DB_ACCESS_MODE}"
  printf 'Rendered values file: %s\n' "${VALUES_FILE}"
  printf 'Task AWS secret name: %s\n' "${TASK_AWS_SECRET_NAME}"
  printf 'Task namespaces: %s\n' "${FLYTE_TASK_NAMESPACES}"
}

main() {
  require_prereqs
  ensure_release_namespace

  local app_secret app_user app_password app_port provider
  app_secret="$(find_app_secret_name)"
  [[ -n "${app_secret}" ]] || fatal "CNPG app secret not found for cluster ${CNPG_CLUSTER}"

  app_user="$(secret_value "${app_secret}" username)"
  app_password="$(secret_value "${app_secret}" password)"
  app_port="$(secret_value "${app_secret}" port)"
  [[ -n "${app_user}" ]] || fatal "username missing from CNPG app secret ${app_secret}"
  [[ -n "${app_password}" ]] || fatal "password missing from CNPG app secret ${app_secret}"

  APP_DB_USER="${app_user}"
  APP_DB_PASSWORD="${app_password}"
  POOLER_PORT="${POOLER_PORT:-${app_port:-5432}}"
  resolve_db_host

  provider="$(detect_storage_provider)"

  case "${provider}" in
    aws)
      if [[ "${USE_IAM}" == "true" ]]; then
        [[ -n "${AWS_ROLE_ARN}" ]] || fatal "AWS_ROLE_ARN is required when USE_IAM=true"
        SERVICE_ANNOTATION_KEY="eks.amazonaws.com/role-arn"
        SERVICE_ANNOTATION_VALUE="${AWS_ROLE_ARN}"
      else
        [[ -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]] || fatal "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when USE_IAM=false"
        SERVICE_ANNOTATION_KEY=""
        SERVICE_ANNOTATION_VALUE=""
        for ns in $(split_namespaces); do
          ensure_task_aws_secret "${ns}"
        done
      fi
      ;;
    gcs)
      [[ -n "${GCP_PROJECT}" ]] || fatal "GCP_PROJECT is required for gcs"
      if [[ -n "${GCP_SA_EMAIL}" ]]; then
        SERVICE_ANNOTATION_KEY="iam.gke.io/gcp-service-account"
        SERVICE_ANNOTATION_VALUE="${GCP_SA_EMAIL}"
      else
        SERVICE_ANNOTATION_KEY=""
        SERVICE_ANNOTATION_VALUE=""
      fi
      ;;
    azure)
      [[ -n "${AZURE_STORAGE_ACCOUNT}" ]] || fatal "AZURE_STORAGE_ACCOUNT is required for azure"
      [[ -n "${FLYTE_STORAGE_CUSTOM_YAML}" ]] || fatal "FLYTE_STORAGE_CUSTOM_YAML is required for azure"
      if [[ -n "${AZURE_CLIENT_ID}" ]]; then
        SERVICE_ANNOTATION_KEY="azure.workload.identity/client-id"
        SERVICE_ANNOTATION_VALUE="${AZURE_CLIENT_ID}"
      else
        SERVICE_ANNOTATION_KEY=""
        SERVICE_ANNOTATION_VALUE=""
      fi
      ;;
    *)
      fatal "unsupported storage provider ${provider}"
      ;;
  esac

  ensure_database "${FLYTE_ADMIN_DB}"
  ensure_database "${FLYTE_DATACATALOG_DB}"
  ensure_secret_literal "${TARGET_NS}" "${DB_SECRET_NAME}" "pass.txt" "${APP_DB_PASSWORD}"

  helm repo add "${CHART_REPO_NAME}" "${CHART_REPO_URL}" >/dev/null 2>&1 || true
  helm repo update >/dev/null

  render_values_file "${provider}"

  local -a helm_args
  helm_args=(
    upgrade
    --install
    "${RELEASE_NAME}"
    "${CHART_REPO_NAME}/${CHART_NAME}"
    --version
    "${CHART_VERSION}"
    -n
    "${TARGET_NS}"
    -f
    "${VALUES_FILE}"
    --wait
    --timeout
    "${READY_TIMEOUT}s"
  )
  if [[ "${FLYTE_ATOMIC}" == "true" ]]; then
    helm_args+=(--atomic)
  fi

  helm "${helm_args[@]}"

  wait_for_rollouts
  print_summary
}

delete_all() {
  kubectl -n "${TARGET_NS}" delete deployment "${RELEASE_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${DB_SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${AUTH_SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  for ns in $(split_namespaces); do
    kubectl -n "${ns}" delete secret "${TASK_AWS_SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  done
  helm uninstall "${RELEASE_NAME}" -n "${TARGET_NS}" >/dev/null 2>&1 || true
  log "deleted Flyte release and secrets"
}

trap 'rc=$?; echo; echo "[DIAG] exit_code=$rc"; dump_diagnostics; exit $rc' ERR

case "${1:---rollout}" in
  --rollout)
    main
    ;;
  --delete)
    require_prereqs
    delete_all
    ;;
  --help|-h)
    cat <<EOF
Usage: $0 [--rollout|--delete]

Environment variables:
  DB_ACCESS_MODE=rw|pooler
  STORAGE_PROVIDER=auto|aws|gcs|azure
  USE_IAM=true|false
  FLYTE_TASK_NAMESPACES=flytesnacks-development[,other-namespace]
  TASK_AWS_SECRET_NAME=flyte-aws-credentials
  FLYTE_SPARK_OPERATOR_ENABLED=true|false
  FLYTE_ATOMIC=true|false
EOF
    ;;
  *)
    fatal "unknown option: ${1:-}"
    ;;
esac