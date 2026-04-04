#!/usr/bin/env bash
# CloudNativePG lifecycle script. Designed for zero-touch, idempotent execution
#
# Manages PostgreSQL cluster lifecycle on Kubernetes using CloudNativePG.
# Covers deployment, backup, restore (latest + PITR), deletion, and status.
# s3://$PG_BACKUPS_S3_BUCKET/postgres_backups/postgres-cluster/base/20260404T071413/ in rfc3339 example:  --target-time "2026-04-04T07:14:13Z"
# Usage:
#   ./postgres_cluster.sh deploy
#   ./postgres_cluster.sh backup
#   ./postgres_cluster.sh restore --latest
#   ./postgres_cluster.sh restore --target-time "<rfc3339>" 
#   ./postgres_cluster.sh destroy
#   ./postgres_cluster.sh status
#
# Required:
#   PG_BACKUPS_S3_BUCKET=<bucket-name>
#
# Optional:
#   BACKUP_DESTINATION_PATH=s3://bucket/prefix/   # default prefix: /postgres_backups/
#
# Authentication:
#   EKS (recommended):
#     IRSA_ROLE_ARN=<iam-role>
#
#   Local / kind:
#     AWS_ACCESS_KEY_ID=<key>
#     AWS_SECRET_ACCESS_KEY=<secret>
# 
# Operations:
#   deploy   → install operator, create cluster and pooler
#   backup   → trigger backup to S3
#   restore  → create cluster from backup (latest or point-in-time)
#   destroy  → delete cluster and resources
#   status   → show cluster state
#
# Notes:
#   - Based on CNPG 1.28.x native backup path (deprecated but stable till 1.30.x)
#   - Storage class setup delegated to:
#       src/infra/core/default_storage_class.sh

set -euo pipefail
IFS=$'\n\t'

K8S_CLUSTER="${K8S_CLUSTER:-kind}"
TARGET_NS="${TARGET_NS:-default}"

ARCHIVE_DIR="${ARCHIVE_DIR:-src/scripts/archive}"
MANIFEST_DIR="${MANIFEST_DIR:-src/manifests/postgres}"
CLUSTER_FILE="${CLUSTER_FILE:-${MANIFEST_DIR}/postgres_cluster.yaml}"
POOLER_FILE="${POOLER_FILE:-${MANIFEST_DIR}/postgres_pooler.yaml}"
SCHEDULED_BACKUP_FILE="${SCHEDULED_BACKUP_FILE:-${MANIFEST_DIR}/postgres_scheduled_backup.yaml}"
SERVER_NAME_HISTORY_FILE="${SERVER_NAME_HISTORY_FILE:-${MANIFEST_DIR}/postgres_backup_server_names.txt}"

CNPG_VERSION="${CNPG_VERSION:-1.28.2}"
CNPG_NAMESPACE="${CNPG_NAMESPACE:-cnpg-system}"

POSTGRES_IMAGE="${POSTGRES_IMAGE:-ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie}"

CLUSTER_NAME="${CLUSTER_NAME:-postgres-cluster}"
RESTORE_CLUSTER_NAME="${RESTORE_CLUSTER_NAME:-${CLUSTER_NAME}-restore}"
POOLER_NAME="${POOLER_NAME:-postgres-pooler}"
RESTORE_POOLER_NAME="${RESTORE_POOLER_NAME:-${RESTORE_CLUSTER_NAME}-pooler}"

BACKUP_CLUSTER_NAME="${BACKUP_CLUSTER_NAME:-${CLUSTER_NAME}}"

OPERATOR_TIMEOUT="${OPERATOR_TIMEOUT:-300}"
POD_TIMEOUT="${POD_TIMEOUT:-900}"
SECRET_TIMEOUT="${SECRET_TIMEOUT:-180}"
BACKUP_TIMEOUT="${BACKUP_TIMEOUT:-1800}"

STORAGE_CLASS_NAME="${STORAGE_CLASS_NAME:-default-storage-class}"

INITDB_DB="${INITDB_DB:-flyte_admin}"
ADDITIONAL_DBS_RAW="${ADDITIONAL_DBS_RAW:-datacatalog mlflow iceberg}"

S3_BUCKET="${PG_BACKUPS_S3_BUCKET:-}"
BACKUP_PREFIX="${BACKUP_PREFIX:-postgres_backups/}"
BACKUP_DESTINATION_PATH="${BACKUP_DESTINATION_PATH:-}"
BACKUP_ENDPOINT_URL="${BACKUP_ENDPOINT_URL:-}"
BACKUP_RETENTION_POLICY="${BACKUP_RETENTION_POLICY:-30d}"
BACKUP_SCHEDULE="${BACKUP_SCHEDULE:-0 0 0 * * *}"

IRSA_ROLE_ARN="${IRSA_ROLE_ARN:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
AWS_CREDS_SECRET_NAME="${AWS_CREDS_SECRET_NAME:-aws-creds}"

BACKUP_SERVER_NAME="${CLUSTER_NAME}"
RESTORE_BACKUP_SERVER_NAME="${RESTORE_BACKUP_SERVER_NAME:-}"
SOURCE_SERVER_NAME="${SOURCE_SERVER_NAME:-}"

# Restore mode: none | latest | time
RESTORE_MODE="${RESTORE_MODE:-none}"
RESTORE_TARGET_TIME="${RESTORE_TARGET_TIME:-}"

# ---------- Derived ----------
parse_additional_dbs() {
  local raw="$1"
  local old_ifs="$IFS"
  local -a items=()
  IFS=' ' read -r -a items <<< "${raw}"
  IFS="$old_ifs"

  local item
  for item in "${items[@]}"; do
    [[ -n "${item}" ]] || continue
    printf '%s\n' "${item}"
  done
}

mapfile -t ADDITIONAL_DBS < <(parse_additional_dbs "${ADDITIONAL_DBS_RAW}")
ALL_DBS=("${INITDB_DB}" "${ADDITIONAL_DBS[@]}")

if [[ "${K8S_CLUSTER}" == "kind" ]]; then
  INSTANCES=1
  CPU_REQUEST="250m"; CPU_LIMIT="1000m"
  MEM_REQUEST="512Mi"; MEM_LIMIT="1Gi"
  STORAGE_SIZE="5Gi"; WAL_SIZE="2Gi"
  POOLER_INSTANCES=1
  POOLER_CPU_REQUEST="50m"; POOLER_MEM_REQUEST="64Mi"
  POOLER_CPU_LIMIT="200m"; POOLER_MEM_LIMIT="256Mi"
else
  INSTANCES=3
  CPU_REQUEST="500m"; CPU_LIMIT="2000m"
  MEM_REQUEST="1Gi"; MEM_LIMIT="4Gi"
  STORAGE_SIZE="20Gi"; WAL_SIZE="10Gi"
  POOLER_INSTANCES=2
  POOLER_CPU_REQUEST="100m"; POOLER_MEM_REQUEST="128Mi"
  POOLER_CPU_LIMIT="500m"; POOLER_MEM_LIMIT="512Mi"
fi

ANNOTATION_CLUSTER_KEY="mlsecops.cnpg.cluster-checksum"
ANNOTATION_POOLER_KEY="mlsecops.cnpg.pooler-checksum"
ANNOTATION_BACKUP_KEY="mlsecops.cnpg.backup-checksum"

# ---------- Logging ----------
log(){ printf '[%s] [cnpg] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
fatal(){ printf '[%s] [cnpg][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; exit 1; }
require_bin(){ command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"; }

trap 'rc=$?; echo; echo "[DIAG] exit_code=$rc"; echo "[DIAG] kubectl context: $(kubectl config current-context 2>/dev/null || true)"; echo "[DIAG] pods (ns ${TARGET_NS}):"; kubectl -n "${TARGET_NS}" get pods -o wide || true; echo "[DIAG] pvc (ns ${TARGET_NS}):"; kubectl -n "${TARGET_NS}" get pvc || true; exit $rc' ERR

# ---------- Helpers ----------
manifest_hash(){ sha256sum "$1" | awk '{print $1}'; }

apply_if_changed(){
  local file="$1" kind="$2" name="$3" ann_key="$4"
  local h existing
  h="$(manifest_hash "${file}")"
  existing="$(kubectl -n "${TARGET_NS}" get "${kind}" "${name}" -o "jsonpath={.metadata.annotations['${ann_key}']}" 2>/dev/null || true)"
  if [[ -n "${existing}" && "${existing}" == "${h}" ]]; then
    log "${kind}/${name} unchanged; skipping"
    return 0
  fi
  kubectl apply --server-side --field-manager=mlsecops-cnpg -f "${file}" >/dev/null
  kubectl -n "${TARGET_NS}" patch "${kind}" "${name}" --type=merge -p "{\"metadata\":{\"annotations\":{\"${ann_key}\":\"${h}\"}}}" >/dev/null 2>&1 || true
  log "applied ${kind}/${name}"
}

mask_uri(){
  echo "$1" | sed -E 's#(:)[^:@]+(@)#:\*\*\*\*@#'
}

safe_dns_name(){
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//; s/-+/-/g'
}

cluster_exists(){
  local name="$1"
  kubectl -n "${TARGET_NS}" get cluster "${name}" >/dev/null 2>&1
}

current_cluster_name(){
  case "${RESTORE_MODE}" in
    none) printf '%s' "${CLUSTER_NAME}" ;;
    latest|time) printf '%s' "${RESTORE_CLUSTER_NAME}" ;;
    *) fatal "unknown RESTORE_MODE ${RESTORE_MODE}" ;;
  esac
}

current_pooler_name(){
  case "${RESTORE_MODE}" in
    none) printf '%s' "${POOLER_NAME}" ;;
    latest|time) printf '%s' "${RESTORE_POOLER_NAME}" ;;
    *) fatal "unknown RESTORE_MODE ${RESTORE_MODE}" ;;
  esac
}

compose_backup_destination_path(){
  if [[ -n "${BACKUP_DESTINATION_PATH}" ]]; then
    BACKUP_DESTINATION_PATH="${BACKUP_DESTINATION_PATH%/}/"
    return 0
  fi

  [[ -n "${S3_BUCKET}" ]] || fatal "set S3_BUCKET or BACKUP_DESTINATION_PATH"
  BACKUP_PREFIX="${BACKUP_PREFIX#/}"
  BACKUP_PREFIX="${BACKUP_PREFIX%/}/"
  BACKUP_DESTINATION_PATH="s3://${S3_BUCKET%/}/${BACKUP_PREFIX}"
}

history_record(){
  local cluster_name="$1"
  local server_name="$2"
  mkdir -p "$(dirname "${SERVER_NAME_HISTORY_FILE}")"
  printf '%s|%s|%s\n' "${cluster_name}" "${server_name}" "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" >> "${SERVER_NAME_HISTORY_FILE}"
}

history_lookup_server(){
  local cluster_name="$1"
  if [[ ! -f "${SERVER_NAME_HISTORY_FILE}" ]]; then
    return 0
  fi

  awk -F'|' -v c="${cluster_name}" '$1==c {last=$2} END {print last}' "${SERVER_NAME_HISTORY_FILE}" 2>/dev/null || true
}

resolve_deploy_server_name(){
  local cluster_name="$1"

  if [[ -n "${BACKUP_SERVER_NAME}" ]]; then
    printf '%s' "$(safe_dns_name "${BACKUP_SERVER_NAME}")"
    return 0
  fi

  if cluster_exists "${cluster_name}"; then
    local existing
    existing="$(kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o jsonpath='{.spec.backup.barmanObjectStore.serverName}' 2>/dev/null || true)"
    if [[ -n "${existing}" ]]; then
      printf '%s' "$(safe_dns_name "${existing}")"
      return 0
    fi

    existing="$(history_lookup_server "${cluster_name}")"
    if [[ -n "${existing}" ]]; then
      printf '%s' "$(safe_dns_name "${existing}")"
      return 0
    fi
  fi

  local stamp pid
  stamp="$(date -u +%Y%m%d%H%M%S)"
  pid="$$"
  printf '%s' "$(safe_dns_name "${cluster_name}-${stamp}-${pid}")"
}

resolve_restore_server_name(){
  local cluster_name="$1"

  if [[ -n "${RESTORE_BACKUP_SERVER_NAME}" ]]; then
    printf '%s' "$(safe_dns_name "${RESTORE_BACKUP_SERVER_NAME}")"
    return 0
  fi

  local stamp pid
  stamp="$(date -u +%Y%m%d%H%M%S)"
  pid="$$"
  printf '%s' "$(safe_dns_name "${cluster_name}-${stamp}-${pid}")"
}

resolve_source_server_name(){
  local source_cluster="$1"

  if [[ -n "${SOURCE_SERVER_NAME}" ]]; then
    printf '%s' "$(safe_dns_name "${SOURCE_SERVER_NAME}")"
    return 0
  fi

  if cluster_exists "${source_cluster}"; then
    local existing
    existing="$(kubectl -n "${TARGET_NS}" get cluster "${source_cluster}" -o jsonpath='{.spec.backup.barmanObjectStore.serverName}' 2>/dev/null || true)"
    if [[ -n "${existing}" ]]; then
      printf '%s' "$(safe_dns_name "${existing}")"
      return 0
    fi
  fi

  local hist
  hist="$(history_lookup_server "${source_cluster}")"
  if [[ -n "${hist}" ]]; then
    printf '%s' "$(safe_dns_name "${hist}")"
    return 0
  fi

  fatal "could not resolve source serverName for ${source_cluster}; set SOURCE_SERVER_NAME"
}

emit_credentials_block(){
  local indent="$1"
  if [[ -z "${IRSA_ROLE_ARN}" ]]; then
    printf '%ss3Credentials:\n' "${indent}"
    printf '%s  accessKeyId:\n' "${indent}"
    printf '%s    name: %s\n' "${indent}" "${AWS_CREDS_SECRET_NAME}"
    printf '%s    key: ACCESS_KEY_ID\n' "${indent}"
    printf '%s  secretAccessKey:\n' "${indent}"
    printf '%s    name: %s\n' "${indent}" "${AWS_CREDS_SECRET_NAME}"
    printf '%s    key: ACCESS_SECRET_KEY\n' "${indent}"
    if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
      printf '%s  sessionToken:\n' "${indent}"
      printf '%s    name: %s\n' "${indent}" "${AWS_CREDS_SECRET_NAME}"
      printf '%s    key: ACCESS_SESSION_TOKEN\n' "${indent}"
    fi
  fi
}

emit_backup_store_block(){
  local indent="$1" server_name="$2"
  printf '%sbarmanObjectStore:\n' "${indent}"
  printf '%s  destinationPath: %s\n' "${indent}" "${BACKUP_DESTINATION_PATH}"
  printf '%s  serverName: %s\n' "${indent}" "${server_name}"
  if [[ -n "${BACKUP_ENDPOINT_URL}" ]]; then
    printf '%s  endpointURL: %s\n' "${indent}" "${BACKUP_ENDPOINT_URL}"
  fi
  emit_credentials_block "${indent}  "
  printf '%s  wal:\n' "${indent}"
  printf '%s    compression: gzip\n' "${indent}"
  printf '%s  data:\n' "${indent}"
  printf '%s    compression: gzip\n' "${indent}"
}

# ---------- Validation ----------
require_prereqs(){
  require_bin kubectl
  require_bin curl
  require_bin sha256sum
  require_bin bash
  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach the cluster"
  mkdir -p "${ARCHIVE_DIR}" "${MANIFEST_DIR}"
}

validate_backup_inputs(){
  compose_backup_destination_path

  if [[ -n "${IRSA_ROLE_ARN}" && -n "${AWS_ACCESS_KEY_ID}" ]]; then
    fatal "use either IRSA_ROLE_ARN or AWS access keys, not both"
  fi

  if [[ -z "${IRSA_ROLE_ARN}" ]]; then
    [[ -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]] || fatal "for non-EKS clusters set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
  fi
}

validate_restore_args(){
  case "${RESTORE_MODE}" in
    latest|time) ;;
    *) fatal "restore requires --latest or --target-time <timestamp>" ;;
  esac
  if [[ "${RESTORE_MODE}" == "time" && -z "${RESTORE_TARGET_TIME}" ]]; then
    fatal "restore --target-time requires a timestamp"
  fi
}

ensure_namespace(){
  kubectl get ns "${TARGET_NS}" >/dev/null 2>&1 || kubectl create ns "${TARGET_NS}" >/dev/null
}

# ---------- Operator ----------
install_cnpg_operator(){
  log "installing CloudNativePG operator ${CNPG_VERSION}"
  kubectl get ns "${CNPG_NAMESPACE}" >/dev/null 2>&1 || kubectl create ns "${CNPG_NAMESPACE}" >/dev/null

  local url archive_file
  url="https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.28/releases/cnpg-${CNPG_VERSION}.yaml"
  archive_file="${ARCHIVE_DIR}/cnpg-${CNPG_VERSION}.yaml"

  curl -fsSL -o "${archive_file}" "${url}" || fatal "failed to download operator manifest"
  kubectl apply --server-side -f "${archive_file}" >/dev/null || fatal "failed to apply operator manifest"
  kubectl -n "${CNPG_NAMESPACE}" rollout status deployment/cnpg-controller-manager --timeout="${OPERATOR_TIMEOUT}s" >/dev/null || fatal "operator rollout failed"
  kubectl get crd clusters.postgresql.cnpg.io >/dev/null 2>&1 || fatal "operator CRD not ready"
  log "operator ready"
}

# ---------- Secrets ----------
ensure_aws_secret(){
  if [[ -n "${IRSA_ROLE_ARN}" ]]; then
    return 0
  fi

  log "creating/updating AWS credentials secret ${AWS_CREDS_SECRET_NAME}"
  kubectl -n "${TARGET_NS}" create secret generic "${AWS_CREDS_SECRET_NAME}" \
    --from-literal=ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
    --from-literal=ACCESS_SECRET_KEY="${AWS_SECRET_ACCESS_KEY}" \
    ${AWS_SESSION_TOKEN:+--from-literal=ACCESS_SESSION_TOKEN="${AWS_SESSION_TOKEN}"} \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

# ---------- Renderers ----------
render_cluster_manifest(){
  local file="$1"
  local cluster_name="$2"
  local mode="$3"            # deploy | restore
  local own_server_name="$4"  # current cluster serverName
  local source_server_name="${5:-}"
  local restore_target_time="${6:-}"

  mkdir -p "$(dirname "${file}")"

  {
    cat <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: ${cluster_name}
  namespace: ${TARGET_NS}
spec:
  instances: ${INSTANCES}
  imageName: ${POSTGRES_IMAGE}
EOF

    if [[ -n "${IRSA_ROLE_ARN}" ]]; then
      cat <<EOF
  serviceAccountTemplate:
    metadata:
      annotations:
        eks.amazonaws.com/role-arn: ${IRSA_ROLE_ARN}
EOF
    fi

    cat <<EOF
  backup:
    retentionPolicy: ${BACKUP_RETENTION_POLICY}
EOF
    emit_backup_store_block "    " "${own_server_name}"

    if [[ "${mode}" == "deploy" ]]; then
      cat <<EOF
  bootstrap:
    initdb:
      database: ${INITDB_DB}
      owner: app
      postInitSQL:
EOF
      for db in "${ADDITIONAL_DBS[@]}"; do
        printf '        - CREATE DATABASE %s OWNER app;\n' "${db}"
      done
      cat <<EOF
      postInitApplicationSQL:
        - ALTER SCHEMA public OWNER TO app;
  storage:
    storageClass: ${STORAGE_CLASS_NAME}
    size: ${STORAGE_SIZE}
  walStorage:
    storageClass: ${STORAGE_CLASS_NAME}
    size: ${WAL_SIZE}
  postgresql:
    parameters:
      shared_buffers: "256MB"
      max_connections: "200"
      wal_compression: "on"
      effective_cache_size: "1GB"
  resources:
    requests:
      cpu: ${CPU_REQUEST}
      memory: ${MEM_REQUEST}
    limits:
      cpu: ${CPU_LIMIT}
      memory: ${MEM_LIMIT}
EOF
    else
      cat <<EOF
  bootstrap:
    recovery:
      source: origin
EOF
      if [[ -n "${restore_target_time}" ]]; then
        cat <<EOF
      recoveryTarget:
        targetTime: ${restore_target_time}
EOF
      fi
      cat <<EOF
  externalClusters:
    - name: origin
      barmanObjectStore:
        destinationPath: ${BACKUP_DESTINATION_PATH}
        serverName: ${source_server_name}
EOF
      if [[ -n "${BACKUP_ENDPOINT_URL}" ]]; then
        cat <<EOF
        endpointURL: ${BACKUP_ENDPOINT_URL}
EOF
      fi
      if [[ -z "${IRSA_ROLE_ARN}" ]]; then
        cat <<EOF
        s3Credentials:
          accessKeyId:
            name: ${AWS_CREDS_SECRET_NAME}
            key: ACCESS_KEY_ID
          secretAccessKey:
            name: ${AWS_CREDS_SECRET_NAME}
            key: ACCESS_SECRET_KEY
EOF
        if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
          cat <<EOF
          sessionToken:
            name: ${AWS_CREDS_SECRET_NAME}
            key: ACCESS_SESSION_TOKEN
EOF
        fi
      fi
      cat <<EOF
        wal:
          compression: gzip
        data:
          compression: gzip
  storage:
    storageClass: ${STORAGE_CLASS_NAME}
    size: ${STORAGE_SIZE}
  walStorage:
    storageClass: ${STORAGE_CLASS_NAME}
    size: ${WAL_SIZE}
  postgresql:
    parameters:
      shared_buffers: "256MB"
      max_connections: "200"
      wal_compression: "on"
      effective_cache_size: "1GB"
  resources:
    requests:
      cpu: ${CPU_REQUEST}
      memory: ${MEM_REQUEST}
    limits:
      cpu: ${CPU_LIMIT}
      memory: ${MEM_LIMIT}
EOF
    fi
  } > "${file}"

  log "wrote ${file}"
}

render_pooler_manifest(){
  local file="$1"
  local cluster_name="$2"
  local pooler_name="$3"

  mkdir -p "$(dirname "${file}")"

  cat > "${file}" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Pooler
metadata:
  name: ${pooler_name}
  namespace: ${TARGET_NS}
spec:
  cluster:
    name: ${cluster_name}
  instances: ${POOLER_INSTANCES}
  type: rw
  pgbouncer:
    poolMode: transaction
    parameters:
      max_client_conn: "1000"
      default_pool_size: "25"
      min_pool_size: "5"
      reserve_pool_size: "10"
      server_idle_timeout: "600"
  template:
    spec:
      securityContext:
        runAsNonRoot: true
      containers:
        - name: pgbouncer
          securityContext:
            runAsNonRoot: true
            allowPrivilegeEscalation: false
          resources:
            requests:
              cpu: ${POOLER_CPU_REQUEST}
              memory: ${POOLER_MEM_REQUEST}
            limits:
              cpu: ${POOLER_CPU_LIMIT}
              memory: ${POOLER_MEM_LIMIT}
EOF
  log "wrote ${file}"
}

render_scheduled_backup_manifest(){
  local file="$1"
  local cluster_name="$2"

  mkdir -p "$(dirname "${file}")"

  cat > "${file}" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: ScheduledBackup
metadata:
  name: ${cluster_name}-backup
  namespace: ${TARGET_NS}
spec:
  cluster:
    name: ${cluster_name}
  schedule: "${BACKUP_SCHEDULE}"
  backupOwnerReference: self
  method: barmanObjectStore
EOF
  log "wrote ${file}"
}

render_manual_backup_manifest(){
  local file="$1"
  local cluster_name="$2"
  local backup_name="$3"

  mkdir -p "$(dirname "${file}")"

  cat > "${file}" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: ${backup_name}
  namespace: ${TARGET_NS}
spec:
  method: barmanObjectStore
  cluster:
    name: ${cluster_name}
EOF
  log "wrote ${file}"
}

# ---------- Status helpers ----------
jsonpath_condition(){
  local cluster_name="$1"
  local condition_type="$2"
  kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" \
    -o jsonpath="{range .status.conditions[?(@.type==\"${condition_type}\")]}{.status}|{.reason}|{.message}{end}" 2>/dev/null || true
}

wait_for_cluster_ready(){
  local cluster_name="$1"
  local timeout="${2:-${POD_TIMEOUT}}"
  log "waiting for cluster readiness (${cluster_name})"

  local start now elapsed ready expected
  start=$(date +%s)

  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    if [[ "${elapsed}" -ge "${timeout}" ]]; then
      fatal "timeout waiting for cluster readiness: ${cluster_name}"
    fi

    ready=$(kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o jsonpath='{.status.readyInstances}' 2>/dev/null || echo 0)
    expected=$(kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o jsonpath='{.spec.instances}' 2>/dev/null || echo 1)

    if [[ -n "${ready}" && -n "${expected}" && "${ready}" -ge "${expected}" ]]; then
      log "cluster ready ${ready}/${expected}: ${cluster_name}"
      return 0
    fi

    sleep 3
  done
}

wait_for_continuous_archiving(){
  local cluster_name="$1"
  local timeout="${2:-${POD_TIMEOUT}}"
  log "waiting for ContinuousArchiving=True (${cluster_name})"

  local start now elapsed cond status reason message
  start=$(date +%s)

  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    if [[ "${elapsed}" -ge "${timeout}" ]]; then
      cond="$(jsonpath_condition "${cluster_name}" "ContinuousArchiving")"
      fatal "timeout waiting for ContinuousArchiving; current=${cond}"
    fi

    cond="$(jsonpath_condition "${cluster_name}" "ContinuousArchiving")"
    status="${cond%%|*}"
    reason_message="${cond#*|}"
    reason="${reason_message%%|*}"
    message="${reason_message#*|}"

    if [[ "${status}" == "True" ]]; then
      log "ContinuousArchiving ready: ${cluster_name}"
      return 0
    fi

    if [[ -n "${status}" && "${status}" == "False" ]]; then
      log "ContinuousArchiving still failing: ${reason} - ${message}"
    fi

    sleep 3
  done
}

wait_for_app_secret(){
  local cluster_name="$1"
  log "waiting for app secret (${cluster_name})"

  local start now elapsed secret
  start=$(date +%s)

  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    secret=$(kubectl -n "${TARGET_NS}" get secret -l "cnpg.io/cluster=${cluster_name},cnpg.io/userType=app" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -n "${secret}" ]]; then
      log "found app secret ${secret}"
      printf '%s' "${secret}"
      return 0
    fi
    if [[ "${elapsed}" -ge "${SECRET_TIMEOUT}" ]]; then
      fatal "timeout waiting for app secret: ${cluster_name}"
    fi
    sleep 2
  done
}

wait_for_backup_completed(){
  local backup_name="$1"
  log "waiting for backup ${backup_name} to complete"

  local start now elapsed phase
  start=$(date +%s)

  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    if [[ "${elapsed}" -ge "${BACKUP_TIMEOUT}" ]]; then
      kubectl -n "${TARGET_NS}" describe backup "${backup_name}" || true
      fatal "timeout waiting for backup completion: ${backup_name}"
    fi

    phase=$(kubectl -n "${TARGET_NS}" get backup "${backup_name}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "${phase}" in
      completed)
        log "backup completed: ${backup_name}"
        return 0
        ;;
      failed)
        kubectl -n "${TARGET_NS}" describe backup "${backup_name}" || true
        fatal "backup failed: ${backup_name}"
        ;;
      running|started|"")
        sleep 5
        ;;
      *)
        log "backup phase=${phase} for ${backup_name}"
        sleep 5
        ;;
    esac
  done
}

get_primary_pod(){
  kubectl -n "${TARGET_NS}" get pods -l 'cnpg.io/instanceRole=primary' -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

ensure_database_exists(){
  local primary db exists
  primary="$(get_primary_pod)"
  [[ -n "${primary}" ]] || fatal "primary pod not found"

  for db in "$@"; do
    exists=$(kubectl -n "${TARGET_NS}" exec "${primary}" -- psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${db}';" 2>/dev/null || echo "")
    if [[ "${exists}" =~ 1 ]]; then
      log "database ${db} already exists"
    else
      log "creating database ${db}"
      kubectl -n "${TARGET_NS}" exec "${primary}" -- psql -U postgres -c "CREATE DATABASE ${db} OWNER app;" >/dev/null
      log "created ${db}"
    fi
  done
}

fix_database_schema_ownership(){
  log "ensuring schema ownership for target DBs"
  local primary db
  primary="$(get_primary_pod)"
  [[ -n "${primary}" ]] || fatal "primary pod not found"
  for db in "${ALL_DBS[@]}"; do
    kubectl -n "${TARGET_NS}" exec "${primary}" -- psql -U postgres -d "${db}" -c "ALTER SCHEMA public OWNER TO app;" >/dev/null 2>&1 || true
  done
  log "schema ownership attempts complete"
}

deploy_pooler_and_wait(){
  local cluster_name="$1"
  local pooler_name="$2"

  render_pooler_manifest "${POOLER_FILE}" "${cluster_name}" "${pooler_name}"
  apply_if_changed "${POOLER_FILE}" pooler "${pooler_name}" "${ANNOTATION_POOLER_KEY}"

  log "waiting for pooler pods to be ready (${pooler_name})"
  local start now elapsed
  start=$(date +%s)

  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    local pods ready need svc
    pods=$(kubectl -n "${TARGET_NS}" get pods -l "cnpg.io/poolerName=${pooler_name}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)

    if [[ -n "${pods}" ]]; then
      ready=$(for p in ${pods}; do kubectl -n "${TARGET_NS}" get pod "$p" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo false; done | grep -c true || true)
      need=$(kubectl -n "${TARGET_NS}" get pooler "${pooler_name}" -o jsonpath='{.spec.instances}' 2>/dev/null || echo "${POOLER_INSTANCES}")

      if [[ "${ready}" -ge "${need}" && "${need}" -gt 0 ]]; then
        svc=$(kubectl -n "${TARGET_NS}" get svc "${pooler_name}" -o jsonpath='{.metadata.name}' 2>/dev/null || true)
        if [[ -n "${svc}" ]]; then
          log "pooler ready: ${pooler_name}"
          return 0
        fi
      fi
    fi

    if [[ "${elapsed}" -ge "${OPERATOR_TIMEOUT}" ]]; then
      fatal "timeout waiting for pooler readiness: ${pooler_name}"
    fi
    sleep 3
  done
}

print_connection_uris(){
  local cluster_name="$1"
  local pooler_name="$2"
  log "printing masked pooler URIs (${cluster_name})"

  local secret user pw port host raw masked
  secret=$(kubectl -n "${TARGET_NS}" get secret -l "cnpg.io/cluster=${cluster_name},cnpg.io/userType=app" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [[ -n "${secret}" ]] || fatal "app secret not found for connection URI output: ${cluster_name}"

  user=$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.username}' 2>/dev/null | base64 -d)
  pw=$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d)
  port=$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.port}' 2>/dev/null | base64 -d || echo 5432)
  host="${pooler_name}.${TARGET_NS}"

  raw="postgresql://${user}:${pw}@${host}:${port}"
  masked="$(mask_uri "${raw}")"

  printf '\nConnection URIs (masked):\n\n'
  for db in "${ALL_DBS[@]}"; do
    printf '%s/%s\n' "${masked}" "${db}"
  done
}

status_cluster(){
  local cluster_name="$1"
  local pooler_name="$2"

  echo
  echo "=== ${cluster_name} ==="
  if ! cluster_exists "${cluster_name}"; then
    echo "not found"
    return 0
  fi

  kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o wide || true
  echo
  echo "conditions:"
  kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o jsonpath='{range .status.conditions[*]}{.type}={" "}{.status}{" "}{.reason}{" "}{.message}{"\n"}{end}' 2>/dev/null || true
  echo
  echo "ContinuousArchiving:"
  jsonpath_condition "${cluster_name}" "ContinuousArchiving" || true
  echo
  echo "backups:"
  kubectl -n "${TARGET_NS}" get backup -l "cnpg.io/cluster=${cluster_name}" -o wide 2>/dev/null || true
  echo
  echo "scheduled backups:"
  kubectl -n "${TARGET_NS}" get scheduledbackup -l "cnpg.io/cluster=${cluster_name}" -o wide 2>/dev/null || true
  echo
  echo "pooler:"
  kubectl -n "${TARGET_NS}" get pooler "${pooler_name}" -o wide 2>/dev/null || true
  echo
  echo "pods:"
  kubectl -n "${TARGET_NS}" get pods -l "cnpg.io/cluster=${cluster_name}" -o wide 2>/dev/null || true
  echo
}

persist_artifacts(){
  cp "${CLUSTER_FILE}" "${MANIFEST_DIR}/postgres_cluster.yaml" 2>/dev/null || true
  cp "${POOLER_FILE}" "${MANIFEST_DIR}/postgres_pooler.yaml" 2>/dev/null || true
  cp "${SCHEDULED_BACKUP_FILE}" "${MANIFEST_DIR}/postgres_scheduled_backup.yaml" 2>/dev/null || true
  cp "${SERVER_NAME_HISTORY_FILE}" "${MANIFEST_DIR}/postgres_backup_server_names.txt" 2>/dev/null || true
  log "artifacts persisted to ${MANIFEST_DIR}"
}

# ---------- Commands ----------
cmd_deploy(){
  local cluster_name pooler_name server_name
  cluster_name="${CLUSTER_NAME}"
  pooler_name="${POOLER_NAME}"

  server_name="$(resolve_deploy_server_name "${cluster_name}")"

  install_cnpg_operator
  ensure_aws_secret

  render_cluster_manifest "${CLUSTER_FILE}" "${cluster_name}" "deploy" "${server_name}"
  apply_if_changed "${CLUSTER_FILE}" cluster "${cluster_name}" "${ANNOTATION_CLUSTER_KEY}"

  history_record "${cluster_name}" "${server_name}"

  wait_for_cluster_ready "${cluster_name}"
  wait_for_continuous_archiving "${cluster_name}"

  wait_for_app_secret "${cluster_name}" >/dev/null
  ensure_database_exists "${ALL_DBS[@]}"
  fix_database_schema_ownership

  render_scheduled_backup_manifest "${SCHEDULED_BACKUP_FILE}" "${cluster_name}"
  apply_if_changed "${SCHEDULED_BACKUP_FILE}" scheduledbackup "${cluster_name}-backup" "${ANNOTATION_BACKUP_KEY}"

  deploy_pooler_and_wait "${cluster_name}" "${pooler_name}"

  persist_artifacts
  print_connection_uris "${cluster_name}" "${pooler_name}"

  printf '\n[SUCCESS] deployed CNPG cluster %s\n' "${cluster_name}"
}

cmd_backup(){
  local cluster_name backup_name backup_file
  cluster_name="${BACKUP_CLUSTER_NAME}"
  [[ -n "${cluster_name}" ]] || fatal "BACKUP_CLUSTER_NAME resolved to empty"
  cluster_exists "${cluster_name}" || fatal "cluster not found: ${cluster_name}"

  wait_for_cluster_ready "${cluster_name}"
  wait_for_continuous_archiving "${cluster_name}"

  backup_name="$(safe_dns_name "${cluster_name}-manual-$(date -u +%Y%m%d%H%M%S)-$$")"
  backup_file="${MANIFEST_DIR}/backup-${backup_name}.yaml"

  render_manual_backup_manifest "${backup_file}" "${cluster_name}" "${backup_name}"
  apply_if_changed "${backup_file}" backup "${backup_name}" "mlsecops.cnpg.backup-${backup_name}"
  wait_for_backup_completed "${backup_name}"

  printf '\n[SUCCESS] backup completed for cluster %s: %s\n' "${cluster_name}" "${backup_name}"
}

cmd_restore(){
  local restore_cluster pooler_name source_cluster source_server_name restore_server_name
  restore_cluster="${RESTORE_CLUSTER_NAME}"
  pooler_name="${RESTORE_POOLER_NAME}"
  source_cluster="${CLUSTER_NAME}"

  validate_restore_args

  if cluster_exists "${restore_cluster}"; then
    fatal "restore cluster already exists: ${restore_cluster}. Run destroy first."
  fi

  source_server_name="$(resolve_source_server_name "${source_cluster}")"
  restore_server_name="$(resolve_restore_server_name "${restore_cluster}")"

  install_cnpg_operator
  ensure_aws_secret

  render_cluster_manifest "${CLUSTER_FILE}" "${restore_cluster}" "restore" "${restore_server_name}" "${source_server_name}" "${RESTORE_TARGET_TIME}"
  apply_if_changed "${CLUSTER_FILE}" cluster "${restore_cluster}" "${ANNOTATION_CLUSTER_KEY}"

  history_record "${restore_cluster}" "${restore_server_name}"

  wait_for_cluster_ready "${restore_cluster}"
  wait_for_continuous_archiving "${restore_cluster}"

  render_scheduled_backup_manifest "${SCHEDULED_BACKUP_FILE}" "${restore_cluster}"
  apply_if_changed "${SCHEDULED_BACKUP_FILE}" scheduledbackup "${restore_cluster}-backup" "${ANNOTATION_BACKUP_KEY}"

  deploy_pooler_and_wait "${restore_cluster}" "${pooler_name}"
  persist_artifacts
  print_connection_uris "${restore_cluster}" "${pooler_name}"

  printf '\n[SUCCESS] restore completed for %s from source %s\n' "${restore_cluster}" "${source_cluster}"
}

cmd_destroy(){
  local cluster pooler

  for cluster in "${CLUSTER_NAME}" "${RESTORE_CLUSTER_NAME}"; do
    pooler="${POOLER_NAME}"
    [[ "${cluster}" == "${RESTORE_CLUSTER_NAME}" ]] && pooler="${RESTORE_POOLER_NAME}"

    kubectl -n "${TARGET_NS}" delete scheduledbackup "${cluster}-backup" --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n "${TARGET_NS}" delete backup -l "cnpg.io/cluster=${cluster}" --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n "${TARGET_NS}" delete pooler "${pooler}" --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n "${TARGET_NS}" delete cluster "${cluster}" --ignore-not-found >/dev/null 2>&1 || true
    log "deleted CNPG resources for ${cluster}"
  done

  log "preserved PV data and S3 backups"
}

cmd_status(){
  status_cluster "${CLUSTER_NAME}" "${POOLER_NAME}"
  if cluster_exists "${RESTORE_CLUSTER_NAME}"; then
    status_cluster "${RESTORE_CLUSTER_NAME}" "${RESTORE_POOLER_NAME}"
  fi
}

show_help(){
  cat <<EOF
Usage:
  $0 deploy
  $0 backup
  $0 restore --latest
  $0 restore --target-time "2026-04-04T03:42:30Z"
  $0 destroy
  $0 status
  $0 help

Required for backup/restore:
  S3_BUCKET=<bucket>
  or BACKUP_DESTINATION_PATH=s3://bucket/postgres_backups/

Auth:
  EKS: set IRSA_ROLE_ARN
  kind / non-EKS: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

Optional:
  BACKUP_PREFIX=postgres_backups/
  BACKUP_ENDPOINT_URL
  BACKUP_RETENTION_POLICY=30d
  BACKUP_SCHEDULE=0 0 0 * * *
  BACKUP_SERVER_NAME
  RESTORE_BACKUP_SERVER_NAME
  SOURCE_SERVER_NAME
  BACKUP_CLUSTER_NAME
  RESTORE_CLUSTER_NAME
  RESTORE_POOLER_NAME
  ADDITIONAL_DBS_RAW="datacatalog mlflow iceberg"
EOF
}

# ---------- Main ----------
main(){
  require_prereqs
  ensure_namespace
  validate_backup_inputs

  local action="${1:-help}"
  shift || true

  case "${action}" in
    deploy)
      [[ $# -eq 0 ]] || fatal "deploy takes no extra args"
      cmd_deploy
      ;;
    backup)
      [[ $# -eq 0 ]] || fatal "backup takes no extra args"
      cmd_backup
      ;;
    restore)
      RESTORE_MODE=""
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --latest)
            [[ -z "${RESTORE_MODE}" ]] || fatal "choose either --latest or --target-time, not both"
            RESTORE_MODE="latest"
            ;;
          --target-time)
            shift
            [[ $# -gt 0 ]] || fatal "--target-time requires a value"
            [[ -z "${RESTORE_MODE}" ]] || fatal "choose either --latest or --target-time, not both"
            RESTORE_MODE="time"
            RESTORE_TARGET_TIME="$1"
            ;;
          --target-time=*)
            [[ -z "${RESTORE_MODE}" ]] || fatal "choose either --latest or --target-time, not both"
            RESTORE_MODE="time"
            RESTORE_TARGET_TIME="${1#*=}"
            ;;
          *)
            fatal "unknown restore argument: $1"
            ;;
        esac
        shift
      done
      cmd_restore
      ;;
    destroy)
      [[ $# -eq 0 ]] || fatal "destroy takes no extra args"
      cmd_destroy
      ;;
    status)
      [[ $# -eq 0 ]] || fatal "status takes no extra args"
      cmd_status
      ;;
    help|-h|--help)
      show_help
      ;;
    *)
      fatal "unknown command: ${action}"
      ;;
  esac
}

main "$@"