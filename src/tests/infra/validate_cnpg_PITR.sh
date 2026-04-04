#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# src/tests/infra/validate_cnpg_PITR.sh
#
# Purpose:
#   1) Create a table named public.temporary_table in the source cluster
#   2) Take a fresh base backup of the source cluster
#   3) Restore a NEW cluster from that latest backup, using a restore cutoff
#      just before the table was created
#   4) Verify temporary_table is absent in the restored cluster
#
# Why PITR is used:
#   A pure latest restore replays WAL and would preserve the table if it was
#   created after the backup. To validate PITR restore deterministically, we restore
#   to a timestamp just before table creation.

# ---------- Config ----------
TARGET_NS="${TARGET_NS:-default}"
SOURCE_CLUSTER_NAME="${SOURCE_CLUSTER_NAME:-postgres-cluster}"
RESTORE_CLUSTER_NAME="${RESTORE_CLUSTER_NAME:-${SOURCE_CLUSTER_NAME}-restore-validate}"
DATABASE_NAME="${DATABASE_NAME:-flyte_admin}"
TABLE_NAME="${TABLE_NAME:-temporary_table}"

POSTGRES_IMAGE="${POSTGRES_IMAGE:-ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie}"
STORAGE_CLASS_NAME="${STORAGE_CLASS_NAME:-default-storage-class}"

# These must match the live source cluster's object store settings.
S3_BUCKET="${S3_BUCKET:-}"
BACKUP_PREFIX="${BACKUP_PREFIX:-postgres_backups/}"
BACKUP_DESTINATION_PATH="${BACKUP_DESTINATION_PATH:-}"
BACKUP_ENDPOINT_URL="${BACKUP_ENDPOINT_URL:-}"

# Auth: same model as the main deploy script
IRSA_ROLE_ARN="${IRSA_ROLE_ARN:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
AWS_CREDS_SECRET_NAME="${AWS_CREDS_SECRET_NAME:-aws-creds}"

# Backup server name of the source cluster. Prefer the live Cluster spec;
# if empty, this can be set explicitly.
SOURCE_SERVER_NAME="${SOURCE_SERVER_NAME:-}"

# Resource defaults for the restore cluster
RESTORE_INSTANCES="${RESTORE_INSTANCES:-1}"
CPU_REQUEST="${CPU_REQUEST:-250m}"
CPU_LIMIT="${CPU_LIMIT:-1000m}"
MEM_REQUEST="${MEM_REQUEST:-512Mi}"
MEM_LIMIT="${MEM_LIMIT:-1Gi}"
WAL_SIZE="${WAL_SIZE:-2Gi}"
STORAGE_SIZE="${STORAGE_SIZE:-5Gi}"

BACKUP_TIMEOUT="${BACKUP_TIMEOUT:-1800}"
CLUSTER_TIMEOUT="${CLUSTER_TIMEOUT:-900}"
DELETE_TIMEOUT="${DELETE_TIMEOUT:-120}"

TMP_DIR="${TMP_DIR:-/tmp/cnpg-restore-validate}"

# ---------- Logging ----------
log() { printf '[%s] [cnpg-restore-test] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
fatal() { printf '[%s] [cnpg-restore-test][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; exit 1; }

require_bin() { command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"; }

# ---------- Cleanup ----------
RESTORE_CREATED=0
SOURCE_TABLE_CREATED=0

cleanup() {
  set +e
  if [[ "${RESTORE_CREATED}" -eq 1 ]]; then
    log "cleanup: deleting restore cluster ${RESTORE_CLUSTER_NAME}"
    kubectl -n "${TARGET_NS}" delete cluster "${RESTORE_CLUSTER_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl -n "${TARGET_NS}" delete pooler "${RESTORE_CLUSTER_NAME}-pooler" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi

  if [[ "${SOURCE_TABLE_CREATED}" -eq 1 ]]; then
    log "cleanup: dropping ${DATABASE_NAME}.${TABLE_NAME} on source cluster"
    drop_source_table || true
  fi
}
trap cleanup EXIT

# ---------- Helpers ----------
ensure_prereqs() {
  require_bin kubectl
  require_bin sha256sum
  require_bin awk
  require_bin sed
  require_bin date

  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach the cluster"
}

validate_env() {
  if [[ -z "${BACKUP_DESTINATION_PATH}" ]]; then
    [[ -n "${S3_BUCKET}" ]] || fatal "set S3_BUCKET or BACKUP_DESTINATION_PATH"
    BACKUP_PREFIX="${BACKUP_PREFIX#/}"
    BACKUP_PREFIX="${BACKUP_PREFIX%/}/"
    BACKUP_DESTINATION_PATH="s3://${S3_BUCKET%/}/${BACKUP_PREFIX}"
  fi

  if [[ -n "${IRSA_ROLE_ARN}" && -n "${AWS_ACCESS_KEY_ID}" ]]; then
    fatal "use either IRSA_ROLE_ARN or AWS access keys, not both"
  fi

  if [[ -z "${IRSA_ROLE_ARN}" ]]; then
    [[ -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]] || fatal "for non-EKS clusters set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
  fi
}

wait_for_cluster_ready() {
  local cluster_name="$1"
  local timeout="${2:-${CLUSTER_TIMEOUT}}"
  log "waiting for cluster readiness: ${cluster_name}"

  local start now elapsed ready expected
  start=$(date +%s)

  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    if [[ "${elapsed}" -ge "${timeout}" ]]; then
      fatal "timeout waiting for cluster ${cluster_name}"
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

wait_for_condition_true() {
  local cluster_name="$1"
  local condition="$2"
  local timeout="${3:-${CLUSTER_TIMEOUT}}"

  log "waiting for condition ${condition}=True on ${cluster_name}"
  local start now elapsed status reason message cond
  start=$(date +%s)

  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    if [[ "${elapsed}" -ge "${timeout}" ]]; then
      cond=$(kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o jsonpath="{range .status.conditions[?(@.type==\"${condition}\")]}{.status}|{.reason}|{.message}{end}" 2>/dev/null || true)
      fatal "timeout waiting for ${condition}; current=${cond}"
    fi

    cond=$(kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o jsonpath="{range .status.conditions[?(@.type==\"${condition}\")]}{.status}|{.reason}|{.message}{end}" 2>/dev/null || true)
    status="${cond%%|*}"
    reason_message="${cond#*|}"
    reason="${reason_message%%|*}"
    message="${reason_message#*|}"

    if [[ "${status}" == "True" ]]; then
      log "condition ready: ${condition}=True"
      return 0
    fi

    if [[ -n "${status}" && "${status}" == "False" ]]; then
      log "condition still failing: ${condition} (${reason}): ${message}"
    fi

    sleep 3
  done
}

get_primary_pod() {
  local cluster_name="$1"
  kubectl -n "${TARGET_NS}" get pods -l "cnpg.io/cluster=${cluster_name},cnpg.io/instanceRole=primary" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

psql_source() {
  local sql="$1"
  local pod
  pod="$(get_primary_pod "${SOURCE_CLUSTER_NAME}")"
  [[ -n "${pod}" ]] || fatal "source primary pod not found"
  kubectl -n "${TARGET_NS}" exec "${pod}" -- psql -U postgres -d "${DATABASE_NAME}" -v ON_ERROR_STOP=1 -tAc "${sql}"
}

psql_restore() {
  local sql="$1"
  local pod
  pod="$(get_primary_pod "${RESTORE_CLUSTER_NAME}")"
  [[ -n "${pod}" ]] || fatal "restore primary pod not found"
  kubectl -n "${TARGET_NS}" exec "${pod}" -- psql -U postgres -d "${DATABASE_NAME}" -v ON_ERROR_STOP=1 -tAc "${sql}"
}

create_source_table() {
  log "creating source table ${DATABASE_NAME}.${TABLE_NAME}"
  psql_source "DROP TABLE IF EXISTS public.${TABLE_NAME};"
  psql_source "CREATE TABLE public.${TABLE_NAME} (id integer PRIMARY KEY, note text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());"
  psql_source "INSERT INTO public.${TABLE_NAME} (id, note) VALUES (1, 'restore-validation-marker');"
  SOURCE_TABLE_CREATED=1
  log "source table created and seeded"
}

drop_source_table() {
  log "dropping source table ${DATABASE_NAME}.${TABLE_NAME}"
  psql_source "DROP TABLE IF EXISTS public.${TABLE_NAME};" >/dev/null 2>&1 || true
}

take_manual_backup() {
  local backup_name
  backup_name="validate-latest-restore-$(date -u +%Y%m%d%H%M%S)"
  log "creating manual backup ${backup_name}"

  kubectl -n "${TARGET_NS}" apply -f - >/dev/null <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: ${backup_name}
spec:
  cluster:
    name: ${SOURCE_CLUSTER_NAME}
  method: barmanObjectStore
EOF

  local start now elapsed phase
  start=$(date +%s)
  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    if [[ "${elapsed}" -ge "${BACKUP_TIMEOUT}" ]]; then
      kubectl -n "${TARGET_NS}" describe backup "${backup_name}" || true
      fatal "timeout waiting for manual backup ${backup_name}"
    fi

    phase=$(kubectl -n "${TARGET_NS}" get backup "${backup_name}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "${phase}" in
      completed)
        log "manual backup completed: ${backup_name}"
        printf '%s' "${backup_name}"
        return 0
        ;;
      failed)
        kubectl -n "${TARGET_NS}" describe backup "${backup_name}" || true
        fatal "manual backup failed: ${backup_name}"
        ;;
      running|started|"")
        sleep 5
        ;;
      *)
        log "manual backup phase=${phase}"
        sleep 5
        ;;
    esac
  done
}

get_source_backup_server_name() {
  if [[ -n "${SOURCE_SERVER_NAME}" ]]; then
    printf '%s' "${SOURCE_SERVER_NAME}"
    return 0
  fi

  local name
  name=$(kubectl -n "${TARGET_NS}" get cluster "${SOURCE_CLUSTER_NAME}" -o jsonpath='{.spec.backup.barmanObjectStore.serverName}' 2>/dev/null || true)
  [[ -n "${name}" ]] || fatal "could not read source serverName from source cluster; set SOURCE_SERVER_NAME"
  printf '%s' "${name}"
}

get_backup_destination_from_source() {
  local path
  path=$(kubectl -n "${TARGET_NS}" get cluster "${SOURCE_CLUSTER_NAME}" -o jsonpath='{.spec.backup.barmanObjectStore.destinationPath}' 2>/dev/null || true)
  [[ -n "${path}" ]] || fatal "could not read destinationPath from source cluster"
  printf '%s' "${path}"
}

get_backup_endpoint_from_source() {
  kubectl -n "${TARGET_NS}" get cluster "${SOURCE_CLUSTER_NAME}" -o jsonpath='{.spec.backup.barmanObjectStore.endpointURL}' 2>/dev/null || true
}

create_restore_manifest() {
  local target_time="$1"
  local source_server_name="$2"
  local destination_path="$3"
  local endpoint_url="$4"
  local manifest="${TMP_DIR}/restore-cluster.yaml"

  mkdir -p "${TMP_DIR}"

  {
    cat <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: ${RESTORE_CLUSTER_NAME}
  namespace: ${TARGET_NS}
spec:
  instances: ${RESTORE_INSTANCES}
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
  bootstrap:
    recovery:
      source: origin
      recoveryTarget:
        targetTime: ${target_time}
  externalClusters:
    - name: origin
      barmanObjectStore:
        destinationPath: ${destination_path}
        serverName: ${source_server_name}
EOF

    if [[ -n "${endpoint_url}" ]]; then
      cat <<EOF
        endpointURL: ${endpoint_url}
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
  } > "${manifest}"

  log "wrote restore manifest ${manifest}"
  kubectl -n "${TARGET_NS}" apply -f "${manifest}" >/dev/null
  RESTORE_CREATED=1
}

verify_table_absent_in_restore() {
  log "verifying ${DATABASE_NAME}.${TABLE_NAME} is absent in restored cluster"
  local present
  present="$(psql_restore "SELECT to_regclass('public.${TABLE_NAME}') IS NOT NULL;")"
  present="$(echo "${present}" | tr -d '[:space:]')"

  if [[ "${present}" == "f" ]]; then
    log "validation passed: ${TABLE_NAME} is absent in restored cluster"
    return 0
  fi

  fatal "validation failed: ${TABLE_NAME} still exists in restored cluster"
}

cleanup_restore_cluster() {
  log "deleting restore cluster ${RESTORE_CLUSTER_NAME}"
  kubectl -n "${TARGET_NS}" delete cluster "${RESTORE_CLUSTER_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

# ---------- Main ----------
main() {
  ensure_prereqs
  validate_env

  log "starting CNPG restore validation"
  log "source cluster: ${SOURCE_CLUSTER_NAME}"
  log "restore cluster: ${RESTORE_CLUSTER_NAME}"
  log "namespace: ${TARGET_NS}"
  log "database: ${DATABASE_NAME}"
  log "table: ${TABLE_NAME}"

  local source_server_name destination_path endpoint_url manual_backup restore_cutoff

  source_server_name="$(get_source_backup_server_name)"
  destination_path="$(get_backup_destination_from_source)"
  endpoint_url="$(get_backup_endpoint_from_source)"

  log "source serverName: ${source_server_name}"
  log "object store destination: ${destination_path}"
  if [[ -n "${endpoint_url}" ]]; then
    log "object store endpointURL: ${endpoint_url}"
  fi

  wait_for_cluster_ready "${SOURCE_CLUSTER_NAME}"
  wait_for_condition_true "${SOURCE_CLUSTER_NAME}" "ContinuousArchiving"

  manual_backup="$(take_manual_backup)"
  log "manual backup object: ${manual_backup}"

  restore_cutoff="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  log "restore cutoff captured before table creation: ${restore_cutoff}"

  sleep 2
  create_source_table

  local source_check
  source_check="$(psql_source "SELECT to_regclass('public.${TABLE_NAME}') IS NOT NULL;")"
  source_check="$(echo "${source_check}" | tr -d '[:space:]')"
  [[ "${source_check}" == "t" ]] || fatal "source table verification failed: ${TABLE_NAME} was not created"

  log "creating restore cluster using latest backup + PITR cutoff"
  create_restore_manifest "${restore_cutoff}" "${source_server_name}" "${destination_path}" "${endpoint_url}"
  wait_for_cluster_ready "${RESTORE_CLUSTER_NAME}"
  wait_for_condition_true "${RESTORE_CLUSTER_NAME}" "ContinuousArchiving"

  verify_table_absent_in_restore

  log "restore validation succeeded"
}

main "$@"