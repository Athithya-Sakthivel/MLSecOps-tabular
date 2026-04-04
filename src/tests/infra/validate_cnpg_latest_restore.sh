#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Validate CNPG latest restore.
#
# Flow:
#   1) Verify the source cluster is ready and archiving WAL
#   2) Create a validation table on the source cluster
#   3) Take a manual backup
#   4) Restore a NEW cluster with --latest
#   5) Verify the table and row exist in the restored cluster
#
# This is a latest-restore smoke test, not PITR.
# The table is a normal table named temporary_table.
# If latest restore works, the table must be present in the restored cluster.

# ---------- Configuration ----------
TARGET_NS="${TARGET_NS:-default}"
SOURCE_CLUSTER_NAME="${SOURCE_CLUSTER_NAME:-postgres-cluster}"
RESTORE_CLUSTER_NAME="${RESTORE_CLUSTER_NAME:-${SOURCE_CLUSTER_NAME}-latest-restore-validate-$$}"
POOLER_NAME="${POOLER_NAME:-postgres-pooler}"
RESTORE_POOLER_NAME="${RESTORE_POOLER_NAME:-${RESTORE_CLUSTER_NAME}-pooler}"

DATABASE_NAME="${DATABASE_NAME:-flyte_admin}"
TABLE_NAME="${TABLE_NAME:-temporary_table}"
MARKER_VALUE="${MARKER_VALUE:-latest-restore-marker}"

MAIN_SCRIPT="${MAIN_SCRIPT:-src/infra/core/postgres_cluster.sh}"

# If your source cluster uses a different backup server name than the cluster name,
# set this explicitly. Otherwise the main script will resolve it from cluster state.
SOURCE_SERVER_NAME="${SOURCE_SERVER_NAME:-}"

# ---------- Logging ----------
log() {
  printf '[%s] [cnpg-latest-restore] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

fatal() {
  printf '[%s] [cnpg-latest-restore][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
  exit 1
}

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"
}

# ---------- Cleanup ----------
cleanup() {
  set +e

  if [[ -n "${RESTORE_CLUSTER_NAME:-}" ]]; then
    log "cleanup: deleting restore cluster ${RESTORE_CLUSTER_NAME}"
    kubectl -n "${TARGET_NS}" delete cluster "${RESTORE_CLUSTER_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl -n "${TARGET_NS}" delete pooler "${RESTORE_POOLER_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi

  if [[ -n "${SOURCE_TABLE_CREATED:-}" && "${SOURCE_TABLE_CREATED}" == "1" ]]; then
    log "cleanup: dropping validation table ${DATABASE_NAME}.${TABLE_NAME} from source cluster"
    drop_source_table || true
  fi
}
trap cleanup EXIT

# ---------- Helpers ----------
ensure_prereqs() {
  require_bin kubectl
  require_bin bash
  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach the cluster"
  [[ -x "${MAIN_SCRIPT}" || -f "${MAIN_SCRIPT}" ]] || fatal "main script not found at ${MAIN_SCRIPT}"
}

cluster_exists() {
  local cluster_name="$1"
  kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" >/dev/null 2>&1
}

wait_for_cluster_ready() {
  local cluster_name="$1"
  local timeout="${2:-900}"
  log "waiting for cluster readiness: ${cluster_name}"

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

wait_for_continuous_archiving() {
  local cluster_name="$1"
  local timeout="${2:-900}"
  log "waiting for ContinuousArchiving=True: ${cluster_name}"

  local start now elapsed cond status reason message
  start=$(date +%s)

  while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    if [[ "${elapsed}" -ge "${timeout}" ]]; then
      cond="$(kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o jsonpath='{range .status.conditions[?(@.type=="ContinuousArchiving")]}{.status}|{.reason}|{.message}{end}' 2>/dev/null || true)"
      fatal "timeout waiting for ContinuousArchiving; current=${cond}"
    fi

    cond="$(kubectl -n "${TARGET_NS}" get cluster "${cluster_name}" -o jsonpath='{range .status.conditions[?(@.type=="ContinuousArchiving")]}{.status}|{.reason}|{.message}{end}' 2>/dev/null || true)"
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

get_primary_pod() {
  local cluster_name="$1"
  kubectl -n "${TARGET_NS}" get pods \
    -l "cnpg.io/cluster=${cluster_name},cnpg.io/instanceRole=primary" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
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
  log "creating validation table ${DATABASE_NAME}.${TABLE_NAME} on source cluster"
  psql_source "DROP TABLE IF EXISTS public.${TABLE_NAME};"
  psql_source "CREATE TABLE public.${TABLE_NAME} (id integer PRIMARY KEY, note text NOT NULL);"
  psql_source "INSERT INTO public.${TABLE_NAME} (id, note) VALUES (1, '${MARKER_VALUE}');"
  SOURCE_TABLE_CREATED=1
  log "validation table created and seeded"
}

drop_source_table() {
  log "dropping validation table ${DATABASE_NAME}.${TABLE_NAME} from source cluster"
  psql_source "DROP TABLE IF EXISTS public.${TABLE_NAME};" >/dev/null 2>&1 || true
}

take_manual_backup() {
  log "taking manual backup of ${SOURCE_CLUSTER_NAME}"
  BACKUP_CLUSTER_NAME="${SOURCE_CLUSTER_NAME}" bash "${MAIN_SCRIPT}" backup
  log "manual backup completed for ${SOURCE_CLUSTER_NAME}"
}

restore_latest() {
  log "starting latest restore into ${RESTORE_CLUSTER_NAME}"
  RESTORE_CLUSTER_NAME="${RESTORE_CLUSTER_NAME}" \
  RESTORE_POOLER_NAME="${RESTORE_POOLER_NAME}" \
  SOURCE_SERVER_NAME="${SOURCE_SERVER_NAME}" \
  bash "${MAIN_SCRIPT}" restore --latest
  log "restore command completed for ${RESTORE_CLUSTER_NAME}"
}

verify_restored_table() {
  log "verifying table exists in restored cluster"
  local exists count note

  exists="$(psql_restore "SELECT to_regclass('public.${TABLE_NAME}') IS NOT NULL;")"
  exists="$(echo "${exists}" | tr -d '[:space:]')"
  [[ "${exists}" == "t" ]] || fatal "restored cluster does not contain table ${TABLE_NAME}"

  count="$(psql_restore "SELECT count(*) FROM public.${TABLE_NAME};")"
  count="$(echo "${count}" | tr -d '[:space:]')"
  [[ "${count}" == "1" ]] || fatal "restored table ${TABLE_NAME} has unexpected row count: ${count}"

  note="$(psql_restore "SELECT note FROM public.${TABLE_NAME} WHERE id = 1;")"
  note="$(echo "${note}" | tr -d '[:space:]')"
  [[ "${note}" == "${MARKER_VALUE}" ]] || fatal "restored row content mismatch: expected=${MARKER_VALUE} got=${note}"

  log "validation passed: table exists and row content matches"
}

main() {
  ensure_prereqs

  log "source cluster: ${SOURCE_CLUSTER_NAME}"
  log "restore cluster: ${RESTORE_CLUSTER_NAME}"
  log "namespace: ${TARGET_NS}"
  log "database: ${DATABASE_NAME}"
  log "table: ${TABLE_NAME}"

  cluster_exists "${SOURCE_CLUSTER_NAME}" || fatal "source cluster not found: ${SOURCE_CLUSTER_NAME}"

  wait_for_cluster_ready "${SOURCE_CLUSTER_NAME}"
  wait_for_continuous_archiving "${SOURCE_CLUSTER_NAME}"

  create_source_table
  take_manual_backup
  restore_latest

  wait_for_cluster_ready "${RESTORE_CLUSTER_NAME}"
  wait_for_continuous_archiving "${RESTORE_CLUSTER_NAME}"
  verify_restored_table

  log "latest-restore validation succeeded"
}

main "$@"