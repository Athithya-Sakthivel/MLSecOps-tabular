#!/usr/bin/env bash
# Unified CLI for the ELT stack:
#   - submit: lint, port-forward Flyte Admin, and launch the ELT workflow remotely
#   - diagnose: inspect a prior Flyte execution and the related Kubernetes pods/events
#   - delete: delete a Flyte execution only
#
# This script is intended for local operator use. Ensure git push before every run
# The workflow itself runs remotely in Flyte-managed k8s pods.
#
# Examples:
#   bash src/workflows/ELT/run.sh --submit
#   bash src/workflows/ELT/run.sh --diagnose <execution_id>
#   bash src/workflows/ELT/run.sh --delete <execution_id>

set -Eeuo pipefail

export ELT_TASK_IMAGE="docker pull ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-27-05-36--625c128"

REMOTE_PROJECT="${REMOTE_PROJECT:-flytesnacks}"
REMOTE_DOMAIN="${REMOTE_DOMAIN:-development}"
TASK_NAMESPACE="${TASK_NAMESPACE:-${REMOTE_PROJECT}-${REMOTE_DOMAIN}}"
export PYTHONPATH="/workspace/src/"
PORT_FORWARD_PID_FILE="${PORT_FORWARD_PID_FILE:-/tmp/flyteadmin-portforward.pid}"
PORT_FORWARD_LOG="${PORT_FORWARD_LOG:-/tmp/flyteadmin-portforward.log}"
PORT_FORWARD_HOST="${PORT_FORWARD_HOST:-127.0.0.1}"
PORT_FORWARD_PORT="${PORT_FORWARD_PORT:-30081}"
WORKFLOW_FILE="${WORKFLOW_FILE:-src/workflows/ELT/workflows/elt_workflow.py}"
WORKFLOW_NAME="${WORKFLOW_NAME:-elt_workflow}"
WORKFLOW_PACKAGE_DIR="${WORKFLOW_PACKAGE_DIR:-src/workflows/ELT}"

VENV_DIR="${VENV_DIR:-.venv_elt}"
if [[ ! -f "${VENV_DIR}/bin/activate" && -f ".venv/bin/activate" ]]; then
  VENV_DIR=".venv"
fi

log() { printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
fatal() { log "FATAL: $*"; exit 1; }

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"
}

activate_venv_if_present() {
  if [[ -f "${VENV_DIR}/bin/activate" ]]; then
    # shellcheck disable=SC1090
    source "${VENV_DIR}/bin/activate"
  else
    fatal "virtual environment not found: ${VENV_DIR}"
  fi
}

require_common_prereqs() {
  require_bin kubectl
  require_bin flytectl
  require_bin pyflyte
  require_bin git
  require_bin awk
  require_bin grep
  require_bin sed
}

require_submit_prereqs() {
  require_bin ruff
}

cleanup() {
  if [[ -f "${PORT_FORWARD_PID_FILE}" ]]; then
    local old_pid
    old_pid="$(cat "${PORT_FORWARD_PID_FILE}")"
    if kill -0 "${old_pid}" >/dev/null 2>&1; then
      kill "${old_pid}" >/dev/null 2>&1 || true
      wait "${old_pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${PORT_FORWARD_PID_FILE}"
  fi
}
trap cleanup EXIT

start_port_forward() {
  if [[ -f "${PORT_FORWARD_PID_FILE}" ]]; then
    local old_pid
    old_pid="$(cat "${PORT_FORWARD_PID_FILE}")"
    if kill -0 "${old_pid}" >/dev/null 2>&1; then
      kill "${old_pid}" >/dev/null 2>&1 || true
      wait "${old_pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${PORT_FORWARD_PID_FILE}"
  fi

  nohup kubectl -n flyte port-forward svc/flyteadmin "${PORT_FORWARD_PORT}:81" >"${PORT_FORWARD_LOG}" 2>&1 &
  echo $! > "${PORT_FORWARD_PID_FILE}"

  for _ in $(seq 1 60); do
    if (echo >"/dev/tcp/${PORT_FORWARD_HOST}/${PORT_FORWARD_PORT}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  fatal "flyteadmin port-forward did not become ready"
}

init_flytectl() {
  flytectl config init \
    --host="${PORT_FORWARD_HOST}:${PORT_FORWARD_PORT}" \
    --insecure \
    --force >/dev/null
}

derive_execution_name() {
  local short_sha="nogit"
  if [[ -d .git ]]; then
    short_sha="$(git rev-parse --short=7 HEAD)"
  fi
  local ts
  ts="$(TZ=Asia/Kolkata date +%Y-%m-%d-%H%M%S)"
  printf '%s-%s' "${short_sha}" "${ts}" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9-'
}

get_pods_for_execution() {
  local exec_id="$1"
  local pods=""
  pods="$(kubectl get pods -n "${TASK_NAMESPACE}" -l "execution-id=${exec_id}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"

  if [[ -z "${pods}" ]]; then
    pods="$(kubectl get pods -n "${TASK_NAMESPACE}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -F "${exec_id}" || true)"
  fi

  printf '%s' "${pods}"
}

print_pod_signal() {
  local pod="$1"

  echo "=== POD: ${pod} ==="
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o wide || true

  echo "--- SUMMARY ---"
  printf 'SERVICE_ACCOUNT='
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o jsonpath='{.spec.serviceAccountName}{"\n"}' 2>/dev/null || true
  printf 'IMAGE='
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o jsonpath='{.spec.containers[0].image}{"\n"}' 2>/dev/null || true
  printf 'IMAGE_PULL_SECRETS='
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o jsonpath='{.spec.imagePullSecrets[*].name}{"\n"}' 2>/dev/null || true
  printf 'ENV_FROM_SECRETS='
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o jsonpath='{range .spec.containers[0].envFrom[*]}{.secretRef.name}{" "}{end}{"\n"}' 2>/dev/null || true

  echo "--- DESCRIBE ---"
  kubectl describe pod "${pod}" -n "${TASK_NAMESPACE}" || true

  echo "--- LOGS ---"
  kubectl logs "${pod}" -n "${TASK_NAMESPACE}" --all-containers=true --tail=300 || true
}

diagnose_execution() {
  local exec_id="$1"

  start_port_forward
  init_flytectl

  echo "=== EXECUTION TREE ==="
  flytectl get execution "${exec_id}" -p "${REMOTE_PROJECT}" -d "${REMOTE_DOMAIN}" --details || true

  echo "=== EXECUTION YAML ==="
  flytectl get execution "${exec_id}" -p "${REMOTE_PROJECT}" -d "${REMOTE_DOMAIN}" --details -o yaml || true

  echo "=== NAMESPACE PODS ==="
  kubectl get pods -n "${TASK_NAMESPACE}" -o wide || true

  local pods
  pods="$(get_pods_for_execution "${exec_id}")"
  if [[ -n "${pods}" ]]; then
    while IFS= read -r pod; do
      [[ -n "${pod}" ]] || continue
      print_pod_signal "${pod}"
    done <<< "${pods}"
  else
    echo "No live pod found for execution ${exec_id}"
  fi

  echo "=== NAMESPACE EVENTS (focused) ==="
  kubectl get events -n "${TASK_NAMESPACE}" --sort-by=.lastTimestamp \
    | grep -E "${exec_id}|Warning|Failed|BackOff|ErrImagePull|ImagePullBackOff|CrashLoopBackOff|OOMKilled|Unschedulable|CreateContainerConfigError|FailedMount" \
    || true

  echo "=== SPARKAPPLICATIONS ==="
  kubectl get sparkapplications -A -o wide || true

  echo "=== SPARKAPPLICATIONS MATCHING EXECUTION ==="
  kubectl get sparkapplications -A -o name | grep -F "${exec_id}" || true

  echo "=== CLUSTER EVENTS (focused) ==="
  kubectl get events -A --sort-by=.lastTimestamp \
    | grep -E "${exec_id}|Warning|Failed|BackOff|ErrImagePull|ImagePullBackOff|CrashLoopBackOff|OOMKilled|Unschedulable|CreateContainerConfigError|FailedMount" \
    || true
}

submit_execution() {
  start_port_forward
  init_flytectl

  local short_sha git_sha exec_name
  if [[ -d .git ]]; then
    git_sha="$(git rev-parse HEAD)"
    short_sha="$(git rev-parse --short=7 HEAD)"
    log "Submitting workflow from commit ${git_sha}"
  else
    short_sha="nogit"
    log "No .git directory found; using ${short_sha}"
  fi

  exec_name="$(derive_execution_name)"
  log "Execution name: ${exec_name}"

  pyflyte run --remote \
    -p "${REMOTE_PROJECT}" \
    -d "${REMOTE_DOMAIN}" \
    "${WORKFLOW_FILE}" \
    "${WORKFLOW_NAME}" 
}

delete_execution() {
  local exec_id="$1"
  [[ -n "${exec_id}" ]] || fatal "execution id is required for delete"

  start_port_forward
  init_flytectl

  log "Deleting execution ${exec_id}"
  flytectl delete execution "${exec_id}" -p "${REMOTE_PROJECT}" -d "${REMOTE_DOMAIN}"
}

lint_workflow() {
  require_submit_prereqs

  if [[ ! -d "${WORKFLOW_PACKAGE_DIR}" ]]; then
    fatal "workflow package directory not found: ${WORKFLOW_PACKAGE_DIR}"
  fi

  log "Running ruff on ${WORKFLOW_PACKAGE_DIR}"
  ruff check "${WORKFLOW_PACKAGE_DIR}"
}

usage() {
  cat <<EOF
Usage:
  $0 --submit
  $0 --diagnose <exec_id>
  $0 --delete <exec_id>
  $0 --lint

Optional environment variables:
  REMOTE_PROJECT=${REMOTE_PROJECT}
  REMOTE_DOMAIN=${REMOTE_DOMAIN}
  TASK_NAMESPACE=${TASK_NAMESPACE}
  WORKFLOW_FILE=${WORKFLOW_FILE}
  WORKFLOW_NAME=${WORKFLOW_NAME}
  WORKFLOW_PACKAGE_DIR=${WORKFLOW_PACKAGE_DIR}
  GHCR_USER=${GHCR_USER:-}
  IMAGE_TAG=${IMAGE_TAG:-}
  ELT_TASK_IMAGE=${ELT_TASK_IMAGE}
  VENV_DIR=${VENV_DIR}
EOF
}

main() {
  activate_venv_if_present
  require_common_prereqs

  case "${1:-}" in
    --submit)
      require_submit_prereqs
      lint_workflow
      submit_execution
      ;;
    --diagnose)
      [[ $# -ge 2 ]] || fatal "--diagnose requires an execution id"
      diagnose_execution "$2"
      ;;
    --delete)
      [[ $# -ge 2 ]] || fatal "--delete requires an execution id"
      delete_execution "$2"
      ;;
    --lint)
      lint_workflow
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      fatal "unknown command: ${1}"
      ;;
  esac
}

main "$@"