#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
umask 077

REPO_ROOT="$(pwd)"
VALUES_FILE="${REPO_ROOT}/src/manifests/signoz/values.yaml"
MANIFESTS_DIR="$(dirname "${VALUES_FILE}")"

PROFILE="${K8S_CLUSTER:-kind}"

SIGNOZ_NAMESPACE="signoz"
SIGNOZ_INFERENCE_NAMESPACE="inference"
SIGNOZ_STORAGE_CLASS="default-storage-class"

HELM_REPO_NAME="signoz"
HELM_REPO_URL="https://charts.signoz.io"
HELM_RELEASE="signoz"
HELM_CHART="signoz/signoz"
HELM_VERSION="0.118.0"
HELM_TIMEOUT="1h"
ROLLOUT_TIMEOUT="30m"

SIGNOZ_CLUSTER_NAME=""
SIGNOZ_CLOUD=""

SIGNOZ_SIGNOZ_REPLICAS="1"
SIGNOZ_CLICKHOUSE_REPLICAS="1"
SIGNOZ_CLICKHOUSE_SHARDS="1"
SIGNOZ_ZOOKEEPER_REPLICAS="1"

SIGNOZ_SIGNOZ_REQUEST_CPU="100m"
SIGNOZ_SIGNOZ_REQUEST_MEMORY="256Mi"
SIGNOZ_SIGNOZ_LIMIT_CPU="500m"
SIGNOZ_SIGNOZ_LIMIT_MEMORY="512Mi"

SIGNOZ_CLICKHOUSE_REQUEST_CPU="200m"
SIGNOZ_CLICKHOUSE_REQUEST_MEMORY="512Mi"
SIGNOZ_CLICKHOUSE_LIMIT_CPU="750m"
SIGNOZ_CLICKHOUSE_LIMIT_MEMORY="1Gi"

SIGNOZ_ZOOKEEPER_REQUEST_CPU="250m"
SIGNOZ_ZOOKEEPER_REQUEST_MEMORY="512Mi"
SIGNOZ_ZOOKEEPER_LIMIT_CPU="1"
SIGNOZ_ZOOKEEPER_LIMIT_MEMORY="1536Mi"

SIGNOZ_SIGNOZ_PERSISTENCE_SIZE="1Gi"
SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE="10Gi"

TEMP_FILES=()

log() { printf '[%s] [%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "${PROFILE}" "$*" >&2; }
fatal() { log "FATAL: $*"; exit 1; }
require_bin() { command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"; }

cleanup() {
  local f
  for f in "${TEMP_FILES[@]:-}"; do
    rm -f -- "$f" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

run_with_timeout() {
  local duration="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout --kill-after=10s "$duration" "$@"
  else
    "$@"
  fi
}

new_temp_file() {
  mkdir -p "${MANIFESTS_DIR}"
  local f
  f="$(mktemp "${MANIFESTS_DIR}/.tmp.XXXXXX")"
  TEMP_FILES+=("$f")
  printf '%s' "$f"
}

ensure_dns_ready() {
  log "waiting for cluster DNS"
  local elapsed=0
  while [[ "$elapsed" -lt 120 ]]; do
    if kubectl -n kube-system get deploy coredns >/dev/null 2>&1 || kubectl -n kube-system get deploy kube-dns >/dev/null 2>&1; then
      if kubectl -n kube-system get pods -l k8s-app=coredns -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -q '^Running$'; then
        return 0
      fi
      if kubectl -n kube-system get pods -l k8s-app=kube-dns -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -q '^Running$'; then
        return 0
      fi
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  fatal "DNS not ready after 120s"
}

ensure_storage_class() {
  if kubectl get storageclass "${SIGNOZ_STORAGE_CLASS}" >/dev/null 2>&1; then
    log "storage class present: ${SIGNOZ_STORAGE_CLASS}"
    return 0
  fi
  fatal "StorageClass '${SIGNOZ_STORAGE_CLASS}' not found; run the storageclass installer first"
}

compute_profile_defaults() {
  case "${PROFILE}" in
    kind)
      SIGNOZ_CLUSTER_NAME="kind-local"
      SIGNOZ_CLOUD="other"
      ;;
    eks)
      SIGNOZ_CLUSTER_NAME="prod-eks"
      SIGNOZ_CLOUD="aws"
      ;;
    *)
      fatal "invalid K8S_CLUSTER '${PROFILE}' (expected kind|eks)"
      ;;
  esac
}

read_password_from_values() {
  [[ -f "${VALUES_FILE}" ]] || return 0
  python3 - "${VALUES_FILE}" <<'PY'
from __future__ import annotations
import sys
from pathlib import Path

try:
    import yaml
except Exception:
    raise SystemExit(0)

path = Path(sys.argv[1])
try:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
except Exception:
    raise SystemExit(0)

clickhouse = data.get("clickhouse") or {}
if isinstance(clickhouse, dict):
    value = clickhouse.get("password")
    if isinstance(value, str) and value.strip():
        print(value.strip())
        raise SystemExit(0)
PY
}

generate_password() {
  python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
}

ensure_password() {
  local existing=""
  existing="$(read_password_from_values || true)"
  if [[ -n "${existing}" ]]; then
    SIGNOZ_CLICKHOUSE_PASSWORD="${existing}"
    log "loaded ClickHouse password from existing values.yaml"
    return 0
  fi

  SIGNOZ_CLICKHOUSE_PASSWORD="$(generate_password)"
  log "generated new ClickHouse password"
}

write_values_file() {
  mkdir -p "${MANIFESTS_DIR}"
  local tmp
  tmp="$(new_temp_file)"

  cat > "${tmp}" <<EOF
global:
  storageClass: "${SIGNOZ_STORAGE_CLASS}"
  clusterDomain: "cluster.local"
  clusterName: "${SIGNOZ_CLUSTER_NAME}"
  cloud: "${SIGNOZ_CLOUD}"

clusterName: "${SIGNOZ_CLUSTER_NAME}"

clickhouse:
  enabled: true
  cluster: "cluster"
  database: "signoz_metrics"
  traceDatabase: "signoz_traces"
  logDatabase: "signoz_logs"
  meterDatabase: "signoz_meter"
  user: "admin"
  password: "${SIGNOZ_CLICKHOUSE_PASSWORD}"
  layout:
    shardsCount: ${SIGNOZ_CLICKHOUSE_SHARDS}
    replicasCount: ${SIGNOZ_CLICKHOUSE_REPLICAS}
  zookeeper:
    enabled: true
    replicaCount: ${SIGNOZ_ZOOKEEPER_REPLICAS}
    resources:
      requests:
        cpu: "${SIGNOZ_ZOOKEEPER_REQUEST_CPU}"
        memory: "${SIGNOZ_ZOOKEEPER_REQUEST_MEMORY}"
      limits:
        cpu: "${SIGNOZ_ZOOKEEPER_LIMIT_CPU}"
        memory: "${SIGNOZ_ZOOKEEPER_LIMIT_MEMORY}"
  resources:
    requests:
      cpu: "${SIGNOZ_CLICKHOUSE_REQUEST_CPU}"
      memory: "${SIGNOZ_CLICKHOUSE_REQUEST_MEMORY}"
    limits:
      cpu: "${SIGNOZ_CLICKHOUSE_LIMIT_CPU}"
      memory: "${SIGNOZ_CLICKHOUSE_LIMIT_MEMORY}"
  securityContext:
    enabled: true
    runAsUser: 101
    runAsGroup: 101
    fsGroup: 101
    fsGroupChangePolicy: OnRootMismatch
  persistence:
    enabled: true
    existingClaim: ""
    storageClass: "${SIGNOZ_STORAGE_CLASS}"
    accessModes:
      - ReadWriteOnce
    size: "${SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE}"
  initContainers:
    enabled: true
    udf:
      enabled: true

signoz:
  name: "signoz"
  replicaCount: ${SIGNOZ_SIGNOZ_REPLICAS}
  env:
    signoz_telemetrystore_provider: "clickhouse"
    signoz_include_only_log_namespaces: "${SIGNOZ_INFERENCE_NAMESPACE}"
  podSecurityContext:
    fsGroup: 1000
  securityContext:
    allowPrivilegeEscalation: false
    capabilities:
      drop: ["ALL"]
    readOnlyRootFilesystem: true
    runAsNonRoot: true
    runAsUser: 1000
  resources:
    requests:
      cpu: "${SIGNOZ_SIGNOZ_REQUEST_CPU}"
      memory: "${SIGNOZ_SIGNOZ_REQUEST_MEMORY}"
    limits:
      cpu: "${SIGNOZ_SIGNOZ_LIMIT_CPU}"
      memory: "${SIGNOZ_SIGNOZ_LIMIT_MEMORY}"
  persistence:
    enabled: true
    existingClaim: ""
    storageClass: "${SIGNOZ_STORAGE_CLASS}"
    accessModes:
      - ReadWriteOnce
    size: "${SIGNOZ_SIGNOZ_PERSISTENCE_SIZE}"
EOF

  mv -f "${tmp}" "${VALUES_FILE}"
  chmod 600 "${VALUES_FILE}"
  log "wrote values file: ${VALUES_FILE}"
}

helm_repo_sync() {
  log "syncing Helm repo ${HELM_REPO_NAME}"
  run_with_timeout 5m helm repo add "${HELM_REPO_NAME}" "${HELM_REPO_URL}" --force-update >/dev/null
  run_with_timeout 5m helm repo update >/dev/null
}

validate_rendered_values() {
  log "validating Helm render for ${HELM_CHART} version ${HELM_VERSION}"
  run_with_timeout 10m helm template "${HELM_RELEASE}" "${HELM_CHART}" \
    --version "${HELM_VERSION}" \
    --namespace "${SIGNOZ_NAMESPACE}" \
    --values "${VALUES_FILE}" >/dev/null
}

install_or_upgrade() {
  log "installing/upgrading release ${HELM_RELEASE} into namespace ${SIGNOZ_NAMESPACE}"
  run_with_timeout "${HELM_TIMEOUT}" helm upgrade --install "${HELM_RELEASE}" "${HELM_CHART}" \
    --namespace "${SIGNOZ_NAMESPACE}" \
    --create-namespace \
    --version "${HELM_VERSION}" \
    --values "${VALUES_FILE}" \
    --wait \
    --atomic \
    --timeout "${HELM_TIMEOUT}" >/dev/null
}

wait_for_rollouts() {
  local kind name
  for kind in deployment statefulset; do
    while IFS= read -r name; do
      [[ -n "${name}" ]] || continue
      log "waiting for ${kind}/${name}"
      kubectl -n "${SIGNOZ_NAMESPACE}" rollout status "${kind}/${name}" --timeout="${ROLLOUT_TIMEOUT}" >/dev/null
    done < <(kubectl -n "${SIGNOZ_NAMESPACE}" get "${kind}" -l "app.kubernetes.io/instance=${HELM_RELEASE}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
  done
}

resolve_service_name() {
  local want_port="$1"
  local label_json
  label_json="$(kubectl -n "${SIGNOZ_NAMESPACE}" get svc -l "app.kubernetes.io/instance=${HELM_RELEASE}" -o json 2>/dev/null || true)"
  LABEL_JSON="${label_json}" python3 - "$want_port" <<'PY'
from __future__ import annotations
import json
import os
import sys

want = int(sys.argv[1])
raw = os.environ.get("LABEL_JSON", "").strip()
if not raw:
    raise SystemExit(0)
obj = json.loads(raw)
items = obj.get("items", [])
for item in items:
    for port in item.get("spec", {}).get("ports", []):
        if int(port.get("port", -1)) == want:
            print(item.get("metadata", {}).get("name", ""))
            raise SystemExit(0)
for item in items:
    name = item.get("metadata", {}).get("name", "")
    if name:
        print(name)
        raise SystemExit(0)
PY
}

wait_for_service_endpoints() {
  local svc_name="$1"
  local max_wait="${2:-180}"
  local elapsed=0
  while true; do
    if kubectl -n "${SIGNOZ_NAMESPACE}" get endpoints "${svc_name}" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | grep -q '.'; then
      log "service ${svc_name} has ready endpoints"
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    [[ "${elapsed}" -lt "${max_wait}" ]] || fatal "service '${svc_name}' has no endpoints after ${max_wait}s"
  done
}

find_clickhouse_pod() {
  local name
  name="$(kubectl -n "${SIGNOZ_NAMESPACE}" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -m1 -i 'clickhouse' || true)"
  [[ -n "${name}" ]] || fatal "ClickHouse pod not found"
  printf '%s' "${name}"
}

wait_for_clickhouse() {
  local pod container elapsed=0
  pod="$(find_clickhouse_pod)"
  container="$(kubectl -n "${SIGNOZ_NAMESPACE}" get pod "${pod}" -o jsonpath='{.spec.containers[0].name}')"
  while true; do
    if kubectl -n "${SIGNOZ_NAMESPACE}" exec -c "${container}" "${pod}" -- sh -lc 'clickhouse-client --query="SELECT 1"' >/dev/null 2>&1; then
      log "ClickHouse health check succeeded"
      return 0
    fi
    sleep 10
    elapsed=$((elapsed + 10))
    [[ "${elapsed}" -lt 900 ]] || fatal "ClickHouse did not become ready after 900s"
  done
}

force_delete_namespaced_resources() {
  log "force deleting remaining namespaced resources in ${SIGNOZ_NAMESPACE}"
  while IFS= read -r resource; do
    [[ -n "${resource}" ]] || continue
    while IFS= read -r item; do
      [[ -n "${item}" ]] || continue
      kubectl patch -n "${SIGNOZ_NAMESPACE}" "${item}" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
      kubectl delete -n "${SIGNOZ_NAMESPACE}" "${item}" --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    done < <(kubectl get "${resource}" -n "${SIGNOZ_NAMESPACE}" -o name 2>/dev/null || true)
  done < <(kubectl api-resources --verbs=list --namespaced -o name 2>/dev/null || true)
}

force_finalize_namespace() {
  local ns_file
  ns_file="$(new_temp_file)"

  if ! kubectl get ns "${SIGNOZ_NAMESPACE}" -o json > "${ns_file}" 2>/dev/null; then
    return 0
  fi

  python3 - "${ns_file}" <<'PY'
from __future__ import annotations
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
obj = json.loads(path.read_text(encoding="utf-8"))
obj.setdefault("spec", {})
obj["spec"]["finalizers"] = []
obj.setdefault("metadata", {})
obj["metadata"].pop("finalizers", None)
path.write_text(json.dumps(obj), encoding="utf-8")
PY

  log "forcing namespace finalization for ${SIGNOZ_NAMESPACE}"
  kubectl replace --raw "/api/v1/namespaces/${SIGNOZ_NAMESPACE}/finalize" -f "${ns_file}" >/dev/null 2>&1 || true
}

delete_all() {
  log "deleting SigNoz release ${HELM_RELEASE} from namespace ${SIGNOZ_NAMESPACE}"
  helm uninstall "${HELM_RELEASE}" -n "${SIGNOZ_NAMESPACE}" >/dev/null 2>&1 || true

  force_delete_namespaced_resources

  kubectl delete ns "${SIGNOZ_NAMESPACE}" --wait=false >/dev/null 2>&1 || true
  sleep 5

  if kubectl get ns "${SIGNOZ_NAMESPACE}" >/dev/null 2>&1; then
    log "namespace still present after force-delete pass; finalizing namespace"
    force_finalize_namespace
  fi

  for _ in 1 2 3 4 5; do
    if ! kubectl get ns "${SIGNOZ_NAMESPACE}" >/dev/null 2>&1; then
      rm -f -- "${VALUES_FILE}" >/dev/null 2>&1 || true
      log "deleted release, force-cleaned namespace, and removed generated values file"
      return 0
    fi
    sleep 2
  done

  fatal "namespace '${SIGNOZ_NAMESPACE}' still exists after force deletion"
}

print_connection_info() {
  local signoz_svc collector_svc clickhouse_svc
  signoz_svc="$(resolve_service_name 8080)"
  collector_svc="$(resolve_service_name 4317)"
  clickhouse_svc="$(resolve_service_name 9000)"

  [[ -n "${signoz_svc}" ]] || signoz_svc="${HELM_RELEASE}"
  [[ -n "${collector_svc}" ]] || collector_svc="${HELM_RELEASE}-otel-collector"
  [[ -n "${clickhouse_svc}" ]] || clickhouse_svc="${HELM_RELEASE}-clickhouse"

  log "SigNoz UI port-forward: kubectl -n ${SIGNOZ_NAMESPACE} port-forward svc/${signoz_svc} 3301:8080"
  log "SigNoz UI URL: http://localhost:3301"
  log "SigNoz namespace: ${SIGNOZ_NAMESPACE}"
  log "Inference namespace: ${SIGNOZ_INFERENCE_NAMESPACE}"
  log "App OTLP endpoint: http://${collector_svc}.${SIGNOZ_NAMESPACE}.svc.cluster.local:4317"
  log "ClickHouse service: ${clickhouse_svc}.${SIGNOZ_NAMESPACE}.svc.cluster.local:9000"
}

rollout() {
  log "starting SigNoz rollout"
  log "profile=${PROFILE}"
  log "namespace=${SIGNOZ_NAMESPACE}, inference-namespace=${SIGNOZ_INFERENCE_NAMESPACE}"
  log "chart=${HELM_CHART}, version=${HELM_VERSION}"
  log "mode=internal-clickhouse, tls=disabled, zookeeper=enabled"

  ensure_dns_ready
  ensure_storage_class
  compute_profile_defaults
  ensure_password
  write_values_file
  helm_repo_sync
  validate_rendered_values
  install_or_upgrade
  wait_for_rollouts

  local signoz_svc collector_svc clickhouse_svc
  signoz_svc="$(resolve_service_name 8080)"
  collector_svc="$(resolve_service_name 4317)"
  clickhouse_svc="$(resolve_service_name 9000)"

  [[ -n "${signoz_svc}" ]] || signoz_svc="${HELM_RELEASE}"
  [[ -n "${collector_svc}" ]] || collector_svc="${HELM_RELEASE}-otel-collector"
  [[ -n "${clickhouse_svc}" ]] || clickhouse_svc="${HELM_RELEASE}-clickhouse"

  wait_for_service_endpoints "${signoz_svc}" 180
  wait_for_service_endpoints "${collector_svc}" 180
  wait_for_service_endpoints "${clickhouse_svc}" 300
  wait_for_clickhouse

  print_connection_info
  log "SigNoz rollout completed successfully"
}

require_prereqs() {
  require_bin kubectl
  require_bin helm
  require_bin python3
  run_with_timeout 30s kubectl cluster-info >/dev/null
}

main() {
  require_prereqs
  case "${1:---rollout}" in
    --rollout)
      rollout
      ;;
    --delete)
      delete_all
      ;;
    --help|-h)
      cat <<EOF
Usage: signoz.sh [--rollout|--delete]

Environment:
  K8S_CLUSTER=kind|eks
EOF
      ;;
    *)
      fatal "unknown option: ${1}"
      ;;
  esac
}

main "${1:-}"