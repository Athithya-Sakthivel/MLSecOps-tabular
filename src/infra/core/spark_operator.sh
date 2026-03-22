#!/usr/bin/env bash
# Installs and manages the Spark Operator via Helm with strict version pinning and deterministic rollout behavior.
# Configures webhook, metrics, and timeout settings based on K8S_CLUSTER to ensure consistent behavior across kind and cloud environments.
# Performs CRD validation, deployment readiness checks, and emits full runtime state (pods, events, logs) for observability.
# Flyte already validates and structures Spark jobs upstream, making Kubernetes admission webhook checks redundant in controlled workflows.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_CLUSTER="${K8S_CLUSTER:-kind}" # or cloud
HELM_RELEASE="${HELM_RELEASE:-spark-operator}"
HELM_REPO="${HELM_REPO:-spark-operator}"
HELM_REPO_URL="${HELM_REPO_URL:-https://kubeflow.github.io/spark-operator}"
HELM_CHART="${HELM_CHART:-spark-operator/spark-operator}"
HELM_VERSION="${HELM_VERSION:-1.4.0}"
NAMESPACE="${NAMESPACE:-spark-operator}"
SPARK_JOB_NAMESPACES="${SPARK_JOB_NAMESPACES:-}"
SPARK_JOB_NAMESPACE_SELECTOR="${SPARK_JOB_NAMESPACE_SELECTOR:-}"
ENABLE_METRICS_KIND="${ENABLE_METRICS_KIND:-false}"
ENABLE_METRICS_CLOUD="${ENABLE_METRICS_CLOUD:-true}"

TIMEOUT_KIND="${TIMEOUT_KIND:-600}"
TIMEOUT_CLOUD="${TIMEOUT_CLOUD:-900}"
CRD_SPARK_APPLICATIONS="${CRD_SPARK_APPLICATIONS:-sparkapplications.sparkoperator.k8s.io}"
CRD_SCHEDULED_SPARK_APPLICATIONS="${CRD_SCHEDULED_SPARK_APPLICATIONS:-scheduledsparkapplications.sparkoperator.k8s.io}"

ENABLE_WEBHOOK_KIND="${ENABLE_WEBHOOK_KIND:-false}"
ENABLE_WEBHOOK_CLOUD="${ENABLE_WEBHOOK_CLOUD:-false}"

log() { printf '[%s] [%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "${K8S_CLUSTER}" "$*" >&2; }
fatal() { printf '[%s] [%s] [FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "${K8S_CLUSTER}" "$*" >&2; exit 1; }
require_bin() { command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"; }

validate_cluster() {
  case "${K8S_CLUSTER}" in
    kind|cloud) ;;
    *) fatal "K8S_CLUSTER must be kind or cloud" ;;
  esac
}

cluster_timeout() {
  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    printf '%s' "${TIMEOUT_KIND}"
  else
    printf '%s' "${TIMEOUT_CLOUD}"
  fi
}

cluster_enable_webhook() {
  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    printf '%s' "${ENABLE_WEBHOOK_KIND}"
  else
    printf '%s' "${ENABLE_WEBHOOK_CLOUD}"
  fi
}

cluster_enable_metrics() {
  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    printf '%s' "${ENABLE_METRICS_KIND}"
  else
    printf '%s' "${ENABLE_METRICS_CLOUD}"
  fi
}

require_prereqs() {
  require_bin helm
  require_bin kubectl
  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach cluster"
}

add_repo() {
  log "adding Helm repo ${HELM_REPO} -> ${HELM_REPO_URL}"
  helm repo add "${HELM_REPO}" "${HELM_REPO_URL}" --force-update >/dev/null
  helm repo update >/dev/null
}

job_namespaces_json() {
  local input="${SPARK_JOB_NAMESPACES}"
  if [[ -z "${input}" ]]; then
    return 1
  fi

  if [[ "${input}" == "*" || "${input}" == "all" || "${input}" == "cluster-wide" ]]; then
    printf '[""]'
    return 0
  fi

  local -a parts
  IFS=',' read -r -a parts <<<"${input}"
  local json='['
  local sep=''
  local ns
  for ns in "${parts[@]}"; do
    json+="${sep}\"${ns}\""
    sep=','
  done
  json+=']'
  printf '%s' "${json}"
}

build_helm_args() {
  local -a args
  args=(
    upgrade
    --install
    "${HELM_RELEASE}"
    "${HELM_CHART}"
    --namespace
    "${NAMESPACE}"
    --create-namespace
    --set
    "hook.upgradeCrd=true"
    --set
    "webhook.enable=$(cluster_enable_webhook)"
    --set
    "prometheus.metrics.enable=$(cluster_enable_metrics)"
    --set
    "controller.leaderElection.enable=true"
    --wait
    --wait-for-jobs
    --atomic
    --timeout
    "$(cluster_timeout)s"
  )

  if [[ -n "${SPARK_JOB_NAMESPACE_SELECTOR}" ]]; then
    args+=(--set "spark.jobNamespaceSelector=${SPARK_JOB_NAMESPACE_SELECTOR}")
  fi

  if json_value="$(job_namespaces_json)"; then
    args+=(--set-json "spark.jobNamespaces=${json_value}")
  fi

  if [[ -n "${HELM_VERSION}" ]]; then
    args+=(--version "${HELM_VERSION}")
  fi

  printf '%s\n' "${args[@]}"
}

find_deployments() {
  kubectl -n "${NAMESPACE}" get deploy -l "app.kubernetes.io/instance=${HELM_RELEASE}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
}

dump_diagnostics() {
  require_bin kubectl
  require_bin helm

  log "=== NAMESPACE PODS ==="
  kubectl -n "${NAMESPACE}" get pods -o wide || true

  log "=== NAMESPACE SERVICES ==="
  kubectl -n "${NAMESPACE}" get svc -o wide || true

  log "=== NAMESPACE EVENTS ==="
  kubectl -n "${NAMESPACE}" get events --sort-by=.lastTimestamp || true

  log "=== CRDs ==="
  kubectl get crd | grep -E 'sparkoperator|sparkapplications|scheduledsparkapplications' || true

  log "=== HELM STATUS ==="
  helm status "${HELM_RELEASE}" -n "${NAMESPACE}" || true

  local deploy_name
  while IFS= read -r deploy_name; do
    [[ -n "${deploy_name}" ]] || continue
    log "=== DEPLOYMENT ${deploy_name} ==="
    kubectl -n "${NAMESPACE}" describe deployment "${deploy_name}" || true
    log "=== LOGS ${deploy_name} ==="
    kubectl -n "${NAMESPACE}" logs deployment/"${deploy_name}" --tail=300 || true
  done < <(find_deployments)
}

on_error() {
  local exit_code="$1"
  local line_no="$2"
  log "failure at line ${line_no} exit_code=${exit_code}"
  dump_diagnostics || true
  exit "${exit_code}"
}

wait_for_deployments() {
  local deploy_name
  local found=0
  while IFS= read -r deploy_name; do
    [[ -n "${deploy_name}" ]] || continue
    found=1
    log "waiting for deployment ${deploy_name}"
    kubectl -n "${NAMESPACE}" rollout status "deployment/${deploy_name}" --timeout="$(cluster_timeout)s" >/dev/null
  done < <(find_deployments)
  [[ "${found}" -eq 1 ]] || fatal "operator deployment not found in namespace ${NAMESPACE}"
}

install_operator() {
  require_prereqs
  validate_cluster
  add_repo
  trap 'on_error $? $LINENO' ERR

  log "installing Spark Operator release=${HELM_RELEASE} namespace=${NAMESPACE} chart=${HELM_CHART} version=${HELM_VERSION}"
  mapfile -t helm_args < <(build_helm_args)
  helm "${helm_args[@]}"

  log "verifying CRDs"
  for crd in "${CRD_SPARK_APPLICATIONS}" "${CRD_SCHEDULED_SPARK_APPLICATIONS}"; do
    kubectl get crd "${crd}" >/dev/null 2>&1 || fatal "CRD ${crd} not found after install"
  done

  wait_for_deployments
}

rollout() {
  log "rollout start"
  install_operator
  log "rollout success"
  log "cluster=${K8S_CLUSTER} namespace=${NAMESPACE} release=${HELM_RELEASE} chart=${HELM_CHART} version=${HELM_VERSION}"
  log "webhook_enabled=$(cluster_enable_webhook) metrics_enabled=$(cluster_enable_metrics) spark_job_namespaces=${SPARK_JOB_NAMESPACES:-default}"
}

cleanup() {
  require_prereqs
  validate_cluster
  log "uninstalling release ${HELM_RELEASE}"
  helm uninstall "${HELM_RELEASE}" -n "${NAMESPACE}" --timeout "$(cluster_timeout)s" >/dev/null 2>&1 || true
  log "deleting namespace ${NAMESPACE}"
  kubectl delete namespace "${NAMESPACE}" --ignore-not-found --timeout=60s >/dev/null 2>&1 || true
  cat <<EOF
[SUCCESS] Cleanup complete
K8S_CLUSTER=${K8S_CLUSTER}
NAMESPACE=${NAMESPACE}
RELEASE=${HELM_RELEASE}
EOF
}

case "${1:-}" in
  --rollout) rollout ;;
  --cleanup) cleanup ;;
  --diagnose) require_prereqs; validate_cluster; dump_diagnostics ;;
  --help|-h)
    cat <<EOF
Usage: $0 [OPTION]

Environment variables:
  K8S_CLUSTER                 kind|cloud (default: kind)
  HELM_RELEASE                Helm release name (default: spark-operator)
  NAMESPACE                   Operator namespace (default: spark-operator)
  SPARK_JOB_NAMESPACES        Comma-separated namespaces; use all|*|cluster-wide for cluster-wide
  SPARK_JOB_NAMESPACE_SELECTOR  Label selector for watched namespaces
  HELM_VERSION                Helm chart version pin (default: 1.4.0)
  ENABLE_WEBHOOK_KIND         kind default for webhook.enable (default: false)
  ENABLE_WEBHOOK_CLOUD        cloud default for webhook.enable (default: true)
  ENABLE_METRICS_KIND         kind default for prometheus.metrics.enable (default: false)
  ENABLE_METRICS_CLOUD        cloud default for prometheus.metrics.enable (default: true)
  TIMEOUT_KIND                kind timeout seconds (default: 600)
  TIMEOUT_CLOUD               cloud timeout seconds (default: 900)

Options:
  --rollout    Install or upgrade Spark Operator
  --cleanup    Remove release and namespace
  --diagnose   Dump diagnostics
  --help, -h   Show this help
EOF
    ;;
  *) fatal "unknown option: ${1:-}" ;;
esac