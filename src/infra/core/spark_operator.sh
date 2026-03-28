#!/usr/bin/env bash
# Installs and manages the Spark Operator for the ELT stack.
# Goals:
# - idempotent rollout and cleanup
# - no fragile Helm --set encoding for list values
# - explicit webhook
# - namespace bootstrap with quota and RBAC
# - target-namespace-only Spark Operator access
# - verify permissions only after the controller is installed
# - focused diagnostics on failure

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}/../.." rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

K8S_CLUSTER="${K8S_CLUSTER:-kind}" # kind|eks

HELM_RELEASE="${HELM_RELEASE:-spark-operator}"
HELM_REPO="${HELM_REPO:-spark-operator}"
HELM_REPO_URL="${HELM_REPO_URL:-https://kubeflow.github.io/spark-operator}"
HELM_CHART="${HELM_CHART:-spark-operator/spark-operator}"
HELM_VERSION="${HELM_VERSION:-2.5.0}"
NAMESPACE="${NAMESPACE:-spark-operator}"

REMOTE_PROJECT="${REMOTE_PROJECT:-flytesnacks}"
REMOTE_DOMAIN="${REMOTE_DOMAIN:-development}"
TASK_NAMESPACE="${TASK_NAMESPACE:-${REMOTE_PROJECT}-${REMOTE_DOMAIN}}"

# Default to the ELT namespace only.
# If set to wildcard, we still bootstrap the ELT namespace only to avoid accidental
# cluster-wide drift.
SPARK_JOB_NAMESPACES_RAW="${SPARK_JOB_NAMESPACES:-${TASK_NAMESPACE}}"

SPARK_APP_SERVICE_ACCOUNT="${SPARK_APP_SERVICE_ACCOUNT:-spark}"
SPARK_OPERATOR_SERVICE_ACCOUNT="${SPARK_OPERATOR_SERVICE_ACCOUNT:-spark-operator-controller}"

ENABLE_WEBHOOK_KIND="${ENABLE_WEBHOOK_KIND:-true}"
ENABLE_WEBHOOK_EKS="${ENABLE_WEBHOOK_EKS:-true}"

ENABLE_METRICS_KIND="${ENABLE_METRICS_KIND:-false}"
ENABLE_METRICS_EKS="${ENABLE_METRICS_EKS:-true}"

TIMEOUT_KIND="${TIMEOUT_KIND:-600}"
TIMEOUT_EKS="${TIMEOUT_EKS:-900}"

RESOURCE_QUOTA_NAME="${RESOURCE_QUOTA_NAME:-spark-workload-quota}"
RESOURCE_QUOTA_KIND_REQUESTS_CPU="${RESOURCE_QUOTA_KIND_REQUESTS_CPU:-2}"
RESOURCE_QUOTA_KIND_REQUESTS_MEMORY="${RESOURCE_QUOTA_KIND_REQUESTS_MEMORY:-1536Mi}"
RESOURCE_QUOTA_KIND_LIMITS_CPU="${RESOURCE_QUOTA_KIND_LIMITS_CPU:-4}"
RESOURCE_QUOTA_KIND_LIMITS_MEMORY="${RESOURCE_QUOTA_KIND_LIMITS_MEMORY:-3000Mi}"
RESOURCE_QUOTA_KIND_PODS="${RESOURCE_QUOTA_KIND_PODS:-20}"

RESOURCE_QUOTA_EKS_REQUESTS_CPU="${RESOURCE_QUOTA_EKS_REQUESTS_CPU:-8}"
RESOURCE_QUOTA_EKS_REQUESTS_MEMORY="${RESOURCE_QUOTA_EKS_REQUESTS_MEMORY:-16Gi}"
RESOURCE_QUOTA_EKS_LIMITS_CPU="${RESOURCE_QUOTA_EKS_LIMITS_CPU:-16}"
RESOURCE_QUOTA_EKS_LIMITS_MEMORY="${RESOURCE_QUOTA_EKS_LIMITS_MEMORY:-32Gi}"
RESOURCE_QUOTA_EKS_PODS="${RESOURCE_QUOTA_EKS_PODS:-100}"

CRD_SPARK_APPLICATIONS="${CRD_SPARK_APPLICATIONS:-sparkapplications.sparkoperator.k8s.io}"
CRD_SCHEDULED_SPARK_APPLICATIONS="${CRD_SCHEDULED_SPARK_APPLICATIONS:-scheduledsparkapplications.sparkoperator.k8s.io}"
CRD_SPARK_CONNECTS="${CRD_SPARK_CONNECTS:-sparkconnects.sparkoperator.k8s.io}"

log() { printf '[%s] [%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "${K8S_CLUSTER}" "$*" >&2; }
fatal() { printf '[%s] [%s] [FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "${K8S_CLUSTER}" "$*" >&2; exit 1; }
require_bin() { command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"; }

validate_cluster() {
  case "${K8S_CLUSTER}" in
    kind|eks) ;;
    *) fatal "K8S_CLUSTER must be kind or eks" ;;
  esac
}

cluster_timeout() {
  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    printf '%s' "${TIMEOUT_KIND}"
  else
    printf '%s' "${TIMEOUT_EKS}"
  fi
}

cluster_enable_webhook() {
  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    printf '%s' "${ENABLE_WEBHOOK_KIND}"
  else
    printf '%s' "${ENABLE_WEBHOOK_EKS}"
  fi
}

cluster_enable_metrics() {
  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    printf '%s' "${ENABLE_METRICS_KIND}"
  else
    printf '%s' "${ENABLE_METRICS_EKS}"
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

ensure_namespace() {
  kubectl get namespace "${TASK_NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${TASK_NAMESPACE}" >/dev/null
}

normalize_job_namespaces() {
  local raw="${SPARK_JOB_NAMESPACES_RAW}"
  if [[ -z "${raw}" || "${raw}" == "*" || "${raw}" == "all" || "${raw}" == "cluster-wide" ]]; then
    printf '%s\n' "${TASK_NAMESPACE}"
    return 0
  fi

  local -a parts
  IFS=',' read -r -a parts <<<"${raw}"
  local seen=""
  local ns
  for ns in "${parts[@]}"; do
    ns="$(printf '%s' "${ns}" | xargs)"
    [[ -n "${ns}" ]] || continue
    case ",${seen}," in
      *,"${ns}",*) ;;
      *) seen="${seen}${seen:+,}${ns}" ;;
    esac
  done

  if [[ -z "${seen}" ]]; then
    printf '%s\n' "${TASK_NAMESPACE}"
  else
    printf '%s\n' "${seen}" | tr ',' '\n'
  fi
}

resource_quota_spec() {
  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    cat <<EOF
apiVersion: v1
kind: ResourceQuota
metadata:
  name: ${RESOURCE_QUOTA_NAME}
  namespace: ${TASK_NAMESPACE}
spec:
  hard:
    requests.cpu: "${RESOURCE_QUOTA_KIND_REQUESTS_CPU}"
    requests.memory: "${RESOURCE_QUOTA_KIND_REQUESTS_MEMORY}"
    limits.cpu: "${RESOURCE_QUOTA_KIND_LIMITS_CPU}"
    limits.memory: "${RESOURCE_QUOTA_KIND_LIMITS_MEMORY}"
    pods: "${RESOURCE_QUOTA_KIND_PODS}"
EOF
  else
    cat <<EOF
apiVersion: v1
kind: ResourceQuota
metadata:
  name: ${RESOURCE_QUOTA_NAME}
  namespace: ${TASK_NAMESPACE}
spec:
  hard:
    requests.cpu: "${RESOURCE_QUOTA_EKS_REQUESTS_CPU}"
    requests.memory: "${RESOURCE_QUOTA_EKS_REQUESTS_MEMORY}"
    limits.cpu: "${RESOURCE_QUOTA_EKS_LIMITS_CPU}"
    limits.memory: "${RESOURCE_QUOTA_EKS_LIMITS_MEMORY}"
    pods: "${RESOURCE_QUOTA_EKS_PODS}"
EOF
  fi
}

bootstrap_target_namespace() {
  log "bootstrapping ELT namespace RBAC and quota for ${TASK_NAMESPACE}"
  ensure_namespace

  kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SPARK_APP_SERVICE_ACCOUNT}
  namespace: ${TASK_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${SPARK_APP_SERVICE_ACCOUNT}
  namespace: ${TASK_NAMESPACE}
rules:
  - apiGroups: [""]
    resources:
      - pods
      - pods/log
      - services
      - configmaps
      - events
      - secrets
      - serviceaccounts
      - persistentvolumeclaims
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${SPARK_APP_SERVICE_ACCOUNT}
  namespace: ${TASK_NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: ${SPARK_APP_SERVICE_ACCOUNT}
    namespace: ${TASK_NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${SPARK_APP_SERVICE_ACCOUNT}
EOF

  kubectl apply -f - <<EOF
$(resource_quota_spec)
EOF

  if ! kubectl auth can-i create pods -n "${TASK_NAMESPACE}" \
      --as="system:serviceaccount:${TASK_NAMESPACE}:${SPARK_APP_SERVICE_ACCOUNT}" >/dev/null 2>&1; then
    fatal "service account ${SPARK_APP_SERVICE_ACCOUNT} cannot create pods in ${TASK_NAMESPACE}"
  fi

  log "ELT namespace RBAC and quota verified for ${TASK_NAMESPACE}"
}

bootstrap_operator_access_rbac() {
  log "bootstrapping Spark Operator controller access for ${TASK_NAMESPACE}"

  kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${HELM_RELEASE}-target-access
  namespace: ${TASK_NAMESPACE}
rules:
  - apiGroups: [""]
    resources:
      - pods
      - pods/log
      - services
      - configmaps
      - events
      - secrets
      - serviceaccounts
      - persistentvolumeclaims
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["sparkoperator.k8s.io"]
    resources:
      - sparkapplications
      - sparkapplications/status
      - sparkapplications/finalizers
      - scheduledsparkapplications
      - scheduledsparkapplications/status
      - scheduledsparkapplications/finalizers
      - sparkconnects
      - sparkconnects/status
      - sparkconnects/finalizers
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${HELM_RELEASE}-target-access
  namespace: ${TASK_NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: ${SPARK_OPERATOR_SERVICE_ACCOUNT}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${HELM_RELEASE}-target-access
EOF

  # Some chart/controller combinations need a narrow cluster-level grant for SparkConnect.
  kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ${HELM_RELEASE}-sparkconnect-access
rules:
  - apiGroups: ["sparkoperator.k8s.io"]
    resources:
      - sparkconnects
      - sparkconnects/status
      - sparkconnects/finalizers
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${HELM_RELEASE}-sparkconnect-access
subjects:
  - kind: ServiceAccount
    name: ${SPARK_OPERATOR_SERVICE_ACCOUNT}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ${HELM_RELEASE}-sparkconnect-access
EOF

  log "Spark Operator controller access manifests applied for ${TASK_NAMESPACE}"
}

write_helm_values_file() {
  local values_file="$1"
  local -a namespaces=()
  local ns

  mapfile -t namespaces < <(normalize_job_namespaces)

  {
    cat <<EOF
hook:
  upgradeCrd: true

webhook:
  enable: $(cluster_enable_webhook)

prometheus:
  metrics:
    enable: $(cluster_enable_metrics)

controller:
  replicas: 1
  leaderElection:
    enable: true
  serviceAccount:
    create: true
    name: ${SPARK_OPERATOR_SERVICE_ACCOUNT}
  rbac:
    create: true

spark:
  serviceAccount:
    create: false
    name: ${SPARK_APP_SERVICE_ACCOUNT}
  rbac:
    create: false
  jobNamespaces:
EOF

    for ns in "${namespaces[@]}"; do
      printf '    - "%s"\n' "${ns}"
    done
  } >"${values_file}"
}

find_deployments() {
  kubectl -n "${NAMESPACE}" get deploy \
    -l "app.kubernetes.io/instance=${HELM_RELEASE}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
}

find_controller_deployment() {
  local deployments deploy
  deployments="$(find_deployments)"

  while IFS= read -r deploy; do
    [[ -n "${deploy}" ]] || continue
    case "${deploy}" in
      *controller*) printf '%s\n' "${deploy}"; return 0 ;;
    esac
  done <<<"${deployments}"

  if [[ -n "${deployments}" ]]; then
    printf '%s\n' "${deployments}" | head -n1
    return 0
  fi

  return 1
}

get_controller_service_account() {
  local deploy_name="$1"
  kubectl -n "${NAMESPACE}" get deploy "${deploy_name}" \
    -o jsonpath='{.spec.template.spec.serviceAccountName}' 2>/dev/null || true
}

can_i() {
  local verb="$1"
  local resource="$2"
  local namespace="$3"
  local subject="$4"

  kubectl auth can-i "${verb}" "${resource}" -n "${namespace}" \
    --as="${subject}" >/dev/null 2>&1
}

verify_operator_access_rbac() {
  local controller_deploy controller_sa subject

  controller_deploy="$(find_controller_deployment)" || fatal "controller deployment not found in ${NAMESPACE}"
  controller_sa="$(get_controller_service_account "${controller_deploy}")"

  if [[ -z "${controller_sa}" ]]; then
    controller_sa="${SPARK_OPERATOR_SERVICE_ACCOUNT}"
  fi

  subject="system:serviceaccount:${NAMESPACE}:${controller_sa}"

  log "verifying controller access with subject ${subject}"

  for resource in pods services configmaps events secrets serviceaccounts persistentvolumeclaims; do
    if ! can_i list "${resource}" "${TASK_NAMESPACE}" "${subject}"; then
      fatal "operator service account ${controller_sa} cannot list ${resource} in ${TASK_NAMESPACE}"
    fi
  done

  if kubectl get crd "${CRD_SPARK_APPLICATIONS}" >/dev/null 2>&1; then
    if ! can_i list "sparkapplications.sparkoperator.k8s.io" "${TASK_NAMESPACE}" "${subject}" && \
       ! can_i list "sparkapplications" "${TASK_NAMESPACE}" "${subject}"; then
      fatal "operator service account ${controller_sa} cannot list sparkapplications in ${TASK_NAMESPACE}"
    fi
  else
    log "skipping sparkapplications RBAC validation because CRD ${CRD_SPARK_APPLICATIONS} is not present yet"
  fi

  if kubectl get crd "${CRD_SCHEDULED_SPARK_APPLICATIONS}" >/dev/null 2>&1; then
    if ! can_i list "scheduledsparkapplications.sparkoperator.k8s.io" "${TASK_NAMESPACE}" "${subject}" && \
       ! can_i list "scheduledsparkapplications" "${TASK_NAMESPACE}" "${subject}"; then
      fatal "operator service account ${controller_sa} cannot list scheduledsparkapplications in ${TASK_NAMESPACE}"
    fi
  else
    log "skipping scheduledsparkapplications RBAC validation because CRD ${CRD_SCHEDULED_SPARK_APPLICATIONS} is not present yet"
  fi

  if kubectl get crd "${CRD_SPARK_CONNECTS}" >/dev/null 2>&1; then
    if ! can_i list "sparkconnects.sparkoperator.k8s.io" "${TASK_NAMESPACE}" "${subject}" && \
       ! can_i list "sparkconnects" "${TASK_NAMESPACE}" "${subject}"; then
      fatal "operator service account ${controller_sa} cannot list sparkconnects in ${TASK_NAMESPACE}"
    fi
  else
    log "skipping sparkconnects RBAC validation because CRD ${CRD_SPARK_CONNECTS} is not present yet"
  fi

  log "controller access verified for ${TASK_NAMESPACE}"
}

dump_diagnostics() {
  require_bin kubectl
  require_bin helm

  log "=== HELM STATUS ==="
  helm status "${HELM_RELEASE}" -n "${NAMESPACE}" || true

  log "=== OPERATOR NAMESPACE RESOURCES ==="
  kubectl -n "${NAMESPACE}" get deploy,pod,svc -o wide || true

  log "=== TARGET NAMESPACE RESOURCES ==="
  kubectl -n "${TASK_NAMESPACE}" get sa,role,rolebinding,clusterrole,clusterrolebinding,resourcequota -o wide || true

  log "=== SPARK CUSTOM RESOURCES ==="
  kubectl api-resources | grep -E 'sparkapplications|scheduledsparkapplications|sparkconnects' || true
  kubectl get sparkapplications -A -o wide || true
  kubectl get scheduledsparkapplications -A -o wide || true
  kubectl get sparkconnects -A -o wide || true

  log "=== CRDs ==="
  kubectl get crd | grep -E 'sparkoperator|sparkapplications|scheduledsparkapplications|sparkconnects' || true

  log "=== FILTERED EVENTS ==="
  kubectl -n "${NAMESPACE}" get events --sort-by=.lastTimestamp \
    | grep -E 'Warning|Failed|BackOff|ErrImagePull|ImagePullBackOff|CrashLoopBackOff|OOMKilled|FailedMount|Unavailable|ProgressDeadlineExceeded' \
    | tail -n 120 || true

  local deploy_name
  while IFS= read -r deploy_name; do
    [[ -n "${deploy_name}" ]] || continue
    log "=== DEPLOYMENT ${deploy_name} ==="
    kubectl -n "${NAMESPACE}" describe deployment "${deploy_name}" || true
    log "=== LOGS ${deploy_name} ==="
    kubectl -n "${NAMESPACE}" logs deployment/"${deploy_name}" --tail=200 || true
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

  bootstrap_target_namespace
  bootstrap_operator_access_rbac

  local values_file
  values_file="$(mktemp "/tmp/${HELM_RELEASE}.values.XXXXXX.yaml")"

  write_helm_values_file "${values_file}"

  log "installing Spark Operator release=${HELM_RELEASE} namespace=${NAMESPACE} chart=${HELM_CHART} version=${HELM_VERSION}"
  helm upgrade --install \
    "${HELM_RELEASE}" \
    "${HELM_CHART}" \
    --namespace "${NAMESPACE}" \
    --create-namespace \
    --version "${HELM_VERSION}" \
    --wait \
    --wait-for-jobs \
    --atomic \
    --timeout "$(cluster_timeout)s" \
    -f "${values_file}"

  rm -f "${values_file}"

  log "verifying CRDs"
  for crd in "${CRD_SPARK_APPLICATIONS}" "${CRD_SCHEDULED_SPARK_APPLICATIONS}" "${CRD_SPARK_CONNECTS}"; do
    if ! kubectl get crd "${crd}" >/dev/null 2>&1; then
      log "CRD ${crd} not present after install; continuing only if the chart does not expose it in this version"
    fi
  done

  wait_for_deployments
  verify_operator_access_rbac
}

rollout() {
  log "rollout start"
  install_operator
  log "rollout success"
  log "cluster=${K8S_CLUSTER} namespace=${NAMESPACE} release=${HELM_RELEASE} chart=${HELM_CHART} version=${HELM_VERSION}"
  log "webhook_enabled=$(cluster_enable_webhook) metrics_enabled=$(cluster_enable_metrics) spark_job_namespaces=${SPARK_JOB_NAMESPACES_RAW}"
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
  K8S_CLUSTER                     kind|eks (default: kind)
  HELM_RELEASE                    Helm release name (default: spark-operator)
  NAMESPACE                       Operator namespace (default: spark-operator)
  HELM_REPO                       Helm repo name (default: spark-operator)
  HELM_REPO_URL                   Helm repo URL (default: https://kubeflow.github.io/spark-operator)
  HELM_CHART                      Helm chart ref (default: spark-operator/spark-operator)
  HELM_VERSION                    Helm chart pin (default: 2.5.0)
  REMOTE_PROJECT                  Used only to derive default TASK_NAMESPACE
  REMOTE_DOMAIN                   Used only to derive default TASK_NAMESPACE
  TASK_NAMESPACE                  Default Spark job namespace (default: ${REMOTE_PROJECT}-${REMOTE_DOMAIN})
  SPARK_JOB_NAMESPACES            Comma-separated namespaces; wildcard defaults to TASK_NAMESPACE
  SPARK_APP_SERVICE_ACCOUNT       Spark app service account name (default: spark)
  SPARK_OPERATOR_SERVICE_ACCOUNT  Spark Operator controller SA (default: spark-operator-controller)
  ENABLE_WEBHOOK_KIND             kind default for webhook.enable (default: true)
  ENABLE_WEBHOOK_EKS              eks default for webhook.enable (default: true)
  ENABLE_METRICS_KIND             kind default for prometheus.metrics.enable (default: false)
  ENABLE_METRICS_EKS              eks default for prometheus.metrics.enable (default: true)
  TIMEOUT_KIND                    kind timeout seconds (default: 600)
  TIMEOUT_EKS                     eks timeout seconds (default: 900)
  RESOURCE_QUOTA_NAME             ResourceQuota name (default: spark-workload-quota)

Options:
  --rollout    Install or upgrade Spark Operator
  --cleanup    Remove release and namespace
  --diagnose   Dump diagnostics
  --help, -h   Show this help
EOF
    ;;
  *) fatal "unknown option: ${1:-}" ;;
esac