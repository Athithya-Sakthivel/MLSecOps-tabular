#!/usr/bin/env bash
set -Eeuo pipefail

# =============================================================================
# Iceberg REST Catalog Deployment Orchestrator
# Usage: ./iceberg.sh [--rollout [--validate]] | --delete | --help
# =============================================================================

TARGET_NS="${TARGET_NS:-default}"
MANIFEST_DIR="${MANIFEST_DIR:-src/manifests/iceberg}"

DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-iceberg-rest}"
SERVICE_NAME="${SERVICE_NAME:-iceberg-rest}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-iceberg-rest-sa}"
SECRET_NAME="${SECRET_NAME:-iceberg-storage-credentials}"
ANNOTATION_KEY="${ANNOTATION_KEY:-mlsecops.iceberg.checksum}"

IMAGE="${IMAGE:-apache/iceberg-rest-fixture:1.10.1@sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f}"
CONTAINER_PORT="${CONTAINER_PORT:-8181}"
SERVICE_PORT="${SERVICE_PORT:-8181}"

AWS_REGION="${AWS_REGION:-ap-south-1}"
S3_BUCKET="${S3_BUCKET:-e2e-mlops-data-681802563986}"
S3_PREFIX="${S3_PREFIX:-iceberg/warehouse}"
S3_ENDPOINT="${S3_ENDPOINT:-}"
S3_PATH_STYLE_ACCESS="${S3_PATH_STYLE_ACCESS:-false}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"

READY_TIMEOUT="${READY_TIMEOUT:-300}"
VALIDATE_SCRIPT="${VALIDATE_SCRIPT:-src/tests/elt/iceberg_server_validate.sh}"

# --- Logging ------------------------------------------------------------------

log() {
  printf '[%s] [iceberg] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

fatal() {
  printf '[%s] [iceberg][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
  exit 1
}

# --- Prerequisites ------------------------------------------------------------

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"
}

require_prereqs() {
  require_bin kubectl
  require_bin python3
  require_bin sha256sum
  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach cluster"
}

# --- Helpers ------------------------------------------------------------------

ensure_namespace() {
  if kubectl get ns "${TARGET_NS}" >/dev/null 2>&1; then
    return 0
  fi
  log "creating namespace ${TARGET_NS}"
  kubectl create ns "${TARGET_NS}" >/dev/null
}

normalize_prefix() {
  local p="${1#/}"
  p="${p%/}"
  printf '%s' "$p"
}

trim_trailing_slash() {
  local s="$1"
  while [[ "$s" == */ ]]; do
    s="${s%/}"
  done
  printf '%s' "$s"
}

warehouse_uri() {
  local prefix
  prefix="$(normalize_prefix "${S3_PREFIX}")"
  if [[ -n "${prefix}" ]]; then
    printf 's3://%s/%s/' "${S3_BUCKET}" "${prefix}"
  else
    printf 's3://%s/' "${S3_BUCKET}"
  fi
}

rest_base_url() {
  printf 'http://%s.%s.svc.cluster.local:%s' "${SERVICE_NAME}" "${TARGET_NS}" "${SERVICE_PORT}"
}

normalized_s3_endpoint() {
  local endpoint="${S3_ENDPOINT}"
  if [[ -z "${endpoint}" ]]; then
    endpoint="https://s3.${AWS_REGION}.amazonaws.com"
  elif [[ "${endpoint}" != http://* && "${endpoint}" != https://* ]]; then
    endpoint="https://${endpoint}"
  fi
  trim_trailing_slash "${endpoint}"
}

secret_fingerprint() {
  python3 - <<'PY'
import hashlib
import os

parts = [
    os.environ.get("AWS_REGION", ""),
    os.environ.get("S3_BUCKET", ""),
    os.environ.get("S3_PREFIX", ""),
    os.environ.get("S3_ENDPOINT", ""),
    os.environ.get("S3_PATH_STYLE_ACCESS", ""),
    os.environ.get("AWS_ACCESS_KEY_ID", ""),
    os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    os.environ.get("AWS_SESSION_TOKEN", ""),
]
h = hashlib.sha256()
for part in parts:
    h.update(part.encode("utf-8"))
    h.update(b"\0")
print(h.hexdigest())
PY
}

# --- K8s Resource Management --------------------------------------------------

apply_secret() {
  if [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
    fatal "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required"
  fi

  log "creating/updating AWS storage secret ${SECRET_NAME}"

  local args=(
    kubectl -n "${TARGET_NS}" create secret generic "${SECRET_NAME}"
    --dry-run=client
    -o yaml
    --from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
    --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
  )

  if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
    args+=(--from-literal=AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}")
  fi

  "${args[@]}" | kubectl -n "${TARGET_NS}" apply -f - >/dev/null
}

render_serviceaccount() {
  cat > "${MANIFEST_DIR}/serviceaccount.yaml" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
  labels:
    app.kubernetes.io/name: iceberg-rest
automountServiceAccountToken: true
EOF
}

render_deployment() {
  local warehouse s3_endpoint
  warehouse="$(warehouse_uri)"
  s3_endpoint="$(normalized_s3_endpoint)"

  cat > "${MANIFEST_DIR}/deployment.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEPLOYMENT_NAME}
  namespace: ${TARGET_NS}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${DEPLOYMENT_NAME}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  template:
    metadata:
      labels:
        app: ${DEPLOYMENT_NAME}
      annotations:
        ${ANNOTATION_KEY}: "pending"
    spec:
      serviceAccountName: ${SERVICE_ACCOUNT_NAME}
      terminationGracePeriodSeconds: 30
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
        fsGroupChangePolicy: OnRootMismatch
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: rest
        image: ${IMAGE}
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: ${CONTAINER_PORT}
        env:
        - name: AWS_REGION
          value: "${AWS_REGION}"
        - name: AWS_DEFAULT_REGION
          value: "${AWS_REGION}"
        - name: AWS_EC2_METADATA_DISABLED
          value: "true"
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AWS_ACCESS_KEY_ID
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AWS_SECRET_ACCESS_KEY
        - name: AWS_SESSION_TOKEN
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AWS_SESSION_TOKEN
              optional: true
        - name: CATALOG_WAREHOUSE
          value: "${warehouse}"
        - name: CATALOG_IO__IMPL
          value: "org.apache.iceberg.aws.s3.S3FileIO"
        - name: CATALOG_CLIENT_REGION
          value: "${AWS_REGION}"
        - name: CATALOG_S3_ENDPOINT
          value: "${s3_endpoint}"
        - name: CATALOG_S3_PATH_STYLE_ACCESS
          value: "${S3_PATH_STYLE_ACCESS}"
        - name: HOME
          value: "/tmp"
        - name: TMPDIR
          value: "/tmp"
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
        startupProbe:
          httpGet:
            path: /v1/config
            port: ${CONTAINER_PORT}
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 2
          failureThreshold: 60
        readinessProbe:
          httpGet:
            path: /v1/config
            port: ${CONTAINER_PORT}
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 2
          failureThreshold: 24
        livenessProbe:
          httpGet:
            path: /v1/config
            port: ${CONTAINER_PORT}
          initialDelaySeconds: 20
          periodSeconds: 10
          timeoutSeconds: 2
          failureThreshold: 6
        resources:
          requests:
            cpu: "250m"
            memory: "512Mi"
          limits:
            cpu: "1000m"
            memory: "1Gi"
        volumeMounts:
        - name: tmp
          mountPath: /tmp
      volumes:
      - name: tmp
        emptyDir: {}
EOF
}

render_service() {
  cat > "${MANIFEST_DIR}/service.yaml" <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${SERVICE_NAME}
  namespace: ${TARGET_NS}
spec:
  type: ClusterIP
  selector:
    app: ${DEPLOYMENT_NAME}
  ports:
  - name: http
    port: ${SERVICE_PORT}
    targetPort: ${CONTAINER_PORT}
EOF
}

compute_manifests_hash() {
  local tmp
  tmp="$(mktemp)"
  cat \
    "${MANIFEST_DIR}/serviceaccount.yaml" \
    "${MANIFEST_DIR}/deployment.yaml" \
    "${MANIFEST_DIR}/service.yaml" > "${tmp}"
  printf '%s\n' "$(secret_fingerprint)" >> "${tmp}"
  sha256sum "${tmp}" | awk '{print $1}'
  rm -f "${tmp}"
}

apply_manifests() {
  local hash existing
  hash="$(compute_manifests_hash)"

  existing="$(
    kubectl -n "${TARGET_NS}" get deployment "${DEPLOYMENT_NAME}" \
      -o "jsonpath={.spec.template.metadata.annotations['${ANNOTATION_KEY}']}" 2>/dev/null || true
  )"

  if [[ "${existing}" == "${hash}" ]]; then
    log "manifests unchanged (hash match); skipping apply"
    return 0
  fi

  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/serviceaccount.yaml" >/dev/null
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/deployment.yaml" >/dev/null
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/service.yaml" >/dev/null

  kubectl -n "${TARGET_NS}" patch deployment "${DEPLOYMENT_NAME}" --type=merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"${ANNOTATION_KEY}\":\"${hash}\"}}}}}" >/dev/null

  log "applied manifests and wrote template annotation ${ANNOTATION_KEY}=${hash}"
}

# --- Diagnostics --------------------------------------------------------------

dump_diagnostics() {
  log "diagnostics: pods"
  kubectl -n "${TARGET_NS}" get pods -o wide 2>&1 || true

  log "diagnostics: deployment"
  kubectl -n "${TARGET_NS}" describe deployment "${DEPLOYMENT_NAME}" 2>&1 || true

  log "diagnostics: service"
  kubectl -n "${TARGET_NS}" get svc "${SERVICE_NAME}" -o wide 2>&1 || true

  log "diagnostics: endpoints"
  kubectl -n "${TARGET_NS}" get endpoints "${SERVICE_NAME}" -o wide 2>&1 || true

  log "diagnostics: recent events"
  kubectl get events -A --sort-by=.lastTimestamp 2>&1 | tail -n 80 || true

  log "diagnostics: logs"
  kubectl -n "${TARGET_NS}" logs deployment/"${DEPLOYMENT_NAME}" --tail=200 2>&1 || true
}

on_err() {
  local rc=$?
  dump_diagnostics
  exit "$rc"
}

wait_for_deployment_ready() {
  log "waiting for deployment availability (timeout=${READY_TIMEOUT}s)"
  kubectl -n "${TARGET_NS}" wait --for=condition=Available deployment/"${DEPLOYMENT_NAME}" --timeout="${READY_TIMEOUT}s" >/dev/null \
    || fatal "timeout waiting for deployment availability"
  log "deployment ready"
}

# --- Main Operations ----------------------------------------------------------

rollout() {
  local run_validate="${1:-false}"

  require_prereqs
  mkdir -p "${MANIFEST_DIR}"
  ensure_namespace

  log "starting iceberg rollout"
  log "namespace=${TARGET_NS}"
  log "image=${IMAGE}"
  log "rest_url=$(rest_base_url)"
  log "warehouse=$(warehouse_uri)"
  log "aws_region=${AWS_REGION}"
  log "s3_endpoint=$(normalized_s3_endpoint)"
  log "s3_path_style_access=${S3_PATH_STYLE_ACCESS}"
  log "secret_name=${SECRET_NAME}"
  log "validate=${run_validate}"

  apply_secret
  render_serviceaccount
  render_deployment
  render_service
  apply_manifests
  wait_for_deployment_ready

  if [[ "${run_validate}" == "true" ]]; then
    if [[ -x "${VALIDATE_SCRIPT}" ]]; then
      log "running validation script ${VALIDATE_SCRIPT}"
      bash "${VALIDATE_SCRIPT}"
    elif [[ -f "${VALIDATE_SCRIPT}" ]]; then
      log "running validation script ${VALIDATE_SCRIPT}"
      bash "${VALIDATE_SCRIPT}"
    else
      fatal "validation script not found: ${VALIDATE_SCRIPT}"
    fi
  else
    log "skipping validation (pass --validate to enable)"
  fi

  log "[SUCCESS] iceberg rollout complete"
}

delete_all() {
  log "deleting iceberg resources"
  kubectl -n "${TARGET_NS}" delete deployment "${DEPLOYMENT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete svc "${SERVICE_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete sa "${SERVICE_ACCOUNT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  rm -f \
    "${MANIFEST_DIR}/serviceaccount.yaml" \
    "${MANIFEST_DIR}/deployment.yaml" \
    "${MANIFEST_DIR}/service.yaml" || true
  log "deleted iceberg resources; data in object storage preserved"
}

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --rollout [--validate]   Deploy Iceberg REST catalog (optionally run validation)
  --delete                 Remove all Iceberg resources
  --help, -h               Show this help message

Examples:
  $0 --rollout                    # Deploy without validation
  $0 --rollout --validate         # Deploy and run validation
  $0 --delete                     # Clean up resources
EOF
}

# --- Entry Point --------------------------------------------------------------

main() {
  local run_validate="false"
  local action=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --rollout)
        action="rollout"
        shift
        ;;
      --validate)
        run_validate="true"
        shift
        ;;
      --delete)
        action="delete"
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        fatal "unknown argument: $1"
        ;;
    esac
  done

  if [[ -z "${action}" ]]; then
    action="rollout"
  fi

  trap on_err ERR

  case "${action}" in
    rollout) rollout "${run_validate}" ;;
    delete) delete_all ;;
  esac
}

main "$@"
