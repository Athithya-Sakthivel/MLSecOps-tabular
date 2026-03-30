#!/usr/bin/env bash
set -Eeuo pipefail

TARGET_NS="${TARGET_NS:-default}"
MANIFEST_DIR="${MANIFEST_DIR:-src/manifests/iceberg}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-iceberg-rest}"
SERVICE_NAME="${SERVICE_NAME:-iceberg-rest}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-iceberg-rest-sa}"
SECRET_NAME="${SECRET_NAME:-iceberg-storage-credentials}"
PORT="${PORT:-8181}"
IMAGE="${IMAGE:-apache/iceberg-rest-fixture:1.10.1@sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f}"
ANNOTATION_KEY="${ANNOTATION_KEY:-mlsecops.iceberg.checksum}"

AWS_REGION="${AWS_REGION:-ap-south-1}"
S3_BUCKET="${S3_BUCKET:-e2e-mlops-data-681802563986}"
S3_PREFIX="${S3_PREFIX:-iceberg/warehouse}"
S3_ENDPOINT="${S3_ENDPOINT:-}"
S3_PATH_STYLE_ACCESS="${S3_PATH_STYLE_ACCESS:-false}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"

READY_TIMEOUT="${READY_TIMEOUT:-300}"
SPARK_PROBE_IMAGE="${SPARK_PROBE_IMAGE:-${ELT_TASK_IMAGE:-}}"

log() { printf '[%s] [iceberg] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
fatal() { printf '[%s] [iceberg][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; exit 1; }
require_bin() { command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"; }

mkdir -p "${MANIFEST_DIR}"

ensure_namespace() {
  kubectl create ns "${TARGET_NS}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

normalize_prefix() {
  local p="${1#/}"
  printf '%s' "${p%/}"
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

compute_checksum() {
  printf '%s\n' \
    "${IMAGE}" \
    "${PORT}" \
    "${AWS_REGION}" \
    "${S3_BUCKET}" \
    "${S3_PREFIX}" \
    "${S3_ENDPOINT}" \
    "${S3_PATH_STYLE_ACCESS}" \
    "${AWS_ACCESS_KEY_ID}" \
    "${AWS_SECRET_ACCESS_KEY}" \
    "${AWS_SESSION_TOKEN}" \
    "$(warehouse_uri)" | sha256sum | awk '{print $1}'
}

create_or_update_storage_secret() {
  [[ -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]] || \
    fatal "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required"

  cat > "${MANIFEST_DIR}/secret.yaml" <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${SECRET_NAME}
  namespace: ${TARGET_NS}
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
  AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
EOF

  if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
    cat >> "${MANIFEST_DIR}/secret.yaml" <<EOF
  AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN}
EOF
  fi

  kubectl apply -f "${MANIFEST_DIR}/secret.yaml" >/dev/null
  log "created/updated AWS storage secret ${SECRET_NAME}"
}

render_files() {
  local warehouse checksum
  warehouse="$(warehouse_uri)"
  checksum="$(compute_checksum)"

  cat > "${MANIFEST_DIR}/serviceaccount.yaml" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
automountServiceAccountToken: true
EOF

  cat > "${MANIFEST_DIR}/deployment.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEPLOYMENT_NAME}
  namespace: ${TARGET_NS}
  annotations:
    ${ANNOTATION_KEY}: "${checksum}"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${DEPLOYMENT_NAME}
  template:
    metadata:
      labels:
        app: ${DEPLOYMENT_NAME}
      annotations:
        ${ANNOTATION_KEY}: "${checksum}"
    spec:
      serviceAccountName: ${SERVICE_ACCOUNT_NAME}
      terminationGracePeriodSeconds: 30
      containers:
      - name: rest
        image: ${IMAGE}
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: ${PORT}
        env:
        - name: AWS_REGION
          value: "${AWS_REGION}"
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
        - name: CATALOG_S3_REGION
          value: "${AWS_REGION}"
        - name: CATALOG_S3_ENDPOINT
          value: "${S3_ENDPOINT}"
        - name: CATALOG_S3_PATH__STYLE__ACCESS
          value: "${S3_PATH_STYLE_ACCESS}"
        startupProbe:
          httpGet:
            path: /v1/config
            port: ${PORT}
          failureThreshold: 120
          periodSeconds: 2
        readinessProbe:
          httpGet:
            path: /v1/config
            port: ${PORT}
          failureThreshold: 12
          periodSeconds: 5
          timeoutSeconds: 2
        livenessProbe:
          httpGet:
            path: /v1/config
            port: ${PORT}
          failureThreshold: 6
          periodSeconds: 10
          timeoutSeconds: 2
EOF

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
    port: ${PORT}
    targetPort: ${PORT}
EOF

  log "rendered serviceaccount, deployment, service"
}

apply_manifests() {
  kubectl apply -f "${MANIFEST_DIR}/serviceaccount.yaml" >/dev/null
  kubectl apply -f "${MANIFEST_DIR}/deployment.yaml" >/dev/null
  kubectl apply -f "${MANIFEST_DIR}/service.yaml" >/dev/null
  log "applied manifests"
}

wait_for_deployment_ready() {
  kubectl -n "${TARGET_NS}" rollout status deployment/"${DEPLOYMENT_NAME}" --timeout="${READY_TIMEOUT}s" >/dev/null \
    || fatal "timeout waiting for deployment readiness"
  log "deployment ready"
}

run_rest_smoke() {
  local rest_url
  rest_url="http://${SERVICE_NAME}.${TARGET_NS}.svc.cluster.local:${PORT}"

  log "running REST smoke"
  kubectl run iceberg-rest-smoke -n "${TARGET_NS}" --rm -i --restart=Never \
    --image=python:3.12-slim \
    --command -- python /dev/stdin <<PY
from urllib.request import Request, urlopen
import json

base = "${rest_url}"

req = Request(base + "/v1/config", method="GET")
with urlopen(req, timeout=20) as resp:
    body = json.loads(resp.read().decode("utf-8"))
    assert resp.status == 200
    assert isinstance(body, dict)

req = Request(
    base + "/v1/namespaces",
    method="POST",
    data=json.dumps({"namespace": ["mlsecops_smoke"]}).encode("utf-8"),
    headers={"Content-Type": "application/json"},
)
with urlopen(req, timeout=20) as resp:
    assert resp.status in (200, 201, 204, 409)

print("rest_smoke_ok")
PY
  log "REST smoke passed"
}

run_s3_smoke() {
  [[ -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]] || return 0
  [[ -n "${SPARK_PROBE_IMAGE}" ]] || { log "SPARK_PROBE_IMAGE not set; skipping Spark smoke"; return 0; }

  cat <<'PY' | kubectl run iceberg-spark-probe -n "${TARGET_NS}" --rm -i --restart=Never \
    --image="${SPARK_PROBE_IMAGE}" \
    --env SPARK_LOCAL_HOSTNAME=localhost \
    --env AWS_REGION="${AWS_REGION}" \
    --env AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
    --env AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
    ${AWS_SESSION_TOKEN:+--env AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}"} \
    --command -- python /dev/stdin
from pyspark.sql import SparkSession
import uuid

spark = (
    SparkSession.builder
    .appName("iceberg-smoke")
    .master("local[1]")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest.default.svc.cluster.local:8181")
    .config("spark.sql.catalog.iceberg.warehouse", "s3://e2e-mlops-data-681802563986/iceberg/warehouse/")
    .config("spark.sql.catalog.iceberg.rest.auth.type", "none")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

print("=== SHOW NAMESPACES ===")
spark.sql("SHOW NAMESPACES IN iceberg").show(truncate=False)
ns = f"mlsecops_probe_{uuid.uuid4().hex[:8]}"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{ns}")
spark.sql(f"DROP NAMESPACE IF EXISTS iceberg.{ns}")
print("spark_smoke_ok")
PY
}

print_connection_details() {
  local warehouse
  warehouse="$(warehouse_uri)"
  printf '\nConnection details:\n\n'
  printf 'ICEBERG_REST_URI=http://iceberg-rest.%s.svc.cluster.local:%s\n' "${TARGET_NS}" "${PORT}"
  printf 'ICEBERG_REST_AUTH_TYPE=none\n'
  printf 'ICEBERG_WAREHOUSE=%s\n' "${warehouse}"
  printf 'ICEBERG_CATALOG=iceberg\n'
  printf 'SPARK_SQL_CATALOG_ICEBERG_TYPE=rest\n'
  printf 'SPARK_SQL_CATALOG_ICEBERG_URI=http://iceberg-rest.%s.svc.cluster.local:%s\n' "${TARGET_NS}" "${PORT}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_WAREHOUSE=%s\n' "${warehouse}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_IO_IMPL=org.apache.iceberg.aws.s3.S3FileIO\n'
  printf 'SPARK_SQL_CATALOG_ICEBERG_REST_AUTH_TYPE=none\n'
}

delete_all() {
  kubectl -n "${TARGET_NS}" delete deployment "${DEPLOYMENT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete svc "${SERVICE_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete sa "${SERVICE_ACCOUNT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  log "deleted iceberg resources; object-store data preserved"
}

rollout() {
  require_bin kubectl
  require_bin python3
  require_bin sha256sum
  ensure_namespace
  create_or_update_storage_secret
  render_files
  apply_manifests
  wait_for_deployment_ready
  run_rest_smoke
  run_s3_smoke
  print_connection_details
  log "[SUCCESS] iceberg rollout complete"
}

case "${1:-}" in
  --rollout|"") rollout ;;
  --delete) delete_all ;;
  --help|-h) printf 'Usage: %s [--rollout|--delete]\n' "$0" ;;
  *) fatal "unknown argument: $1" ;;
esac