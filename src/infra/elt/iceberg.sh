#!/usr/bin/env bash
set -Eeuo pipefail

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
SPARK_PROBE_IMAGE="${SPARK_PROBE_IMAGE:-${ELT_TASK_IMAGE:-}}"
REQUIRE_SPARK_SMOKE="${REQUIRE_SPARK_SMOKE:-1}"

log() {
  printf '[%s] [iceberg] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

fatal() {
  printf '[%s] [iceberg][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
  exit 1
}

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

ensure_namespace() {
  if kubectl get ns "${TARGET_NS}" >/dev/null 2>&1; then
    return 0
  fi
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
  local endpoint
  endpoint="${S3_ENDPOINT}"
  if [[ -z "${endpoint}" ]]; then
    endpoint="https://s3.${AWS_REGION}.amazonaws.com"
  elif [[ "${endpoint}" != http://* && "${endpoint}" != https://* ]]; then
    endpoint="https://${endpoint}"
  fi
  trim_trailing_slash "${endpoint}"
}

spark_s3a_credential_provider() {
  if [[ -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]]; then
    if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
      printf '%s' 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider'
    else
      printf '%s' 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
    fi
    return 0
  fi
  printf '%s' 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain'
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

  existing="$(kubectl -n "${TARGET_NS}" get deployment "${DEPLOYMENT_NAME}" -o "jsonpath={.metadata.annotations['${ANNOTATION_KEY}']}" 2>/dev/null || true)"

  if [[ "${existing}" == "${hash}" ]]; then
    log "manifests unchanged (hash match); skipping apply"
    return 0
  fi

  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/serviceaccount.yaml" >/dev/null
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/deployment.yaml" >/dev/null
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/service.yaml" >/dev/null

  kubectl -n "${TARGET_NS}" patch deployment "${DEPLOYMENT_NAME}" --type=merge \
    -p "{\"metadata\":{\"annotations\":{\"${ANNOTATION_KEY}\":\"${hash}\"}}}" >/dev/null

  log "applied manifests and wrote annotation ${ANNOTATION_KEY}=${hash}"
}

get_primary_pod() {
  kubectl -n "${TARGET_NS}" get pod -l app="${DEPLOYMENT_NAME}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

assert_server_env() {
  local pod
  pod="$(get_primary_pod)"
  [[ -n "${pod}" ]] || fatal "unable to find iceberg-rest pod"

  local warehouse expected_endpoint
  warehouse="$(warehouse_uri)"
  expected_endpoint="$(normalized_s3_endpoint)"

  log "validating server pod env on ${pod}"

  local actual_warehouse actual_impl actual_region actual_client_region actual_endpoint actual_path_style
  actual_warehouse="$(kubectl -n "${TARGET_NS}" exec "${pod}" -- sh -c 'printf %s "$CATALOG_WAREHOUSE"' 2>/dev/null || true)"
  actual_impl="$(kubectl -n "${TARGET_NS}" exec "${pod}" -- sh -c 'printf %s "$CATALOG_IO__IMPL"' 2>/dev/null || true)"
  actual_region="$(kubectl -n "${TARGET_NS}" exec "${pod}" -- sh -c 'printf %s "$AWS_REGION"' 2>/dev/null || true)"
  actual_client_region="$(kubectl -n "${TARGET_NS}" exec "${pod}" -- sh -c 'printf %s "$CATALOG_CLIENT_REGION"' 2>/dev/null || true)"
  actual_endpoint="$(kubectl -n "${TARGET_NS}" exec "${pod}" -- sh -c 'printf %s "$CATALOG_S3_ENDPOINT"' 2>/dev/null || true)"
  actual_path_style="$(kubectl -n "${TARGET_NS}" exec "${pod}" -- sh -c 'printf %s "$CATALOG_S3_PATH_STYLE_ACCESS"' 2>/dev/null || true)"

  [[ "${actual_warehouse}" == "${warehouse}" ]] || fatal "server warehouse mismatch: expected ${warehouse}, got ${actual_warehouse}"
  [[ "${actual_impl}" == "org.apache.iceberg.aws.s3.S3FileIO" ]] || fatal "server io impl mismatch: got ${actual_impl}"
  [[ "${actual_region}" == "${AWS_REGION}" ]] || fatal "server region mismatch: expected ${AWS_REGION}, got ${actual_region}"
  [[ "${actual_client_region}" == "${AWS_REGION}" ]] || fatal "server client region mismatch: expected ${AWS_REGION}, got ${actual_client_region}"
  [[ "${actual_path_style}" == "${S3_PATH_STYLE_ACCESS}" ]] || fatal "server path-style mismatch: expected ${S3_PATH_STYLE_ACCESS}, got ${actual_path_style}"
  if [[ -n "${actual_endpoint}" ]]; then
    [[ "${actual_endpoint}" == "${expected_endpoint}" ]] || fatal "server endpoint mismatch: expected ${expected_endpoint}, got ${actual_endpoint}"
  fi
}

dump_diagnostics() {
  log "diagnostics: pods"
  kubectl -n "${TARGET_NS}" get pods -o wide || true

  log "diagnostics: deployment"
  kubectl -n "${TARGET_NS}" describe deployment "${DEPLOYMENT_NAME}" || true

  log "diagnostics: service"
  kubectl -n "${TARGET_NS}" get svc "${SERVICE_NAME}" -o wide || true

  log "diagnostics: endpoints"
  kubectl -n "${TARGET_NS}" get endpoints "${SERVICE_NAME}" -o wide || true

  log "diagnostics: recent events"
  kubectl get events -A --sort-by=.lastTimestamp | tail -n 80 || true

  log "diagnostics: logs"
  kubectl -n "${TARGET_NS}" logs deployment/"${DEPLOYMENT_NAME}" --tail=200 || true
}

on_err() {
  local rc=$?
  dump_diagnostics
  exit "$rc"
}

trap on_err ERR

wait_for_deployment_ready() {
  kubectl -n "${TARGET_NS}" rollout status deployment/"${DEPLOYMENT_NAME}" --timeout="${READY_TIMEOUT}s" >/dev/null \
    || fatal "timeout waiting for deployment readiness"
  log "deployment ready"
}

rest_smoke() {
  local base smoke_pod
  base="$(rest_base_url)"
  smoke_pod="iceberg-rest-smoke-$(date +%s)-$$"

  log "running REST smoke"
  log "REST smoke base_url=${base}"

  cat <<'PY' | kubectl -n "${TARGET_NS}" run "${smoke_pod}" --rm -i --restart=Never \
    --image=python:3.12-slim \
    --env="BASE_URL=${base}" \
    --command -- python /dev/stdin
from __future__ import annotations

import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

base = os.environ["BASE_URL"].rstrip("/")


def request(path: str, *, method: str = "GET", body: bytes | None = None, headers: dict[str, str] | None = None):
    req = Request(f"{base}{path}", data=body, method=method, headers=headers or {})
    try:
        with urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            print(f"{method} {path} -> {resp.status}")
            if raw:
                print(raw[:4000])
            return resp.status, raw
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        print(f"{method} {path} -> {exc.code}", file=sys.stderr)
        if raw:
            print(raw[:4000], file=sys.stderr)
        return exc.code, raw
    except URLError as exc:
        print(f"{method} {path} -> {exc}", file=sys.stderr)
        raise

status, body = request("/v1/config")
if status != 200:
    raise SystemExit(f"unexpected /v1/config status: {status}")

parsed = json.loads(body)
if not isinstance(parsed, dict):
    raise SystemExit(f"/v1/config returned unexpected payload: {type(parsed)!r}")

status, body = request("/v1/namespaces")
if status != 200:
    raise SystemExit(f"unexpected /v1/namespaces status: {status}")

print("rest_smoke_ok")
PY

  log "REST smoke passed"
}

spark_smoke() {
  if [[ -z "${SPARK_PROBE_IMAGE}" ]]; then
    if [[ "${REQUIRE_SPARK_SMOKE}" == "1" ]]; then
      fatal "SPARK_PROBE_IMAGE is required for full Spark write-path validation"
    fi
    log "SPARK_PROBE_IMAGE not set; skipping Spark smoke"
    return 0
  fi

  local ak sk token warehouse uri endpoint path_style provider spark_pod
  ak="$(kubectl -n "${TARGET_NS}" get secret "${SECRET_NAME}" -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' 2>/dev/null | base64 -d || true)"
  sk="$(kubectl -n "${TARGET_NS}" get secret "${SECRET_NAME}" -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' 2>/dev/null | base64 -d || true)"
  token="$(kubectl -n "${TARGET_NS}" get secret "${SECRET_NAME}" -o jsonpath='{.data.AWS_SESSION_TOKEN}' 2>/dev/null | base64 -d || true)"
  warehouse="$(warehouse_uri)"
  uri="$(rest_base_url)"
  endpoint="$(normalized_s3_endpoint)"
  path_style="${S3_PATH_STYLE_ACCESS}"
  provider="$(spark_s3a_credential_provider)"
  spark_pod="iceberg-spark-probe-$(date +%s)-$$"

  if [[ -z "${ak}" || -z "${sk}" ]]; then
    fatal "AWS credentials not available for Spark smoke"
  fi

  log "running Spark smoke"
  log "Spark probe image=${SPARK_PROBE_IMAGE}"

  local tmp_py
  tmp_py="$(mktemp)"
  cat > "${tmp_py}" <<'PY'
from __future__ import annotations

import os
import uuid
from pyspark.sql import SparkSession

uri = os.environ["ICEBERG_URI"]
warehouse = os.environ["ICEBERG_WAREHOUSE"]
endpoint = os.environ["ICEBERG_S3_ENDPOINT"]
path_style = os.environ["ICEBERG_S3_PATH_STYLE_ACCESS"]
region = os.environ["AWS_REGION"]
provider = os.environ["ICEBERG_S3A_CREDENTIAL_PROVIDER"]

spark = (
    SparkSession.builder
    .appName("iceberg-spark-probe")
    .master("local[1]")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", uri)
    .config("spark.sql.catalog.iceberg.warehouse", warehouse)
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.rest.auth.type", "none")
    .config("spark.sql.catalog.iceberg.client.region", region)
    .config("spark.sql.catalog.iceberg.s3.endpoint", endpoint)
    .config("spark.sql.catalog.iceberg.s3.path-style-access", path_style)
    .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.path.style.access", path_style)
    .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.aws.credentials.provider", provider)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", path_style)
    .getOrCreate()
)

print("=== spark catalog conf ===")
for k, v in sorted(spark.sparkContext.getConf().getAll()):
    if k.startswith("spark.sql.catalog.iceberg"):
        print(f"{k}={v}")

print("=== namespaces ===")
spark.sql("SHOW NAMESPACES IN iceberg").show(truncate=False)

ns = f"mlsecops_probe_{uuid.uuid4().hex[:8]}"
table = f"iceberg.{ns}.probe_{uuid.uuid4().hex[:8]}"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{ns}")

rows = spark.createDataFrame([(1, "ok")], ["id", "status"])
rows.writeTo(table).tableProperty("format-version", "2").create()

count = spark.table(table).count()
if count != 1:
    raise SystemExit(f"unexpected row count: {count}")

spark.sql(f"DROP TABLE {table}")
spark.sql(f"DROP NAMESPACE IF EXISTS iceberg.{ns}")
print("spark_smoke_ok")
PY

  local args=(
    kubectl -n "${TARGET_NS}" run "${spark_pod}" --rm -i --restart=Never
    --image="${SPARK_PROBE_IMAGE}"
    --env "SPARK_LOCAL_HOSTNAME=localhost"
    --env "AWS_REGION=${AWS_REGION}"
    --env "AWS_ACCESS_KEY_ID=${ak}"
    --env "AWS_SECRET_ACCESS_KEY=${sk}"
    --env "ICEBERG_URI=${uri}"
    --env "ICEBERG_WAREHOUSE=${warehouse}"
    --env "ICEBERG_S3_ENDPOINT=${endpoint}"
    --env "ICEBERG_S3_PATH_STYLE_ACCESS=${path_style}"
    --env "ICEBERG_S3A_CREDENTIAL_PROVIDER=${provider}"
    --command -- python /dev/stdin
  )

  if [[ -n "${token}" ]]; then
    args+=(--env "AWS_SESSION_TOKEN=${token}")
  fi

  "${args[@]}" < "${tmp_py}"
  rm -f "${tmp_py}"

  log "Spark smoke passed"
}

print_contracts() {
  local base warehouse endpoint
  base="$(rest_base_url)"
  warehouse="$(warehouse_uri)"
  endpoint="$(normalized_s3_endpoint)"

  printf '\nServer contract:\n\n'
  printf 'AWS_REGION=%s\n' "${AWS_REGION}"
  printf 'CATALOG_WAREHOUSE=%s\n' "${warehouse}"
  printf 'CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO\n'
  printf 'CATALOG_CLIENT_REGION=%s\n' "${AWS_REGION}"
  printf 'CATALOG_S3_ENDPOINT=%s\n' "${endpoint}"
  printf 'CATALOG_S3_PATH_STYLE_ACCESS=%s\n' "${S3_PATH_STYLE_ACCESS}"
  printf 'ICEBERG_REST_URI=%s\n' "${base}"
  printf 'ICEBERG_REST_AUTH_TYPE=none\n'

  printf '\nClient contract:\n\n'
  printf 'SPARK_SQL_CATALOG_ICEBERG_TYPE=rest\n'
  printf 'SPARK_SQL_CATALOG_ICEBERG_URI=%s\n' "${base}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_WAREHOUSE=%s\n' "${warehouse}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_IO_IMPL=org.apache.iceberg.aws.s3.S3FileIO\n'
  printf 'SPARK_SQL_CATALOG_ICEBERG_REST_AUTH_TYPE=none\n'
  printf 'SPARK_SQL_CATALOG_ICEBERG_CLIENT_REGION=%s\n' "${AWS_REGION}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_S3_ENDPOINT=%s\n' "${endpoint}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_S3_PATH_STYLE_ACCESS=%s\n' "${S3_PATH_STYLE_ACCESS}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_HADOOP_FS_S3A_ENDPOINT=%s\n' "${endpoint}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_HADOOP_FS_S3A_PATH_STYLE_ACCESS=%s\n' "${S3_PATH_STYLE_ACCESS}"
  printf 'SPARK_SQL_CATALOG_ICEBERG_HADOOP_FS_S3A_AWS_CREDENTIALS_PROVIDER=%s\n' "$(spark_s3a_credential_provider)"
}

delete_all() {
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

rollout() {
  require_prereqs
  mkdir -p "${MANIFEST_DIR}"
  ensure_namespace

  local endpoint
  endpoint="$(normalized_s3_endpoint)"

  log "starting iceberg rollout"
  log "namespace=${TARGET_NS}"
  log "image=${IMAGE}"
  log "rest_url=$(rest_base_url)"
  log "warehouse=$(warehouse_uri)"
  log "aws_region=${AWS_REGION}"
  log "s3_endpoint=${endpoint}"
  log "s3_path_style_access=${S3_PATH_STYLE_ACCESS}"
  log "secret_name=${SECRET_NAME}"
  log "spark_probe_image=${SPARK_PROBE_IMAGE:-<empty>}"

  apply_secret
  render_serviceaccount
  render_deployment
  render_service
  apply_manifests
  wait_for_deployment_ready
  assert_server_env
  rest_smoke
  spark_smoke
  print_contracts
  log "[SUCCESS] iceberg rollout complete"
}

case "${1:-}" in
  --rollout|"") rollout ;;
  --delete) delete_all ;;
  --help|-h) printf 'Usage: %s [--rollout|--delete]\n' "$0" ;;
  *) fatal "unknown argument: $1" ;;
esac