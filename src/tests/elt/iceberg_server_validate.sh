
#!/usr/bin/env bash
set -Eeuo pipefail

TARGET_NS="${TARGET_NS:-default}"

DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-iceberg-rest}"
SERVICE_NAME="${SERVICE_NAME:-iceberg-rest}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-iceberg-rest-sa}"
SECRET_NAME="${SECRET_NAME:-iceberg-storage-credentials}"

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

SPARK_PROBE_IMAGE="${SPARK_PROBE_IMAGE:-${ELT_TASK_IMAGE:-ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-31-07-13--8cb3f14}}"

log() {
  printf '[%s] [iceberg-validate] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

fatal() {
  printf '[%s] [iceberg-validate][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
  exit 1
}

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"
}

require_prereqs() {
  require_bin kubectl
  require_bin python3
  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach cluster"
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

print_contracts() {
  local base warehouse endpoint
  base="$(rest_base_url)"
  warehouse="$(warehouse_uri)"
  endpoint="$(normalized_s3_endpoint)"

  printf '\nServer contract:\n\n'
  printf 'AWS_REGION=%s\n' "${AWS_REGION}"
  printf 'AWS_DEFAULT_REGION=%s\n' "${AWS_REGION}"
  printf 'AWS_EC2_METADATA_DISABLED=true\n'
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

capture_json() {
  local resource="$1"
  local name="$2"
  local out="$3"
  kubectl -n "${TARGET_NS}" get "${resource}" "${name}" -o json > "${out}"
}

capture_endpoints_json() {
  local out="$1"
  kubectl -n "${TARGET_NS}" get endpoints "${SERVICE_NAME}" -o json > "${out}"
}

validate_server_spec() {
  local dep_json secret_json svc_json eps_json
  dep_json="$(mktemp)"
  secret_json="$(mktemp)"
  svc_json="$(mktemp)"
  eps_json="$(mktemp)"
  trap 'rm -f "${dep_json}" "${secret_json}" "${svc_json}" "${eps_json}"' RETURN

  capture_json deployment "${DEPLOYMENT_NAME}" "${dep_json}"
  capture_json secret "${SECRET_NAME}" "${secret_json}"
  capture_json service "${SERVICE_NAME}" "${svc_json}"
  capture_endpoints_json "${eps_json}"

  python3 - "$dep_json" "$secret_json" "$svc_json" "$eps_json" "$IMAGE" "$TARGET_NS" "$SERVICE_NAME" "$DEPLOYMENT_NAME" "$CONTAINER_PORT" <<'PY'
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

dep_path, secret_path, svc_path, eps_path, expected_image, target_ns, service_name, deployment_name, container_port = sys.argv[1:10]

dep = json.loads(Path(dep_path).read_text())
secret = json.loads(Path(secret_path).read_text())
svc = json.loads(Path(svc_path).read_text())
eps = json.loads(Path(eps_path).read_text())

if dep.get("metadata", {}).get("namespace") != target_ns:
    raise SystemExit(f"deployment namespace mismatch: {dep.get('metadata', {}).get('namespace')!r}")

containers = dep.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
if not containers:
    raise SystemExit("deployment has no containers")
container = None
for c in containers:
    if c.get("name") == "rest":
        container = c
        break
if container is None:
    raise SystemExit("deployment container named 'rest' not found")

if container.get("image") != expected_image:
    raise SystemExit(f"deployment image mismatch: expected {expected_image!r}, got {container.get('image')!r}")

ports = container.get("ports", [])
if not any(str(p.get("containerPort")) == container_port for p in ports):
    raise SystemExit(f"container port {container_port} not found")

env = {item.get("name"): item for item in container.get("env", []) if item.get("name")}
required_literals = {
    "AWS_REGION": None,
    "AWS_DEFAULT_REGION": None,
    "AWS_EC2_METADATA_DISABLED": "true",
    "CATALOG_WAREHOUSE": None,
    "CATALOG_IO__IMPL": "org.apache.iceberg.aws.s3.S3FileIO",
    "CATALOG_CLIENT_REGION": None,
    "CATALOG_S3_PATH_STYLE_ACCESS": None,
    "HOME": "/tmp",
    "TMPDIR": "/tmp",
}
for key, expected in required_literals.items():
    item = env.get(key)
    if item is None:
        raise SystemExit(f"missing env var {key}")
    if "value" not in item:
        raise SystemExit(f"env var {key} is not a literal value")
    if expected is not None and item["value"] != expected:
        raise SystemExit(f"env var {key} mismatch: expected {expected!r}, got {item['value']!r}")

aws_region = env["AWS_REGION"]["value"]
if env["AWS_DEFAULT_REGION"]["value"] != aws_region:
    raise SystemExit("AWS_DEFAULT_REGION must equal AWS_REGION")
if env["CATALOG_CLIENT_REGION"]["value"] != aws_region:
    raise SystemExit("CATALOG_CLIENT_REGION must equal AWS_REGION")

for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
    item = env.get(key)
    if item is None:
        raise SystemExit(f"missing env var {key}")
    ref = item.get("valueFrom", {}).get("secretKeyRef", {})
    if ref.get("name") != secret.get("metadata", {}).get("name"):
        raise SystemExit(f"{key} secret name mismatch")
    if ref.get("key") != key:
        raise SystemExit(f"{key} secret key mismatch")

token_item = env.get("AWS_SESSION_TOKEN")
if token_item is None:
    raise SystemExit("missing env var AWS_SESSION_TOKEN")
token_ref = token_item.get("valueFrom", {}).get("secretKeyRef", {})
if token_ref.get("name") != secret.get("metadata", {}).get("name"):
    raise SystemExit("AWS_SESSION_TOKEN secret name mismatch")
if token_ref.get("key") != "AWS_SESSION_TOKEN":
    raise SystemExit("AWS_SESSION_TOKEN secret key mismatch")
if token_ref.get("optional") is not True:
    raise SystemExit("AWS_SESSION_TOKEN should be optional")

secret_data = secret.get("data", {})
for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
    if key not in secret_data:
        raise SystemExit(f"secret missing key {key}")
    if not base64.b64decode(secret_data[key]).decode("utf-8"):
        raise SystemExit(f"secret key {key} is empty after decode")

if "AWS_SESSION_TOKEN" in secret_data:
    if not base64.b64decode(secret_data["AWS_SESSION_TOKEN"]).decode("utf-8"):
        raise SystemExit("secret key AWS_SESSION_TOKEN is empty after decode")

selector = svc.get("spec", {}).get("selector", {})
if selector.get("app") != deployment_name:
    raise SystemExit("service selector mismatch")

subsets = eps.get("subsets", [])
if not subsets:
    raise SystemExit("service has no endpoints")
if not any(subset.get("addresses") for subset in subsets):
    raise SystemExit("service endpoints have no addresses")

print("server_spec_ok")
PY
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
    --command -- python3 -
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
  local ak sk token warehouse uri endpoint path_style provider spark_pod
  if [[ -z "${SPARK_PROBE_IMAGE}" ]]; then
    fatal "SPARK_PROBE_IMAGE is required for full Spark write-path validation"
  fi

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
ak = os.environ["AWS_ACCESS_KEY_ID"]
sk = os.environ["AWS_SECRET_ACCESS_KEY"]
token = os.environ.get("AWS_SESSION_TOKEN", "")

spark = (
    SparkSession.builder
    .appName("iceberg-spark-probe")
    .master("local[1]")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", uri)
    .config("spark.sql.catalog.iceberg.warehouse", warehouse)
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.rest.auth.type", "none")
    .config("spark.sql.catalog.iceberg.s3.endpoint", endpoint)
    .config("spark.sql.catalog.iceberg.s3.path-style-access", path_style)
    .config("spark.sql.catalog.iceberg.s3.access-key-id", ak)
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", sk)
    .config("spark.sql.catalog.iceberg.s3.preload-client-enabled", "true")
    .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.path.style.access", path_style)
    .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.aws.credentials.provider", provider)
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", path_style)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
    .config("spark.hadoop.fs.s3a.access.key", ak)
    .config("spark.hadoop.fs.s3a.secret.key", sk)
)

if token:
    spark = spark.config("spark.sql.catalog.iceberg.s3.session-token", token)
    spark = spark.config("spark.hadoop.fs.s3a.session.token", token)

spark = spark.getOrCreate()

print("=== spark catalog conf ===")
for k, v in sorted(spark.sparkContext.getConf().getAll()):
    if k.startswith("spark.sql.catalog.iceberg") or k.startswith("spark.hadoop.fs.s3a"):
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
    --command -- python3 -
  )

  if [[ -n "${token}" ]]; then
    args+=(--env "AWS_SESSION_TOKEN=${token}")
  fi

  "${args[@]}" < "${tmp_py}"
  rm -f "${tmp_py}"

  log "Spark smoke passed"
}

validate() {
  require_prereqs

  log "validating current iceberg deployment"
  log "namespace=${TARGET_NS}"
  log "image=${IMAGE}"
  log "rest_url=$(rest_base_url)"
  log "warehouse=$(warehouse_uri)"
  log "aws_region=${AWS_REGION}"
  log "s3_endpoint=$(normalized_s3_endpoint)"
  log "s3_path_style_access=${S3_PATH_STYLE_ACCESS}"
  log "spark_probe_image=${SPARK_PROBE_IMAGE}"

  print_contracts
  validate_server_spec
  rest_smoke
  spark_smoke

  log "[SUCCESS] iceberg server validation complete"
}

case "${1:-}" in
  --validate|"") validate ;;
  --help|-h) printf 'Usage: %s [--validate]\n' "$0" ;;
  *) fatal "unknown argument: $1" ;;
esac