#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
AUTH_DIR="${ROOT_DIR}/src/workflows/auth"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8001}"
BASE_URL="${BASE_URL:-http://${HOST}:${PORT}}"

REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-10}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-180}"

PG_MODE="${PG_MODE:-docker}"   # docker|external
PG_PORT="${PG_PORT:-55432}"
PG_CONTAINER_NAME="${PG_CONTAINER_NAME:-auth-postgres-local}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:18}"

OTEL_COLLECTOR_IMAGE="${OTEL_COLLECTOR_IMAGE:-otel/opentelemetry-collector-contrib:0.149.0}"
OTEL_COLLECTOR_CONTAINER_NAME="${OTEL_COLLECTOR_CONTAINER_NAME:-auth-otel-local}"
OTEL_COLLECTOR_CONFIG_DIR="$(mktemp -d /tmp/auth-otel-XXXXXX)"
COLLECTOR_GRPC_PORT="${COLLECTOR_GRPC_PORT:-4317}"
COLLECTOR_HTTP_PORT="${COLLECTOR_HTTP_PORT:-4318}"
COLLECTOR_HEALTH_PORT="${COLLECTOR_HEALTH_PORT:-13133}"

SESSION_COOKIE_NAME="${SESSION_COOKIE_NAME:-auth_session}"

if [[ -f "${ROOT_DIR}/.venv_auth/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.venv_auth/bin/activate"
fi

PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || true)}"
[[ -n "${PYTHON_BIN}" ]] || { echo "ERROR: python3 is required" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || { echo "ERROR: docker is required" >&2; exit 1; }

if [[ "${POSTGRES_IMAGE}" == *"cloudnative-pg"* ]]; then
  echo "WARN: CNPG image detected for local testing; falling back to postgres:18." >&2
  POSTGRES_IMAGE="postgres:18"
fi

export PYTHONUNBUFFERED=1
export PYTHONPATH="${AUTH_DIR}:${PYTHONPATH:-}"

export ENVIRONMENT="${ENVIRONMENT:-local}"
export APP_NAME="${APP_NAME:-auth-service}"
export SERVICE_NAME="${SERVICE_NAME:-auth-service}"
export SERVICE_VERSION="${SERVICE_VERSION:-v1}"
export DEPLOYMENT_ENVIRONMENT="${DEPLOYMENT_ENVIRONMENT:-local-env}"
export K8S_CLUSTER_NAME="${K8S_CLUSTER_NAME:-local-cluster}"
export POD_NAME="${POD_NAME:-local-1}"

export APP_BASE_URL="${APP_BASE_URL:-${BASE_URL}}"
export AUTH_VALIDATE_ROUTE_PATH="${AUTH_VALIDATE_ROUTE_PATH:-/validate}"
export AUTH_ME_ROUTE_PATH="${AUTH_ME_ROUTE_PATH:-/me}"

export SESSION_COOKIE_NAME="${SESSION_COOKIE_NAME}"
export SESSION_COOKIE_SECURE="${SESSION_COOKIE_SECURE:-false}"
export SESSION_COOKIE_SAMESITE="${SESSION_COOKIE_SAMESITE:-lax}"
export SESSION_COOKIE_PATH="${SESSION_COOKIE_PATH:-/}"
export SESSION_COOKIE_DOMAIN="${SESSION_COOKIE_DOMAIN:-}"

export SESSION_TTL_SECONDS="${SESSION_TTL_SECONDS:-86400}"
export AUTH_TX_TTL_SECONDS="${AUTH_TX_TTL_SECONDS:-600}"

export GOOGLE_CLIENT_ID="${GOOGLE_CLIENT_ID:-}"
export GOOGLE_CLIENT_SECRET="${GOOGLE_CLIENT_SECRET:-}"
export MS_CLIENT_ID="${MS_CLIENT_ID:-}"
export MS_CLIENT_SECRET="${MS_CLIENT_SECRET:-}"
export MS_TENANT_ID="${MS_TENANT_ID:-common}"
export GITHUB_CLIENT_ID="${GITHUB_CLIENT_ID:-}"
export GITHUB_CLIENT_SECRET="${GITHUB_CLIENT_SECRET:-}"

export GOOGLE_ALLOWED_DOMAINS="${GOOGLE_ALLOWED_DOMAINS:-}"
export MICROSOFT_ALLOWED_TENANT_IDS="${MICROSOFT_ALLOWED_TENANT_IDS:-}"
export MICROSOFT_ALLOWED_DOMAINS="${MICROSOFT_ALLOWED_DOMAINS:-}"
export GITHUB_ALLOWED_ORGS="${GITHUB_ALLOWED_ORGS:-}"

export METRICS_ENABLED="${METRICS_ENABLED:-true}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"

export OTEL_TRACES_SAMPLER="${OTEL_TRACES_SAMPLER:-parentbased_traceidratio}"
export OTEL_TRACES_SAMPLER_ARG="${OTEL_TRACES_SAMPLER_ARG:-1.0}"
export OTEL_TIMEOUT_SECONDS="${OTEL_TIMEOUT_SECONDS:-10.0}"
export OTEL_METRIC_EXPORT_INTERVAL_MS="${OTEL_METRIC_EXPORT_INTERVAL_MS:-5000}"
export OTEL_METRIC_EXPORT_TIMEOUT_MS="${OTEL_METRIC_EXPORT_TIMEOUT_MS:-3000}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:${COLLECTOR_GRPC_PORT}}"

section() {
  printf '\n\033[1;36m==> %s\033[0m\n' "$1"
}

info() {
  printf '\033[1;32m%s\033[0m\n' "$1"
}

warn() {
  printf '\033[1;33m%s\033[0m\n' "$1" >&2
}

fail() {
  printf '\033[1;31mERROR:\033[0m %s\n' "$1" >&2
  exit 1
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_s="$3"

  "${PYTHON_BIN}" - "$host" "$port" "$timeout_s" <<'PY'
from __future__ import annotations

import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
timeout_s = int(sys.argv[3])

deadline = time.time() + timeout_s
last_error = ""

while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            sys.exit(0)
    except Exception as exc:
        last_error = str(exc)
        time.sleep(0.5)

raise SystemExit(f"Timed out waiting for {host}:{port}: {last_error}")
PY
}

wait_for_http_200() {
  local url="$1"
  local timeout_s="$2"

  "${PYTHON_BIN}" - "$url" "$timeout_s" "$REQUEST_TIMEOUT_SECONDS" <<'PY'
from __future__ import annotations

import sys
import time
import urllib.error
import urllib.request

url = sys.argv[1]
timeout_s = int(sys.argv[2])
request_timeout = float(sys.argv[3])

deadline = time.time() + timeout_s
attempt = 0
last_error = ""

while time.time() < deadline:
    attempt += 1
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=request_timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if 200 <= resp.status < 300:
                print(f"{url} is ready (HTTP {resp.status})", flush=True)
                sys.exit(0)
            last_error = f"HTTP {resp.status}: {body}"
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        last_error = f"HTTP {exc.code}: {body}"
    except Exception as exc:
        last_error = str(exc)

    if attempt % 5 == 0:
        print(f"Waiting for {url} ... last error: {last_error}", flush=True)

    time.sleep(1)

raise SystemExit(f"Timed out waiting for {url}: {last_error}")
PY
}

wait_for_postgres_ready() {
  local dsn="$1"
  local timeout_s="$2"

  "${PYTHON_BIN}" - "$dsn" "$timeout_s" <<'PY'
from __future__ import annotations

import asyncio
import sys
import time

import asyncpg

dsn = sys.argv[1]
timeout_s = int(sys.argv[2])

async def main() -> None:
    deadline = time.time() + timeout_s
    last_error = ""
    while time.time() < deadline:
        try:
            conn = await asyncpg.connect(dsn=dsn, timeout=5)
            try:
                await conn.execute("SELECT 1")
            finally:
                await conn.close()
            return
        except Exception as exc:
            last_error = str(exc)
            await asyncio.sleep(1)
    raise SystemExit(f"Timed out waiting for PostgreSQL readiness: {last_error}")

asyncio.run(main())
PY
}

header_ci() {
  local key="$1"
  local headers_json="$2"
  "${PYTHON_BIN}" - "$key" "$headers_json" <<'PY'
from __future__ import annotations

import json
import sys

needle = sys.argv[1].lower()
headers = json.loads(sys.argv[2])

for k, v in headers.items():
    if k.lower() == needle:
        print(v)
        break
PY
}

docker_logs_tail() {
  local name="$1"
  printf '\n----- docker logs: %s -----\n' "$name"
  docker logs --tail 200 "$name" 2>&1 || true
  printf '----- end docker logs: %s -----\n\n'
}

cleanup() {
  local exit_code=$?
  set +e

  if [[ -n "${SERVICE_PID:-}" ]]; then
    kill "${SERVICE_PID}" >/dev/null 2>&1 || true
    wait "${SERVICE_PID}" >/dev/null 2>&1 || true
  fi

  docker rm -f "${PG_CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker rm -f "${OTEL_COLLECTOR_CONTAINER_NAME}" >/dev/null 2>&1 || true

  rm -rf "${OTEL_COLLECTOR_CONFIG_DIR}" >/dev/null 2>&1 || true
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM

section "Environment"
info "ROOT_DIR=${ROOT_DIR}"
info "AUTH_DIR=${AUTH_DIR}"
info "BASE_URL=${BASE_URL}"
info "POSTGRES_IMAGE=${POSTGRES_IMAGE}"
info "POSTGRES_DSN=${POSTGRES_DSN:-<unset>}"
info "OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT}"

section "Starting OTEL collector"

cat >"${OTEL_COLLECTOR_CONFIG_DIR}/config.yaml" <<EOF
extensions:
  health_check:
    endpoint: 0.0.0.0:13133

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:

exporters:
  debug:
    verbosity: normal

service:
  extensions: [health_check]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
EOF

docker rm -f "${OTEL_COLLECTOR_CONTAINER_NAME}" >/dev/null 2>&1 || true
docker run -d \
  --name "${OTEL_COLLECTOR_CONTAINER_NAME}" \
  -p "127.0.0.1:${COLLECTOR_GRPC_PORT}:4317" \
  -p "127.0.0.1:${COLLECTOR_HTTP_PORT}:4318" \
  -p "127.0.0.1:${COLLECTOR_HEALTH_PORT}:13133" \
  -v "${OTEL_COLLECTOR_CONFIG_DIR}:/etc/otelcol:ro" \
  "${OTEL_COLLECTOR_IMAGE}" \
  --config=/etc/otelcol/config.yaml >/dev/null

wait_for_port 127.0.0.1 "${COLLECTOR_GRPC_PORT}" 30
wait_for_http_200 "http://127.0.0.1:${COLLECTOR_HEALTH_PORT}/" 30

section "Starting PostgreSQL"

if [[ "${PG_MODE}" == "external" ]]; then
  [[ -n "${POSTGRES_DSN:-}" ]] || fail "PG_MODE=external requires POSTGRES_DSN to be set"
  info "Using external POSTGRES_DSN=${POSTGRES_DSN}"
  wait_for_postgres_ready "${POSTGRES_DSN}" "${READY_TIMEOUT_SECONDS}"
else
  docker rm -f "${PG_CONTAINER_NAME}" >/dev/null 2>&1 || true

  docker run -d \
    --name "${PG_CONTAINER_NAME}" \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=auth \
    -p "127.0.0.1:${PG_PORT}:5432" \
    --health-cmd='pg_isready -U postgres -d auth' \
    --health-interval=3s \
    --health-timeout=3s \
    --health-retries=30 \
    "${POSTGRES_IMAGE}" >/dev/null

  export POSTGRES_DSN="${POSTGRES_DSN:-postgresql://postgres:postgres@127.0.0.1:${PG_PORT}/auth}"

  info "Waiting for container health"
  "${PYTHON_BIN}" - "${PG_CONTAINER_NAME}" "${READY_TIMEOUT_SECONDS}" <<'PY'
from __future__ import annotations

import subprocess
import sys
import time

name = sys.argv[1]
timeout_s = int(sys.argv[2])

deadline = time.time() + timeout_s
last_status = ""

while time.time() < deadline:
    try:
        raw = subprocess.check_output(
            [
                "docker",
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}",
                name,
            ],
            text=True,
        ).strip()
        last_status = raw
        if raw in {"healthy", "no-healthcheck"}:
            sys.exit(0)
    except Exception as exc:
        last_status = str(exc)
    time.sleep(1)

raise SystemExit(f"Timed out waiting for docker health on {name}: {last_status}")
PY

  info "Waiting for database connections"
  if ! wait_for_postgres_ready "${POSTGRES_DSN}" "${READY_TIMEOUT_SECONDS}"; then
    docker_logs_tail "${PG_CONTAINER_NAME}"
    fail "PostgreSQL was not accepting connections"
  fi

  info "PostgreSQL is ready at ${POSTGRES_DSN}"
fi

section "Starting auth service"
cd "${AUTH_DIR}"
"${PYTHON_BIN}" -m uvicorn main:app \
  --host "${HOST}" \
  --port "${PORT}" \
  --log-level "${LOG_LEVEL,,}" &
SERVICE_PID=$!
cd "${ROOT_DIR}"
info "Auth service PID: ${SERVICE_PID}"

section "Waiting for auth readiness"
wait_for_http_200 "${BASE_URL}/readyz" "${READY_TIMEOUT_SECONDS}"

section "Local E2E checks"
"${PYTHON_BIN}" - "${BASE_URL}" "${POSTGRES_DSN}" "${SESSION_COOKIE_NAME}" "${REQUEST_TIMEOUT_SECONDS}" <<'PY'
from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
import urllib.error
import urllib.request
import uuid

import asyncpg

base_url = sys.argv[1].rstrip("/")
postgres_dsn = sys.argv[2]
cookie_name = sys.argv[3]
request_timeout = float(sys.argv[4])

def request(method: str, path: str, *, headers: dict[str, str] | None = None, body: bytes | None = None):
    req = urllib.request.Request(
        f"{base_url}{path}",
        data=body,
        headers=headers or {},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=request_timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, dict(resp.headers.items()), raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        return exc.code, dict(exc.headers.items()), raw

def get_json(path: str):
    status, headers, raw = request("GET", path)
    if status < 200 or status >= 300:
        raise RuntimeError(f"GET {path} failed: HTTP {status} {raw}")
    return json.loads(raw), headers

def assert_ok(cond: bool, msg: str):
    if not cond:
        raise RuntimeError(msg)

def get_header(headers: dict[str, str], name: str) -> str | None:
    needle = name.lower()
    for key, value in headers.items():
        if key.lower() == needle:
            return value
    return None

async def seed_session() -> tuple[str, str]:
    conn = await asyncpg.connect(postgres_dsn)
    try:
        user_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        csrf_token = str(uuid.uuid4())
        expires_at = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)

        await conn.execute(
            """
            INSERT INTO users (id, primary_email, display_name, status, created_at, updated_at, last_login_at)
            VALUES ($1, $2, $3, 'active', now(), now(), now())
            """,
            user_id,
            "local.test@example.com",
            "Local Test",
        )

        await conn.execute(
            """
            INSERT INTO app_sessions (
                id, user_id, provider, provider_sub, provider_email, provider_name, tenant_id,
                created_at, expires_at, revoked_at, last_seen_at, ip_addr, user_agent,
                csrf_token, session_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8, NULL, now(), $9, $10, $11, 1)
            """,
            session_id,
            user_id,
            "local",
            "local-subject-1",
            "local.test@example.com",
            "Local Test",
            None,
            expires_at,
            "127.0.0.1",
            "auth-local-test",
            csrf_token,
        )
        return session_id, user_id
    finally:
        await conn.close()

health, health_headers = get_json("/healthz")
ready, ready_headers = get_json("/readyz")
root, root_headers = get_json("/")

assert_ok(health.get("status") == "ok", f"unexpected /healthz payload: {health}")
assert_ok(ready.get("status") == "ok", f"unexpected /readyz payload: {ready}")
assert_ok(root.get("status") == "ok", f"unexpected / payload: {root}")
assert_ok(get_header(health_headers, "X-Request-Id"), "missing X-Request-Id on /healthz")
assert_ok(get_header(ready_headers, "X-Request-Id"), "missing X-Request-Id on /readyz")

status, _, raw = request("GET", "/validate")
assert_ok(status == 401, f"/validate without session must be 401, got {status}: {raw}")

status, _, raw = request("GET", "/me")
assert_ok(status == 401, f"/me without session must be 401, got {status}: {raw}")

status, headers, raw = request("GET", "/login")
assert_ok(status == 200, f"/login failed: {status} {raw}")
assert_ok("text/html" in headers.get("Content-Type", ""), f"/login content type unexpected: {headers.get('Content-Type')}")

session_id, user_id = asyncio.run(seed_session())

cookie_header = {"Cookie": f"{cookie_name}={session_id}"}
bearer_header = {"Authorization": f"Bearer {session_id}"}

status, headers, raw = request("GET", "/validate", headers=cookie_header)
assert_ok(status == 200, f"/validate with cookie failed: {status} {raw}")
payload = json.loads(raw)
assert_ok(payload.get("status") == "ok", f"/validate payload invalid: {payload}")
assert_ok(payload.get("user_id") == user_id, f"/validate user_id mismatch: {payload}")
assert_ok(payload.get("email") == "local.test@example.com", f"/validate email mismatch: {payload}")

status, headers, raw = request("GET", "/me", headers=bearer_header)
assert_ok(status == 200, f"/me with bearer failed: {status} {raw}")
payload = json.loads(raw)
assert_ok(payload.get("status") == "ok", f"/me payload invalid: {payload}")
assert_ok(payload.get("user_id") == user_id, f"/me user_id mismatch: {payload}")

status, headers, raw = request("POST", "/logout", headers=cookie_header)
assert_ok(status == 200, f"/logout failed: {status} {raw}")

status, headers, raw = request("GET", "/validate", headers=cookie_header)
assert_ok(status == 401, f"/validate after logout must be 401, got {status}: {raw}")

print(json.dumps(
    {
        "health": health,
        "ready": ready,
        "root": root,
        "session_id": session_id,
        "user_id": user_id,
        "result": "ok",
    },
    indent=2,
    ensure_ascii=False,
))
PY

section "Done"
info "E2E auth checks passed."