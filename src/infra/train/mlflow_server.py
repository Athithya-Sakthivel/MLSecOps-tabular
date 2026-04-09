#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import os
import shutil
import socket
import subprocess
import sys
import time
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import yaml

MANIFEST_DIR = Path(os.environ.get("MANIFEST_DIR", "src/manifests/mlflow"))
TARGET_NS = os.environ.get("TARGET_NS", "mlflow")
POSTGRES_NS = os.environ.get("POSTGRES_NS", "default")

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()

CNPG_CLUSTER = os.environ.get("CNPG_CLUSTER", "postgres-cluster")
POOLER_SERVICE = os.environ.get("POOLER_SERVICE", "postgres-pooler")
DB_ACCESS_MODE = os.environ.get("DB_ACCESS_MODE", "pooler").strip().lower()

DB_HOST = os.environ.get("DB_HOST", "")
DB_NAME = os.environ.get("DB_NAME", "mlflow")

DB_SECRET_NAMESPACE = os.environ.get("DB_SECRET_NAMESPACE", POSTGRES_NS)
DB_USERNAME_KEY = os.environ.get("DB_USERNAME_KEY", "username")  # which field in the secret contains the DB username
DB_PASSWORD_KEY = os.environ.get("DB_PASSWORD_KEY", "password")  # which field in the secret contains the DB password
DB_PORT_KEY = os.environ.get("DB_PORT_KEY", "port")

MLFLOW_NAME = os.environ.get("MLFLOW_NAME", "mlflow")
MLFLOW_SERVICE = os.environ.get("MLFLOW_SERVICE", "mlflow")
SERVICE_ACCOUNT_NAME = os.environ.get("SERVICE_ACCOUNT_NAME", MLFLOW_NAME)

MLFLOW_PORT = int(os.environ.get("MLFLOW_PORT", "5000"))
MLFLOW_IMAGE = os.environ.get(
    "MLFLOW_IMAGE",
    "ghcr.io/athithya-sakthivel/mlflow:2026-04-08-05-42--36c7d29@sha256:8415ae125e0b2ea8185c4c98d07d96c970ee54af1859b59becd48ac5879cacfa",
).strip()

MLFLOW_WORKERS = int(os.environ.get("MLFLOW_WORKERS", "1"))
MLFLOW_SECRETS_CACHE_TTL = int(os.environ.get("MLFLOW_SECRETS_CACHE_TTL", "30"))
MLFLOW_SECRETS_CACHE_MAX_SIZE = int(os.environ.get("MLFLOW_SECRETS_CACHE_MAX_SIZE", "128"))

MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE = os.environ.get(
    "MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE",
    "false",
).strip().lower() in {"1", "true", "yes", "y", "on"}

MLFLOW_SERVER_ENABLE_JOB_EXECUTION = os.environ.get(
    "MLFLOW_SERVER_ENABLE_JOB_EXECUTION",
    "false",
).strip().lower() in {"1", "true", "yes", "y", "on"}

MLFLOW_DB_AUTO_MIGRATE_ON_START = os.environ.get(
    "MLFLOW_DB_AUTO_MIGRATE_ON_START",
    "false",
).strip().lower() in {"1", "true", "yes", "y", "on"}

MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS = int(os.environ.get("MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS", "900"))

MLFLOW_SERVER_ALLOWED_HOSTS = os.environ.get("MLFLOW_SERVER_ALLOWED_HOSTS", "").strip()
MLFLOW_SERVER_CORS_ALLOWED_ORIGINS = os.environ.get("MLFLOW_SERVER_CORS_ALLOWED_ORIGINS", "").strip()
MLFLOW_SERVER_X_FRAME_OPTIONS = os.environ.get("MLFLOW_SERVER_X_FRAME_OPTIONS", "SAMEORIGIN").strip().upper()

READY_TIMEOUT = int(os.environ.get("READY_TIMEOUT", "1200"))
HTTP_READY_TIMEOUT = int(os.environ.get("HTTP_READY_TIMEOUT", "300"))
HTTP_READY_POLL_INTERVAL = int(os.environ.get("HTTP_READY_POLL_INTERVAL", "5"))

MLFLOW_S3_BUCKET = os.environ.get("MLFLOW_S3_BUCKET", "e2e-mlops-data-681802563986").strip()
MLFLOW_S3_PREFIX = os.environ.get("MLFLOW_S3_PREFIX", "").strip()
MLFLOW_S3_ENDPOINT_URL = os.environ.get("MLFLOW_S3_ENDPOINT_URL", "").strip()

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "")
AWS_ROLE_ARN = os.environ.get("AWS_ROLE_ARN", "")
MLFLOW_USE_IAM = os.environ.get("MLFLOW_USE_IAM", os.environ.get("USE_IAM", "false")).lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

DB_AUTH_SECRET_NAME = os.environ.get("DB_AUTH_SECRET_NAME", "mlflow-db-auth")
S3_AUTH_SECRET_NAME = os.environ.get("S3_AUTH_SECRET_NAME", "mlflow-s3-auth")

MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

APP_DB_USER = ""
APP_DB_PASSWORD = ""
RESOLVED_DB_PORT = ""

if K8S_CLUSTER == "kind":
    MLFLOW_CPU_REQUEST = "200m"
    MLFLOW_CPU_LIMIT = "800m"
    MLFLOW_MEM_REQUEST = "512Mi"
    MLFLOW_MEM_LIMIT = "1Gi"
else:
    MLFLOW_CPU_REQUEST = "500m"
    MLFLOW_CPU_LIMIT = "1500m"
    MLFLOW_MEM_REQUEST = "1Gi"
    MLFLOW_MEM_LIMIT = "1.5Gi"

MLFLOW_REPLICAS = int(os.environ.get("MLFLOW_REPLICAS", "1"))
MLFLOW_MIN_READY_SECONDS = int(os.environ.get("MLFLOW_MIN_READY_SECONDS", "10"))


def ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(f"[{ts()}] [mlflow] {msg}", file=sys.stderr, flush=True)


def fatal(msg: str) -> None:
    print(f"[{ts()}] [mlflow][FATAL] {msg}", file=sys.stderr, flush=True)
    raise SystemExit(1)


def require_bin(name: str) -> None:
    if shutil.which(name) is None:
        fatal(f"{name} required in PATH")


def run_live(cmd: list[str], *, input_text: str | None = None) -> None:
    log(f"running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, input=input_text, text=True)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def run_text(cmd: list[str], *, input_text: str | None = None, check: bool = True) -> str:
    proc = subprocess.run(cmd, input=input_text, text=True, capture_output=True)
    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr)
    return proc.stdout.strip()


def yaml_dump(obj: dict[str, Any]) -> str:
    return yaml.safe_dump(obj, sort_keys=False, default_flow_style=False, width=120)


def apply_manifest(obj: dict[str, Any], filename: str, *, persist: bool = True) -> Path | None:
    rendered = yaml_dump(obj)
    if persist:
        path = MANIFEST_DIR / filename
        path.write_text(rendered, encoding="utf-8")
        log(f"applying {filename} from {path}")
        run_live(["kubectl", "apply", "-f", str(path)])
        return path

    log(f"applying {filename} from stdin")
    run_live(["kubectl", "apply", "-f", "-"], input_text=rendered)
    return None


def ensure_namespace(namespace: str) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": namespace,
                "labels": {"app.kubernetes.io/part-of": "mlflow"},
            },
        },
        "namespace.yaml",
    )


def ensure_secret(namespace: str, name: str, string_data: dict[str, str], filename: str) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {"app.kubernetes.io/part-of": "mlflow"},
            },
            "type": "Opaque",
            "stringData": string_data,
        },
        filename,
        persist=False,
    )


def find_app_secret_name() -> str:
    selector = f"cnpg.io/cluster={CNPG_CLUSTER},cnpg.io/userType=app"
    try:
        secret_name = run_text(
            [
                "kubectl",
                "-n",
                DB_SECRET_NAMESPACE,
                "get",
                "secret",
                "-l",
                selector,
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ]
        )
        if secret_name:
            return secret_name
    except subprocess.CalledProcessError:
        pass

    fallback = f"{CNPG_CLUSTER}-app"
    try:
        run_text(["kubectl", "-n", DB_SECRET_NAMESPACE, "get", "secret", fallback])
        return fallback
    except subprocess.CalledProcessError:
        return ""


def secret_value(namespace: str, secret_name: str, key: str) -> str:
    try:
        raw = run_text(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "secret",
                secret_name,
                "-o",
                f"jsonpath={{.data.{key}}}",
            ]
        )
    except subprocess.CalledProcessError:
        return ""

    if not raw:
        return ""

    with suppress(Exception):
        return base64.b64decode(raw).decode("utf-8")
    return ""


def resolve_db_identity() -> tuple[str, str, str]:
    app_secret = find_app_secret_name()
    if not app_secret:
        fatal(f"CNPG app secret not found for cluster {CNPG_CLUSTER}")

    user = secret_value(DB_SECRET_NAMESPACE, app_secret, DB_USERNAME_KEY)
    password = secret_value(DB_SECRET_NAMESPACE, app_secret, DB_PASSWORD_KEY)
    port = secret_value(DB_SECRET_NAMESPACE, app_secret, DB_PORT_KEY) or "5432"

    if not user:
        fatal(f"username missing from source secret {DB_SECRET_NAMESPACE}/{app_secret} key {DB_USERNAME_KEY}")
    if not password:
        fatal(f"password missing from source secret {DB_SECRET_NAMESPACE}/{app_secret} key {DB_PASSWORD_KEY}")

    return user, password, port


def resolve_db_host() -> str:
    global DB_HOST
    if DB_HOST:
        return DB_HOST
    if DB_ACCESS_MODE == "pooler":
        DB_HOST = f"{POOLER_SERVICE}.{POSTGRES_NS}.svc.cluster.local"
    elif DB_ACCESS_MODE == "rw":
        DB_HOST = f"{CNPG_CLUSTER}-rw.{POSTGRES_NS}.svc.cluster.local"
    else:
        fatal(f"unsupported DB_ACCESS_MODE={DB_ACCESS_MODE}; use pooler or rw")
    return DB_HOST


def resolve_allowed_hosts() -> str:
    global MLFLOW_SERVER_ALLOWED_HOSTS
    if MLFLOW_SERVER_ALLOWED_HOSTS:
        return MLFLOW_SERVER_ALLOWED_HOSTS

    service_fqdn = f"{MLFLOW_SERVICE}.{TARGET_NS}.svc.cluster.local"
    service_short = f"{MLFLOW_SERVICE}.{TARGET_NS}.svc"
    MLFLOW_SERVER_ALLOWED_HOSTS = ",".join(
        [
            "localhost:*",
            "127.0.0.1:*",
            "[::1]:*",
            MLFLOW_SERVICE,
            f"{MLFLOW_SERVICE}:{MLFLOW_PORT}",
            service_short,
            f"{service_short}:{MLFLOW_PORT}",
            service_fqdn,
            f"{service_fqdn}:{MLFLOW_PORT}",
        ]
    )
    return MLFLOW_SERVER_ALLOWED_HOSTS


def validate_x_frame_options() -> str:
    if MLFLOW_SERVER_X_FRAME_OPTIONS not in {"SAMEORIGIN", "DENY", "NONE"}:
        fatal("MLFLOW_SERVER_X_FRAME_OPTIONS must be one of SAMEORIGIN, DENY, or NONE")
    return MLFLOW_SERVER_X_FRAME_OPTIONS


def artifact_destination() -> str:
    if not MLFLOW_S3_BUCKET:
        fatal("MLFLOW_S3_BUCKET is required; local fallback has been removed")
    prefix = MLFLOW_S3_PREFIX.strip("/")
    return f"s3://{MLFLOW_S3_BUCKET}/{prefix}" if prefix else f"s3://{MLFLOW_S3_BUCKET}"


def backend_uri_masked() -> str:
    if not APP_DB_USER or not DB_HOST or not RESOLVED_DB_PORT:
        return "postgresql://<resolved>:*****@<resolved>/mlflow"
    return f"postgresql://{APP_DB_USER}:*****@{DB_HOST}:{RESOLVED_DB_PORT}/{DB_NAME}"


def ensure_db_auth_secret() -> None:
    ensure_secret(TARGET_NS, DB_AUTH_SECRET_NAME, {"password": APP_DB_PASSWORD}, "db-auth-secret.yaml")


def ensure_s3_auth_secret() -> None:
    if MLFLOW_USE_IAM:
        return
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        fatal("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when MLFLOW_USE_IAM=false")

    data = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    }
    if AWS_SESSION_TOKEN:
        data["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN

    ensure_secret(TARGET_NS, S3_AUTH_SECRET_NAME, data, "s3-auth-secret.yaml")


def ensure_service_account() -> None:
    annotations: dict[str, str] = {}
    if MLFLOW_USE_IAM:
        if not AWS_ROLE_ARN:
            fatal("AWS_ROLE_ARN is required when MLFLOW_USE_IAM=true")
        annotations["eks.amazonaws.com/role-arn"] = AWS_ROLE_ARN

    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": SERVICE_ACCOUNT_NAME,
                "namespace": TARGET_NS,
                "labels": {"app.kubernetes.io/name": MLFLOW_NAME},
                "annotations": annotations,
            },
        },
        "serviceaccount.yaml",
    )


def build_common_env() -> list[dict[str, Any]]:
    env: list[dict[str, Any]] = [
        {"name": "DB_HOST", "value": DB_HOST},
        {"name": "DB_PORT", "value": RESOLVED_DB_PORT},
        {"name": "DB_NAME", "value": DB_NAME},
        {"name": "DB_USER", "value": APP_DB_USER},
        {"name": "DB_PASSWORD", "valueFrom": {"secretKeyRef": {"name": DB_AUTH_SECRET_NAME, "key": "password"}}},
        {"name": "MLFLOW_PORT", "value": str(MLFLOW_PORT)},
        {"name": "MLFLOW_WORKERS", "value": str(MLFLOW_WORKERS)},
        {"name": "MLFLOW_SECRETS_CACHE_TTL", "value": str(MLFLOW_SECRETS_CACHE_TTL)},
        {"name": "MLFLOW_SECRETS_CACHE_MAX_SIZE", "value": str(MLFLOW_SECRETS_CACHE_MAX_SIZE)},
        {"name": "MLFLOW_DB_AUTO_MIGRATE_ON_START", "value": "true" if MLFLOW_DB_AUTO_MIGRATE_ON_START else "false"},
        {"name": "MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS", "value": str(MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS)},
        {"name": "MLFLOW_ARTIFACTS_DESTINATION", "value": artifact_destination()},
        {
            "name": "MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE",
            "value": "true" if MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE else "false",
        },
        {
            "name": "MLFLOW_SERVER_ENABLE_JOB_EXECUTION",
            "value": "true" if MLFLOW_SERVER_ENABLE_JOB_EXECUTION else "false",
        },
        {"name": "MLFLOW_SERVER_ALLOWED_HOSTS", "value": resolve_allowed_hosts()},
        {"name": "MLFLOW_SERVER_X_FRAME_OPTIONS", "value": validate_x_frame_options()},
        {"name": "AWS_REGION", "value": AWS_REGION},
        {"name": "AWS_DEFAULT_REGION", "value": AWS_REGION},
        {"name": "HOME", "value": "/tmp"},
        {"name": "TMPDIR", "value": "/tmp"},
        {"name": "XDG_CACHE_HOME", "value": "/tmp/.cache"},
    ]

    if MLFLOW_SERVER_CORS_ALLOWED_ORIGINS:
        env.append({"name": "MLFLOW_SERVER_CORS_ALLOWED_ORIGINS", "value": MLFLOW_SERVER_CORS_ALLOWED_ORIGINS})

    if MLFLOW_S3_BUCKET:
        env.append({"name": "MLFLOW_S3_BUCKET", "value": MLFLOW_S3_BUCKET})
        env.append({"name": "MLFLOW_S3_PREFIX", "value": MLFLOW_S3_PREFIX})
        if MLFLOW_S3_ENDPOINT_URL:
            env.append({"name": "MLFLOW_S3_ENDPOINT_URL", "value": MLFLOW_S3_ENDPOINT_URL})

        if MLFLOW_USE_IAM:
            env.append({"name": "AWS_ROLE_ARN", "value": AWS_ROLE_ARN})
        else:
            env.extend(
                [
                    {
                        "name": "AWS_ACCESS_KEY_ID",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": S3_AUTH_SECRET_NAME,
                                "key": "AWS_ACCESS_KEY_ID",
                            }
                        },
                    },
                    {
                        "name": "AWS_SECRET_ACCESS_KEY",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": S3_AUTH_SECRET_NAME,
                                "key": "AWS_SECRET_ACCESS_KEY",
                            }
                        },
                    },
                ]
            )
            if AWS_SESSION_TOKEN:
                env.append(
                    {
                        "name": "AWS_SESSION_TOKEN",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": S3_AUTH_SECRET_NAME,
                                "key": "AWS_SESSION_TOKEN",
                            }
                        },
                    }
                )

    return env


def server_cli_args() -> list[str]:
    args = [
        '--host 0.0.0.0',
        '--port "${MLFLOW_PORT}"',
    ]

    if MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE:
        args.append('--disable-security-middleware')

    args.append('--allowed-hosts "${MLFLOW_SERVER_ALLOWED_HOSTS}"')

    if MLFLOW_SERVER_CORS_ALLOWED_ORIGINS:
        args.append('--cors-allowed-origins "${MLFLOW_SERVER_CORS_ALLOWED_ORIGINS}"')

    args.extend(
        [
            '--x-frame-options "${MLFLOW_SERVER_X_FRAME_OPTIONS}"',
            '--backend-store-uri "${BACKEND_URI}"',
            '--registry-store-uri "${BACKEND_URI}"',
            '--artifacts-destination "${MLFLOW_ARTIFACTS_DESTINATION}"',
            '--serve-artifacts',
            '--workers "${MLFLOW_WORKERS}"',
            '--secrets-cache-ttl "${MLFLOW_SECRETS_CACHE_TTL}"',
            '--secrets-cache-max-size "${MLFLOW_SECRETS_CACHE_MAX_SIZE}"',
        ]
    )
    return args


def migration_script() -> str:
    return r'''if [ "${MLFLOW_DB_AUTO_MIGRATE_ON_START}" = "true" ]; then
  echo "[mlflow][server] running database migration check" >&2
  python3 - <<'PY'
import hashlib
import os
import subprocess
import sys
from contextlib import suppress
from urllib.parse import quote

import psycopg2

def fatal(msg: str) -> None:
    print(f"[mlflow][migrate][FATAL] {msg}", file=sys.stderr, flush=True)
    raise SystemExit(1)

user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
host = os.environ["DB_HOST"]
port = int(os.environ["DB_PORT"])
dbname = os.environ["DB_NAME"]
timeout_seconds = int(os.environ.get("MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS", "900"))

backend_uri = "postgresql://%s:%s@%s:%s/%s" % (
    quote(user, safe=""),
    quote(password, safe=""),
    host,
    port,
    dbname,
)

lock_seed = f"{host}:{port}/{dbname}".encode("utf-8")
digest = hashlib.sha256(lock_seed).digest()
lock_key_a = int.from_bytes(digest[:4], "big", signed=True)
lock_key_b = int.from_bytes(digest[4:8], "big", signed=True)

conn = None
try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s, %s)", (lock_key_a, lock_key_b))
    print("[mlflow][migrate] acquired advisory lock", file=sys.stderr, flush=True)

    cmd = ["mlflow", "db", "upgrade", backend_uri]
    print(f"[mlflow][migrate] running: {' '.join(cmd[:-1])} <masked-uri>", file=sys.stderr, flush=True)
    subprocess.run(cmd, check=True, timeout=timeout_seconds)
    print("[mlflow][migrate] database schema is current", file=sys.stderr, flush=True)
finally:
    if conn is not None:
        with suppress(Exception):
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s, %s)", (lock_key_a, lock_key_b))
        with suppress(Exception):
            conn.close()
PY
fi
'''


def server_script() -> str:
    cli_args = server_cli_args()
    cli_lines = ["exec mlflow server \\"]
    for idx, arg in enumerate(cli_args):
        suffix = " \\" if idx < len(cli_args) - 1 else ""
        cli_lines.append(f"  {arg}{suffix}")
    cli_block = "\n".join(cli_lines)
    migrate_block = migration_script()

    return rf'''set -eu
echo "[mlflow][server] starting" >&2
echo "[mlflow][server] python3=$(python3 --version 2>&1)" >&2
echo "[mlflow][server] mlflow=$(mlflow --version 2>&1)" >&2
echo "[mlflow][server] backend=${{DB_HOST}}:${{DB_PORT}}/${{DB_NAME}}" >&2
echo "[mlflow][server] artifacts=${{MLFLOW_ARTIFACTS_DESTINATION}}" >&2
echo "[mlflow][server] workers=${{MLFLOW_WORKERS}}" >&2
echo "[mlflow][server] disable_security_middleware=${{MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE}}" >&2
echo "[mlflow][server] enable_job_execution=${{MLFLOW_SERVER_ENABLE_JOB_EXECUTION}}" >&2
echo "[mlflow][server] db_auto_migrate_on_start=${{MLFLOW_DB_AUTO_MIGRATE_ON_START}}" >&2
echo "[mlflow][server] db_migrate_timeout_seconds=${{MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS}}" >&2
echo "[mlflow][server] allowed_hosts=${{MLFLOW_SERVER_ALLOWED_HOSTS}}" >&2
echo "[mlflow][server] x_frame_options=${{MLFLOW_SERVER_X_FRAME_OPTIONS}}" >&2
echo "[mlflow][server] secrets_cache_ttl=${{MLFLOW_SECRETS_CACHE_TTL}}" >&2
echo "[mlflow][server] secrets_cache_max_size=${{MLFLOW_SECRETS_CACHE_MAX_SIZE}}" >&2
BACKEND_URI="$(python3 - <<'PY'
import os
from urllib.parse import quote

user = quote(os.environ["DB_USER"], safe="")
password = quote(os.environ["DB_PASSWORD"], safe="")
print(
    "postgresql://%s:%s@%s:%s/%s"
    % (
        user,
        password,
        os.environ["DB_HOST"],
        os.environ["DB_PORT"],
        os.environ["DB_NAME"],
    )
)
PY
)"
echo "[mlflow][server] backend_uri=postgresql://${{DB_USER}}:*****@${{DB_HOST}}:${{DB_PORT}}/${{DB_NAME}}" >&2
{migrate_block}
{cli_block}
'''


def base_pod_security_context() -> dict[str, Any]:
    return {
        "runAsNonRoot": True,
        "runAsUser": 10001,
        "runAsGroup": 10001,
        "fsGroup": 10001,
        "seccompProfile": {"type": "RuntimeDefault"},
    }


def base_container_security_context() -> dict[str, Any]:
    return {
        "runAsNonRoot": True,
        "runAsUser": 10001,
        "runAsGroup": 10001,
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }


def startup_probe() -> dict[str, Any]:
    return {
        "tcpSocket": {"port": "http"},
        "timeoutSeconds": 1,
        "periodSeconds": 5,
        "failureThreshold": 120,
    }


def readiness_probe() -> dict[str, Any]:
    script = "\n".join(
        [
            "python3 - <<'PY'",
            "import sys",
            "from urllib.request import Request, urlopen",
            f'url = "http://127.0.0.1:{MLFLOW_PORT}/"',
            f'host = "{MLFLOW_SERVICE}.{TARGET_NS}.svc.cluster.local:{MLFLOW_PORT}"',
            'req = Request(url, method="GET", headers={"Host": host, "User-Agent": "mlflow-readiness-probe/1.0"})',
            "try:",
            "    with urlopen(req, timeout=5) as resp:",
            "        if 200 <= getattr(resp, 'status', 200) < 400:",
            "            sys.exit(0)",
            "except Exception:",
            "    pass",
            "sys.exit(1)",
            "PY",
        ]
    )
    return {
        "exec": {"command": ["/bin/sh", "-lc", script]},
        "timeoutSeconds": 6,
        "periodSeconds": 5,
        "failureThreshold": 6,
        "successThreshold": 1,
    }


def liveness_probe() -> dict[str, Any]:
    return {
        "tcpSocket": {"port": "http"},
        "timeoutSeconds": 1,
        "periodSeconds": 20,
        "failureThreshold": 6,
    }


def service_labels() -> dict[str, str]:
    return {
        "app.kubernetes.io/name": MLFLOW_NAME,
        "app.kubernetes.io/component": "server",
    }


def pod_template_labels() -> dict[str, str]:
    return service_labels()


def topology_spread_constraints() -> list[dict[str, Any]]:
    return [
        {
            "maxSkew": 1,
            "topologyKey": "kubernetes.io/hostname",
            "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {"matchLabels": pod_template_labels()},
        }
    ]


def deployment_manifest() -> dict[str, Any]:
    checksum_source = {
        "db_host": DB_HOST,
        "db_port": RESOLVED_DB_PORT,
        "db_name": DB_NAME,
        "artifact_destination": artifact_destination(),
        "s3_bucket": MLFLOW_S3_BUCKET,
        "s3_prefix": MLFLOW_S3_PREFIX,
        "s3_endpoint": MLFLOW_S3_ENDPOINT_URL,
        "use_iam": MLFLOW_USE_IAM,
        "image": MLFLOW_IMAGE,
        "access_mode": DB_ACCESS_MODE,
        "replicas": MLFLOW_REPLICAS,
        "min_ready_seconds": MLFLOW_MIN_READY_SECONDS,
        "workers": MLFLOW_WORKERS,
        "security_middleware_disabled": MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE,
        "job_execution_enabled": MLFLOW_SERVER_ENABLE_JOB_EXECUTION,
        "db_auto_migrate_on_start": MLFLOW_DB_AUTO_MIGRATE_ON_START,
        "db_migrate_timeout_seconds": MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS,
        "allowed_hosts": resolve_allowed_hosts(),
        "cors_allowed_origins": MLFLOW_SERVER_CORS_ALLOWED_ORIGINS,
        "x_frame_options": validate_x_frame_options(),
        "secrets_cache_ttl": MLFLOW_SECRETS_CACHE_TTL,
        "secrets_cache_max_size": MLFLOW_SECRETS_CACHE_MAX_SIZE,
        "cpu_request": MLFLOW_CPU_REQUEST,
        "cpu_limit": MLFLOW_CPU_LIMIT,
        "mem_request": MLFLOW_MEM_REQUEST,
        "mem_limit": MLFLOW_MEM_LIMIT,
    }
    checksum = hashlib.sha256(yaml_dump(checksum_source).encode("utf-8")).hexdigest()

    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": MLFLOW_NAME,
            "namespace": TARGET_NS,
            "labels": {
                "app.kubernetes.io/name": MLFLOW_NAME,
                "app.kubernetes.io/component": "server",
            },
        },
        "spec": {
            "replicas": MLFLOW_REPLICAS,
            "minReadySeconds": MLFLOW_MIN_READY_SECONDS,
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxUnavailable": 0,
                    "maxSurge": 1,
                },
            },
            "selector": {
                "matchLabels": pod_template_labels(),
            },
            "template": {
                "metadata": {
                    "labels": pod_template_labels(),
                    "annotations": {"mlflow.io/config-checksum": checksum},
                },
                "spec": {
                    "serviceAccountName": SERVICE_ACCOUNT_NAME,
                    "securityContext": base_pod_security_context(),
                    "topologySpreadConstraints": topology_spread_constraints(),
                    "containers": [
                        {
                            "name": "mlflow",
                            "image": MLFLOW_IMAGE,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/sh", "-lc"],
                            "args": [server_script()],
                            "env": build_common_env(),
                            "ports": [{"name": "http", "containerPort": MLFLOW_PORT, "protocol": "TCP"}],
                            "resources": {
                                "requests": {
                                    "cpu": MLFLOW_CPU_REQUEST,
                                    "memory": MLFLOW_MEM_REQUEST,
                                },
                                "limits": {
                                    "cpu": MLFLOW_CPU_LIMIT,
                                    "memory": MLFLOW_MEM_LIMIT,
                                },
                            },
                            "startupProbe": startup_probe(),
                            "readinessProbe": readiness_probe(),
                            "livenessProbe": liveness_probe(),
                            "securityContext": base_container_security_context(),
                            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
                        }
                    ],
                    "terminationGracePeriodSeconds": 30,
                    "volumes": [{"name": "tmp", "emptyDir": {}}],
                },
            },
        },
    }


def pdb_manifest() -> dict[str, Any]:
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": f"{MLFLOW_NAME}-pdb",
            "namespace": TARGET_NS,
            "labels": {"app.kubernetes.io/name": MLFLOW_NAME},
        },
        "spec": {
            "minAvailable": 1,
            "selector": {
                "matchLabels": pod_template_labels(),
            },
        },
    }


def service_manifest() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": MLFLOW_SERVICE,
            "namespace": TARGET_NS,
            "labels": {"app.kubernetes.io/name": MLFLOW_NAME},
        },
        "spec": {
            "type": "ClusterIP",
            "selector": pod_template_labels(),
            "ports": [
                {
                    "name": "http",
                    "port": MLFLOW_PORT,
                    "targetPort": MLFLOW_PORT,
                    "protocol": "TCP",
                }
            ],
        },
    }


def wait_for_rollout(dep_name: str) -> None:
    log(f"waiting for rollout of deployment/{dep_name}")
    run_live(
        [
            "kubectl",
            "-n",
            TARGET_NS,
            "rollout",
            "status",
            f"deployment/{dep_name}",
            f"--timeout={READY_TIMEOUT}s",
        ]
    )


def _probe_http(url: str) -> tuple[bool, str]:
    req = Request(url, method="GET", headers={"User-Agent": "mlflow-rollout-check/1.0"})
    try:
        with urlopen(req, timeout=5) as resp:
            status = getattr(resp, "status", 200)
            body = resp.read(256).decode("utf-8", errors="replace")
            if 200 <= status < 400:
                return True, f"status={status} body={body[:120]!r}"
            return False, f"status={status} body={body[:120]!r}"
    except Exception as exc:
        return False, str(exc)


def _find_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_for_service_http_ready() -> None:
    start = datetime.now(UTC)
    last_log = start

    while True:
        elapsed = (datetime.now(UTC) - start).total_seconds()
        if elapsed >= HTTP_READY_TIMEOUT:
            fatal(f"timeout waiting for service HTTP readiness on port {MLFLOW_PORT}")

        local_port = _find_free_local_port()
        port_forward = subprocess.Popen(
            [
                "kubectl",
                "-n",
                TARGET_NS,
                "port-forward",
                "--address",
                "127.0.0.1",
                f"svc/{MLFLOW_SERVICE}",
                f"{local_port}:{MLFLOW_PORT}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            while True:
                if port_forward.poll() is not None:
                    stderr = ""
                    with suppress(Exception):
                        stderr = port_forward.stderr.read() if port_forward.stderr else ""
                    log(f"port-forward exited; will retry ({stderr.strip() or 'no stderr'})")
                    break

                ok, detail = _probe_http(f"http://127.0.0.1:{local_port}/")
                if ok:
                    log(f"service HTTP ready on / ({detail})")
                    return

                elapsed = (datetime.now(UTC) - start).total_seconds()
                if elapsed >= HTTP_READY_TIMEOUT:
                    fatal(f"timeout waiting for service HTTP readiness on port {MLFLOW_PORT}")

                if (datetime.now(UTC) - last_log).total_seconds() >= HTTP_READY_POLL_INTERVAL:
                    log(f"waiting for service HTTP readiness ... {detail}")
                    last_log = datetime.now(UTC)

                time.sleep(HTTP_READY_POLL_INTERVAL)
        finally:
            with suppress(Exception):
                port_forward.terminate()
            with suppress(Exception):
                port_forward.wait(timeout=10)


def dump_debug_state() -> None:
    log("dumping debug state")
    commands = [
        ["kubectl", "-n", TARGET_NS, "get", "pods", "-o", "wide"],
        ["kubectl", "-n", TARGET_NS, "get", "svc", "-o", "wide"],
        ["kubectl", "-n", TARGET_NS, "get", "deploy", MLFLOW_NAME, "-o", "wide"],
        ["kubectl", "-n", TARGET_NS, "describe", "deploy", MLFLOW_NAME],
        ["kubectl", "-n", TARGET_NS, "get", "pdb", "-o", "wide"],
        ["kubectl", "-n", TARGET_NS, "get", "rs", "-o", "wide"],
        ["kubectl", "-n", TARGET_NS, "get", "events", "--sort-by=.lastTimestamp"],
        ["kubectl", "-n", TARGET_NS, "logs", f"deployment/{MLFLOW_NAME}", "--all-containers=true", "--tail=200"],
    ]
    for cmd in commands:
        try:
            run_live(cmd)
        except Exception:
            pass


def print_summary() -> None:
    tracking_uri = f"http://{MLFLOW_SERVICE}.{TARGET_NS}.svc.cluster.local:{MLFLOW_PORT}"
    print()
    print(f"Tracking URI: {tracking_uri}")
    print(f"Backend URI: {backend_uri_masked()}")
    print(f"Artifact destination: {artifact_destination()}")
    print(f"DB access mode: {DB_ACCESS_MODE}")
    print(f"Resource profile: {K8S_CLUSTER}")
    print(f"Replicas: {MLFLOW_REPLICAS}")
    print(f"MLFLOW_IMAGE: {MLFLOW_IMAGE}")
    print(f"Workers: {MLFLOW_WORKERS}")
    print(f"Security middleware disabled: {MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE}")
    print(f"Job execution enabled: {MLFLOW_SERVER_ENABLE_JOB_EXECUTION}")
    print(f"DB auto-migrate on start: {MLFLOW_DB_AUTO_MIGRATE_ON_START}")
    print(f"DB migrate timeout seconds: {MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS}")
    print(f"Allowed hosts: {resolve_allowed_hosts()}")
    print(f"X-Frame-Options: {validate_x_frame_options()}")
    if MLFLOW_SERVER_CORS_ALLOWED_ORIGINS:
        print(f"CORS allowed origins: {MLFLOW_SERVER_CORS_ALLOWED_ORIGINS}")
    print(f"Secrets cache TTL: {MLFLOW_SECRETS_CACHE_TTL}")
    print(f"Secrets cache max size: {MLFLOW_SECRETS_CACHE_MAX_SIZE}")
    print()
    print(f"Port-forward: kubectl -n {TARGET_NS} port-forward svc/{MLFLOW_SERVICE} {MLFLOW_PORT}:{MLFLOW_PORT}")
    print(f"MLFLOW_TRACKING_URI=http://127.0.0.1:{MLFLOW_PORT}")


def require_prereqs() -> None:
    log("checking prerequisites")
    require_bin("kubectl")
    require_bin("python3")
    run_live(["kubectl", "cluster-info"])


def main() -> None:
    global APP_DB_USER, APP_DB_PASSWORD, RESOLVED_DB_PORT

    log("starting mlflow rollout")
    log(
        "resource profile: "
        f"replicas={MLFLOW_REPLICAS} "
        f"workers={MLFLOW_WORKERS} "
        f"cpu={MLFLOW_CPU_REQUEST}/{MLFLOW_CPU_LIMIT} "
        f"mem={MLFLOW_MEM_REQUEST}/{MLFLOW_MEM_LIMIT}"
    )
    log(f"configured image: {MLFLOW_IMAGE}")

    require_prereqs()

    if not MLFLOW_IMAGE:
        fatal("MLFLOW_IMAGE is required")
    if "@sha256:" not in MLFLOW_IMAGE:
        fatal("MLFLOW_IMAGE must be digest-pinned and include @sha256:...")
    if not MLFLOW_S3_BUCKET:
        fatal("MLFLOW_S3_BUCKET is required; local fallback has been removed")
    if DB_ACCESS_MODE not in {"pooler", "rw"}:
        fatal("DB_ACCESS_MODE must be pooler or rw")
    if MLFLOW_USE_IAM and not AWS_ROLE_ARN:
        fatal("AWS_ROLE_ARN is required when MLFLOW_USE_IAM=true")
    if not MLFLOW_USE_IAM and (not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY):
        fatal("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when MLFLOW_USE_IAM=false")

    if MLFLOW_REPLICAS > 1 and MLFLOW_SERVER_ENABLE_JOB_EXECUTION:
        fatal("MLFLOW_SERVER_ENABLE_JOB_EXECUTION must be false when MLFLOW_REPLICAS > 1")

    resolve_allowed_hosts()
    validate_x_frame_options()

    ensure_namespace(TARGET_NS)

    log("resolving postgres credentials from cnpg secret")
    APP_DB_USER, APP_DB_PASSWORD, RESOLVED_DB_PORT = resolve_db_identity()
    resolve_db_host()
    log(f"resolved db host={DB_HOST} port={RESOLVED_DB_PORT} user={APP_DB_USER}")

    log("ensuring kubernetes secrets and service account")
    ensure_db_auth_secret()
    if not MLFLOW_USE_IAM:
        ensure_s3_auth_secret()
    ensure_service_account()

    log("rendering manifests")
    apply_manifest(service_manifest(), "service.yaml")
    apply_manifest(pdb_manifest(), "pdb.yaml")
    apply_manifest(deployment_manifest(), "deployment.yaml")

    wait_for_rollout(MLFLOW_NAME)
    wait_for_service_http_ready()

    print_summary()


def delete_all() -> None:
    log("deleting mlflow resources")
    run_live(["kubectl", "-n", TARGET_NS, "delete", "deployment", MLFLOW_NAME, "--ignore-not-found"])
    run_live(["kubectl", "-n", TARGET_NS, "delete", "service", MLFLOW_SERVICE, "--ignore-not-found"])
    run_live(["kubectl", "-n", TARGET_NS, "delete", "poddisruptionbudget", f"{MLFLOW_NAME}-pdb", "--ignore-not-found"])
    run_live(["kubectl", "-n", TARGET_NS, "delete", "secret", DB_AUTH_SECRET_NAME, "--ignore-not-found"])
    run_live(["kubectl", "-n", TARGET_NS, "delete", "secret", S3_AUTH_SECRET_NAME, "--ignore-not-found"])
    run_live(["kubectl", "-n", TARGET_NS, "delete", "serviceaccount", SERVICE_ACCOUNT_NAME, "--ignore-not-found"])


def cli() -> int:
    try:
        if len(sys.argv) == 1 or sys.argv[1] == "--rollout":
            main()
            return 0
        if sys.argv[1] == "--delete":
            require_prereqs()
            delete_all()
            return 0
        if sys.argv[1] in {"--help", "-h"}:
            print(
                "Usage: mlflow_server.py [--rollout|--delete]\n\n"
                "Defaults:\n"
                "  MLFLOW_S3_BUCKET=e2e-mlops-data-681802563986\n"
                "  MLFLOW_IMAGE=ghcr.io/athithya-sakthivel/mlflow:2026-04-03-20-21--861d47a@sha256:9bb811f9af11963e9eecd331cc05fe8dd7a9f683216ec1a36453643fe85e55d7\n\n"
                "Useful env:\n"
                "  MLFLOW_REPLICAS=2\n"
                "  MLFLOW_MIN_READY_SECONDS=10\n"
                "  K8S_CLUSTER=kind|other\n"
                "  MLFLOW_USE_IAM=true|false\n"
                "  AWS_ROLE_ARN=arn:aws:iam::... (required when MLFLOW_USE_IAM=true)\n"
                "  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (required when MLFLOW_USE_IAM=false)\n"
                "  DB_ACCESS_MODE=pooler|rw\n"
                "  POOLER_SERVICE=postgres-pooler\n"
                "  CNPG_CLUSTER=postgres-cluster\n"
                "  MLFLOW_WORKERS=1\n"
                "  MLFLOW_SECRETS_CACHE_TTL=30\n"
                "  MLFLOW_SECRETS_CACHE_MAX_SIZE=128\n"
                "  MLFLOW_DB_AUTO_MIGRATE_ON_START=true|false\n"
                "  MLFLOW_DB_MIGRATE_TIMEOUT_SECONDS=900\n"
                "  MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE=false\n"
                "  MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false\n"
                "  MLFLOW_SERVER_ALLOWED_HOSTS=auto\n"
                "  MLFLOW_SERVER_CORS_ALLOWED_ORIGINS=<optional>\n"
                "  MLFLOW_SERVER_X_FRAME_OPTIONS=SAMEORIGIN|DENY|NONE\n"
            )
            return 0
        fatal(f"unknown option: {sys.argv[1]}")
    except subprocess.CalledProcessError as exc:
        print(f"[{ts()}] [mlflow][FATAL] command failed: {exc.cmd}", file=sys.stderr, flush=True)
        dump_debug_state()
        raise SystemExit(exc.returncode) from exc
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[{ts()}] [mlflow][FATAL] {exc}", file=sys.stderr, flush=True)
        dump_debug_state()
        raise SystemExit(1) from exc


if __name__ == "__main__":
    raise SystemExit(cli())