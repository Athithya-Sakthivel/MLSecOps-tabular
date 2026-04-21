from __future__ import annotations

import argparse
import base64
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import yaml

MANIFEST_DIR = Path("src/manifests/auth")
STATE_DIR = MANIFEST_DIR / ".state"
RENDERED_PATH = STATE_DIR / "rendered.yaml"
HASH_PATH = STATE_DIR / "rendered.sha256"

SUPPORTED_CLUSTERS = {"kind", "eks"}
K8S_CLUSTER_ENV = "K8S_CLUSTER"

APP_NAME = "auth-service"
SERVICE_NAME = "auth-svc"
SERVICE_ACCOUNT_NAME = "auth-sa"
SECRET_NAME = "auth-secrets"
NAMESPACE = "inference"
DEPLOYMENT_NAME = "auth-service"
CONTAINER_PORT = 8000

CNPG_CLUSTER_DEFAULT = "postgres-cluster"
POOLER_SERVICE_DEFAULT = "postgres-pooler"
POSTGRES_NS_DEFAULT = "default"
DB_NAME_DEFAULT = "auth"
DB_ACCESS_MODE_DEFAULT = "pooler"

DEV_ENVIRONMENTS = {"dev", "development", "local", "test"}
VALID_SAMESITE = {"strict", "lax", "none"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

APP_ENV_DEFAULTS: dict[str, str] = {
    "APP_NAME": APP_NAME,
    "SERVICE_NAME": APP_NAME,
    "SERVICE_VERSION": "v1",
    "ENVIRONMENT": "production",
    "DEPLOYMENT_ENVIRONMENT": "production",
    "K8S_CLUSTER_NAME": "production-cluster",
    "APP_BASE_URL": "__REQUIRED_APP_BASE_URL__",
    "APP_HOME_URL": "__REQUIRED_APP_HOME_URL__",
    "AUTH_VALIDATE_ROUTE_PATH": "/validate",
    "AUTH_ME_ROUTE_PATH": "/me",
    "POSTGRES_DSN": "__DERIVED_POSTGRES_DSN__",
    "POSTGRES_MIN_SIZE": "1",
    "POSTGRES_MAX_SIZE": "10",
    "SESSION_COOKIE_NAME": "auth_session",
    "SESSION_TTL_SECONDS": "86400",
    "AUTH_TX_TTL_SECONDS": "600",
    "SESSION_COOKIE_SECURE": "true",
    "SESSION_COOKIE_SAMESITE": "none",
    "SESSION_COOKIE_PATH": "/",
    "SESSION_COOKIE_DOMAIN": "",
    "GOOGLE_CLIENT_ID": "",
    "GOOGLE_CLIENT_SECRET": "",
    "MS_CLIENT_ID": "",
    "MS_CLIENT_SECRET": "",
    "MS_TENANT_ID": "common",
    "GITHUB_CLIENT_ID": "",
    "GITHUB_CLIENT_SECRET": "",
    "GOOGLE_ALLOWED_DOMAINS": "",
    "MICROSOFT_ALLOWED_TENANT_IDS": "",
    "MICROSOFT_ALLOWED_DOMAINS": "",
    "GITHUB_ALLOWED_ORGS": "",
    "METRICS_ENABLED": "true",
    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://signoz-otel-collector.signoz.svc.cluster.local:4317",
    "OTEL_TRACES_SAMPLER": "parentbased_traceidratio",
    "OTEL_TRACES_SAMPLER_ARG": "0.1",
    "OTEL_TIMEOUT_SECONDS": "10.0",
    "OTEL_METRIC_EXPORT_INTERVAL_MS": "15000",
    "OTEL_METRIC_EXPORT_TIMEOUT_MS": "10000",
    "LOG_LEVEL": "INFO",
}

APP_ENV_ORDER = list(APP_ENV_DEFAULTS.keys())

SECRET_ENV_NAMES = {
    "POSTGRES_DSN",
    "GOOGLE_CLIENT_SECRET",
    "MS_CLIENT_SECRET",
    "GITHUB_CLIENT_SECRET",
}

PROVIDER_PAIRS: dict[str, tuple[str, str]] = {
    "google": ("GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET"),
    "microsoft": ("MS_CLIENT_ID", "MS_CLIENT_SECRET"),
    "github": ("GITHUB_CLIENT_ID", "GITHUB_CLIENT_SECRET"),
}


@dataclass(frozen=True, slots=True)
class DeploymentSettings:
    cluster: str
    namespace: str
    deployment_name: str
    service_name: str
    service_account_name: str
    secret_name: str
    image: str
    replicas: int
    cpu_request: str
    cpu_limit: str
    memory_request: str
    memory_limit: str
    image_pull_policy: str
    run_as_user: int
    run_as_group: int
    fs_group: int
    app_port: int
    enable_pdb: bool

    cnpg_cluster: str
    postgres_ns: str
    pooler_service: str
    db_access_mode: str
    db_name: str
    db_secret_namespace: str
    db_username_key: str
    db_password_key: str
    db_port_key: str
    db_host_override: str

    def __post_init__(self) -> None:
        if self.cluster not in SUPPORTED_CLUSTERS:
            raise RuntimeError(
                f"{K8S_CLUSTER_ENV} must be one of {sorted(SUPPORTED_CLUSTERS)}, got {self.cluster!r}"
            )

        for field_name, value in (
            ("namespace", self.namespace),
            ("deployment_name", self.deployment_name),
            ("service_name", self.service_name),
            ("service_account_name", self.service_account_name),
            ("secret_name", self.secret_name),
            ("image", self.image),
            ("cpu_request", self.cpu_request),
            ("cpu_limit", self.cpu_limit),
            ("memory_request", self.memory_request),
            ("memory_limit", self.memory_limit),
            ("image_pull_policy", self.image_pull_policy),
            ("cnpg_cluster", self.cnpg_cluster),
            ("postgres_ns", self.postgres_ns),
            ("pooler_service", self.pooler_service),
            ("db_access_mode", self.db_access_mode),
            ("db_name", self.db_name),
            ("db_secret_namespace", self.db_secret_namespace),
            ("db_username_key", self.db_username_key),
            ("db_password_key", self.db_password_key),
            ("db_port_key", self.db_port_key),
        ):
            if not value.strip():
                raise RuntimeError(f"{field_name} must not be empty")

        if self.replicas < 1:
            raise RuntimeError("AUTH_REPLICAS must be >= 1")
        if self.run_as_user < 1:
            raise RuntimeError("RUN_AS_USER must be >= 1")
        if self.run_as_group < 1:
            raise RuntimeError("RUN_AS_GROUP must be >= 1")
        if self.fs_group < 1:
            raise RuntimeError("FS_GROUP must be >= 1")
        if self.app_port < 1 or self.app_port > 65535:
            raise RuntimeError("AUTH_PORT must be a valid TCP port")
        if self.db_access_mode not in {"pooler", "rw"}:
            raise RuntimeError("DB_ACCESS_MODE must be one of: pooler, rw")
        if "@sha256:" not in self.image:
            raise RuntimeError("AUTH_IMAGE must be digest-pinned and include @sha256:...")


def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip()
    return value if value else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw.strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be numeric, got {raw!r}") from exc


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_list(name: str, default: list[str] | None = None, sep: str = ",") -> list[str]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return list(default or [])
    return [part.strip() for part in raw.split(sep) if part.strip()]


def _validate_log_level(value: str) -> str:
    normalized = value.strip().upper()
    if normalized == "WARN":
        normalized = "WARNING"
    if not normalized:
        return "WARNING"
    if normalized not in VALID_LOG_LEVELS:
        raise RuntimeError(f"LOG_LEVEL must be one of {sorted(VALID_LOG_LEVELS)}, got {value!r}")
    return normalized


def _validate_path(name: str, value: str) -> None:
    if not value:
        raise RuntimeError(f"{name} must not be empty")
    if not value.startswith("/"):
        raise RuntimeError(f"{name} must start with '/'")
    if any(ch.isspace() for ch in value):
        raise RuntimeError(f"{name} must not contain whitespace")


def _validate_domain_token(name: str, value: str) -> None:
    token = value.strip()
    if not token:
        raise RuntimeError(f"{name} must not be empty")
    if any(ch.isspace() for ch in token):
        raise RuntimeError(f"{name} must not contain whitespace")
    if "://" in token or "/" in token:
        raise RuntimeError(f"{name} must be a bare host/domain token")


def _normalize_samesite(raw: str) -> str:
    value = raw.strip().lower()
    if value not in VALID_SAMESITE:
        raise RuntimeError("SESSION_COOKIE_SAMESITE must be one of: strict, lax, none")
    return value


def _require_absolute_url(name: str, value: str) -> None:
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError(f"{name} must be an absolute URL")


def _cluster() -> str:
    raw = os.getenv(K8S_CLUSTER_ENV, "kind").strip().lower()
    if raw not in SUPPORTED_CLUSTERS:
        raise RuntimeError(f"{K8S_CLUSTER_ENV} must be one of {sorted(SUPPORTED_CLUSTERS)}, got {raw!r}")
    return raw


def _default_dev_mode(environment: str) -> bool:
    return environment in DEV_ENVIRONMENTS


def _derive_app_base_url(environment: str) -> str:
    explicit = _env_str("APP_BASE_URL", "").strip().rstrip("/")
    if explicit:
        return explicit

    if _default_dev_mode(environment):
        return "http://127.0.0.1:8001"

    domain = _env_str("DOMAIN", "").strip().rstrip(".")
    if domain:
        return f"https://auth.api.{domain}"

    raise RuntimeError("APP_BASE_URL is required")


def _enabled_providers(env: dict[str, str]) -> list[str]:
    providers: list[str] = []
    for _provider, (client_id_name, client_secret_name) in PROVIDER_PAIRS.items():
        if env.get(client_id_name, "").strip() and env.get(client_secret_name, "").strip():
            providers.append(_provider)
    return providers


def _validate_provider_pairs(env: dict[str, str], dev_mode: bool) -> None:
    for _provider, (client_id_name, client_secret_name) in PROVIDER_PAIRS.items():
        client_id = env.get(client_id_name, "").strip()
        client_secret = env.get(client_secret_name, "").strip()
        if any((client_id, client_secret)) and not all((client_id, client_secret)):
            raise RuntimeError(f"{client_id_name} and {client_secret_name} must both be set")

    if not dev_mode and not _enabled_providers(env):
        raise RuntimeError("At least one OAuth provider must be configured in production")


def load_deployment_settings() -> DeploymentSettings:
    cluster = _cluster()
    return DeploymentSettings(
        cluster=cluster,
        namespace=_env_str("NAMESPACE", NAMESPACE),
        deployment_name=_env_str("AUTH_DEPLOYMENT_NAME", DEPLOYMENT_NAME),
        service_name=_env_str("AUTH_SERVICE_NAME", SERVICE_NAME),
        service_account_name=_env_str("SERVICE_ACCOUNT_NAME", SERVICE_ACCOUNT_NAME),
        secret_name=_env_str("AUTH_SECRET_NAME", SECRET_NAME),
        image=_env_str("AUTH_IMAGE", "ghcr.io/athithya-sakthivel/auth-service:2026-04-21-17-45--e626ab3@sha256:b90ca4571d80bc1f7165ef321909dd6edaaf3b779d0cd4d0d2ea39c820925631"),
        replicas=_env_int("AUTH_REPLICAS", 1),
        cpu_request=_env_str("AUTH_CPU_REQUEST", "500m"),
        cpu_limit=_env_str("AUTH_CPU_LIMIT", "1000m"),
        memory_request=_env_str("AUTH_MEMORY_REQUEST", "512Mi"),
        memory_limit=_env_str("AUTH_MEMORY_LIMIT", "1024Mi"),
        image_pull_policy=_env_str("AUTH_IMAGE_PULL_POLICY", "IfNotPresent"),
        run_as_user=_env_int("RUN_AS_USER", 1000),
        run_as_group=_env_int("RUN_AS_GROUP", 1000),
        fs_group=_env_int("FS_GROUP", 1000),
        app_port=_env_int("AUTH_PORT", CONTAINER_PORT),
        enable_pdb=_env_bool("AUTH_ENABLE_PDB", True),
        cnpg_cluster=_env_str("CNPG_CLUSTER", CNPG_CLUSTER_DEFAULT),
        postgres_ns=_env_str("POSTGRES_NS", POSTGRES_NS_DEFAULT),
        pooler_service=_env_str("POOLER_SERVICE", POOLER_SERVICE_DEFAULT),
        db_access_mode=_env_str("DB_ACCESS_MODE", DB_ACCESS_MODE_DEFAULT).lower(),
        db_name=_env_str("DB_NAME", DB_NAME_DEFAULT),
        db_secret_namespace=_env_str("DB_SECRET_NAMESPACE", _env_str("POSTGRES_NS", POSTGRES_NS_DEFAULT)),
        db_username_key=_env_str("DB_USERNAME_KEY", "username"),
        db_password_key=_env_str("DB_PASSWORD_KEY", "password"),
        db_port_key=_env_str("DB_PORT_KEY", "port"),
        db_host_override=_env_str("DB_HOST", ""),
    )


def _base_labels(settings: DeploymentSettings) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": settings.deployment_name,
        "app.kubernetes.io/managed-by": "auth-service",
        "app.kubernetes.io/component": "auth",
        "app.kubernetes.io/part-of": settings.service_name,
    }


def _pod_labels(settings: DeploymentSettings) -> dict[str, str]:
    labels = _base_labels(settings).copy()
    labels["app.kubernetes.io/version"] = _env_str("SERVICE_VERSION", APP_ENV_DEFAULTS["SERVICE_VERSION"])
    return labels


def _node_selector_for_cluster(settings: DeploymentSettings) -> dict[str, str] | None:
    if settings.cluster == "eks":
        return {"node-type": "general"}
    return None


def _tolerations_for_cluster(settings: DeploymentSettings) -> list[dict[str, Any]]:
    if settings.cluster == "eks":
        return [
            {
                "key": "node-type",
                "operator": "Equal",
                "value": "general",
                "effect": "NoSchedule",
            }
        ]

    return [
        {
            "key": "node-role.kubernetes.io/control-plane",
            "operator": "Exists",
            "effect": "NoSchedule",
        },
        {
            "key": "node-role.kubernetes.io/master",
            "operator": "Exists",
            "effect": "NoSchedule",
        },
    ]


def _topology_spread_constraints(settings: DeploymentSettings) -> list[dict[str, Any]]:
    return [
        {
            "maxSkew": 1,
            "topologyKey": "kubernetes.io/hostname",
            "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {
                "matchLabels": {
                    "app.kubernetes.io/name": settings.deployment_name,
                    "app.kubernetes.io/component": "auth",
                }
            },
        }
    ]


def _shared_volumes() -> list[dict[str, Any]]:
    return [{"name": "tmp", "emptyDir": {}}]


def _shared_mounts() -> list[dict[str, Any]]:
    return [{"name": "tmp", "mountPath": "/tmp"}]


def _container_security_context() -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }


def _pod_security_context(settings: DeploymentSettings) -> dict[str, Any]:
    return {
        "runAsNonRoot": True,
        "runAsUser": settings.run_as_user,
        "runAsGroup": settings.run_as_group,
        "fsGroup": settings.fs_group,
        "seccompProfile": {"type": "RuntimeDefault"},
    }


def _http_probe(
    path: str,
    port: int,
    timeout_seconds: int,
    period_seconds: int,
    failure_threshold: int,
) -> dict[str, Any]:
    return {
        "httpGet": {"path": path, "port": port, "scheme": "HTTP"},
        "initialDelaySeconds": 0,
        "timeoutSeconds": timeout_seconds,
        "periodSeconds": period_seconds,
        "failureThreshold": failure_threshold,
        "successThreshold": 1,
    }


def _secret_ref_env(name: str, secret_name: str) -> dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {
            "secretKeyRef": {
                "name": secret_name,
                "key": name,
            }
        },
    }


def _resolve_db_host(settings: DeploymentSettings) -> str:
    if settings.db_host_override:
        return settings.db_host_override

    if settings.db_access_mode == "pooler":
        return f"{settings.pooler_service}.{settings.postgres_ns}.svc.cluster.local"

    if settings.db_access_mode == "rw":
        return f"{settings.cnpg_cluster}-rw.{settings.postgres_ns}.svc.cluster.local"

    raise RuntimeError("DB_ACCESS_MODE must be one of: pooler, rw")


def require_bin(name: str) -> None:
    if shutil.which(name) is None:
        raise RuntimeError(f"{name} required in PATH")


def ts() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(f"[{ts()}] [auth] {msg}", file=sys.stderr, flush=True)


def fatal(msg: str) -> None:
    print(f"[{ts()}] [auth][FATAL] {msg}", file=sys.stderr, flush=True)
    raise SystemExit(1)


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


def ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def write_state(rendered_yaml: str, digest: str) -> None:
    ensure_state_dir()
    RENDERED_PATH.write_text(rendered_yaml, encoding="utf-8")
    HASH_PATH.write_text(digest + "\n", encoding="utf-8")


def write_temp_yaml(text: str) -> Path:
    tmp = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    try:
        tmp.write(text)
        tmp.flush()
        return Path(tmp.name)
    finally:
        tmp.close()


def kubectl_apply(path: Path) -> None:
    subprocess.run(["kubectl", "apply", "-f", str(path)], check=True)


def kubectl_delete(path: Path) -> None:
    subprocess.run(["kubectl", "delete", "-f", str(path), "--ignore-not-found"], check=True)


def apply_rendered_manifest(doc: dict[str, Any], filename: str) -> None:
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    path = MANIFEST_DIR / filename
    path.write_text(yaml_dump(doc), encoding="utf-8")
    kind = doc.get("kind", "Unknown")
    name = doc.get("metadata", {}).get("name", "unknown")
    namespace = doc.get("metadata", {}).get("namespace", NAMESPACE)
    log(f"applying persisted {kind}/{name} in namespace={namespace} from {path}")
    kubectl_apply(path)


def apply_ephemeral_manifest(doc: dict[str, Any]) -> None:
    rendered = yaml_dump(doc)
    path = write_temp_yaml(rendered)
    kind = doc.get("kind", "Unknown")
    name = doc.get("metadata", {}).get("name", "unknown")
    namespace = doc.get("metadata", {}).get("namespace", NAMESPACE)
    try:
        log(f"applying ephemeral {kind}/{name} in namespace={namespace}")
        kubectl_apply(path)
    finally:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass


def delete_doc(doc: dict[str, Any]) -> None:
    path = write_temp_yaml(yaml_dump(doc))
    try:
        kubectl_delete(path)
    finally:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass


def find_app_secret_name(settings: DeploymentSettings) -> str:
    selector = f"cnpg.io/cluster={settings.cnpg_cluster},cnpg.io/userType=app"
    log(
        "searching CNPG app secret "
        f"namespace={settings.db_secret_namespace} selector={selector}"
    )
    try:
        secret_name = run_text(
            [
                "kubectl",
                "-n",
                settings.db_secret_namespace,
                "get",
                "secret",
                "-l",
                selector,
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ]
        )
        if secret_name:
            log(f"using CNPG app secret {settings.db_secret_namespace}/{secret_name}")
            return secret_name
    except subprocess.CalledProcessError:
        pass

    fallback = f"{settings.cnpg_cluster}-app"
    log(f"trying CNPG fallback app secret {settings.db_secret_namespace}/{fallback}")
    try:
        run_text(["kubectl", "-n", settings.db_secret_namespace, "get", "secret", fallback])
        log(f"using CNPG fallback app secret {settings.db_secret_namespace}/{fallback}")
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

    try:
        return base64.b64decode(raw).decode("utf-8")
    except Exception:
        return ""


def resolve_db_identity(settings: DeploymentSettings) -> tuple[str, str, str]:
    app_secret = find_app_secret_name(settings)
    if not app_secret:
        fatal(f"CNPG app secret not found for cluster {settings.cnpg_cluster}")

    log(
        "resolving DB identity from CNPG app secret "
        f"namespace={settings.db_secret_namespace} secret={app_secret}"
    )

    user = secret_value(settings.db_secret_namespace, app_secret, settings.db_username_key)
    password = secret_value(settings.db_secret_namespace, app_secret, settings.db_password_key)
    port = secret_value(settings.db_secret_namespace, app_secret, settings.db_port_key) or "5432"

    if not user:
        fatal(
            f"username missing from source secret "
            f"{settings.db_secret_namespace}/{app_secret} key={settings.db_username_key}"
        )
    if not password:
        fatal(
            f"password missing from source secret "
            f"{settings.db_secret_namespace}/{app_secret} key={settings.db_password_key}"
        )

    log(f"resolved DB port={port} from CNPG app secret")
    log("resolved DB username/password from CNPG app secret (redacted)")
    return user, password, port


def resolve_postgres_dsn(settings: DeploymentSettings) -> str:
    explicit = _env_str("POSTGRES_DSN", "").strip()
    if explicit:
        log("using explicit POSTGRES_DSN override from environment")
        return explicit

    log("POSTGRES_DSN not set explicitly; deriving from CNPG app secret")
    user, password, port = resolve_db_identity(settings)
    host = _resolve_db_host(settings)
    log(f"derived DB endpoint host={host} port={port} db={settings.db_name} mode={settings.db_access_mode}")
    return f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/{settings.db_name}"


def _validate_runtime_app_env(app_env: dict[str, str]) -> None:
    if not app_env["APP_BASE_URL"].strip():
        raise RuntimeError("APP_BASE_URL is required")
    _require_absolute_url("APP_BASE_URL", app_env["APP_BASE_URL"])

    deployment_environment = app_env["DEPLOYMENT_ENVIRONMENT"].strip().lower()
    if deployment_environment not in DEV_ENVIRONMENTS and not app_env["APP_BASE_URL"].startswith("https://"):
        raise RuntimeError("APP_BASE_URL must use https in production")

    if app_env["SESSION_COOKIE_SAMESITE"] == "none" and app_env["SESSION_COOKIE_SECURE"].lower() not in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }:
        raise RuntimeError("SESSION_COOKIE_SECURE must be true when SESSION_COOKIE_SAMESITE=none")

    for name in ("AUTH_VALIDATE_ROUTE_PATH", "AUTH_ME_ROUTE_PATH", "SESSION_COOKIE_PATH"):
        _validate_path(name, app_env[name])

    if app_env["SESSION_COOKIE_DOMAIN"].strip():
        _validate_domain_token("SESSION_COOKIE_DOMAIN", app_env["SESSION_COOKIE_DOMAIN"])

    postgres_min_size = int(app_env["POSTGRES_MIN_SIZE"])
    postgres_max_size = int(app_env["POSTGRES_MAX_SIZE"])
    if postgres_min_size <= 0:
        raise RuntimeError("POSTGRES_MIN_SIZE must be > 0")
    if postgres_max_size <= 0:
        raise RuntimeError("POSTGRES_MAX_SIZE must be > 0")
    if postgres_max_size < postgres_min_size:
        raise RuntimeError("POSTGRES_MAX_SIZE must be >= POSTGRES_MIN_SIZE")

    session_ttl_seconds = int(app_env["SESSION_TTL_SECONDS"])
    auth_tx_ttl_seconds = int(app_env["AUTH_TX_TTL_SECONDS"])
    if session_ttl_seconds <= 0:
        raise RuntimeError("SESSION_TTL_SECONDS must be > 0")
    if auth_tx_ttl_seconds <= 0:
        raise RuntimeError("AUTH_TX_TTL_SECONDS must be > 0")

    if not _enabled_providers(app_env) and deployment_environment not in DEV_ENVIRONMENTS:
        raise RuntimeError("At least one OAuth provider must be configured in production")

    if float(app_env["OTEL_TIMEOUT_SECONDS"]) <= 0:
        raise RuntimeError("OTEL_TIMEOUT_SECONDS must be > 0")
    if int(app_env["OTEL_METRIC_EXPORT_INTERVAL_MS"]) <= 0:
        raise RuntimeError("OTEL_METRIC_EXPORT_INTERVAL_MS must be > 0")
    if int(app_env["OTEL_METRIC_EXPORT_TIMEOUT_MS"]) <= 0:
        raise RuntimeError("OTEL_METRIC_EXPORT_TIMEOUT_MS must be > 0")

    sampler = app_env["OTEL_TRACES_SAMPLER"].strip().lower()
    if sampler not in {
        "always_on",
        "always_off",
        "traceidratio",
        "parentbased_always_on",
        "parentbased_always_off",
        "parentbased_traceidratio",
    }:
        raise RuntimeError(
            "OTEL_TRACES_SAMPLER must be one of: "
            "always_on, always_off, traceidratio, parentbased_always_on, "
            "parentbased_always_off, parentbased_traceidratio"
        )

    ratio = float(app_env["OTEL_TRACES_SAMPLER_ARG"])
    if not 0.0 <= ratio <= 1.0:
        raise RuntimeError("OTEL_TRACES_SAMPLER_ARG must be between 0.0 and 1.0")

    _validate_log_level(app_env["LOG_LEVEL"])


def load_app_env(settings: DeploymentSettings) -> dict[str, str]:
    environment = _env_str("ENVIRONMENT", APP_ENV_DEFAULTS["ENVIRONMENT"]).strip().lower() or "production"
    dev_mode = _default_dev_mode(environment)

    env: dict[str, str] = {}
    for name in APP_ENV_ORDER:
        if name == "APP_BASE_URL":
            value = _derive_app_base_url(environment)
        elif name == "POSTGRES_DSN":
            value = resolve_postgres_dsn(settings)
        elif name == "SESSION_COOKIE_SECURE":
            value = _env_str(name, "false" if dev_mode else "true")
        else:
            value = _env_str(name, APP_ENV_DEFAULTS[name])

        if name == "SESSION_COOKIE_SAMESITE":
            value = _normalize_samesite(value)
        elif name == "LOG_LEVEL":
            value = _validate_log_level(value)
        elif name in {"AUTH_VALIDATE_ROUTE_PATH", "AUTH_ME_ROUTE_PATH", "SESSION_COOKIE_PATH"}:
            _validate_path(name, value)
        elif name == "SESSION_COOKIE_DOMAIN" and value:
            _validate_domain_token(name, value)

        env[name] = value

    _validate_provider_pairs(env, dev_mode)
    _validate_runtime_app_env(env)

    if not env["POSTGRES_DSN"].strip():
        raise RuntimeError("POSTGRES_DSN must not be empty")

    return env


def _secret_data(settings: DeploymentSettings, app_env: dict[str, str]) -> dict[str, str]:
    secret: dict[str, str] = {"POSTGRES_DSN": app_env["POSTGRES_DSN"]}
    for name in ("GOOGLE_CLIENT_SECRET", "MS_CLIENT_SECRET", "GITHUB_CLIENT_SECRET"):
        value = app_env.get(name, "").strip()
        if value:
            secret[name] = value
    return secret


def _build_container_env(
    app_env: dict[str, str],
    secret_names: set[str],
    settings: DeploymentSettings,
) -> list[dict[str, Any]]:
    env: list[dict[str, Any]] = []
    for name in APP_ENV_ORDER:
        if name in SECRET_ENV_NAMES:
            if name in secret_names:
                env.append(_secret_ref_env(name, settings.secret_name))
            continue

        value = app_env.get(name, "")
        if not value and name not in {"SESSION_COOKIE_DOMAIN", "OTEL_EXPORTER_OTLP_ENDPOINT"}:
            continue

        env.append({"name": name, "value": value})
    return env


def build_namespace_doc(settings: DeploymentSettings) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": settings.namespace,
            "labels": _base_labels(settings),
        },
    }


def build_service_account_doc(settings: DeploymentSettings) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": settings.service_account_name,
            "namespace": settings.namespace,
            "labels": _base_labels(settings),
        },
        "automountServiceAccountToken": False,
    }


def build_secret_doc(settings: DeploymentSettings, secret_data: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": settings.secret_name,
            "namespace": settings.namespace,
            "labels": _base_labels(settings),
        },
        "type": "Opaque",
        "stringData": secret_data,
    }


def build_secret_delete_doc(settings: DeploymentSettings) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": settings.secret_name,
            "namespace": settings.namespace,
        },
    }


def build_pdb_doc(settings: DeploymentSettings) -> dict[str, Any] | None:
    if not settings.enable_pdb or settings.replicas < 2:
        return None

    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": f"{settings.deployment_name}-pdb",
            "namespace": settings.namespace,
            "labels": _base_labels(settings),
        },
        "spec": {
            "minAvailable": 1,
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": settings.deployment_name,
                    "app.kubernetes.io/component": "auth",
                }
            },
        },
    }


def build_deployment_doc(
    settings: DeploymentSettings,
    app_env: dict[str, str],
    secret_names: set[str],
) -> dict[str, Any]:
    pod_labels = _pod_labels(settings)
    container_env = _build_container_env(app_env, secret_names, settings)

    checksum_source = {
        "image": settings.image,
        "replicas": settings.replicas,
        "cpu_request": settings.cpu_request,
        "cpu_limit": settings.cpu_limit,
        "memory_request": settings.memory_request,
        "memory_limit": settings.memory_limit,
        "app_env": app_env,
        "secret_keys": sorted(secret_names),
        "db_host": _resolve_db_host(settings),
        "db_name": settings.db_name,
        "db_access_mode": settings.db_access_mode,
    }
    checksum = hashlib.sha256(yaml_dump(checksum_source).encode("utf-8")).hexdigest()

    pod_spec: dict[str, Any] = {
        "serviceAccountName": settings.service_account_name,
        "automountServiceAccountToken": False,
        "nodeSelector": _node_selector_for_cluster(settings),
        "tolerations": _tolerations_for_cluster(settings),
        "topologySpreadConstraints": _topology_spread_constraints(settings),
        "securityContext": _pod_security_context(settings),
        "terminationGracePeriodSeconds": 30,
        "volumes": _shared_volumes(),
        "containers": [
            {
                "name": "auth",
                "image": settings.image,
                "imagePullPolicy": settings.image_pull_policy,
                "ports": [
                    {
                        "name": "http",
                        "containerPort": settings.app_port,
                        "protocol": "TCP",
                    }
                ],
                "env": container_env,
                "volumeMounts": _shared_mounts(),
                "securityContext": _container_security_context(),
                "resources": {
                    "requests": {
                        "cpu": settings.cpu_request,
                        "memory": settings.memory_request,
                    },
                    "limits": {
                        "cpu": settings.cpu_limit,
                        "memory": settings.memory_limit,
                    },
                },
                "startupProbe": _http_probe(
                    "/healthz",
                    settings.app_port,
                    timeout_seconds=2,
                    period_seconds=5,
                    failure_threshold=60,
                ),
                "livenessProbe": _http_probe(
                    "/healthz",
                    settings.app_port,
                    timeout_seconds=2,
                    period_seconds=10,
                    failure_threshold=6,
                ),
                "readinessProbe": _http_probe(
                    "/readyz",
                    settings.app_port,
                    timeout_seconds=2,
                    period_seconds=5,
                    failure_threshold=6,
                ),
            }
        ],
    }

    pod_spec = {key: value for key, value in pod_spec.items() if value is not None}

    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": settings.deployment_name,
            "namespace": settings.namespace,
            "labels": _base_labels(settings),
        },
        "spec": {
            "replicas": settings.replicas,
            "revisionHistoryLimit": 3,
            "progressDeadlineSeconds": 600,
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1},
            },
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": settings.deployment_name,
                    "app.kubernetes.io/component": "auth",
                }
            },
            "template": {
                "metadata": {
                    "labels": pod_labels,
                    "annotations": {
                        "auth-service.io/config-checksum": checksum,
                    },
                },
                "spec": pod_spec,
            },
        },
    }


def build_service_doc(settings: DeploymentSettings) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": settings.service_name,
            "namespace": settings.namespace,
            "labels": _base_labels(settings),
        },
        "spec": {
            "type": "ClusterIP",
            "selector": {
                "app.kubernetes.io/name": settings.deployment_name,
                "app.kubernetes.io/component": "auth",
            },
            "ports": [
                {
                    "name": "http",
                    "port": 80,
                    "targetPort": settings.app_port,
                    "protocol": "TCP",
                }
            ],
        },
    }


def build_documents(
    settings: DeploymentSettings,
    app_env: dict[str, str],
) -> tuple[list[dict[str, Any]], dict[str, Any], set[str]]:
    secret_data = _secret_data(settings, app_env)
    secret_names = set(secret_data.keys())

    docs: list[dict[str, Any]] = [
        build_namespace_doc(settings),
        build_service_account_doc(settings),
    ]

    pdb_doc = build_pdb_doc(settings)
    if pdb_doc is not None:
        docs.append(pdb_doc)

    docs.append(build_deployment_doc(settings, app_env, secret_names))
    docs.append(build_service_doc(settings))
    return docs, build_secret_doc(settings, secret_data), secret_names


def render_documents(docs: list[dict[str, Any]]) -> str:
    return yaml.safe_dump_all(
        docs,
        sort_keys=False,
        default_flow_style=False,
        explicit_start=True,
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def rollout() -> int:
    settings = load_deployment_settings()

    log(
        "starting auth rollout "
        f"cluster={settings.cluster} namespace={settings.namespace} "
        f"deployment={settings.deployment_name} service={settings.service_name} "
        f"image={settings.image}"
    )
    log(
        "db target configuration "
        f"cnpg_cluster={settings.cnpg_cluster} "
        f"db_namespace={settings.db_secret_namespace} "
        f"pooler_service={settings.pooler_service} "
        f"db_name={settings.db_name} "
        f"db_access_mode={settings.db_access_mode}"
    )

    if not settings.image.strip():
        raise RuntimeError("AUTH_IMAGE is required")

    app_env = load_app_env(settings)
    docs, secret_doc, secret_names = build_documents(settings, app_env)

    namespace_doc = next(doc for doc in docs if doc.get("kind") == "Namespace")
    service_account_doc = next(doc for doc in docs if doc.get("kind") == "ServiceAccount")
    pdb_doc = next((doc for doc in docs if doc.get("kind") == "PodDisruptionBudget"), None)
    deployment_doc = next(doc for doc in docs if doc.get("kind") == "Deployment")
    service_doc = next(doc for doc in docs if doc.get("kind") == "Service")

    log("app env resolved successfully")
    log(f"secret keys to materialize: {sorted(secret_names)}")
    log(f"computed DB host: {_resolve_db_host(settings)}")

    apply_rendered_manifest(namespace_doc, "namespace.yaml")
    apply_rendered_manifest(service_account_doc, "serviceaccount.yaml")
    apply_ephemeral_manifest(secret_doc)

    if pdb_doc is not None:
        apply_rendered_manifest(pdb_doc, "pdb.yaml")

    apply_rendered_manifest(deployment_doc, "deployment.yaml")
    apply_rendered_manifest(service_doc, "service.yaml")

    rendered_nonsecret = render_documents(docs)
    digest_source = {
        "rendered": rendered_nonsecret,
        "secret_digest": sha256_text(yaml_dump(secret_doc)),
        "secret_keys": sorted(secret_names),
        "db_host": _resolve_db_host(settings),
        "db_name": settings.db_name,
        "db_access_mode": settings.db_access_mode,
        "image": settings.image,
        "replicas": settings.replicas,
    }
    digest = sha256_text(yaml_dump(digest_source))
    write_state(rendered_nonsecret, digest)

    print(f"[OK] Rollout applied; hash={digest}")
    return 0


def delete() -> int:
    settings = load_deployment_settings()

    log(
        "starting auth delete "
        f"cluster={settings.cluster} namespace={settings.namespace} "
        f"deployment={settings.deployment_name} service={settings.service_name}"
    )

    service_account_doc = build_service_account_doc(settings)
    secret_delete_doc = build_secret_delete_doc(settings)
    pdb_doc = build_pdb_doc(settings)

    deployment_doc: dict[str, Any] | None = None
    service_doc: dict[str, Any] | None = None

    if RENDERED_PATH.exists():
        rendered_yaml = RENDERED_PATH.read_text(encoding="utf-8")
        docs = list(yaml.safe_load_all(rendered_yaml))
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            if doc.get("kind") == "Deployment":
                deployment_doc = doc
            elif doc.get("kind") == "Service":
                service_doc = doc

    if deployment_doc is None or service_doc is None:
        app_env = load_app_env(settings)
        docs, _, _ = build_documents(settings, app_env)
        for doc in docs:
            if doc.get("kind") == "Deployment":
                deployment_doc = doc
            elif doc.get("kind") == "Service":
                service_doc = doc

    if service_doc is not None:
        delete_doc(service_doc)
    if deployment_doc is not None:
        delete_doc(deployment_doc)
    if pdb_doc is not None:
        delete_doc(pdb_doc)
    delete_doc(secret_delete_doc)
    delete_doc(service_account_doc)

    for path in (RENDERED_PATH, HASH_PATH):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass

    print("[OK] Delete applied")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="oidc_server.py",
        description="Render and lifecycle-manage the auth service deployment.",
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--rollout", action="store_true", help="Render and apply manifests")
    action.add_argument("--delete", action="store_true", help="Delete rendered manifests")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        require_bin("kubectl")
        if args.rollout:
            return rollout()
        if args.delete:
            return delete()
        return 1
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] kubectl failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode or 1
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
