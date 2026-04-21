from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import urlparse

_DEV_ENVIRONMENTS = {"dev", "development", "local", "test"}
_VALID_SAMESITE = {"strict", "lax", "none"}
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_VALID_OTEL_SAMPLERS = {
    "always_on",
    "always_off",
    "traceidratio",
    "parentbased_always_on",
    "parentbased_always_off",
    "parentbased_traceidratio",
}


@dataclass(frozen=True, slots=True)
class Settings:
    app_name: str
    service_name: str
    service_version: str
    environment: str
    deployment_environment: str
    dev_mode: bool
    cluster_name: str
    instance_id: str

    app_base_url: str
    app_home_url: str
    web_allowed_origins: list[str]
    validate_route_path: str
    me_route_path: str

    postgres_dsn: str
    postgres_min_size: int
    postgres_max_size: int

    session_cookie_name: str
    session_ttl_seconds: int
    auth_tx_ttl_seconds: int
    session_cookie_secure: bool
    session_cookie_samesite: str
    session_cookie_path: str
    session_cookie_domain: str | None

    google_client_id: str
    google_client_secret: str
    microsoft_client_id: str
    microsoft_client_secret: str
    microsoft_tenant_id: str
    github_client_id: str
    github_client_secret: str

    google_allowed_domains: list[str]
    microsoft_allowed_tenant_ids: list[str]
    microsoft_allowed_domains: list[str]
    github_allowed_orgs: list[str]

    metrics_enabled: bool
    otel_endpoint: str | None
    otel_traces_sampler: str
    otel_traces_sampler_arg: float
    trace_sample_ratio: float
    otel_timeout_seconds: float
    otel_metric_export_interval_ms: int
    otel_metric_export_timeout_ms: int
    log_level: str


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return default if value is None else value


def _csv(name: str) -> list[str]:
    raw = _env(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _bool(name: str, default: str = "false") -> bool:
    return _env(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: str) -> int:
    raw = _env(name, default).strip()
    try:
        return int(raw)
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"{name} must be an integer") from exc


def _float(name: str, default: str) -> float:
    raw = _env(name, default).strip()
    try:
        return float(raw)
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"{name} must be numeric") from exc


def _require_absolute_url(name: str, value: str) -> None:
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError(f"{name} must be an absolute URL")
    if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
        raise RuntimeError(f"{name} must not contain a path, query, or fragment")


def _validate_origin(name: str, value: str) -> None:
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError(f"{name} must be an absolute origin URL")
    if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
        raise RuntimeError(f"{name} must not contain a path, query, or fragment")


def _normalize_samesite(raw: str) -> str:
    value = raw.strip().lower()
    if value not in _VALID_SAMESITE:
        raise RuntimeError("SESSION_COOKIE_SAMESITE must be one of: strict, lax, none")
    return value


def _validate_domain_token(name: str, value: str) -> None:
    token = value.strip()
    if not token:
        raise RuntimeError(f"{name} must not be empty")
    if any(ch.isspace() for ch in token):
        raise RuntimeError(f"{name} must not contain whitespace")
    if "://" in token or "/" in token:
        raise RuntimeError(f"{name} must be a bare host/domain token")


def _validate_path(name: str, value: str) -> None:
    if not value:
        raise RuntimeError(f"{name} must not be empty")
    if not value.startswith("/"):
        raise RuntimeError(f"{name} must start with '/'")
    if any(ch.isspace() for ch in value):
        raise RuntimeError(f"{name} must not contain whitespace")


def _normalize_log_level(raw: str) -> str:
    level = raw.strip().upper()
    if level == "WARN":
        level = "WARNING"
    if level not in _VALID_LOG_LEVELS:
        raise RuntimeError("LOG_LEVEL must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    return level


def _derive_auth_base_url(environment: str) -> str:
    explicit = _env("APP_BASE_URL", "").strip().rstrip("/")
    if explicit:
        return explicit

    if environment in _DEV_ENVIRONMENTS:
        return "http://127.0.0.1:8001"

    domain = _env("DOMAIN", "").strip().rstrip(".")
    if domain:
        return f"https://auth.{domain}"

    raise RuntimeError("APP_BASE_URL is required")


def _derive_app_home_url(environment: str) -> str:
    explicit = _env("APP_HOME_URL", "").strip().rstrip("/")
    if explicit:
        return explicit

    if environment in _DEV_ENVIRONMENTS:
        return "http://127.0.0.1:3000"

    domain = _env("DOMAIN", "").strip().rstrip(".")
    if domain:
        return f"https://app.{domain}"

    raise RuntimeError("APP_HOME_URL is required")


def _derive_session_cookie_domain() -> str | None:
    explicit = _env("SESSION_COOKIE_DOMAIN", "").strip()
    if explicit:
        return explicit

    domain = _env("DOMAIN", "").strip().rstrip(".")
    if domain:
        return f".{domain}"

    return None


def _derive_web_allowed_origins(app_home_url: str, dev_mode: bool) -> list[str]:
    explicit = _csv("WEB_ALLOWED_ORIGINS")
    if explicit:
        return explicit

    parsed = urlparse(app_home_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if dev_mode:
        return [
            origin,
            "http://127.0.0.1:3000",
            "http://localhost:3000",
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    return [origin]


def _env_otlp_timeout_seconds() -> float:
    """
    Supports the common OpenTelemetry timeout envs.

    Accepted forms:
      - OTEL_EXPORTER_OTLP_TIMEOUT in milliseconds
      - OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS in seconds
      - OTEL_TIMEOUT_SECONDS in seconds (legacy auth env)
    """
    raw_ms = _env("OTEL_EXPORTER_OTLP_TIMEOUT", "").strip()
    if raw_ms:
        try:
            return float(raw_ms) / 1000.0
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("OTEL_EXPORTER_OTLP_TIMEOUT must be numeric") from exc

    raw_seconds = _env("OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS", "").strip()
    if raw_seconds:
        try:
            return float(raw_seconds)
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS must be numeric") from exc

    raw_legacy = _env("OTEL_TIMEOUT_SECONDS", "").strip()
    if raw_legacy:
        try:
            return float(raw_legacy)
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("OTEL_TIMEOUT_SECONDS must be numeric") from exc

    return 10.0


def _normalize_sampler_name(raw: str) -> str:
    value = raw.strip().lower()
    if value not in _VALID_OTEL_SAMPLERS:
        raise RuntimeError(
            "OTEL_TRACES_SAMPLER must be one of: "
            "always_on, always_off, traceidratio, parentbased_always_on, "
            "parentbased_always_off, parentbased_traceidratio"
        )
    return value


def _sampler_ratio(sampler: str, raw_arg: str | None) -> float:
    sampler = _normalize_sampler_name(sampler)

    if sampler in {"always_on", "parentbased_always_on"}:
        return 1.0
    if sampler in {"always_off", "parentbased_always_off"}:
        return 0.0

    if sampler in {"traceidratio", "parentbased_traceidratio"}:
        if raw_arg is None or raw_arg.strip() == "":
            return 0.1
        value = _float("OTEL_TRACES_SAMPLER_ARG", raw_arg)
        if not 0.0 <= value <= 1.0:
            raise RuntimeError("OTEL_TRACES_SAMPLER_ARG must be in the range [0.0, 1.0]")
        return value

    raise RuntimeError(
        "OTEL_TRACES_SAMPLER must be one of: "
        "always_on, always_off, traceidratio, parentbased_always_on, "
        "parentbased_always_off, parentbased_traceidratio"
    )


def load_settings() -> Settings:
    environment = _env("ENVIRONMENT", "production").strip().lower()
    dev_mode = environment in _DEV_ENVIRONMENTS

    app_name = _env("APP_NAME", "auth-service").strip() or "auth-service"
    service_name = _env("SERVICE_NAME", app_name).strip() or app_name
    service_version = _env("SERVICE_VERSION", "v1").strip() or "v1"
    deployment_environment = _env("DEPLOYMENT_ENVIRONMENT", environment).strip().lower() or environment
    cluster_name = _env("K8S_CLUSTER_NAME", "local-cluster").strip() or "local-cluster"
    instance_id = _env("POD_NAME", _env("HOSTNAME", "local")).strip() or "local"

    app_base_url = _derive_auth_base_url(environment)
    app_home_url = _derive_app_home_url(environment)
    _require_absolute_url("APP_BASE_URL", app_base_url)
    _require_absolute_url("APP_HOME_URL", app_home_url)

    validate_route_path = _env("AUTH_VALIDATE_ROUTE_PATH", "/validate").strip() or "/validate"
    me_route_path = _env("AUTH_ME_ROUTE_PATH", "/me").strip() or "/me"
    _validate_path("AUTH_VALIDATE_ROUTE_PATH", validate_route_path)
    _validate_path("AUTH_ME_ROUTE_PATH", me_route_path)

    postgres_dsn = _env("POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/auth").strip()
    postgres_min_size = _int("POSTGRES_MIN_SIZE", "1")
    postgres_max_size = _int("POSTGRES_MAX_SIZE", "10")
    if postgres_min_size <= 0:
        raise RuntimeError("POSTGRES_MIN_SIZE must be > 0")
    if postgres_max_size <= 0:
        raise RuntimeError("POSTGRES_MAX_SIZE must be > 0")
    if postgres_max_size < postgres_min_size:
        raise RuntimeError("POSTGRES_MAX_SIZE must be >= POSTGRES_MIN_SIZE")

    session_cookie_name = _env("SESSION_COOKIE_NAME", "auth_session").strip() or "auth_session"
    session_ttl_seconds = _int("SESSION_TTL_SECONDS", "86400")
    auth_tx_ttl_seconds = _int("AUTH_TX_TTL_SECONDS", "600")
    if session_ttl_seconds <= 0:
        raise RuntimeError("SESSION_TTL_SECONDS must be > 0")
    if auth_tx_ttl_seconds <= 0:
        raise RuntimeError("AUTH_TX_TTL_SECONDS must be > 0")

    session_cookie_secure = _bool("SESSION_COOKIE_SECURE", "true" if not dev_mode else "false")
    session_cookie_samesite = _normalize_samesite(_env("SESSION_COOKIE_SAMESITE", "lax"))
    session_cookie_path = _env("SESSION_COOKIE_PATH", "/").strip() or "/"
    _validate_path("SESSION_COOKIE_PATH", session_cookie_path)
    session_cookie_domain = _derive_session_cookie_domain()
    if session_cookie_domain is not None:
        _validate_domain_token("SESSION_COOKIE_DOMAIN", session_cookie_domain)

    google_client_id = _env("GOOGLE_CLIENT_ID", "").strip()
    google_client_secret = _env("GOOGLE_CLIENT_SECRET", "").strip()
    microsoft_client_id = _env("MS_CLIENT_ID", "").strip()
    microsoft_client_secret = _env("MS_CLIENT_SECRET", "").strip()
    microsoft_tenant_id = _env("MS_TENANT_ID", "common").strip() or "common"
    github_client_id = _env("GITHUB_CLIENT_ID", "").strip()
    github_client_secret = _env("GITHUB_CLIENT_SECRET", "").strip()

    google_allowed_domains = _csv("GOOGLE_ALLOWED_DOMAINS")
    microsoft_allowed_tenant_ids = _csv("MICROSOFT_ALLOWED_TENANT_IDS")
    microsoft_allowed_domains = _csv("MICROSOFT_ALLOWED_DOMAINS")
    github_allowed_orgs = _csv("GITHUB_ALLOWED_ORGS")

    metrics_enabled = _bool("METRICS_ENABLED", "true")

    raw_otel_endpoint = _env(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://signoz-otel-collector.signoz.svc.cluster.local:4317",
    ).strip()
    otel_endpoint = raw_otel_endpoint or "http://signoz-otel-collector.signoz.svc.cluster.local:4317"

    otel_traces_sampler = _normalize_sampler_name(
        _env("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
    )
    otel_traces_sampler_arg = _float("OTEL_TRACES_SAMPLER_ARG", _env("OTEL_TRACES_SAMPLER_ARG", "0.1"))
    trace_sample_ratio = _sampler_ratio(otel_traces_sampler, _env("OTEL_TRACES_SAMPLER_ARG", ""))

    otel_timeout_seconds = _env_otlp_timeout_seconds()
    otel_metric_export_interval_ms = _int("OTEL_METRIC_EXPORT_INTERVAL_MS", "15000")
    otel_metric_export_timeout_ms = _int("OTEL_METRIC_EXPORT_TIMEOUT_MS", "10000")
    log_level = _normalize_log_level(_env("LOG_LEVEL", "INFO"))

    web_allowed_origins = _derive_web_allowed_origins(app_home_url, dev_mode)
    for origin in web_allowed_origins:
        _validate_origin("WEB_ALLOWED_ORIGINS", origin)

    settings = Settings(
        app_name=app_name,
        service_name=service_name,
        service_version=service_version,
        environment=environment,
        deployment_environment=deployment_environment,
        dev_mode=dev_mode,
        cluster_name=cluster_name,
        instance_id=instance_id,
        app_base_url=app_base_url,
        app_home_url=app_home_url,
        web_allowed_origins=web_allowed_origins,
        validate_route_path=validate_route_path,
        me_route_path=me_route_path,
        postgres_dsn=postgres_dsn,
        postgres_min_size=postgres_min_size,
        postgres_max_size=postgres_max_size,
        session_cookie_name=session_cookie_name,
        session_ttl_seconds=session_ttl_seconds,
        auth_tx_ttl_seconds=auth_tx_ttl_seconds,
        session_cookie_secure=session_cookie_secure,
        session_cookie_samesite=session_cookie_samesite,
        session_cookie_path=session_cookie_path,
        session_cookie_domain=session_cookie_domain,
        google_client_id=google_client_id,
        google_client_secret=google_client_secret,
        microsoft_client_id=microsoft_client_id,
        microsoft_client_secret=microsoft_client_secret,
        microsoft_tenant_id=microsoft_tenant_id,
        github_client_id=github_client_id,
        github_client_secret=github_client_secret,
        google_allowed_domains=google_allowed_domains,
        microsoft_allowed_tenant_ids=microsoft_allowed_tenant_ids,
        microsoft_allowed_domains=microsoft_allowed_domains,
        github_allowed_orgs=github_allowed_orgs,
        metrics_enabled=metrics_enabled,
        otel_endpoint=otel_endpoint,
        otel_traces_sampler=otel_traces_sampler,
        otel_traces_sampler_arg=otel_traces_sampler_arg,
        trace_sample_ratio=trace_sample_ratio,
        otel_timeout_seconds=otel_timeout_seconds,
        otel_metric_export_interval_ms=otel_metric_export_interval_ms,
        otel_metric_export_timeout_ms=otel_metric_export_timeout_ms,
        log_level=log_level,
    )

    validate_runtime_settings(settings)
    return settings


def validate_runtime_settings(settings: Settings) -> None:
    _require_absolute_url("APP_BASE_URL", settings.app_base_url)
    _require_absolute_url("APP_HOME_URL", settings.app_home_url)

    if not settings.dev_mode:
        if urlparse(settings.app_base_url).scheme != "https":
            raise RuntimeError("APP_BASE_URL must use https in production")
        if urlparse(settings.app_home_url).scheme != "https":
            raise RuntimeError("APP_HOME_URL must use https in production")

    if settings.postgres_min_size <= 0:
        raise RuntimeError("POSTGRES_MIN_SIZE must be > 0")
    if settings.postgres_max_size <= 0:
        raise RuntimeError("POSTGRES_MAX_SIZE must be > 0")
    if settings.postgres_max_size < settings.postgres_min_size:
        raise RuntimeError("POSTGRES_MAX_SIZE must be >= POSTGRES_MIN_SIZE")

    if settings.session_ttl_seconds <= 0:
        raise RuntimeError("SESSION_TTL_SECONDS must be > 0")
    if settings.auth_tx_ttl_seconds <= 0:
        raise RuntimeError("AUTH_TX_TTL_SECONDS must be > 0")

    _validate_path("SESSION_COOKIE_PATH", settings.session_cookie_path)
    if settings.session_cookie_samesite not in _VALID_SAMESITE:
        raise RuntimeError("SESSION_COOKIE_SAMESITE must be one of: strict, lax, none")
    if settings.session_cookie_samesite == "none" and not settings.session_cookie_secure:
        raise RuntimeError("SESSION_COOKIE_SECURE must be true when SESSION_COOKIE_SAMESITE=none")

    if settings.session_cookie_domain is not None:
        _validate_domain_token("SESSION_COOKIE_DOMAIN", settings.session_cookie_domain)

    if any((settings.google_client_id, settings.google_client_secret)) and not all(
        (settings.google_client_id, settings.google_client_secret)
    ):
        raise RuntimeError("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must both be set")
    if any((settings.microsoft_client_id, settings.microsoft_client_secret)) and not all(
        (settings.microsoft_client_id, settings.microsoft_client_secret)
    ):
        raise RuntimeError("MS_CLIENT_ID and MS_CLIENT_SECRET must both be set")
    if any((settings.github_client_id, settings.github_client_secret)) and not all(
        (settings.github_client_id, settings.github_client_secret)
    ):
        raise RuntimeError("GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET must both be set")

    if not settings.dev_mode and settings.microsoft_client_id:
        tenant = settings.microsoft_tenant_id.lower()
        if tenant in {"", "common", "organizations", "consumers"}:
            raise RuntimeError("MS_TENANT_ID must be a concrete tenant id in production")

    if not settings.dev_mode and not enabled_providers(settings):
        raise RuntimeError("At least one OAuth provider must be configured in production")

    if settings.otel_endpoint is not None:
        parsed = urlparse(
            settings.otel_endpoint if "://" in settings.otel_endpoint else f"//{settings.otel_endpoint}",
            scheme="http",
        )
        if parsed.scheme not in ("", "http", "https"):
            raise RuntimeError(f"OTEL_EXPORTER_OTLP_ENDPOINT has unsupported scheme: {parsed.scheme!r}")
        if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
            raise RuntimeError("OTEL_EXPORTER_OTLP_ENDPOINT must point to OTLP/gRPC, not OTLP/HTTP")
        authority = (parsed.netloc or parsed.path).rstrip("/")
        if not authority:
            raise RuntimeError("OTEL_EXPORTER_OTLP_ENDPOINT is invalid")

    if settings.otel_timeout_seconds <= 0:
        raise RuntimeError("OTEL_TIMEOUT_SECONDS / OTEL_EXPORTER_OTLP_TIMEOUT must be > 0")
    if settings.otel_metric_export_interval_ms <= 0:
        raise RuntimeError("OTEL_METRIC_EXPORT_INTERVAL_MS must be > 0")
    if settings.otel_metric_export_timeout_ms <= 0:
        raise RuntimeError("OTEL_METRIC_EXPORT_TIMEOUT_MS must be > 0")

    if settings.otel_traces_sampler not in _VALID_OTEL_SAMPLERS:
        raise RuntimeError(
            "OTEL_TRACES_SAMPLER must be one of: "
            "always_on, always_off, traceidratio, parentbased_always_on, "
            "parentbased_always_off, parentbased_traceidratio"
        )
    if not 0.0 <= settings.trace_sample_ratio <= 1.0:
        raise RuntimeError("OTEL_TRACES_SAMPLER_ARG / trace_sample_ratio must be between 0.0 and 1.0")

    if settings.log_level not in _VALID_LOG_LEVELS:
        raise RuntimeError("LOG_LEVEL must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL")


def enabled_providers(settings: Settings) -> list[str]:
    providers: list[str] = []
    if settings.google_client_id and settings.google_client_secret:
        providers.append("google")
    if settings.microsoft_client_id and settings.microsoft_client_secret:
        providers.append("microsoft")
    if settings.github_client_id and settings.github_client_secret:
        providers.append("github")
    return providers


def callback_url(settings: Settings, provider: str) -> str:
    return f"{settings.app_base_url}/callback/{provider.lower()}"


def validate_url(settings: Settings) -> str:
    return f"{settings.app_base_url}{settings.validate_route_path}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()