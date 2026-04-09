from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class Settings:
    app_name: str
    environment: str
    dev_mode: bool
    app_base_url: str
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


def _env(name: str, default: str = '') -> str:
    value = os.getenv(name)
    return default if value is None else value


def _csv(name: str) -> list[str]:
    raw = _env(name, '')
    return [x.strip() for x in raw.split(',') if x.strip()]


def _bool(name: str, default: str = 'false') -> bool:
    return _env(name, default).lower() in {'1', 'true', 'yes', 'on'}


def load_settings() -> Settings:
    env = _env('ENVIRONMENT', 'production').lower()
    return Settings(
        app_name=_env('APP_NAME', 'auth-service'),
        environment=env,
        dev_mode=env in {'dev', 'development', 'local', 'test'},
        app_base_url=_env('APP_BASE_URL', '').rstrip('/'),
        postgres_dsn=_env('POSTGRES_DSN', 'postgresql://postgres:postgres@postgres:5432/auth'),
        postgres_min_size=int(_env('POSTGRES_MIN_SIZE', '1')),
        postgres_max_size=int(_env('POSTGRES_MAX_SIZE', '10')),
        session_cookie_name=_env('SESSION_COOKIE_NAME', 'auth_session'),
        session_ttl_seconds=int(_env('SESSION_TTL_SECONDS', '86400')),
        auth_tx_ttl_seconds=int(_env('AUTH_TX_TTL_SECONDS', '600')),
        session_cookie_secure=_bool('SESSION_COOKIE_SECURE', 'true'),
        session_cookie_samesite=_env('SESSION_COOKIE_SAMESITE', 'strict').lower(),
        session_cookie_path=_env('SESSION_COOKIE_PATH', '/'),
        session_cookie_domain=_env('SESSION_COOKIE_DOMAIN') or None,
        google_client_id=_env('GOOGLE_CLIENT_ID'),
        google_client_secret=_env('GOOGLE_CLIENT_SECRET'),
        microsoft_client_id=_env('MS_CLIENT_ID'),
        microsoft_client_secret=_env('MS_CLIENT_SECRET'),
        microsoft_tenant_id=_env('MS_TENANT_ID', 'common'),
        github_client_id=_env('GITHUB_CLIENT_ID'),
        github_client_secret=_env('GITHUB_CLIENT_SECRET'),
        google_allowed_domains=_csv('GOOGLE_ALLOWED_DOMAINS'),
        microsoft_allowed_tenant_ids=_csv('MICROSOFT_ALLOWED_TENANT_IDS'),
        microsoft_allowed_domains=_csv('MICROSOFT_ALLOWED_DOMAINS'),
        github_allowed_orgs=_csv('GITHUB_ALLOWED_ORGS'),
        metrics_enabled=_bool('METRICS_ENABLED', 'true'),
    )


def validate_runtime_settings(settings: Settings) -> None:
    if not settings.app_base_url:
        raise RuntimeError('APP_BASE_URL is required')

    parsed = urlparse(settings.app_base_url)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError('APP_BASE_URL must be an absolute URL')

    if not settings.dev_mode and parsed.scheme != 'https':
        raise RuntimeError('APP_BASE_URL must use https in production')

    if not settings.dev_mode and settings.microsoft_client_id and settings.microsoft_tenant_id.lower() in {'', 'common', 'organizations', 'consumers'}:
        raise RuntimeError('MS_TENANT_ID must be a concrete tenant id in production')


def enabled_providers(settings: Settings) -> list[str]:
    providers: list[str] = []
    if settings.google_client_id and settings.google_client_secret:
        providers.append('google')
    if settings.microsoft_client_id and settings.microsoft_client_secret:
        providers.append('microsoft')
    if settings.github_client_id and settings.github_client_secret:
        providers.append('github')
    return providers


def callback_url(settings: Settings, provider: str) -> str:
    return f'{settings.app_base_url}/callback/{provider}'