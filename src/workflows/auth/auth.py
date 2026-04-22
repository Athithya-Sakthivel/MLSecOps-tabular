from __future__ import annotations

import base64
import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse

import httpx
import jwt
from jwt import InvalidTokenError
from jwt.algorithms import RSAAlgorithm
from jwt.exceptions import ImmatureSignatureError, InvalidIssuedAtError
from jwt.exceptions import InvalidTokenError as PyJWTInvalidTokenError
from settings import Settings


@dataclass(frozen=True, slots=True)
class ProviderConfig:
    provider: str
    client_id: str
    client_secret: str
    authorize_url: str
    token_url: str
    userinfo_url: str | None
    jwks_uri: str | None
    issuer: str | list[str] | None
    scopes: tuple[str, ...]
    oidc: bool


def pkce_verifier() -> str:
    return secrets.token_urlsafe(96)


def pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def normalize_next(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return "/"

    parsed = urlparse(raw)
    if parsed.scheme or parsed.netloc:
        return "/"

    path = parsed.path or "/"
    if not path.startswith("/"):
        return "/"

    query = f"?{parsed.query}" if parsed.query else ""
    fragment = f"#{parsed.fragment}" if parsed.fragment else ""
    return f"{path}{query}{fragment}"


def callback_url(settings: Settings, provider: str) -> str:
    return f"{settings.app_base_url.rstrip('/')}/callback/{provider.lower().strip()}"


def return_to_url(settings: Settings, next_value: str | None) -> str:
    path = normalize_next(next_value)
    base = settings.app_home_url.rstrip("/")
    if path == "/":
        return base
    return f"{base}{path}"


def _provider_name(provider: str) -> str:
    return provider.lower().strip()


def _settings_attr(settings: Settings, name: str, default: str = "") -> str:
    value = getattr(settings, name, default)
    return str(value or "").strip()


async def provider_config(settings: Settings, provider: str) -> ProviderConfig:
    provider = _provider_name(provider)

    if provider == "google":
        client_id = _settings_attr(settings, "google_client_id")
        client_secret = _settings_attr(settings, "google_client_secret")
        if not client_id or not client_secret:
            raise ValueError("google provider is not configured")

        return ProviderConfig(
            provider="google",
            client_id=client_id,
            client_secret=client_secret,
            authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
            token_url="https://oauth2.googleapis.com/token",
            userinfo_url="https://openidconnect.googleapis.com/v1/userinfo",
            jwks_uri="https://www.googleapis.com/oauth2/v3/certs",
            issuer=["https://accounts.google.com", "accounts.google.com"],
            scopes=("openid", "email", "profile"),
            oidc=True,
        )

    if provider == "microsoft":
        client_id = _settings_attr(settings, "microsoft_client_id")
        client_secret = _settings_attr(settings, "microsoft_client_secret")
        tenant_id = _settings_attr(settings, "microsoft_tenant_id", "common")
        if not client_id or not client_secret:
            raise ValueError("microsoft provider is not configured")

        issuer = f"https://login.microsoftonline.com/{tenant_id}/v2.0"
        discovery_url = f"{issuer}/.well-known/openid-configuration"
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            response = await client.get(discovery_url)
            response.raise_for_status()
            discovery = response.json()

        return ProviderConfig(
            provider="microsoft",
            client_id=client_id,
            client_secret=client_secret,
            authorize_url=str(discovery["authorization_endpoint"]),
            token_url=str(discovery["token_endpoint"]),
            userinfo_url=str(discovery.get("userinfo_endpoint") or ""),
            jwks_uri=str(discovery["jwks_uri"]),
            issuer=issuer,
            scopes=("openid", "profile", "email"),
            oidc=True,
        )

    if provider == "github":
        client_id = _settings_attr(settings, "github_client_id")
        client_secret = _settings_attr(settings, "github_client_secret")
        if not client_id or not client_secret:
            raise ValueError("github provider is not configured")

        return ProviderConfig(
            provider="github",
            client_id=client_id,
            client_secret=client_secret,
            authorize_url="https://github.com/login/oauth/authorize",
            token_url="https://github.com/login/oauth/access_token",
            userinfo_url="https://api.github.com/user",
            jwks_uri=None,
            issuer=None,
            scopes=("read:user", "user:email"),
            oidc=False,
        )

    raise LookupError(provider)


def build_authorize_url(cfg: ProviderConfig, *, state: str, code_challenge: str, nonce: str) -> str:
    params: dict[str, str] = {
        "client_id": cfg.client_id,
        "redirect_uri": "",
        "response_type": "code",
        "scope": " ".join(cfg.scopes),
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }

    if cfg.oidc:
        params["nonce"] = nonce

    return f"{cfg.authorize_url}?{urlencode({k: v for k, v in params.items() if v != ''})}"


async def exchange_code(cfg: ProviderConfig, code: str, redirect_uri: str, code_verifier: str) -> dict[str, Any]:
    data: dict[str, str] = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": cfg.client_id,
        "client_secret": cfg.client_secret,
        "code_verifier": code_verifier,
    }

    headers = {
        "Accept": "application/json",
        "User-Agent": "auth-service/1.0",
    }

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, headers=headers) as client:
        response = await client.post(cfg.token_url, data=data)
        response.raise_for_status()

        try:
            payload = response.json()
        except ValueError:
            payload = dict(parse_qsl(response.text, keep_blank_values=True))

    if not isinstance(payload, dict):
        raise ValueError("token endpoint returned an invalid payload")

    if payload.get("error"):
        raise ValueError(str(payload.get("error_description") or payload.get("error")))

    return payload


async def fetch_userinfo(cfg: ProviderConfig, access_token: str) -> dict[str, Any]:
    if not access_token or not cfg.userinfo_url:
        return {}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": "auth-service/1.0",
    }

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, headers=headers) as client:
        response = await client.get(cfg.userinfo_url)
        if response.status_code in {401, 403}:
            return {}
        response.raise_for_status()
        data = response.json()

    return data if isinstance(data, dict) else {}


async def fetch_github_orgs(access_token: str) -> list[str]:
    if not access_token:
        return []

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "auth-service/1.0",
    }

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, headers=headers) as client:
        response = await client.get("https://api.github.com/user/orgs?per_page=100")
        if response.status_code in {401, 403}:
            return []
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, list):
        return []

    orgs: list[str] = []
    for item in payload:
        if isinstance(item, dict):
            login = str(item.get("login") or "").strip()
            if login:
                orgs.append(login)
    return orgs


def _jwks_to_key(jwks_uri: str, kid: str) -> Any:
    response = httpx.get(jwks_uri, timeout=10.0, follow_redirects=True)
    response.raise_for_status()
    jwks = response.json()

    keys = jwks.get("keys", [])
    if not isinstance(keys, list):
        raise ValueError("jwks payload is invalid")

    for key in keys:
        if isinstance(key, dict) and key.get("kid") == kid:
            return RSAAlgorithm.from_jwk(json.dumps(key))

    raise ValueError("matching jwk not found")


async def validate_id_token(cfg: ProviderConfig, id_token: str, nonce: str) -> dict[str, Any]:
    if not cfg.oidc:
        raise ValueError("id token validation is only available for oidc providers")
    if not id_token:
        raise ValueError("missing id_token")
    if not cfg.jwks_uri:
        raise ValueError("missing jwks_uri")
    if not cfg.issuer:
        raise ValueError("missing issuer")

    try:
        header = jwt.get_unverified_header(id_token)
    except InvalidTokenError as exc:
        raise ValueError("invalid id_token header") from exc

    kid = str(header.get("kid") or "").strip()
    if not kid:
        raise ValueError("missing kid in id_token header")

    key = _jwks_to_key(cfg.jwks_uri, kid)

    options = {
        "verify_signature": True,
        "verify_exp": True,
        "verify_nbf": True,
        "verify_iat": True,
        "verify_aud": True,
        "verify_iss": True,
        "verify_sub": True,
    }

    try:
        claims = jwt.decode(
            id_token,
            key=key,
            algorithms=["RS256"],
            audience=cfg.client_id,
            issuer=cfg.issuer,
            options=options,
            leeway=60,
            nonce=nonce,
        )
    except (ImmatureSignatureError, InvalidIssuedAtError) as exc:
        raise ValueError("the token is not yet valid") from exc
    except PyJWTInvalidTokenError as exc:
        raise ValueError(f"id_token validation failed: {exc}") from exc

    if not isinstance(claims, dict):
        raise ValueError("id_token claims are invalid")

    return claims


def normalize_identity(provider: str, claims: dict[str, Any]) -> dict[str, Any]:
    provider = _provider_name(provider)

    def pick(*keys: str) -> str | None:
        for key in keys:
            value = claims.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    email = pick("email", "preferred_username", "upn", "mail")
    name = pick("name", "display_name", "login", "preferred_username", "upn", "email")
    sub = pick("sub", "id", "user_id", "login", "preferred_username", "email")
    tenant_id = pick("tid", "tenant_id")

    if not sub:
        raise ValueError("identity is missing a subject")
    if not email and provider != "github":
        raise ValueError("identity is missing an email address")

    return {
        "provider": provider,
        "sub": sub,
        "email": email,
        "name": name,
        "tenant_id": tenant_id,
    }


def enforce_policy(settings: Settings, identity: dict[str, Any], claims: dict[str, Any]) -> None:
    provider = _provider_name(str(identity.get("provider") or ""))

    if provider == "google" and getattr(settings, "google_allowed_domains", None):
        email = str(identity.get("email") or "").strip().lower()
        if "@" not in email:
            raise ValueError("google account email missing")
        domain = email.split("@", 1)[1]
        allowed = {str(item).strip().lower() for item in settings.google_allowed_domains if str(item).strip()}
        if allowed and domain not in allowed:
            raise ValueError("google_domain_not_allowed")

    if provider == "microsoft":
        tenant_id = str(identity.get("tenant_id") or "").strip().lower()
        allowed_tenants = {
            str(item).strip().lower()
            for item in getattr(settings, "microsoft_allowed_tenant_ids", [])
            if str(item).strip()
        }
        if allowed_tenants and tenant_id not in allowed_tenants:
            raise ValueError("microsoft_tenant_not_allowed")

        allowed_domains = {
            str(item).strip().lower()
            for item in getattr(settings, "microsoft_allowed_domains", [])
            if str(item).strip()
        }
        email = str(identity.get("email") or "").strip().lower()
        if allowed_domains and "@" in email:
            domain = email.split("@", 1)[1]
            if domain not in allowed_domains:
                raise ValueError("microsoft_domain_not_allowed")


def build_validate_payload(session: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "ok",
        "user_id": str(session.get("user_id") or session.get("id") or ""),
        "session_id": str(session.get("id") or ""),
        "provider": session.get("provider"),
        "email": session.get("email"),
        "name": session.get("name"),
        "tenant_id": session.get("tenant_id"),
        "expires_at": session.get("expires_at"),
        "created_at": session.get("created_at"),
    }