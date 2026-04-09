from __future__ import annotations

import base64
import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
import jwt
from settings import Settings, callback_url


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    client_id: str
    client_secret: str
    scope: str
    redirect_uri: str
    authorize_url: str
    token_url: str
    userinfo_url: str | None
    jwks_url: str | None
    issuer: str | None
    oidc: bool


@dataclass(frozen=True)
class NormalizedIdentity:
    provider: str
    sub: str
    email: str
    name: str
    tenant_id: str | None = None
    raw_claims: dict[str, Any] | None = None


_DISCOVERY_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_DISCOVERY_TTL_SECONDS = 3600


def random_urlsafe(nbytes: int = 32) -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(nbytes)).rstrip(b'=').decode('ascii')


def pkce_verifier() -> str:
    return random_urlsafe(48)


def pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode('ascii')).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b'=').decode('ascii')


def normalize_next(next_value: str | None) -> str:
    if not next_value:
        return '/'
    if next_value.startswith('/') and not next_value.startswith('//'):
        return next_value
    return '/'


def _provider_scope(settings: Settings, provider: str) -> str:
    if provider == 'google':
        return 'openid email profile'
    if provider == 'microsoft':
        return 'openid email profile'
    if provider == 'github':
        scope = ['read:user', 'user:email']
        if settings.github_allowed_orgs:
            scope.append('read:org')
        return ' '.join(scope)
    raise ValueError(provider)


async def _fetch_json(url: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).timestamp()
    cached = _DISCOVERY_CACHE.get(url)
    if cached and cached[0] > now:
        return cached[1]
    async with httpx.AsyncClient(timeout=10.0, headers={'Accept': 'application/json'}) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
    _DISCOVERY_CACHE[url] = (now + _DISCOVERY_TTL_SECONDS, data)
    return data


async def provider_config(settings: Settings, provider: str) -> ProviderConfig:
    provider = provider.lower()
    redirect_uri = callback_url(settings, provider)
    if provider == 'google':
        discovery = await _fetch_json('https://accounts.google.com/.well-known/openid-configuration')
        return ProviderConfig(
            name='google',
            client_id=settings.google_client_id,
            client_secret=settings.google_client_secret,
            scope=_provider_scope(settings, 'google'),
            redirect_uri=redirect_uri,
            authorize_url=discovery['authorization_endpoint'],
            token_url=discovery['token_endpoint'],
            userinfo_url=discovery.get('userinfo_endpoint'),
            jwks_url=discovery['jwks_uri'],
            issuer=discovery['issuer'],
            oidc=True,
        )
    if provider == 'microsoft':
        tenant = settings.microsoft_tenant_id
        discovery = await _fetch_json(f'https://login.microsoftonline.com/{tenant}/v2.0/.well-known/openid-configuration')
        return ProviderConfig(
            name='microsoft',
            client_id=settings.microsoft_client_id,
            client_secret=settings.microsoft_client_secret,
            scope=_provider_scope(settings, 'microsoft'),
            redirect_uri=redirect_uri,
            authorize_url=discovery['authorization_endpoint'],
            token_url=discovery['token_endpoint'],
            userinfo_url=discovery.get('userinfo_endpoint'),
            jwks_url=discovery['jwks_uri'],
            issuer=discovery['issuer'],
            oidc=True,
        )
    if provider == 'github':
        return ProviderConfig(
            name='github',
            client_id=settings.github_client_id,
            client_secret=settings.github_client_secret,
            scope=_provider_scope(settings, 'github'),
            redirect_uri=redirect_uri,
            authorize_url='https://github.com/login/oauth/authorize',
            token_url='https://github.com/login/oauth/access_token',
            userinfo_url='https://api.github.com/user',
            jwks_url=None,
            issuer=None,
            oidc=False,
        )
    raise ValueError(provider)


def build_authorize_url(cfg: ProviderConfig, *, state: str, code_challenge: str, nonce: str) -> str:
    params = [
        ('response_type', 'code'),
        ('client_id', cfg.client_id),
        ('redirect_uri', cfg.redirect_uri),
        ('scope', cfg.scope),
        ('state', state),
        ('code_challenge', code_challenge),
        ('code_challenge_method', 'S256'),
    ]
    if cfg.oidc:
        params.append(('nonce', nonce))
    if cfg.name == 'google':
        params.append(('prompt', 'select_account'))
    if cfg.name == 'github':
        params.append(('allow_signup', 'false'))
    return f'{cfg.authorize_url}?{urlencode(params)}'


async def exchange_code(cfg: ProviderConfig, code: str, code_verifier: str) -> dict[str, Any]:
    data: dict[str, str] = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': cfg.redirect_uri,
        'client_id': cfg.client_id,
        'code_verifier': code_verifier,
    }
    if cfg.client_secret:
        data['client_secret'] = cfg.client_secret
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=False) as client:
        resp = await client.post(cfg.token_url, data=data, headers={'Accept': 'application/json'})
        resp.raise_for_status()
        return resp.json()


async def fetch_userinfo(cfg: ProviderConfig, access_token: str) -> dict[str, Any]:
    if not access_token:
        return {}
    if cfg.name == 'github':
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
            'User-Agent': 'auth-service',
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(cfg.userinfo_url, headers=headers)
            resp.raise_for_status()
            user = resp.json()
            email = user.get('email')
            if not email:
                resp2 = await client.get('https://api.github.com/user/emails', headers=headers)
                if resp2.status_code == 200:
                    emails = resp2.json()
                    for item in emails:
                        if item.get('primary') and item.get('verified'):
                            email = item.get('email')
                            break
                    if not email:
                        for item in emails:
                            if item.get('verified'):
                                email = item.get('email')
                                break
            user['email'] = email
            return user
    if cfg.userinfo_url:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(cfg.userinfo_url, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
            if resp.status_code == 200:
                return resp.json()
    return {}


async def fetch_github_orgs(access_token: str) -> list[str]:
    if not access_token:
        return []
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'auth-service',
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get('https://api.github.com/user/orgs', headers=headers)
        if resp.status_code != 200:
            return []
        orgs = resp.json()
        return [str(item.get('login')).lower() for item in orgs if isinstance(item, dict) and item.get('login')]


async def validate_id_token(cfg: ProviderConfig, id_token: str, nonce: str) -> dict[str, Any]:
    if not cfg.oidc:
        return {}
    if not id_token:
        raise ValueError('missing_id_token')
    if not cfg.jwks_url or not cfg.issuer:
        raise ValueError('oidc_metadata_incomplete')
    jwk_client = jwt.PyJWKClient(cfg.jwks_url)
    signing_key = jwk_client.get_signing_key_from_jwt(id_token).key
    claims = jwt.decode(
        id_token,
        signing_key,
        algorithms=['RS256', 'RS384', 'RS512'],
        audience=cfg.client_id,
        issuer=cfg.issuer,
        options={
            'require': ['exp', 'iat', 'iss', 'aud', 'sub'],
            'verify_signature': True,
            'verify_exp': True,
            'verify_iat': True,
            'verify_aud': True,
            'verify_iss': True,
        },
    )
    token_nonce = claims.get('nonce')
    if token_nonce != nonce:
        raise ValueError('nonce_mismatch')
    return claims


def normalize_identity(provider: str, claims: dict[str, Any]) -> NormalizedIdentity:
    provider = provider.lower()
    if provider == 'google':
        return NormalizedIdentity(
            provider='google',
            sub=str(claims.get('sub') or ''),
            email=str(claims.get('email') or ''),
            name=str(claims.get('name') or claims.get('given_name') or claims.get('email') or ''),
            raw_claims=claims,
        )
    if provider == 'microsoft':
        return NormalizedIdentity(
            provider='microsoft',
            sub=str(claims.get('sub') or claims.get('oid') or ''),
            email=str(claims.get('email') or claims.get('preferred_username') or claims.get('upn') or claims.get('mail') or ''),
            name=str(claims.get('name') or claims.get('preferred_username') or claims.get('email') or ''),
            tenant_id=str(claims.get('tid') or claims.get('tenantId') or '') or None,
            raw_claims=claims,
        )
    if provider == 'github':
        return NormalizedIdentity(
            provider='github',
            sub=str(claims.get('id') or claims.get('node_id') or ''),
            email=str(claims.get('email') or ''),
            name=str(claims.get('name') or claims.get('login') or claims.get('email') or ''),
            raw_claims=claims,
        )
    raise ValueError(provider)


def enforce_policy(settings: Settings, identity: NormalizedIdentity, claims: dict[str, Any]) -> None:
    email = (identity.email or '').lower()
    domain = email.rsplit('@', 1)[-1] if '@' in email else ''
    if identity.provider == 'google' and settings.google_allowed_domains:
        if domain not in {x.lower() for x in settings.google_allowed_domains}:
            raise ValueError('google_domain_not_allowed')
    if identity.provider == 'microsoft':
        tid = str(claims.get('tid') or claims.get('tenantId') or '').lower()
        if settings.microsoft_allowed_tenant_ids and tid not in {x.lower() for x in settings.microsoft_allowed_tenant_ids}:
            raise ValueError('microsoft_tenant_not_allowed')
        if settings.microsoft_allowed_domains and domain not in {x.lower() for x in settings.microsoft_allowed_domains}:
            raise ValueError('microsoft_domain_not_allowed')
    if identity.provider == 'github' and not identity.email:
        raise ValueError('github_email_required')