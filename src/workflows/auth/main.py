from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx
from auth import (
    build_authorize_url,
    build_validate_payload,
    callback_url,
    enforce_policy,
    exchange_code,
    fetch_github_orgs,
    fetch_userinfo,
    normalize_identity,
    normalize_next,
    pkce_challenge,
    pkce_verifier,
    provider_config,
    validate_id_token,
)
from db import (
    audit,
    consume_auth_transaction,
    create_pool,
    create_session,
    find_or_create_user,
    get_session,
    init_schema,
    insert_auth_transaction,
    prune,
    revoke_session,
    touch_session,
)
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from jwt.exceptions import ImmatureSignatureError, PyJWTError
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind, Status, StatusCode, get_current_span
from settings import enabled_providers, get_settings, validate_url
from telemetry import initialize_telemetry
from ui import denied_page, login_page

SETTINGS = get_settings()
logger = logging.getLogger("auth-service")
tracer = trace.get_tracer("auth-service.http")

REQUEST_ID_HEADER = "X-Request-Id"
SESSION_COOKIE_NAME = SETTINGS.session_cookie_name
SESSION_TTL_SECONDS = SETTINGS.session_ttl_seconds
AUTH_TX_TTL_SECONDS = SETTINGS.auth_tx_ttl_seconds


def _route_key(path: str) -> str:
    cleaned = path.rstrip("/")
    return cleaned if cleaned else "/"


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _request_id(request: Request) -> str:
    rid = getattr(request.state, "request_id", None)
    if isinstance(rid, str) and rid.strip():
        return rid.strip()

    header = request.headers.get("x-request-id", "").strip()
    if header:
        return header

    return uuid.uuid4().hex


def _security_headers(response: Response) -> None:
    response.headers.setdefault("Cache-Control", "no-store")
    response.headers.setdefault("Pragma", "no-cache")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "interest-cohort=()")
    response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")


def _json(payload: Any, status_code: int, request_id: str | None = None) -> JSONResponse:
    response = JSONResponse(payload, status_code=status_code)
    if request_id:
        response.headers[REQUEST_ID_HEADER] = request_id
    return response


def _html(content: str, status_code: int = 200, request_id: str | None = None) -> HTMLResponse:
    response = HTMLResponse(
        content=content,
        status_code=status_code,
        media_type="text/html; charset=utf-8",
    )
    if request_id:
        response.headers[REQUEST_ID_HEADER] = request_id
    return response


def _redirect(location: str, request_id: str | None = None) -> RedirectResponse:
    response = RedirectResponse(url=location, status_code=303)
    if request_id:
        response.headers[REQUEST_ID_HEADER] = request_id
    return response


def _html_error(title: str, detail: str, status_code: int, request_id: str) -> HTMLResponse:
    response = _html(
        denied_page(title, detail),
        status_code=status_code,
        request_id=request_id,
    )
    _security_headers(response)
    return response


def _frontend_home_url() -> str:
    return SETTINGS.app_home_url.rstrip("/") or SETTINGS.app_home_url


def _safe_relative_path(next_value: str | None) -> str:
    path = normalize_next(next_value)
    if not path:
        return "/"
    if not path.startswith("/") or path.startswith("//"):
        return "/"
    if any(ch in path for ch in ("\x00", "\r", "\n")):
        return "/"
    return path


def _frontend_url(path: str | None) -> str:
    home = _frontend_home_url()
    rel = _safe_relative_path(path)
    if rel == "/":
        return home
    return f"{home}{rel}"


def _resolve_session_id(request: Request) -> str | None:
    cookie_value = request.cookies.get(SESSION_COOKIE_NAME)
    if cookie_value and cookie_value.strip():
        return cookie_value.strip()

    authorization = request.headers.get("authorization", "").strip()
    if authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
        if token:
            return token

    return None


def _client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for", "").strip()
    if forwarded:
        candidate = forwarded.split(",", 1)[0].strip()
        if candidate:
            return candidate
    if request.client and request.client.host:
        return request.client.host
    return None


def _user_agent(request: Request) -> str | None:
    value = request.headers.get("user-agent", "").strip()
    return value or None


def _current_span() -> trace.Span | None:
    try:
        span = get_current_span()
        ctx = span.get_span_context()
        if ctx is None or not ctx.is_valid:
            return None
        return span
    except Exception:
        return None


def _span_event(event: str, **attrs: Any) -> None:
    span = _current_span()
    if span is not None:
        span.add_event(event, attributes={k: v for k, v in attrs.items() if v is not None})


def _span_error(exc: BaseException, **attrs: Any) -> None:
    span = _current_span()
    if span is not None:
        for key, value in attrs.items():
            if value is not None:
                span.set_attribute(key, value)
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR))


def _pool(request: Request):
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise RuntimeError("database pool is not initialized")
    return pool


def _provider_links(next_value: str) -> dict[str, str]:
    safe_next = quote(_safe_relative_path(next_value), safe="/?=&")
    return {
        provider: f"/login/start/{provider}?next={safe_next}"
        for provider in enabled_providers(SETTINGS)
    }


async def _provider_cfg(provider: str):
    provider = provider.lower().strip()
    if provider not in enabled_providers(SETTINGS):
        raise LookupError(provider)
    return await provider_config(SETTINGS, provider)


async def _session_lookup(request: Request, request_id: str, session_id: str) -> JSONResponse:
    try:
        session = await get_session(_pool(request), session_id)
        if session is not None:
            with suppress(Exception):
                await touch_session(_pool(request), session_id)
    except Exception as exc:
        _span_error(exc, request_id=request_id, error_type=exc.__class__.__name__)
        logger.exception("session_lookup_failed", extra={"request_id": request_id})
        return _json({"error": "auth_service_unavailable"}, 503, request_id)

    if session is None:
        return _json({"error": "invalid_session"}, 401, request_id)

    return _json(build_validate_payload(session), 200, request_id)


async def _login_start(request: Request, provider: str, next_value: str, request_id: str) -> Response:
    next_path = _safe_relative_path(next_value)

    try:
        cfg = await _provider_cfg(provider)
    except LookupError:
        return _html_error(
            "Provider unavailable",
            "That login provider is not enabled.",
            404,
            request_id,
        )
    except Exception as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.exception("provider_config_failed", extra={"provider": provider, "request_id": request_id})
        return _html_error(
            "Login unavailable",
            "Could not initialize the selected provider.",
            503,
            request_id,
        )

    state = uuid.uuid4().hex
    verifier = pkce_verifier()
    nonce = uuid.uuid4().hex
    challenge = pkce_challenge(verifier)
    redirect_uri = callback_url(SETTINGS, provider)
    expires_at = datetime.now(UTC) + timedelta(seconds=AUTH_TX_TTL_SECONDS)

    try:
        await insert_auth_transaction(
            _pool(request),
            state=state,
            provider=provider,
            code_verifier=verifier,
            nonce=nonce,
            redirect_uri=redirect_uri,
            return_to=next_path,
            expires_at=expires_at,
            ip_addr=_client_ip(request),
            user_agent=_user_agent(request),
        )
    except Exception as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.exception("auth_transaction_create_failed", extra={"provider": provider, "request_id": request_id})
        return _html_error(
            "Login unavailable",
            "Unable to create the login transaction.",
            503,
            request_id,
        )

    _span_event("auth.login.start", request_id=request_id, provider=provider, return_to=next_path)
    response = _redirect(build_authorize_url(cfg, state=state, code_challenge=challenge, nonce=nonce), request_id)
    _security_headers(response)
    return response


async def _callback(request: Request, provider: str, request_id: str) -> Response:
    provider = provider.lower().strip()

    if request.query_params.get("error"):
        details = f"{request.query_params.get('error')}: {request.query_params.get('error_description') or ''}".strip()
        return _html_error(
            "Login denied",
            "The identity provider rejected the login attempt."
            + (f" Details: {details}" if details else ""),
            403,
            request_id,
        )

    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code or not state:
        return _html_error(
            "Login failed",
            "Missing authorization response.",
            400,
            request_id,
        )

    try:
        cfg = await _provider_cfg(provider)
    except LookupError:
        return _html_error(
            "Provider unavailable",
            "That login provider is not enabled.",
            404,
            request_id,
        )
    except Exception as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.exception("provider_config_failed", extra={"provider": provider, "request_id": request_id})
        return _html_error(
            "Login unavailable",
            "Could not initialize the selected provider.",
            503,
            request_id,
        )

    try:
        tx = await consume_auth_transaction(_pool(request), state)
        if not isinstance(tx, dict):
            raise ValueError("invalid_auth_transaction")
        if tx.get("provider") != provider:
            raise ValueError("provider_mismatch")
        if tx.get("redirect_uri") != callback_url(SETTINGS, provider):
            raise ValueError("redirect_uri_mismatch")
        return_to = _safe_relative_path(str(tx.get("return_to") or "/"))
        nonce = str(tx.get("nonce") or "")
        code_verifier = str(tx.get("code_verifier") or "")
    except (ValueError, KeyError, LookupError) as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.warning(
            "auth_transaction_invalid",
            extra={"provider": provider, "request_id": request_id, "error": str(exc)},
        )
        return _html_error(
            "Login failed",
            "The login transaction is invalid or expired.",
            400,
            request_id,
        )
    except Exception as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.exception("auth_transaction_consume_failed", extra={"provider": provider, "request_id": request_id})
        return _html_error(
            "Login unavailable",
            "The authentication service is temporarily unavailable.",
            503,
            request_id,
        )

    try:
        token_response = await exchange_code(cfg, code, code_verifier)
        access_token = token_response.get("access_token") or ""
        id_token = token_response.get("id_token") or ""

        claims: dict[str, Any] = {}
        if cfg.oidc:
            try:
                claims.update(await validate_id_token(cfg, id_token, nonce))
            except ImmatureSignatureError:
                return _html_error(
                    "Login unavailable",
                    "The identity provider token was issued too recently. Please try again.",
                    503,
                    request_id,
                )
            except PyJWTError as exc:
                _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
                logger.warning("id_token_invalid", extra={"provider": provider, "request_id": request_id})
                return _html_error(
                    "Login failed",
                    "The identity provider token could not be validated.",
                    403,
                    request_id,
                )

            with suppress(Exception):
                userinfo = await fetch_userinfo(cfg, access_token)
                if userinfo:
                    claims.update(userinfo)
        else:
            userinfo = await fetch_userinfo(cfg, access_token)
            if userinfo:
                claims.update(userinfo)
            if access_token:
                claims["orgs"] = await fetch_github_orgs(access_token)

        identity = normalize_identity(provider, claims)
        if not identity.sub:
            raise ValueError("identity_missing_subject")
        if not identity.email:
            raise ValueError("identity_missing_email")

        enforce_policy(SETTINGS, identity, claims)

        if provider == "github" and SETTINGS.github_allowed_orgs:
            allowed = {org.lower() for org in SETTINGS.github_allowed_orgs}
            orgs = {str(org).lower() for org in claims.get("orgs", []) if str(org).strip()}
            if not orgs.intersection(allowed):
                raise ValueError("github_org_not_allowed")

        user, _external = await find_or_create_user(
            _pool(request),
            {
                "provider": identity.provider,
                "sub": identity.sub,
                "email": identity.email,
                "name": identity.name or identity.email,
                "tenant_id": identity.tenant_id,
            },
        )

        session = await create_session(
            _pool(request),
            user_id=user["id"],
            ttl_seconds=SESSION_TTL_SECONDS,
            ip_addr=_client_ip(request),
            user_agent=_user_agent(request),
        )

        with suppress(Exception):
            await audit(
                _pool(request),
                "login_success",
                provider=identity.provider,
                user_id=user["id"],
                session_id=session["id"],
                subject=identity.sub,
                detail={"return_to": return_to},
                ip_addr=_client_ip(request),
                user_agent=_user_agent(request),
            )

        response = _redirect(_frontend_url(return_to), request_id)
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=session["id"],
            max_age=SESSION_TTL_SECONDS,
            expires=SESSION_TTL_SECONDS,
            path=SETTINGS.session_cookie_path,
            domain=SETTINGS.session_cookie_domain,
            secure=SETTINGS.session_cookie_secure,
            httponly=True,
            samesite=SETTINGS.session_cookie_samesite,
        )
        _security_headers(response)
        return response

    except httpx.TimeoutException as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type="timeout")
        logger.warning("provider_timeout", extra={"provider": provider, "request_id": request_id})
        return _html_error(
            "Login unavailable",
            "The identity provider timed out.",
            503,
            request_id,
        )
    except httpx.RequestError as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.warning("provider_unreachable", extra={"provider": provider, "request_id": request_id})
        return _html_error(
            "Login unavailable",
            "The identity provider is unavailable.",
            503,
            request_id,
        )
    except httpx.HTTPStatusError as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.warning("provider_http_error", extra={"provider": provider, "request_id": request_id})
        return _html_error(
            "Login failed",
            "The identity provider rejected the exchange.",
            502,
            request_id,
        )
    except ValueError as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.warning("login_failed", extra={"provider": provider, "request_id": request_id, "error": str(exc)})
        return _html_error(
            "Login failed",
            "Authentication was rejected.",
            403,
            request_id,
        )
    except Exception as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.exception("callback_failed", extra={"provider": provider, "request_id": request_id})
        return _html_error(
            "Login failed",
            "The authentication flow could not be completed.",
            502,
            request_id,
        )


async def _logout(request: Request, request_id: str) -> Response:
    session_id = _resolve_session_id(request)
    if session_id:
        try:
            session = await get_session(_pool(request), session_id)
            if session is not None:
                with suppress(Exception):
                    await revoke_session(_pool(request), session_id)
                with suppress(Exception):
                    await audit(
                        _pool(request),
                        "logout",
                        provider=None,
                        user_id=session.get("user_id"),
                        session_id=session.get("id"),
                        subject=None,
                        detail={},
                        ip_addr=_client_ip(request),
                        user_agent=_user_agent(request),
                    )
        except Exception as exc:
            _span_error(exc, request_id=request_id, error_type=exc.__class__.__name__)
            logger.exception("logout_session_lookup_failed", extra={"request_id": request_id})

    response = _redirect(_frontend_home_url(), request_id)
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path=SETTINGS.session_cookie_path,
        domain=SETTINGS.session_cookie_domain,
        secure=SETTINGS.session_cookie_secure,
        httponly=True,
        samesite=SETTINGS.session_cookie_samesite,
    )
    _security_headers(response)
    return response


async def _readyz(request: Request, request_id: str) -> Response:
    try:
        await _pool(request).fetchval("SELECT 1")
    except Exception as exc:
        _span_error(exc, request_id=request_id, error_type=exc.__class__.__name__)
        return _json({"status": "error", "detail": "database unavailable"}, 503, request_id)

    return _json(
        {
            "status": "ok",
            "service_name": SETTINGS.service_name,
            "service_version": SETTINGS.service_version,
            "app_home_url": _frontend_home_url(),
            "validate_route": validate_url(SETTINGS),
            "me_route": f"{SETTINGS.app_base_url}{SETTINGS.me_route_path}",
        },
        200,
        request_id,
    )


async def _root(_: Request, request_id: str) -> Response:
    return _json(
        {
            "status": "ok",
            "service_name": SETTINGS.service_name,
            "service_version": SETTINGS.service_version,
            "environment": SETTINGS.environment,
            "enabled_providers": enabled_providers(SETTINGS),
            "app_home_url": _frontend_home_url(),
            "validate_route": validate_url(SETTINGS),
            "me_route": f"{SETTINGS.app_base_url}{SETTINGS.me_route_path}",
        },
        200,
        request_id,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    telemetry = None
    pool = None
    prune_task: asyncio.Task[None] | None = None

    try:
        try:
            telemetry = initialize_telemetry(SETTINGS)
        except Exception:
            logger.exception("telemetry_initialization_failed")
            telemetry = None

        pool = await create_pool(
            SETTINGS.postgres_dsn,
            SETTINGS.postgres_min_size,
            SETTINGS.postgres_max_size,
        )
        await init_schema(pool)
        await prune(pool)

        app.state.pool = pool
        app.state.telemetry = telemetry

        async def _prune_loop() -> None:
            interval = max(300, min(3600, SETTINGS.auth_tx_ttl_seconds // 2 or 300))
            while True:
                try:
                    await asyncio.sleep(interval)
                    await prune(pool)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("periodic_prune_failed")

        prune_task = asyncio.create_task(_prune_loop())
        app.state.prune_task = prune_task

        yield

    finally:
        if prune_task is not None:
            prune_task.cancel()
            try:
                await prune_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("prune_task_shutdown_failed")

        if pool is not None:
            try:
                await pool.close()
            except Exception:
                logger.exception("db_pool_close_failed")

        if telemetry is not None:
            try:
                telemetry.shutdown()
            except Exception:
                logger.exception("telemetry_shutdown_failed")


app = FastAPI(
    title=SETTINGS.app_name,
    version=SETTINGS.service_version,
    lifespan=lifespan,
    docs_url=None if not SETTINGS.dev_mode else "/docs",
    redoc_url=None if not SETTINGS.dev_mode else "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=SETTINGS.web_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "HEAD", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=[REQUEST_ID_HEADER],
)


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    request_id = _request_id(request)
    request.state.request_id = request_id

    route = _route_key(request.url.path)
    method = request.method.upper()
    ctx = extract(dict(request.headers))
    started = _now_ms()
    status_code = 500

    with tracer.start_as_current_span(
        f"HTTP {method} {route}",
        context=ctx,
        kind=SpanKind.SERVER,
    ) as span:
        span.set_attribute("http.method", method)
        span.set_attribute("http.route", route)
        span.set_attribute("http.request_id", request_id)
        span.set_attribute("service.name", SETTINGS.service_name)
        span.set_attribute("service.version", SETTINGS.service_version)
        span.set_attribute("deployment.environment", SETTINGS.deployment_environment)
        span.set_attribute("k8s.cluster.name", SETTINGS.cluster_name)
        span.set_attribute("service.instance.id", SETTINGS.instance_id)

        try:
            response = await call_next(request)
            status_code = response.status_code
            span.set_attribute("http.status_code", status_code)
            if status_code >= 500:
                span.set_status(Status(StatusCode.ERROR))
        except asyncio.CancelledError:
            span.set_status(Status(StatusCode.ERROR))
            raise
        except Exception as exc:
            _span_error(exc, http_route=route, request_id=request_id, error_type=exc.__class__.__name__)
            logger.exception("request_failed", extra={"route": route, "request_id": request_id})
            response = _json({"detail": "Internal server error"}, 500, request_id)
            status_code = 500

        elapsed = _now_ms() - started
        span.set_attribute("http.duration_ms", round(elapsed, 3))
        span.set_attribute("http.status_code", status_code)

    _security_headers(response)
    if not response.headers.get(REQUEST_ID_HEADER):
        response.headers[REQUEST_ID_HEADER] = request_id
    return response


@app.api_route("/", methods=["GET", "HEAD"])
async def root(request: Request):
    return await _root(request, _request_id(request))


@app.api_route("/healthz", methods=["GET", "HEAD"])
async def healthz(request: Request):
    return _json({"status": "ok"}, 200, _request_id(request))


@app.api_route("/readyz", methods=["GET", "HEAD"])
async def readyz(request: Request):
    return await _readyz(request, _request_id(request))


@app.api_route(SETTINGS.me_route_path, methods=["GET", "HEAD"])
async def me(request: Request):
    request_id = _request_id(request)
    session_id = _resolve_session_id(request)
    if not session_id:
        return _json({"error": "invalid_session"}, 401, request_id)
    return await _session_lookup(request, request_id, session_id)


@app.api_route(SETTINGS.validate_route_path, methods=["GET", "HEAD"])
async def validate(request: Request):
    request_id = _request_id(request)
    session_id = _resolve_session_id(request)
    if not session_id:
        return _json({"error": "invalid_session"}, 401, request_id)
    return await _session_lookup(request, request_id, session_id)


@app.api_route("/logout", methods=["GET", "POST"])
async def logout(request: Request):
    return await _logout(request, _request_id(request))


@app.api_route("/login", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def login(request: Request):
    request_id = _request_id(request)
    next_value = _safe_relative_path(request.query_params.get("next") or "/")
    providers = enabled_providers(SETTINGS)
    html = login_page(
        SETTINGS.app_name,
        providers,
        _provider_links(next_value),
        "Use an enabled provider to sign in.",
    )
    return _html(html, 200, request_id)


@app.get("/login/start/{provider}")
async def login_start(request: Request, provider: str):
    return await _login_start(
        request,
        provider,
        request.query_params.get("next") or "/",
        _request_id(request),
    )


@app.get("/callback/{provider}")
async def callback(request: Request, provider: str):
    return await _callback(request, provider, _request_id(request))


@app.get("/metrics")
async def metrics_endpoint(request: Request):
    request_id = _request_id(request)
    if not SETTINGS.metrics_enabled:
        return Response(status_code=404, headers={REQUEST_ID_HEADER: request_id})

    try:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
    except Exception:
        return Response(status_code=404, headers={REQUEST_ID_HEADER: request_id})

    response = Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
    response.headers[REQUEST_ID_HEADER] = request_id
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level=SETTINGS.log_level.lower(),
    )