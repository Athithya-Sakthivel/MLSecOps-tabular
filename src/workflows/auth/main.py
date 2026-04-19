from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

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
    validate_session,
)
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind, Status, StatusCode
from settings import enabled_providers, get_settings, validate_url
from telemetry import initialize_telemetry
from ui import denied_page, login_page

SETTINGS = get_settings()

logger = logging.getLogger("auth-service")
tracer = trace.get_tracer("auth-service.http")

SESSION_COOKIE_NAME = SETTINGS.session_cookie_name
SESSION_TTL_SECONDS = SETTINGS.session_ttl_seconds
AUTH_TX_TTL_SECONDS = SETTINGS.auth_tx_ttl_seconds
REQ_ID_HEADER = "X-Request-Id"


def _route_key(path: str) -> str:
    cleaned = path.rstrip("/")
    return cleaned if cleaned else "/"


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _request_id(request: Request) -> str:
    rid = (request.headers.get("x-request-id") or uuid.uuid4().hex).strip()
    return rid or uuid.uuid4().hex


def _request_id_from_state(request: Request) -> str:
    rid = getattr(request.state, "request_id", None)
    if isinstance(rid, str) and rid.strip():
        return rid.strip()
    return _request_id(request)


def _client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for", "").strip()
    if forwarded:
        ip = forwarded.split(",", 1)[0].strip()
        if ip:
            return ip
    if request.client and request.client.host:
        return request.client.host
    return None


def _user_agent(request: Request) -> str | None:
    ua = request.headers.get("user-agent", "").strip()
    return ua or None


def _json(payload: Any, status_code: int, request_id: str | None = None) -> JSONResponse:
    headers = {REQ_ID_HEADER: request_id} if request_id else None
    return JSONResponse(payload, status_code=status_code, headers=headers)


def _html(html: str, status_code: int = 200, request_id: str | None = None) -> HTMLResponse:
    headers = {REQ_ID_HEADER: request_id} if request_id else None
    return HTMLResponse(html, status_code=status_code, headers=headers)


def _redirect(location: str, request_id: str | None = None) -> RedirectResponse:
    resp = RedirectResponse(location=location, status_code=303)
    if request_id:
        resp.headers[REQ_ID_HEADER] = request_id
    return resp


def _security_headers(response: Response) -> None:
    response.headers.setdefault("Cache-Control", "no-store")
    response.headers.setdefault("Pragma", "no-cache")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "interest-cohort=()")
    response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")


def _set_session_cookie(response: Response, session_id: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        max_age=SESSION_TTL_SECONDS,
        expires=SESSION_TTL_SECONDS,
        path=SETTINGS.session_cookie_path,
        domain=SETTINGS.session_cookie_domain,
        secure=SETTINGS.session_cookie_secure,
        httponly=True,
        samesite=SETTINGS.session_cookie_samesite,
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path=SETTINGS.session_cookie_path,
        domain=SETTINGS.session_cookie_domain,
    )


def _resolve_session_id(request: Request) -> str | None:
    cookie_value = request.cookies.get(SESSION_COOKIE_NAME)
    if cookie_value and cookie_value.strip():
        return cookie_value.strip()

    authz = request.headers.get("authorization", "").strip()
    if authz.lower().startswith("bearer "):
        token = authz[7:].strip()
        if token:
            return token
    return None


def _span_event(event: str, **attrs: Any) -> None:
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx is not None and ctx.is_valid:
        span.add_event(event, attributes={k: v for k, v in attrs.items() if v is not None})


def _span_error(exc: BaseException, **attrs: Any) -> None:
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx is not None and ctx.is_valid:
        for key, value in attrs.items():
            if value is not None:
                span.set_attribute(key, value)
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR))


def _auth_error(
    title: str,
    message: str,
    *,
    details: str | None = None,
    allowed: str | None = None,
    request_id: str | None = None,
    status_code: int = 400,
) -> HTMLResponse:
    return _html(
        denied_page(title, message, details=details, allowed=allowed),
        status_code=status_code,
        request_id=request_id,
    )


def _active_payload(session: dict[str, Any]) -> dict[str, Any]:
    payload = build_validate_payload(session)
    payload["user_status"] = session.get("user_status")
    return payload


def _pool(request: Request):
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise RuntimeError("database pool is not initialized")
    return pool


async def _provider_config(provider: str):
    provider = provider.lower().strip()
    if provider not in enabled_providers(SETTINGS):
        raise LookupError(provider)
    return await provider_config(SETTINGS, provider)


def _provider_links(next_value: str) -> dict[str, str]:
    return {p: f"/login/start/{p}?next={next_value}" for p in enabled_providers(SETTINGS)}


async def _login_start(request: Request, provider: str, next_value: str, request_id: str) -> Response:
    next_path = normalize_next(next_value)
    try:
        cfg = await _provider_config(provider)
    except LookupError:
        return _auth_error(
            "Provider unavailable",
            "That login provider is not enabled.",
            request_id=request_id,
            status_code=404,
        )
    except Exception as exc:
        _span_error(exc, request_id=request_id, provider=provider)
        logger.exception("provider_config_failed", extra={"provider": provider, "request_id": request_id})
        return _auth_error(
            "Login unavailable",
            "Could not initialize the selected provider.",
            details=str(exc),
            request_id=request_id,
            status_code=503,
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
        return _auth_error(
            "Login unavailable",
            "Unable to create the login transaction.",
            details=str(exc),
            request_id=request_id,
            status_code=503,
        )

    _span_event("auth.login.start", request_id=request_id, provider=provider, return_to=next_path)
    response = _redirect(
        build_authorize_url(cfg, state=state, code_challenge=challenge, nonce=nonce),
        request_id,
    )
    _security_headers(response)
    return response


async def _callback(request: Request, provider: str, request_id: str) -> Response:
    provider = provider.lower().strip()
    pool = _pool(request)

    if request.query_params.get("error"):
        return _auth_error(
            "Login denied",
            "The identity provider rejected the login attempt.",
            details=f"{request.query_params.get('error')}: {request.query_params.get('error_description') or ''}".strip(),
            request_id=request_id,
            status_code=403,
        )

    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code or not state:
        return _auth_error("Login failed", "Missing authorization response.", request_id=request_id, status_code=400)

    try:
        cfg = await _provider_config(provider)
    except LookupError:
        return _auth_error(
            "Provider unavailable",
            "That login provider is not enabled.",
            request_id=request_id,
            status_code=404,
        )
    except Exception as exc:
        _span_error(exc, request_id=request_id, provider=provider)
        logger.exception("provider_config_failed", extra={"provider": provider, "request_id": request_id})
        return _auth_error(
            "Login unavailable",
            "Could not initialize the selected provider.",
            details=str(exc),
            request_id=request_id,
            status_code=503,
        )

    try:
        tx = await consume_auth_transaction(
            pool,
            state,
            expected_provider=provider,
            expected_redirect_uri=callback_url(SETTINGS, provider),
        )
    except ValueError as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.warning(
            "auth_transaction_invalid",
            extra={"provider": provider, "request_id": request_id, "error": str(exc)},
        )
        return _auth_error(
            "Login failed",
            "The login transaction is invalid or expired.",
            details=str(exc),
            request_id=request_id,
            status_code=400,
        )

    try:
        token_response = await exchange_code(cfg, code, tx["code_verifier"])
        access_token = token_response.get("access_token") or ""
        id_token = token_response.get("id_token") or ""

        claims: dict[str, Any] = {}
        if cfg.oidc:
            claims.update(await validate_id_token(cfg, id_token, tx["nonce"]))
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
        enforce_policy(SETTINGS, identity, claims)

        if provider == "github" and SETTINGS.github_allowed_orgs:
            allowed = {org.lower() for org in SETTINGS.github_allowed_orgs}
            orgs = {str(org).lower() for org in claims.get("orgs", []) if str(org).strip()}
            if not orgs.intersection(allowed):
                raise ValueError("github_org_not_allowed")

        user, external = await find_or_create_user(pool, identity)
        session = await create_session(
            pool,
            user_id=user["id"],
            ttl_seconds=SESSION_TTL_SECONDS,
            ip_addr=_client_ip(request),
            user_agent=_user_agent(request),
            provider=identity.provider,
            provider_sub=identity.sub,
            provider_email=identity.email,
            provider_name=identity.name,
            tenant_id=identity.tenant_id,
        )

        await audit(
            pool,
            "login_success",
            provider=identity.provider,
            user_id=user["id"],
            session_id=session["id"],
            subject=identity.sub,
            detail={"external_identity_id": external.get("id"), "return_to": tx["return_to"]},
            ip_addr=_client_ip(request),
            user_agent=_user_agent(request),
        )

        response = _redirect(normalize_next(tx["return_to"]), request_id)
        _set_session_cookie(response, session["id"])
        _security_headers(response)
        return response

    except httpx.TimeoutException as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type="timeout")
        logger.warning("provider_timeout", extra={"provider": provider, "request_id": request_id})
        return _auth_error(
            "Login unavailable",
            "The identity provider timed out.",
            details=str(exc),
            request_id=request_id,
            status_code=503,
        )
    except httpx.RequestError as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.warning("provider_unreachable", extra={"provider": provider, "request_id": request_id})
        return _auth_error(
            "Login unavailable",
            "The identity provider is unavailable.",
            details=str(exc),
            request_id=request_id,
            status_code=503,
        )
    except httpx.HTTPStatusError as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.warning("provider_http_error", extra={"provider": provider, "request_id": request_id})
        return _auth_error(
            "Login failed",
            "The identity provider rejected the exchange.",
            details=str(exc),
            request_id=request_id,
            status_code=502,
        )
    except ValueError as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.warning("login_failed", extra={"provider": provider, "request_id": request_id, "error": str(exc)})
        return _auth_error(
            "Login failed",
            "Authentication was rejected.",
            details=str(exc),
            request_id=request_id,
            status_code=403,
        )
    except Exception as exc:
        _span_error(exc, request_id=request_id, provider=provider, error_type=exc.__class__.__name__)
        logger.exception("callback_failed", extra={"provider": provider, "request_id": request_id})
        return _auth_error(
            "Login failed",
            "The authentication flow could not be completed.",
            details=str(exc),
            request_id=request_id,
            status_code=502,
        )


async def _me(request: Request, request_id: str) -> Response:
    session_id = _resolve_session_id(request)
    if not session_id:
        return _json({"error": "invalid_session"}, 401, request_id)

    try:
        session = await get_session(_pool(request), session_id)
    except Exception as exc:
        _span_error(exc, request_id=request_id, error_type=exc.__class__.__name__)
        logger.exception("session_lookup_failed", extra={"request_id": request_id})
        return _json({"error": "auth_service_unavailable"}, 503, request_id)

    if session is None:
        return _json({"error": "invalid_session"}, 401, request_id)

    _span_event("auth.me.success", request_id=request_id, session_id=session["id"], user_id=session["user_id"])
    return _json(_active_payload(session), 200, request_id)


async def _validate(request: Request, request_id: str) -> Response:
    session_id = _resolve_session_id(request)
    if not session_id:
        return _json({"error": "invalid_session"}, 401, request_id)

    try:
        session = await validate_session(_pool(request), session_id)
    except Exception as exc:
        _span_error(exc, request_id=request_id, error_type=exc.__class__.__name__)
        logger.exception("session_validation_failed", extra={"request_id": request_id})
        return _json({"error": "auth_service_unavailable"}, 503, request_id)

    if session is None:
        return _json({"error": "invalid_session"}, 401, request_id)

    _span_event(
        "auth.validate.success",
        request_id=request_id,
        session_id=session["id"],
        user_id=session["user_id"],
        provider=session.get("provider"),
    )
    return _json(_active_payload(session), 200, request_id)


async def _logout(request: Request, request_id: str) -> Response:
    pool = _pool(request)
    session_id = _resolve_session_id(request)

    if session_id:
        try:
            session = await get_session(pool, session_id)
        except Exception as exc:
            _span_error(exc, request_id=request_id, error_type=exc.__class__.__name__)
            logger.exception("logout_session_lookup_failed", extra={"request_id": request_id})
            return _json({"error": "auth_service_unavailable"}, 503, request_id)

        if session is not None:
            try:
                await revoke_session(pool, session_id)
                await audit(
                    pool,
                    "logout",
                    provider=session.get("provider"),
                    user_id=session.get("user_id"),
                    session_id=session.get("id"),
                    subject=session.get("provider_sub"),
                    detail={},
                    ip_addr=_client_ip(request),
                    user_agent=_user_agent(request),
                )
            except Exception as exc:
                _span_error(exc, request_id=request_id, error_type=exc.__class__.__name__)
                logger.exception("logout_failed", extra={"request_id": request_id})
                return _json({"error": "auth_service_unavailable"}, 503, request_id)

            _span_event(
                "auth.logout",
                request_id=request_id,
                session_id=session_id,
                user_id=session.get("user_id"),
            )

    response = _json({"status": "ok"}, 200, request_id)
    _clear_session_cookie(response)
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
        telemetry = initialize_telemetry(SETTINGS)

        meter = metrics.get_meter("auth-service.http")
        app.state.meter = meter
        app.state.http_request_counter = meter.create_counter(
            name="http.server.request_count",
            description="Total HTTP requests",
            unit="1",
        )
        app.state.http_error_counter = meter.create_counter(
            name="http.server.errors",
            description="Total failed HTTP requests",
            unit="1",
        )
        app.state.http_duration_histogram = meter.create_histogram(
            name="http.server.duration",
            description="HTTP request latency",
            unit="ms",
        )
        app.state.http_active_requests = meter.create_up_down_counter(
            name="active.requests",
            description="Number of in-flight HTTP requests",
            unit="1",
        )

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


@app.middleware("http")
async def middleware(request: Request, call_next):
    request_id = _request_id(request)
    request.state.request_id = request_id

    route = _route_key(request.url.path)
    method = request.method.upper()

    meter_request_counter = getattr(request.app.state, "http_request_counter", None)
    meter_error_counter = getattr(request.app.state, "http_error_counter", None)
    meter_duration = getattr(request.app.state, "http_duration_histogram", None)
    meter_active = getattr(request.app.state, "http_active_requests", None)

    if meter_request_counter is not None:
        meter_request_counter.add(1, attributes={"route": route, "method": method})
    if meter_active is not None:
        meter_active.add(1, attributes={"route": route})

    ctx = extract(dict(request.headers))
    started = _now_ms()
    status_code = 500
    response: Response

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
        if meter_duration is not None:
            meter_duration.record(
                elapsed,
                attributes={"route": route, "method": method, "status_code": status_code},
            )
        if meter_error_counter is not None and status_code >= 400:
            meter_error_counter.add(
                1,
                attributes={"route": route, "method": method, "status_code": status_code},
            )
        if meter_active is not None:
            meter_active.add(-1, attributes={"route": route})

    _security_headers(response)
    if request_id and not response.headers.get(REQ_ID_HEADER):
        response.headers[REQ_ID_HEADER] = request_id
    return response


@app.get("/")
async def root(request: Request):
    return await _root(request, _request_id_from_state(request))


@app.get("/healthz")
async def healthz(request: Request):
    return _json({"status": "ok"}, 200, _request_id_from_state(request))


@app.get("/readyz")
async def readyz(request: Request):
    return await _readyz(request, _request_id_from_state(request))


@app.get(SETTINGS.me_route_path)
async def me(request: Request):
    return await _me(request, _request_id_from_state(request))


@app.get(SETTINGS.validate_route_path)
async def validate(request: Request):
    return await _validate(request, _request_id_from_state(request))


@app.post("/logout")
async def logout(request: Request):
    return await _logout(request, _request_id_from_state(request))


@app.get("/login")
async def login(request: Request):
    next_value = normalize_next(request.query_params.get("next"))
    html = login_page(
        SETTINGS.app_name,
        enabled_providers(SETTINGS),
        _provider_links(next_value),
        "Use an enabled provider to sign in.",
    )
    return _html(html, 200, _request_id_from_state(request))


@app.get("/login/start/{provider}")
async def login_start(request: Request, provider: str):
    return await _login_start(
        request,
        provider,
        request.query_params.get("next") or "/",
        _request_id_from_state(request),
    )


@app.get("/callback/{provider}")
async def callback(request: Request, provider: str):
    return await _callback(request, provider, _request_id_from_state(request))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level=SETTINGS.log_level.lower(),
    )