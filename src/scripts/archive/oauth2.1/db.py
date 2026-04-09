from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import asyncpg
except Exception as exc:  # pragma: no cover
    asyncpg = None  # type: ignore
    _ASYNC_PG_IMPORT_ERROR = exc
else:
    _ASYNC_PG_IMPORT_ERROR = None

Pool = asyncpg.Pool if asyncpg is not None else Any


async def create_pool(dsn: str, min_size: int, max_size: int) -> Pool:
    if asyncpg is None:
        raise RuntimeError(f'asyncpg is required but could not be imported: {_ASYNC_PG_IMPORT_ERROR!r}')
    return await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size, command_timeout=20)


async def init_schema(pool: Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                primary_email TEXT,
                display_name TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_login_at TIMESTAMPTZ
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_identities (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                provider TEXT NOT NULL,
                provider_sub TEXT NOT NULL,
                provider_email TEXT,
                provider_name TEXT,
                tenant_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (provider, provider_sub)
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_transactions (
                state TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                code_verifier TEXT NOT NULL,
                nonce TEXT NOT NULL,
                redirect_uri TEXT NOT NULL,
                return_to TEXT NOT NULL DEFAULT '/',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at TIMESTAMPTZ NOT NULL,
                consumed_at TIMESTAMPTZ,
                ip_addr TEXT,
                user_agent TEXT
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at TIMESTAMPTZ NOT NULL,
                revoked_at TIMESTAMPTZ,
                last_seen_at TIMESTAMPTZ,
                ip_addr TEXT,
                user_agent TEXT,
                csrf_token TEXT NOT NULL,
                session_version INTEGER NOT NULL DEFAULT 1
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_events (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                event_type TEXT NOT NULL,
                provider TEXT,
                user_id TEXT,
                session_id TEXT,
                subject TEXT,
                detail JSONB NOT NULL DEFAULT '{}'::jsonb,
                ip_addr TEXT,
                user_agent TEXT
            );
            """
        )
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_auth_transactions_expires_at ON auth_transactions (expires_at);')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_app_sessions_user_id ON app_sessions (user_id);')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_app_sessions_expires_at ON app_sessions (expires_at);')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_external_identities_user_id ON external_identities (user_id);')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events (created_at DESC);')
        await conn.execute("DELETE FROM auth_transactions WHERE expires_at < now() - interval '1 day';")
        await conn.execute("DELETE FROM app_sessions WHERE expires_at < now() - interval '1 day' AND revoked_at IS NULL;")


async def prune(pool: Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute('DELETE FROM auth_transactions WHERE expires_at < now();')
        await conn.execute('UPDATE app_sessions SET revoked_at = COALESCE(revoked_at, now()) WHERE expires_at < now() AND revoked_at IS NULL;')


async def audit(pool: Pool, event_type: str, *, provider: str | None = None, user_id: str | None = None, session_id: str | None = None, subject: str | None = None, detail: dict[str, Any] | None = None, ip_addr: str | None = None, user_agent: str | None = None) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO audit_events (event_type, provider, user_id, session_id, subject, detail, ip_addr, user_agent)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            """,
            event_type,
            provider,
            user_id,
            session_id,
            subject,
            json.dumps(detail or {}, separators=(',', ':')),
            ip_addr,
            user_agent,
        )


async def insert_auth_transaction(pool: Pool, *, state: str, provider: str, code_verifier: str, nonce: str, redirect_uri: str, return_to: str, expires_at: datetime, ip_addr: str | None, user_agent: str | None) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO auth_transactions (state, provider, code_verifier, nonce, redirect_uri, return_to, created_at, expires_at, consumed_at, ip_addr, user_agent)
            VALUES ($1, $2, $3, $4, $5, $6, now(), $7, NULL, $8, $9)
            ON CONFLICT (state) DO NOTHING
            """,
            state,
            provider,
            code_verifier,
            nonce,
            redirect_uri,
            return_to,
            expires_at,
            ip_addr,
            user_agent,
        )


async def consume_auth_transaction(pool: Pool, state: str) -> dict[str, Any]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow('SELECT * FROM auth_transactions WHERE state = $1 FOR UPDATE', state)
            if not row:
                raise ValueError('invalid_state')
            if row['consumed_at'] is not None:
                raise ValueError('state_consumed')
            if row['expires_at'] < datetime.now(timezone.utc):
                raise ValueError('state_expired')
            await conn.execute('UPDATE auth_transactions SET consumed_at = now() WHERE state = $1', state)
            return dict(row)


async def find_or_create_user(pool: Pool, identity: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    provider = identity['provider']
    sub = identity['sub']
    email = identity.get('email') or None
    name = identity.get('name') or ''
    tenant_id = identity.get('tenant_id') or None

    async with pool.acquire() as conn:
        async with conn.transaction():
            ext = await conn.fetchrow('SELECT * FROM external_identities WHERE provider = $1 AND provider_sub = $2', provider, sub)
            if ext:
                user = await conn.fetchrow('SELECT * FROM users WHERE id = $1', ext['user_id'])
                if not user:
                    raise RuntimeError('dangling_external_identity')
                await conn.execute(
                    """
                    UPDATE users
                    SET primary_email = COALESCE($2, primary_email),
                        display_name = COALESCE(NULLIF($3, ''), display_name),
                        updated_at = now(),
                        last_login_at = now()
                    WHERE id = $1
                    """,
                    user['id'],
                    email,
                    name,
                )
                await conn.execute(
                    """
                    UPDATE external_identities
                    SET provider_email = $2,
                        provider_name = NULLIF($3, ''),
                        tenant_id = $4,
                        updated_at = now()
                    WHERE provider = $1 AND provider_sub = $5
                    """,
                    provider,
                    email,
                    name,
                    tenant_id,
                    sub,
                )
                user = await conn.fetchrow('SELECT * FROM users WHERE id = $1', user['id'])
                ext = await conn.fetchrow('SELECT * FROM external_identities WHERE provider = $1 AND provider_sub = $2', provider, sub)
                return dict(user), dict(ext)

            user_id = str(uuid.uuid4())
            user = await conn.fetchrow(
                """
                INSERT INTO users (id, primary_email, display_name, status, created_at, updated_at, last_login_at)
                VALUES ($1, $2, $3, 'active', now(), now(), now())
                RETURNING *
                """,
                user_id,
                email,
                name or email or sub,
            )
            ext = await conn.fetchrow(
                """
                INSERT INTO external_identities (user_id, provider, provider_sub, provider_email, provider_name, tenant_id, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, now(), now())
                RETURNING *
                """,
                user_id,
                provider,
                sub,
                email,
                name,
                tenant_id,
            )
            return dict(user), dict(ext)


async def create_session(pool: Pool, *, user_id: str, ttl_seconds: int, ip_addr: str | None, user_agent: str | None) -> dict[str, Any]:
    session_id = str(uuid.uuid4())
    csrf_token = str(uuid.uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO app_sessions (id, user_id, created_at, expires_at, revoked_at, last_seen_at, ip_addr, user_agent, csrf_token, session_version)
            VALUES ($1, $2, now(), $3, NULL, now(), $4, $5, $6, 1)
            RETURNING *
            """,
            session_id,
            user_id,
            expires_at,
            ip_addr,
            user_agent,
            csrf_token,
        )
        return dict(row)


async def touch_session(pool: Pool, session_id: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute('UPDATE app_sessions SET last_seen_at = now() WHERE id = $1', session_id)


async def get_session(pool: Pool, session_id: str) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT s.*, u.primary_email AS user_email, u.display_name AS user_name, u.status AS user_status
            FROM app_sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.id = $1
            """,
            session_id,
        )
        if not row:
            return None
        if row['revoked_at'] is not None or row['expires_at'] < datetime.now(timezone.utc):
            return None
        return dict(row)


async def revoke_session(pool: Pool, session_id: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute('UPDATE app_sessions SET revoked_at = now() WHERE id = $1', session_id)