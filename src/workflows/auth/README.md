# Auth Service

Production-oriented authentication service implementing OAuth 2.1 authorization-code flow with PKCE, OpenID Connect for Google and Microsoft, and OAuth-based GitHub login. The service stores auth transactions, identities, and sessions in PostgreSQL and exposes a browser-facing login surface plus session-backed user lookup.

## Architecture

The service is split into five files with strict responsibilities:

- `main.py` — FastAPI application, routes, middleware, startup/shutdown lifecycle, metrics, and request orchestration.
- `settings.py` — Environment-driven configuration loading and runtime validation.
- `db.py` — PostgreSQL schema bootstrap, connection management, transactional persistence, and audit/session/user operations.
- `auth.py` — OAuth/OIDC protocol logic, PKCE generation, authorization URL creation, token exchange, ID token validation, provider-specific identity normalization, and policy enforcement.
- `ui.py` — HTML rendering for login, redirects, success, and denial pages.

The service follows a backend-for-frontend pattern for browser login. The browser receives an opaque session cookie. The server owns provider state, token exchange, and session lifecycle

## Runtime control flow

### 1. Startup

`main.py` loads settings, validates runtime configuration, and starts FastAPI with a lifespan handler.

On startup:

1. A PostgreSQL pool is created.
2. Database schema is initialized idempotently.
3. Expired auth transactions and sessions are pruned.
4. The pool is stored on `app.state`.

Startup fails fast if:
- `APP_BASE_URL` is missing or invalid.
- Production is configured with a non-HTTPS base URL.
- Microsoft is configured with a non-concrete tenant in production.
- PostgreSQL is unreachable.

### 2. Login page

`GET /login` renders provider buttons for enabled providers only.

Provider enablement is derived from configured client ID/secret pairs:
- Google
- Microsoft
- GitHub

The page is informational only. It does not begin authentication.

### 3. Login start

`GET /login/start/{provider}` begins the authorization-code flow.

For each request the service:

1. Generates a fresh `state`.
2. Generates a fresh PKCE `code_verifier`.
3. Derives `code_challenge = S256(code_verifier)`.
4. Generates a fresh `nonce`.
5. Persists all transaction state in `auth_transactions`.
6. Builds a provider authorization URL.
7. Redirects the browser to the provider.

This step is stateful by design. The auth transaction row is the source of truth for the callback.

### 4. Callback

`GET /callback/{provider}` completes the flow.

The service:

1. Verifies the provider is enabled.
2. Handles provider error redirects.
3. Reads `code` and `state`.
4. Consumes the corresponding auth transaction in a database transaction.
5. Exchanges the code for tokens using the stored `code_verifier`.
6. Verifies the ID token when the provider is OIDC-capable.
7. Fetches userinfo for GitHub and as fallback for OIDC providers if needed.
8. Normalizes claims into a canonical identity structure.
9. Applies provider-specific policy checks.
10. Finds or creates the internal user.
11. Creates a server-side session row.
12. Sets the session cookie.
13. Redirects the browser to the original return path.

### 5. Session lookup

`GET /me` reads the opaque session cookie, resolves the session in PostgreSQL, and returns the active identity.

This endpoint is the canonical user session lookup for browser requests.

### 6. Logout

`POST /logout` revokes the session row and deletes the session cookie.

Logout is server-authoritative. A deleted cookie alone is not sufficient; the session row is also marked revoked.

## Provider behavior

### Google

- Uses OpenID Connect.
- Requests `openid email profile`.
- Requires exact issuer and audience verification.
- Enforces allowed email domains when configured.

### Microsoft

- Uses OpenID Connect.
- Requests `openid email profile`.
- Uses tenant-scoped discovery.
- Requires issuer, audience, expiry, and nonce validation.
- Enforces tenant and domain restrictions when configured.

### GitHub

- Uses OAuth 2.1-style authorization-code + PKCE flow.
- Fetches identity through GitHub API endpoints.
- Requires a verified email.
- Optionally requires membership in at least one allowed organization.

## Decisions made

### 1. Authorization-code + PKCE only

The service does not use implicit flow or password grant. Authorization-code + PKCE is the only login path.

Reason:
- Removes token exposure from the browser redirect.
- Aligns with hardened OAuth 2.1 guidance.
- Works for both public and confidential clients.

### 2. State is persisted server-side

`state`, `nonce`, and `code_verifier` are stored in PostgreSQL, not in browser-local storage or framework session middleware.

Reason:
- Full control over expiry and replay protection.
- No dependence on framework-managed temporary cookies.
- Easier auditing and debugging.
- Stronger fit for a browser BFF architecture.

### 3. ID tokens are verified cryptographically

The service validates:
- signature
- issuer
- audience
- expiry
- nonce

No unsigned or unverifiable ID token is accepted.

### 4. Browser session is opaque

The browser receives only a session identifier cookie.

Reason:
- Keeps bearer tokens out of browser storage.
- Supports revocation.
- Keeps the browser-facing surface minimal.

### 5. PostgreSQL is the system of record

PostgreSQL stores:
- auth transactions
- users
- external identities
- sessions
- audit events

Reason:
- Durability
- transactional integrity
- revocation support
- clear audit trail

### 6. Idempotent schema bootstrap

Schema creation runs at startup with `IF NOT EXISTS` semantics.

Reason:
- Safe repeated bootstrapping
- Predictable startup behavior
- Easier deployment in ephemeral clusters

### 7. Strict runtime validation

Configuration errors fail during startup, not during first login.

Reason:
- Prevents partially working deployments
- Exposes misconfiguration early
- Removes ambiguous runtime behavior

### 8. Security-first response headers

The app applies:
- `Content-Security-Policy`
- `Strict-Transport-Security`
- `X-Frame-Options`
- `X-Content-Type-Options`
- `Referrer-Policy`
- `Permissions-Policy`
- `Cache-Control: no-store`

Reason:
- Reduce browser attack surface
- Prevent clickjacking
- Reduce token leakage through caching
- Make HTML responses less permissive by default

## Invariants and contracts

### Configuration invariants

- `APP_BASE_URL` must be absolute.
- Production deployments must use HTTPS.
- Microsoft production deployments must use a concrete tenant ID.
- OAuth client IDs and secrets must be present for enabled providers.
- Redirect URIs must be stable and externally registered.

### Transaction invariants

For every login attempt:
- `state` is unique.
- `code_verifier` is random and stored server-side.
- `nonce` is random and stored server-side.
- The auth transaction has a bounded TTL.
- A transaction may be consumed once only.
- Provider mismatch invalidates the callback.

### Token validation invariants

For OIDC providers:
- Token signature must validate against JWKS.
- Issuer must match the provider issuer.
- Audience must match the configured client ID.
- Expiry must be valid.
- Nonce must match exactly.

### Identity invariants

- Internal user identity is derived from provider identity.
- Provider subject is the stable external key.
- Provider email may be absent only where policy allows it.
- GitHub users must have a verified email.
- Identity policy failures deny login.

### Session invariants

- Session IDs are opaque and server-generated.
- Sessions have bounded TTLs.
- Revoked sessions are not accepted.
- `/me` only resolves active sessions.
- Logout revokes server state and removes the cookie.

### Database invariants

- Duplicate external identities are not allowed.
- Auth transactions cannot be reused.
- Sessions are linked to exactly one user.
- Audit events are append-only.
- Schema bootstrap can be run repeatedly without destructive effects.

## Runtime guarantees

The service guarantees the following properties when configured correctly:

- No login succeeds without a valid auth transaction.
- No OIDC login succeeds without a valid ID token.
- No browser session exists without a PostgreSQL session row.
- No session row is created without a corresponding user.
- No provider identity bypasses policy enforcement.
- No revoked session is treated as authenticated.

## Failure modes

Expected hard failures include:

- Missing or invalid `APP_BASE_URL`
- Wrong provider client credentials
- Redirect URI mismatch at the provider
- Missing or invalid JWKS metadata
- Token exchange failure
- Nonce mismatch
- Expired or replayed auth transaction
- PostgreSQL outage
- Schema bootstrap failure
- Cookie blocked by the browser
- GitHub identity lacking a verified email
- Provider org/tenant/domain policy rejection

These are deliberate fail-closed behaviors.

## Endpoints

- `GET /` — service summary
- `GET /healthz` — liveness
- `GET /readyz` — readiness with DB check
- `GET /metrics` — Prometheus metrics, if enabled
- `GET /login` — login page
- `GET /redirects` — redirect URI reference
- `GET /login/start/{provider}` — begin login
- `GET /callback/{provider}` — complete login
- `GET /me` — current session lookup
- `POST /logout` — revoke session
- `GET /debug/schema` — schema summary for operators

## Operational model

### Stateless parts
- Authorization requests from provider to callback, after the transaction row is consumed
- Request validation of issued session cookies, if viewed as opaque handles

### Stateful parts
- Auth transactions
- User identity mapping
- Session records
- Revocation
- Audit trail

The correct classification is hybrid, not purely stateless.

## Environment variables

### Required for production

- `APP_BASE_URL`
- `POSTGRES_DSN`
- `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` if Google is enabled
- `MS_CLIENT_ID` / `MS_CLIENT_SECRET` if Microsoft is enabled
- `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` if GitHub is enabled

### Policy controls

- `GOOGLE_ALLOWED_DOMAINS`
- `MICROSOFT_ALLOWED_TENANT_IDS`
- `MICROSOFT_ALLOWED_DOMAINS`
- `GITHUB_ALLOWED_ORGS`

### Session and database controls

- `SESSION_COOKIE_NAME`
- `SESSION_TTL_SECONDS`
- `AUTH_TX_TTL_SECONDS`
- `SESSION_COOKIE_SECURE`
- `SESSION_COOKIE_SAMESITE`
- `SESSION_COOKIE_PATH`
- `SESSION_COOKIE_DOMAIN`
- `POSTGRES_MIN_SIZE`
- `POSTGRES_MAX_SIZE`

### Observability

- `METRICS_ENABLED`

## Deployment notes

- Run behind HTTPS only.
- Register exact callback URLs with each provider.
- Keep `APP_BASE_URL` fixed per environment.
- Use a dedicated PostgreSQL database named `auth`.
- Use a dedicated DB role with least privilege.
- Keep secrets in the cluster secret manager, not in source.
- Do not expose the login service directly without the intended edge controls.

## Maintenance notes

- Auth transactions and sessions should be pruned periodically.
- Discovery metadata is cached and should be allowed to refresh naturally.
- Provider policy changes should be treated as breaking changes.
- Schema changes should be migrated intentionally, even though bootstrap is idempotent.

## Non-goals

- No password storage
- No implicit flow
- No in-browser token storage
- No multi-cluster auth routing
- No custom cryptography beyond standard library, JWT verification, and provider JWKS

## Summary

This service is a hardened, browser-oriented authentication boundary. Its design intentionally combines:
- OAuth 2.1 authorization-code + PKCE
- OIDC for Google and Microsoft
- PostgreSQL-backed state for transactions, sessions, and identities
- Opaque browser sessions
- Strict fail-closed policy enforcement