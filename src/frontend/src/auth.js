const AUTH_ORIGIN = new URL(import.meta.env.VITE_AUTH_ORIGIN || 'https://auth.athithya.site');
const DEFAULT_NEXT = '/';

const listeners = new Set();
const state = {
  ready: false,
  loading: true,
  authenticated: false,
  user: null,
  session: null,
  error: null,
};

function snapshot() {
  return {
    ready: state.ready,
    loading: state.loading,
    authenticated: state.authenticated,
    user: state.user ? { ...state.user } : null,
    session: state.session ? { ...state.session } : null,
    error: state.error,
  };
}

function emit() {
  const value = snapshot();
  for (const listener of listeners) {
    try {
      listener(value);
    } catch (error) {
      console.error('auth listener failed', error);
    }
  }
}

function setState(next) {
  Object.assign(state, next);
  emit();
}

export function getAuthState() {
  return snapshot();
}

export function subscribeAuth(listener) {
  listeners.add(listener);
  listener(snapshot());
  return () => {
    listeners.delete(listener);
  };
}

export function loginUrl(nextPath = window.location.pathname + window.location.search + window.location.hash) {
  const url = new URL('/login', AUTH_ORIGIN);
  url.searchParams.set('next', nextPath || DEFAULT_NEXT);
  return url.toString();
}

export function currentPath() {
  return window.location.pathname + window.location.search + window.location.hash || DEFAULT_NEXT;
}

export async function refreshAuth() {
  setState({ loading: true, error: null });

  try {
    const response = await fetch(new URL('/me', AUTH_ORIGIN), {
      method: 'GET',
      credentials: 'include',
      mode: 'cors',
      headers: {
        Accept: 'application/json',
      },
      cache: 'no-store',
    });

    if (!response.ok) {
      setState({
        ready: true,
        loading: false,
        authenticated: false,
        user: null,
        session: null,
        error: response.status === 401 ? null : `auth status ${response.status}`,
      });
      return snapshot();
    }

    const payload = await response.json();
    const authenticated = payload && payload.status === 'ok';
    const user = authenticated
      ? {
          userId: payload.user_id ?? null,
          email: payload.email ?? null,
          name: payload.name ?? null,
          provider: payload.provider ?? null,
          tenantId: payload.tenant_id ?? null,
        }
      : null;
    const session = authenticated
      ? {
          sessionId: payload.session_id ?? null,
          expiresAt: payload.expires_at ?? null,
          createdAt: payload.created_at ?? null,
        }
      : null;

    setState({
      ready: true,
      loading: false,
      authenticated,
      user,
      session,
      error: null,
    });
    return snapshot();
  } catch (error) {
    setState({
      ready: true,
      loading: false,
      authenticated: false,
      user: null,
      session: null,
      error: error instanceof Error ? error.message : 'auth refresh failed',
    });
    return snapshot();
  }
}

export async function logout(redirectTo = '/') {
  try {
    await fetch(new URL('/logout', AUTH_ORIGIN), {
      method: 'GET',
      credentials: 'include',
      mode: 'cors',
      redirect: 'follow',
      cache: 'no-store',
    });
  } catch (error) {
    console.warn('logout request failed', error);
  }

  setState({
    ready: true,
    loading: false,
    authenticated: false,
    user: null,
    session: null,
    error: null,
  });

  if (redirectTo) {
    window.location.assign(redirectTo);
  }
}

export function displayName(authState = state) {
  const name = authState.user?.name?.trim();
  if (name) return name;
  const email = authState.user?.email?.trim();
  if (email) return email;
  return authState.authenticated ? 'Signed in' : 'Guest';
}
