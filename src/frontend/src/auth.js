const AUTH_URL = "https://auth.athithya.site";

export const authState = {
  user: null,
};

export async function fetchMe() {
  try {
    const res = await fetch(`${AUTH_URL}/me`, {
      credentials: "include",
    });

    if (!res.ok) {
      authState.user = null;
      return null;
    }

    const data = await res.json();
    authState.user = data?.status === "ok" ? data : null;

    return authState.user;
  } catch {
    authState.user = null;
    return null;
  }
}

export function login(provider = "google") {
  const next = encodeURIComponent(window.location.pathname);
  window.location.href = `${AUTH_URL}/login/start/${provider}?next=${next}`;
}

export async function logout() {
  try {
    await fetch(`${AUTH_URL}/logout`, {
      credentials: "include",
    });
  } finally {
    authState.user = null;
  }
}

export function isAuthed() {
  return !!authState.user;
}