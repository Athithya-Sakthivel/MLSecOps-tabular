import { authState, login, logout } from "../auth.js";

let listeners = new Set();

/**
 * subscribe to auth changes
 */
export function subscribeNavbar(fn) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export function notifyNavbar() {
  listeners.forEach((fn) => fn());
}

/**
 * Top-right reactive navbar
 */
export function renderNavbar() {
  const root = document.getElementById("navbar-root");
  if (!root) return;

  const user = authState.user;

  root.innerHTML = `
    <div style="
      position:fixed;
      top:16px;
      right:16px;
      z-index:9999;
      display:flex;
      align-items:center;
      gap:10px;
    ">

      ${
        user
          ? `
        <span style="
          background:#e0f2fe;
          color:#1d4ed8;
          padding:6px 10px;
          border-radius:999px;
          font-weight:600;
          font-size:13px;
        ">
          ${user.name || user.email || "User"}
        </span>

        <button id="authBtn"
          style="
            background:#dc2626;
            color:white;
            border:none;
            padding:8px 12px;
            border-radius:8px;
            cursor:pointer;
            font-weight:600;
          ">
          Logout
        </button>
      `
          : `
        <button id="authBtn"
          style="
            background:#2563eb;
            color:white;
            border:none;
            padding:8px 12px;
            border-radius:8px;
            cursor:pointer;
            font-weight:600;
          ">
          Login
        </button>
      `
      }

    </div>
  `;

  const btn = document.getElementById("authBtn");

  if (user) {
    btn.onclick = async () => {
      await logout();
      notifyNavbar();
      renderNavbar();
    };
  } else {
    btn.onclick = () => login("google");
  }
}

/**
 * Reactive update trigger
 */
export function refreshNavbar() {
  renderNavbar();
}