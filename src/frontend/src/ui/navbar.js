import { displayName, loginUrl, logout } from '../auth.js';

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function pageLabel(pathname) {
  if (pathname.endsWith('/predict.html')) return 'Predict';
  return 'Home';
}

export function renderNavbar(root, authState) {
  if (!root) return;

  const loggedIn = Boolean(authState?.authenticated);
  const userLabel = escapeHtml(displayName(authState));
  const actionClass = loggedIn ? 'button button--logout' : 'button button--login';
  const actionText = loggedIn ? 'Logout' : 'Login';
  const actionHref = loggedIn ? '#' : loginUrl(window.location.pathname + window.location.search + window.location.hash);
  const page = pageLabel(window.location.pathname);
  const subtitle = loggedIn
    ? 'Session active'
    : 'Sign in to use the prediction form';

  root.innerHTML = `
    <header class="topbar">
      <a class="brand" href="/" aria-label="Tabular ML Inference home">
        <img class="brand__logo" src="/assets/logo.png" alt="" width="44" height="44" />
        <span class="brand__text">
          <strong>Tabular ML Inference</strong>
          <small>${page} · ${subtitle}</small>
        </span>
      </a>

      <div class="auth-area">
        <span class="auth-chip ${loggedIn ? 'auth-chip--ok' : 'auth-chip--idle'}">${userLabel}</span>
        ${loggedIn
          ? `<button type="button" class="${actionClass}" data-auth-logout>${actionText}</button>`
          : `<a class="${actionClass}" href="${actionHref}" data-auth-login>${actionText}</a>`}
      </div>
    </header>
  `;

  const logoutButton = root.querySelector('[data-auth-logout]');
  if (logoutButton) {
    logoutButton.addEventListener('click', () => {
      void logout('/');
    });
  }
}
