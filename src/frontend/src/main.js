import '../assets/styles.css';

import { displayName, loginUrl, refreshAuth, subscribeAuth } from './auth.js';
import { renderNavbar } from './ui/navbar.js';

const header = document.getElementById('site-header');
const app = document.getElementById('app');

function renderLandingPage(authState) {
  if (!app) return;

  const isAuth = Boolean(authState?.authenticated);
  const userLabel = displayName(authState);
  const ctaHref = '/predict.html';
  const authHref = loginUrl('/');

  app.innerHTML = `
    <section class="hero-card card hero-card--landing">
      <div class="hero-card__copy">
        <p class="eyebrow">Production UI</p>
        <h1>Fast, structured predictions with session-aware access.</h1>
        <p class="lead">
          This frontend is a static Cloudflare Pages app. It talks directly to the auth service for session state
          and to the inference service for predictions, with no raw JSON input in the main flow.
        </p>
        <div class="button-row">
          <a class="button button--primary" href="${ctaHref}">Open prediction form</a>
          ${isAuth
            ? '<span class="pill pill--ok">Session active</span>'
            : `<a class="button button--ghost" href="${authHref}">Sign in</a>`}
        </div>
      </div>
      <div class="hero-card__meta hero-card__meta--stacked">
        <div class="stat">
          <span class="stat__value">13</span>
          <span class="stat__label">required model inputs</span>
        </div>
        <div class="stat">
          <span class="stat__value">OAuth</span>
          <span class="stat__label">auth service session</span>
        </div>
        <div class="stat">
          <span class="stat__value">${isAuth ? 'Yes' : 'No'}</span>
          <span class="stat__label">signed in as ${userLabel}</span>
        </div>
      </div>
    </section>

    <section class="grid-two">
      <article class="card">
        <p class="eyebrow">Flow</p>
        <h2>Login, predict, logout</h2>
        <p class="muted">
          The same top-right control updates with auth state. Login opens the auth service; logout clears the session
          and returns you home.
        </p>
      </article>
      <article class="card">
        <p class="eyebrow">Model</p>
        <h2>Structured feature form</h2>
        <p class="muted">
          Pickup datetime becomes hour, day-of-week, month, and weekend flags automatically. The remaining feature
          names are explicit and persisted between visits.
        </p>
      </article>
    </section>
  `;
}

async function bootstrap() {
  const authState = await refreshAuth();
  if (header) renderNavbar(header, authState);
  renderLandingPage(authState);

  subscribeAuth((nextState) => {
    if (header) renderNavbar(header, nextState);
    renderLandingPage(nextState);
  });
}

void bootstrap();
