import '../assets/styles.css';

import { displayName, loginUrl, refreshAuth, subscribeAuth } from './auth.js';
import { renderNavbar } from './ui/navbar.js';
import {
  renderPredictionPage,
  renderPredictionResult,
  setPredictBanner,
  wirePredictionForm,
} from './ui/form.js';

const header = document.getElementById('site-header');
const app = document.getElementById('app');

const PREDICT_ORIGIN = new URL(import.meta.env.VITE_PREDICT_ORIGIN || 'https://predict.athithya.site');

async function submitPrediction(payload) {
  const response = await fetch(new URL('/predict', PREDICT_ORIGIN), {
    method: 'POST',
    credentials: 'include',
    mode: 'cors',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const text = await response.text();
  let data;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  return { response, data };
}

function renderPage(state) {
  if (!app) return;
  app.innerHTML = renderPredictionPage({ authState: state });
  setPredictBanner(app, { authenticated: state.authenticated, userLabel: displayName(state) });

  const authRequiredButton = app.querySelector('[data-auth-banner] .button');
  if (authRequiredButton && !state.authenticated) {
    authRequiredButton.href = loginUrl('/predict.html');
  }

  wirePredictionForm(app, {
    authenticated: () => Boolean(state.authenticated),
    onSubmit: async ({ kind, payload }) => {
      const result = app.querySelector('#predict-result');
      if (kind === 'auth_required') {
        if (result) result.textContent = 'Please sign in first.';
        window.location.assign(loginUrl('/predict.html'));
        return;
      }

      try {
        const { response, data } = await submitPrediction(payload);
        if (!response.ok) {
          if (response.status === 401 || response.status === 403) {
            await refreshAuth();
            if (result) result.textContent = 'Your session expired. Sign in again.';
            return;
          }
          if (result) result.textContent = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
          return;
        }

        renderPredictionResult(app, data);
      } catch (error) {
        if (result) {
          result.textContent = error instanceof Error ? `Network error: ${error.message}` : 'Network error.';
        }
      }
    },
  });
}

async function bootstrap() {
  const state = await refreshAuth();
  if (header) renderNavbar(header, state);
  renderPage(state);

  subscribeAuth((nextState) => {
    if (header) renderNavbar(header, nextState);
    renderPage(nextState);
  });
}

void bootstrap();
