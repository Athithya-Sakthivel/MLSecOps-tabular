const AUTH_URL = "https://auth.athithya.site";
const PREDICT_URL = "https://predict.athithya.site";

const FEATURE_SCHEMA = [
  { name: "pickup_hour", type: "number", min: 0, max: 23, step: 1, placeholder: "0 to 23" },
  { name: "pickup_dow", type: "number", min: 0, max: 6, step: 1, placeholder: "0 to 6" },
  { name: "pickup_month", type: "number", min: 1, max: 12, step: 1, placeholder: "1 to 12" },
  { name: "pickup_is_weekend", type: "number", min: 0, max: 1, step: 1, placeholder: "0 or 1" },
  { name: "pickup_borough_id", type: "number", step: 1, placeholder: "numeric id" },
  { name: "pickup_zone_id", type: "number", step: 1, placeholder: "numeric id" },
  { name: "pickup_service_zone_id", type: "number", step: 1, placeholder: "numeric id" },
  { name: "dropoff_borough_id", type: "number", step: 1, placeholder: "numeric id" },
  { name: "dropoff_zone_id", type: "number", step: 1, placeholder: "numeric id" },
  { name: "dropoff_service_zone_id", type: "number", step: 1, placeholder: "numeric id" },
  { name: "route_pair_id", type: "number", step: 1, placeholder: "numeric id" },
  { name: "avg_duration_7d_zone_hour", type: "number", step: "any", placeholder: "numeric value" },
  { name: "avg_fare_30d_zone", type: "number", step: "any", placeholder: "numeric value" },
  { name: "trip_count_90d_zone_hour", type: "number", step: "any", placeholder: "numeric value" },
];

function pageName() {
  return document.body?.dataset?.page || "home";
}

function nextPath() {
  return pageName() === "predict" ? "/predict.html" : "/";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function parseValue(raw) {
  const text = String(raw ?? "").trim();
  if (text === "") return "";
  if (/^-?\d+(\.\d+)?$/.test(text)) return Number(text);
  if (text.toLowerCase() === "true") return true;
  if (text.toLowerCase() === "false") return false;
  return text;
}

async function loadFragment(el) {
  const path = el.dataset.fragment;
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to load ${path}: ${response.status}`);
  }
  el.innerHTML = await response.text();
}

async function loadAllFragments() {
  const elements = document.querySelectorAll("[data-fragment]");
  await Promise.all([...elements].map(loadFragment));
}

async function getMe() {
  try {
    const response = await fetch(`${AUTH_URL}/me`, {
      method: "GET",
      credentials: "include",
      headers: { Accept: "application/json" },
      cache: "no-store",
    });

    if (!response.ok) return null;
    return await response.json();
  } catch {
    return null;
  }
}

function loginUrl() {
  return `${AUTH_URL}/login?next=${encodeURIComponent(nextPath())}`;
}

function logoutUrl() {
  return `${AUTH_URL}/logout`;
}

function updateLoginLinks() {
  document.querySelectorAll("[data-login-link]").forEach((el) => {
    el.setAttribute("href", loginUrl());
  });
}

function setAuthSlotLoading() {
  const slot = document.querySelector("[data-auth-slot]");
  if (!slot) return;
  slot.innerHTML = `<span class="auth-loading">Checking session…</span>`;
}

function renderNavbar(me) {
  const slot = document.querySelector("[data-auth-slot]");
  if (!slot) return;

  if (me && me.status === "ok") {
    const displayName = me.name || me.email || "Signed in";
    slot.innerHTML = `
      <span class="user-chip">${escapeHtml(displayName)}</span>
      <a class="button auth-button logout" href="${logoutUrl()}">Logout</a>
    `;
    return;
  }

  slot.innerHTML = `
    <a class="button auth-button login" href="${loginUrl()}">Login</a>
  `;
}

function renderAuthPanel(me) {
  const panel = document.getElementById("auth-panel");
  if (!panel) return;

  if (me && me.status === "ok") {
    const name = me.name || me.email || "Signed in";
    panel.innerHTML = `
      <section class="card auth-state-card ok">
        <h2>Signed in</h2>
        <p class="lead">Hello, ${escapeHtml(name)}.</p>
      </section>
    `;
    return;
  }

  panel.innerHTML = `
    <section class="card auth-state-card fail">
      <h2>Sign in required</h2>
      <p class="lead">Authenticate first, then open the prediction page.</p>
      <div class="button-row">
        <a class="button" href="${loginUrl()}">Login</a>
      </div>
    </section>
  `;
}

function renderPredictGate(me) {
  const shell = document.getElementById("predict-shell");
  if (!shell) return;

  if (me && me.status === "ok") {
    shell.classList.remove("hidden");
    return;
  }

  shell.classList.add("hidden");

  const authPanel = document.getElementById("auth-panel");
  if (authPanel) {
    authPanel.innerHTML = `
      <section class="card auth-state-card fail">
        <h2>Sign in required</h2>
        <p class="lead">Login before using the prediction form.</p>
        <div class="button-row">
          <a class="button" href="${loginUrl()}">Login</a>
        </div>
      </section>
    `;
  }
}

function renderPredictResult(message, ok = true) {
  const output = document.getElementById("predict-result");
  if (!output) return;

  if (typeof message === "string") {
    output.textContent = message;
  } else {
    output.textContent = JSON.stringify(message, null, 2);
  }

  output.classList.toggle("result--error", !ok);
  output.classList.toggle("result--success", ok);
}

function buildPayloadFromSchema() {
  const payload = {};
  for (const field of FEATURE_SCHEMA) {
    const input = document.querySelector(`[name="${field.name}"]`);
    if (!input) continue;
    const value = parseValue(input.value);
    payload[field.name] = value;
  }
  return payload;
}

function fillSampleValues() {
  const sample = {
    pickup_hour: 14,
    pickup_dow: 2,
    pickup_month: 4,
    pickup_is_weekend: 0,
    pickup_borough_id: 2,
    pickup_zone_id: 123,
    pickup_service_zone_id: 1,
    dropoff_borough_id: 3,
    dropoff_zone_id: 321,
    dropoff_service_zone_id: 1,
    route_pair_id: 42,
    avg_duration_7d_zone_hour: 18.5,
    avg_fare_30d_zone: 22.1,
    trip_count_90d_zone_hour: 8,
  };

  for (const [key, value] of Object.entries(sample)) {
    const input = document.querySelector(`[name="${key}"]`);
    if (input) input.value = String(value);
  }
}

function clearForm() {
  FEATURE_SCHEMA.forEach((field) => {
    const input = document.querySelector(`[name="${field.name}"]`);
    if (input) input.value = "";
  });
}

function wirePredictForm(me) {
  const form = document.getElementById("predict-form");
  if (!form) return;

  const submitButton = document.getElementById("predict-submit");
  const resultEl = document.getElementById("predict-result");

  const sampleButton = document.getElementById("fill-sample");
  if (sampleButton) {
    sampleButton.addEventListener("click", () => fillSampleValues());
  }

  const clearButton = document.getElementById("clear-form");
  if (clearButton) {
    clearButton.addEventListener("click", () => clearForm());
  }

  if (!(me && me.status === "ok")) {
    form.querySelectorAll("input,button").forEach((el) => {
      if (el instanceof HTMLButtonElement && el.id === "clear-form") return;
      if (el instanceof HTMLButtonElement && el.id === "fill-sample") return;
      el.disabled = true;
    });
    if (resultEl) {
      resultEl.textContent = "Login required before prediction.";
      resultEl.classList.add("result--error");
    }
    return;
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();

    if (submitButton) {
      submitButton.disabled = true;
      submitButton.textContent = "Predicting…";
    }

    if (resultEl) {
      resultEl.textContent = "Loading…";
      resultEl.classList.remove("result--error");
      resultEl.classList.add("result--success");
    }

    try {
      const payload = buildPayloadFromSchema();
      const response = await fetch(`${PREDICT_URL}/predict`, {
        method: "POST",
        credentials: "include",
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json",
        },
        body: JSON.stringify(payload),
      });

      const text = await response.text();

      if (response.status === 401 || response.status === 403) {
        renderPredictResult("Session expired. Please log in again.", false);
        const refreshed = await getMe();
        renderNavbar(refreshed);
        renderAuthPanel(refreshed);
        renderPredictGate(refreshed);
        return;
      }

      if (!response.ok) {
        renderPredictResult(text || `Request failed (${response.status})`, false);
        return;
      }

      try {
        renderPredictResult(JSON.parse(text), true);
      } catch {
        renderPredictResult(text, true);
      }
    } catch (error) {
      renderPredictResult(`Network error: ${error.message}`, false);
    } finally {
      if (submitButton) {
        submitButton.disabled = false;
        submitButton.textContent = "Predict";
      }
    }
  });
}

async function main() {
  setAuthSlotLoading();
  await loadAllFragments();
  updateLoginLinks();

  const me = await getMe();
  renderNavbar(me);
  renderAuthPanel(me);
  renderPredictGate(me);
  wirePredictForm(me);
}

document.addEventListener("DOMContentLoaded", () => {
  main().catch((error) => {
    console.error(error);
  });
});