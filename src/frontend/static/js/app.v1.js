const AUTH_URL = "https://auth.athithya.site";
const PREDICT_URL = "https://predict.athithya.site";

const NEXT_PATH = location.pathname.endsWith("predict.html") ? "/predict.html" : "/";

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
  const response = await fetch(`${AUTH_URL}/me`, {
    method: "GET",
    credentials: "include",
    headers: { Accept: "application/json" },
    cache: "no-store",
  });

  if (!response.ok) return null;
  return await response.json();
}

function loginUrl(provider) {
  return `${AUTH_URL}/login/start/${provider}?next=${encodeURIComponent(NEXT_PATH)}`;
}

function renderNavbar(me) {
  const slot = document.querySelector("[data-auth-slot]");
  if (!slot) return;

  if (me && me.status === "ok") {
    const displayName = me.name || me.email || "Signed in";
    slot.innerHTML = `
      <span class="user-chip">${escapeHtml(displayName)}</span>
      <a class="button secondary" href="${AUTH_URL}/logout">Logout</a>
    `;
    return;
  }

  slot.innerHTML = `
    <a class="button secondary" href="${AUTH_URL}/login?next=${encodeURIComponent(NEXT_PATH)}">Login</a>
  `;
}

async function renderAuthPanel(me) {
  const panel = document.getElementById("auth-panel");
  if (!panel) return;

  if (me && me.status === "ok") {
    const name = me.name || me.email || "Signed in";
    panel.innerHTML = `
      <section class="card">
        <h2>Signed in</h2>
        <p class="lead">Hello, ${escapeHtml(name)}.</p>
      </section>
    `;
    return;
  }

  const response = await fetch("/partials/login.html", { cache: "no-store" });
  panel.innerHTML = await response.text();

  panel.querySelectorAll("[data-login-provider]").forEach((el) => {
    const provider = el.getAttribute("data-login-provider");
    el.setAttribute("href", loginUrl(provider));
  });
}

function renderPredictResult(message, ok = true) {
  const output = document.getElementById("predict-result");
  if (!output) return;
  output.textContent = typeof message === "string" ? message : JSON.stringify(message, null, 2);
  output.classList.toggle("result--error", !ok);
}

function collectFeatures(form) {
  const rows = form.querySelectorAll("[data-feature-row]");
  const payload = {};

  for (const row of rows) {
    const nameInput = row.querySelector('[name="feature_name"]');
    const valueInput = row.querySelector('[name="feature_value"]');

    const name = String(nameInput?.value ?? "").trim();
    if (!name) continue;

    payload[name] = parseValue(valueInput?.value);
  }

  return payload;
}

function wirePredictForm() {
  const form = document.getElementById("predict-form");
  if (!form) return;

  const rowsContainer = document.getElementById("feature-rows");
  const addButton = document.getElementById("add-feature");
  const template = document.getElementById("feature-row-template");

  const bindRemoveButtons = () => {
    rowsContainer?.querySelectorAll("[data-remove-row]").forEach((button) => {
      button.onclick = () => {
        const row = button.closest("[data-feature-row]");
        if (row && rowsContainer.querySelectorAll("[data-feature-row]").length > 1) {
          row.remove();
        }
      };
    });
  };

  addButton?.addEventListener("click", () => {
    if (!rowsContainer || !template) return;
    rowsContainer.appendChild(template.content.cloneNode(true));
    bindRemoveButtons();
  });

  bindRemoveButtons();

  form.addEventListener("submit", async (event) => {
    event.preventDefault();

    const payload = collectFeatures(form);
    const resultEl = document.getElementById("predict-result");
    if (resultEl) {
      resultEl.textContent = "Loading...";
    }

    try {
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

      if (!response.ok) {
        renderPredictResult(text || `Request failed (${response.status})`, false);
        if (response.status === 401 || response.status === 403) {
          await renderAuthPanel(null);
          renderNavbar(null);
        }
        return;
      }

      try {
        renderPredictResult(JSON.parse(text), true);
      } catch {
        renderPredictResult(text, true);
      }
    } catch (error) {
      renderPredictResult(`Network error: ${error.message}`, false);
    }
  });
}

async function main() {
  await loadAllFragments();

  const me = await getMe();
  renderNavbar(me);
  await renderAuthPanel(me);

  const authPanel = document.getElementById("auth-panel");
  if (authPanel && !me) {
    const links = authPanel.querySelectorAll("[data-login-provider]");
    links.forEach((el) => {
      const provider = el.getAttribute("data-login-provider");
      el.setAttribute("href", loginUrl(provider));
    });
  }

  wirePredictForm();
}

document.addEventListener("DOMContentLoaded", () => {
  main().catch((error) => {
    console.error(error);
  });
});