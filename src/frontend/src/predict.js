import { fetchMe, isAuthed, login } from "./auth.js";
import { renderNavbar } from "./ui/navbar.js";

const PREDICT_URL = "https://predict.athithya.site";

const state = {
  features: [{ name: "", value: "" }],
  result: null,
  loading: false,
};

function render() {
  const app = document.getElementById("app");

  app.innerHTML = `
    <div style="max-width:900px;margin:60px auto;padding:20px;">

      <div id="navbar-root"></div>

      <h1>Prediction</h1>

      ${
        !isAuthed()
          ? `
        <div style="background:#fee2e2;color:#991b1b;padding:10px;border-radius:8px;margin:10px 0;">
          You are not logged in.
          <button id="loginBtn">Login</button>
        </div>
      `
          : ""
      }

      <div style="margin-top:20px;">
        ${state.features
          .map(
            (f, i) => `
          <div style="display:flex;gap:10px;margin-bottom:8px;">
            <input data-i="${i}" data-k="name"
                   value="${f.name}"
                   placeholder="feature" />

            <input data-i="${i}" data-k="value"
                   value="${f.value}"
                   placeholder="value" />

            <button data-remove="${i}">x</button>
          </div>
        `
          )
          .join("")}

        <div style="display:flex;gap:10px;margin-top:10px;">
          <button id="addBtn">Add feature</button>
          <button id="predictBtn">Predict</button>
        </div>
      </div>

      <pre style="margin-top:20px;background:#0f172a;color:white;padding:12px;border-radius:8px;">
${state.loading ? "Loading..." : JSON.stringify(state.result, null, 2)}
      </pre>
    </div>
  `;

  bind();
  renderNavbar("navbar-root");

  const loginBtn = document.getElementById("loginBtn");
  if (loginBtn) loginBtn.onclick = () => login();
}

function bind() {
  document.querySelectorAll("input").forEach((input) => {
    input.oninput = (e) => {
      const i = Number(e.target.dataset.i);
      const key = e.target.dataset.k;
      state.features[i][key] = e.target.value;
    };
  });

  document.querySelectorAll("[data-remove]").forEach((btn) => {
    btn.onclick = () => {
      const i = Number(btn.dataset.remove);
      state.features.splice(i, 1);
      render();
    };
  });

  document.getElementById("addBtn").onclick = () => {
    state.features.push({ name: "", value: "" });
    render();
  };

  document.getElementById("predictBtn").onclick = async () => {
    if (!isAuthed()) return;

    state.loading = true;
    render();

    const payload = {};
    for (const f of state.features) {
      if (f.name) payload[f.name] = f.value;
    }

    try {
      const res = await fetch(`${PREDICT_URL}/predict`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      state.result = await res.json();
    } catch (e) {
      state.result = { error: e.message };
    } finally {
      state.loading = false;
      render();
    }
  };
}

async function boot() {
  await fetchMe();
  render();
}

boot();