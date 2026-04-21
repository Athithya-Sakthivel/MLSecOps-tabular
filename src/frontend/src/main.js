import { fetchMe } from "./auth.js";
import { renderNavbar } from "./ui/navbar.js";

function renderHome() {
  const app = document.getElementById("app");

  app.innerHTML = `
    <div style="max-width:900px;margin:80px auto;padding:20px;">
      <h1>Tabular ML Inference</h1>

      <p style="color:#475569;">
        Sign in to run predictions using structured feature inputs.
      </p>

      <div style="display:flex;gap:12px;margin-top:20px;">
        <a href="/predict.html"
           style="padding:10px 14px;background:#111827;color:white;border-radius:8px;text-decoration:none;">
          Open Predict
        </a>

        <a href="https://auth.athithya.site/login?next=/"
           style="padding:10px 14px;background:#2563eb;color:white;border-radius:8px;text-decoration:none;">
          Login
        </a>
      </div>
    </div>
  `;
}

async function boot() {
  await fetchMe();
  renderNavbar();
  renderHome();
}

boot();