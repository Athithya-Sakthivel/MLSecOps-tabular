function setDisabled() {
  const btn = document.getElementById("predictBtn");
  btn.disabled = true;
  btn.innerText = "Login required";

  btn.onclick = () => {
    window.location.href = `${CONFIG.AUTH_URL}/login`;
  };
}

function setEnabled() {
  const btn = document.getElementById("predictBtn");
  btn.disabled = false;
  btn.innerText = "Predict";
  btn.onclick = predict;
}

async function checkAuth() {
  const ctrl = document.getElementById("auth-controls");
  const token = localStorage.getItem("app_jwt");

  if (!token) {
    ctrl.innerHTML = `<a href="${CONFIG.AUTH_URL}/login">Login</a>`;
    setDisabled();
    return;
  }

  try {
    const res = await fetch(`${CONFIG.AUTH_URL}/me`, {
      headers: {
        Authorization: "Bearer " + token
      }
    });

    if (!res.ok) throw new Error();

    const data = await res.json();

    ctrl.innerHTML = `
      ${data.user.email}
      <button id="logout">Logout</button>
    `;

    document.getElementById("logout").onclick = () => {
      localStorage.removeItem("app_jwt");
      location.reload();
    };

    setEnabled();

  } catch (e) {
    localStorage.removeItem("app_jwt");
    setDisabled();
  }
}

async function predict() {
  const input = document.getElementById("input").value;
  const token = localStorage.getItem("app_jwt");

  document.getElementById("output").innerText = "Loading...";

  try {
    const res = await fetch(`${CONFIG.PREDICT_URL}/predict`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + token
      },
      body: input
    });

    const text = await res.text();
    document.getElementById("output").innerText = text;

  } catch (err) {
    document.getElementById("output").innerText = "Error calling predict API";
  }
}

document.addEventListener("DOMContentLoaded", checkAuth);