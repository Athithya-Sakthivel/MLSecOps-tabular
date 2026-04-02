#!/usr/bin/env bash

sudo apt update && sudo apt upgrade -y
sudo apt install unzip gh make tree vim python3-pip python3-venv jq -y

# --- Preconditions ---
echo "[INFO] Ensure basic tools"
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl gnupg apt-transport-https || true

# --- Add keyrings & repo (official) ---
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://get.opentofu.org/opentofu.gpg \
  | sudo tee /etc/apt/keyrings/opentofu.gpg >/dev/null
curl -fsSL https://packages.opentofu.org/opentofu/tofu/gpgkey \
  | sudo gpg --dearmor -o /tmp/opentofu-repo.gpg >/dev/null
sudo mv /tmp/opentofu-repo.gpg /etc/apt/keyrings/opentofu-repo.gpg
sudo chmod a+r /etc/apt/keyrings/opentofu.gpg /etc/apt/keyrings/opentofu-repo.gpg

echo "deb [signed-by=/etc/apt/keyrings/opentofu.gpg,/etc/apt/keyrings/opentofu-repo.gpg] https://packages.opentofu.org/opentofu/tofu/any/ any main" \
  | sudo tee /etc/apt/sources.list.d/opentofu.list >/dev/null
sudo chmod a+r /etc/apt/sources.list.d/opentofu.list

# --- Refresh package lists ---
sudo apt-get update -qq

# --- Determine installable version ---
echo "[INFO] Querying apt for candidate version"
CANDIDATE="$(apt-cache policy tofu 2>/dev/null | awk '/Candidate:/ {print $2; exit}')"

if [ -z "$CANDIDATE" ] || [ "$CANDIDATE" = "(none)" ]; then
  echo "[WARN] apt Candidate empty, trying apt-cache madison"
  CANDIDATE="$(apt-cache madison tofu 2>/dev/null | awk '{print $3}' | sed -n '1p' || true)"
fi

if [ -z "$CANDIDATE" ] || [ "$CANDIDATE" = "(none)" ]; then
  echo "[WARN] apt-cache madison returned nothing, falling back to package index scrape"
  CANDIDATE="$(curl -fsSL https://packages.opentofu.org/opentofu/tofu/packages/any/any/ \
    | grep -oE 'tofu_[0-9]+\.[0-9]+\.[0-9](_[0-9]+)?_amd64\.deb' \
    | sed -E 's/tofu_([0-9]+\.[0-9]+\.[0-9]).*/\1/' \
    | sort -V | tail -n1 || true)"
fi

if [ -z "$CANDIDATE" ] || [ "$CANDIDATE" = "(none)" ]; then
  echo "[ERROR] No installable tofu version found in APT repo. Abort." >&2
  echo "[TIP] Run: apt-cache policy tofu ; apt-cache madison tofu ; curl -fsSL https://packages.opentofu.org/opentofu/tofu/packages/any/any/ | sed -n '1,200p'"
  exit 1
fi

echo "[INFO] Selected version: $CANDIDATE"

# --- Install exact version and hold it ---
sudo apt-get install -y --allow-downgrades "tofu=${CANDIDATE}" || {
  echo "[ERROR] apt install failed for tofu=${CANDIDATE}" >&2
  apt-cache policy tofu || true
  exit 1
}
sudo apt-mark hold tofu


curl -LO https://dl.k8s.io/release/v1.30.1/bin/linux/amd64/kubectl && chmod +x kubectl && sudo mv kubectl /usr/local/bin/

curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.25.0/kind-linux-amd64 && \
chmod +x ./kind && sudo mv ./kind /usr/local/bin/


curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip -q awscliv2.zip && sudo ./aws/install && rm -rf aws awscliv2.zip


echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Install packages imported only outside of flyte task definitions
python3 -m venv .venv_elt
.venv_elt/bin/python -m pip install --upgrade pip wheel setuptools
.venv_elt/bin/python -m pip install \
  flytekit==1.16.15 \
  flytekitplugins-spark==1.16.15 \
  pyspark==4.1.1 \
  cloudpickle==3.1.2
  
python3 -m venv .venv_train
.venv_train/bin/python -m pip install --upgrade pip wheel setuptools
.venv_train/bin/python -m pip install \
  flytekit==1.16.15 \
  flyteplugins-ray==2.0.6 \
  numpy==1.26.4 \
  pandas==2.2.3


python3 -m venv .venv_deploy
.venv_deploy/bin/python -m pip install --upgrade pip wheel setuptools
.venv_deploy/bin/python -m pip install -r src/workflows/deploy/requirements.txt

curl -sL https://ctl.flyte.org/install | sudo bash -s -- -b /usr/local/bin v0.9.8

curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | DESIRED_VERSION=v3.15.4 bash
wget -qO- https://github.com/gitleaks/gitleaks/releases/download/v8.30.0/gitleaks_8.30.0_linux_x64.tar.gz | tar xz && \
sudo mv gitleaks /usr/local/bin/gitleaks
curl -LsSf https://astral.sh/ruff/0.14.11/install.sh | sh


python3 -m pip install --no-cache-dir --break-system-packages pyarrow==23.0.1 boto3==1.42.81 pandas==3.0.1 pre-commit==4.5.1 datasets==4.8.4
pre-commit install --install-hooks

clear
echo "gitleaks version $(gitleaks version)"
echo "helm version: $(helm version)"
aws --version
ruff version
pre-commit --version
kubectl version
tofu version
flytectl version