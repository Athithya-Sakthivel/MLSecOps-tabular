#!/usr/bin/env bash

export DEBIAN_FRONTEND=noninteractive

log() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

fatal() {
  printf '[ERROR] %s\n' "$*" >&2
  exit 1
}

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"
}

TMPDIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMPDIR}"
}
trap cleanup EXIT

log "updating apt metadata"
sudo apt-get update -qq

log "installing base packages"
sudo apt-get install -y -qq \
  ca-certificates \
  curl \
  gnupg \
  apt-transport-https \
  unzip \
  gh \
  make \
  tree \
  vim \
  python3-pip \
  python3-venv \
  jq \
  wget

log "configuring OpenTofu apt repo"
sudo install -m 0755 -d /etc/apt/keyrings

curl -fsSL https://get.opentofu.org/opentofu.gpg \
  | sudo tee /etc/apt/keyrings/opentofu.gpg >/dev/null

curl -fsSL https://packages.opentofu.org/opentofu/tofu/gpgkey \
  | sudo gpg --dearmor -o "${TMPDIR}/opentofu-repo.gpg" >/dev/null

sudo mv "${TMPDIR}/opentofu-repo.gpg" /etc/apt/keyrings/opentofu-repo.gpg
sudo chmod a+r /etc/apt/keyrings/opentofu.gpg /etc/apt/keyrings/opentofu-repo.gpg

echo "deb [signed-by=/etc/apt/keyrings/opentofu.gpg,/etc/apt/keyrings/opentofu-repo.gpg] https://packages.opentofu.org/opentofu/tofu/any/ any main" \
  | sudo tee /etc/apt/sources.list.d/opentofu.list >/dev/null
sudo chmod a+r /etc/apt/sources.list.d/opentofu.list

log "refreshing apt metadata after repo add"
sudo apt-get update -qq

log "resolving installable tofu version"
CANDIDATE="$(apt-cache policy tofu 2>/dev/null | awk '/Candidate:/ {print $2; exit}')"

if [[ -z "${CANDIDATE}" || "${CANDIDATE}" == "(none)" ]]; then
  warn "apt Candidate empty, trying apt-cache madison"
  CANDIDATE="$(apt-cache madison tofu 2>/dev/null | awk '{print $3}' | sed -n '1p' || true)"
fi

if [[ -z "${CANDIDATE}" || "${CANDIDATE}" == "(none)" ]]; then
  warn "apt-cache madison returned nothing, falling back to package index scrape"
  CANDIDATE="$(
    curl -fsSL https://packages.opentofu.org/opentofu/tofu/packages/any/any/ \
      | grep -oE 'tofu_[0-9]+\.[0-9]+\.[0-9](_[0-9]+)?_amd64\.deb' \
      | sed -E 's/^tofu_([0-9]+\.[0-9]+\.[0-9]).*$/\1/' \
      | sort -V \
      | tail -n1 \
      || true
  )"
fi

[[ -n "${CANDIDATE}" && "${CANDIDATE}" != "(none)" ]] || fatal "No installable tofu version found in APT repo"

log "installing tofu ${CANDIDATE}"
sudo apt-get install -y --allow-downgrades "tofu=${CANDIDATE}" || {
  apt-cache policy tofu || true
  fatal "apt install failed for tofu=${CANDIDATE}"
}
sudo apt-mark hold tofu

log "installing kubectl into /usr/local/bin"
KUBECTL_VERSION="v1.30.1"
curl -fsSL -o "${TMPDIR}/kubectl" \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
chmod +x "${TMPDIR}/kubectl"
sudo install -m 0755 "${TMPDIR}/kubectl" /usr/local/bin/kubectl

log "installing kind into /usr/local/bin"
KIND_VERSION="v0.25.0"
curl -fsSL -o "${TMPDIR}/kind" \
  "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
chmod +x "${TMPDIR}/kind"
sudo install -m 0755 "${TMPDIR}/kind" /usr/local/bin/kind

log "installing AWS CLI v2"
curl -fsSL -o "${TMPDIR}/awscliv2.zip" \
  "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
unzip -q "${TMPDIR}/awscliv2.zip" -d "${TMPDIR}/awscli"
sudo "${TMPDIR}/awscli/aws/install" --update

log "installing flytectl"
curl -fsSL https://ctl.flyte.org/install | sudo bash -s -- -b /usr/local/bin v0.9.8

log "installing helm"
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | DESIRED_VERSION=v3.15.4 bash

log "installing gitleaks into /usr/local/bin"
curl -fsSL -o "${TMPDIR}/gitleaks.tar.gz" \
  https://github.com/gitleaks/gitleaks/releases/download/v8.30.0/gitleaks_8.30.0_linux_x64.tar.gz
tar -xzf "${TMPDIR}/gitleaks.tar.gz" -C "${TMPDIR}"
sudo install -m 0755 "${TMPDIR}/gitleaks" /usr/local/bin/gitleaks

log "installing ruff"
curl -fsSL https://astral.sh/ruff/0.14.11/install.sh | sh

if [[ -d "${HOME}/.local/bin" ]]; then
  export PATH="${HOME}/.local/bin:${PATH}"
fi

if ! grep -qs 'export PATH=$HOME/.local/bin:$PATH' "${HOME}/.bashrc"; then
  echo 'export PATH=$HOME/.local/bin:$PATH' >> "${HOME}/.bashrc"
fi

log "creating Python virtual environments"
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

log "installing Python packages"
python3 -m pip install --no-cache-dir --break-system-packages \
  pyarrow==23.0.1 \
  boto3==1.42.81 \
  pandas==3.0.1 \
  pre-commit==4.5.1 \
  datasets==4.8.4

log "installing pre-commit hooks"
pre-commit install --install-hooks

clear
log "verifying installed tools"
gitleaks version
helm version
aws --version
ruff version
pre-commit --version
kubectl version --client
tofu version
flytectl version

log "done"