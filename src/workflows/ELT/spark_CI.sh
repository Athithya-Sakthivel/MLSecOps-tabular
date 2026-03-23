#!/usr/bin/env bash
set -euo pipefail

log() { printf '\033[0;34m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[0;33m[WARN]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[0;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_cmd docker
require_cmd curl
require_cmd git
require_cmd sha256sum
require_cmd awk
require_cmd tar

repo_root="${GITHUB_WORKSPACE:-$(pwd)}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "${repo_root}"

IMAGE_NAME="${IMAGE_NAME:-flyte-elt-spark-base}"
IMAGE_TAG="${IMAGE_TAG:-${GITHUB_SHA:-dev}}"
BUILD_CONTEXT="${BUILD_CONTEXT:-src/workflows/ELT}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-src/workflows/ELT/Dockerfile.spark}"
PLATFORMS="${PLATFORMS:-linux/amd64}"
REGISTRY_TYPE="${REGISTRY_TYPE:-ghcr}"

GHCR_IMAGE_REPO="${GHCR_IMAGE_REPO:-}"
GHCR_USERNAME="${GHCR_USERNAME:-athithya-sakthivel}"
ECR_IMAGE_REPO="${ECR_IMAGE_REPO:-}"

TRIVY_IMAGE="${TRIVY_IMAGE:-aquasec/trivy@sha256:3d1f862cb6c4fe13c1506f96f816096030d8d5ccdb2380a3069f7bf07daa86aa}"
TRIVY_SCANNERS="${TRIVY_SCANNERS:-vuln,secret,misconfig,license}"
TRIVY_SEVERITY="${TRIVY_SEVERITY:-HIGH,CRITICAL}"
TRIVY_IGNORE_UNFIXED="${TRIVY_IGNORE_UNFIXED:-true}"
TRIVY_TIMEOUT="${TRIVY_TIMEOUT:-10m}"

GITLEAKS_VERSION="${GITLEAKS_VERSION:-8.30.1}"
GITLEAKS_CONFIG="${GITLEAKS_CONFIG:-.gitleaks.toml}"

PUSH_IMAGE="${PUSH_IMAGE:-true}"
GIT_PAT="${GIT_PAT:-}"

TEMP_DIR="$(mktemp -d)"
BUILDER_NAME="spark-ci-${GITHUB_RUN_ID:-local}"

cleanup() {
  docker buildx rm "${BUILDER_NAME}" >/dev/null 2>&1 || true
  rm -rf "${TEMP_DIR}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

registry_repo() {
  case "${REGISTRY_TYPE}" in
    ghcr)
      [ -n "${GHCR_IMAGE_REPO}" ] || die "GHCR_IMAGE_REPO is required for REGISTRY_TYPE=ghcr"
      echo "${GHCR_IMAGE_REPO}"
      ;;
    ecr)
      [ -n "${ECR_IMAGE_REPO}" ] || die "ECR_IMAGE_REPO is required for REGISTRY_TYPE=ecr"
      echo "${ECR_IMAGE_REPO}"
      ;;
    *)
      die "REGISTRY_TYPE must be ghcr or ecr"
      ;;
  esac
}

resolve_gitleaks_config() {
  local config_input="${GITLEAKS_CONFIG:-}"
  local candidates=()
  local candidate

  if [ -n "${config_input}" ]; then
    if [ -f "${config_input}" ]; then
      printf '%s\n' "${config_input}"
      return 0
    fi
    candidates+=("${repo_root}/${config_input}")
    candidates+=("${script_dir}/${config_input}")
  fi

  candidates+=("${repo_root}/.gitleaks.toml")
  candidates+=("${script_dir}/.gitleaks.toml")

  for candidate in "${candidates[@]}"; do
    if [ -f "${candidate}" ]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
}

install_gitleaks() {
  local asset base_url checksum_file tarball expected_sha actual_sha

  case "$(uname -m)" in
    x86_64) asset="gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz" ;;
    aarch64|arm64) asset="gitleaks_${GITLEAKS_VERSION}_linux_arm64.tar.gz" ;;
    *)
      die "unsupported architecture for gitleaks: $(uname -m)"
      ;;
  esac

  base_url="https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}"
  checksum_file="${TEMP_DIR}/gitleaks_checksums.txt"
  tarball="${TEMP_DIR}/${asset}"

  log "Downloading Gitleaks ${GITLEAKS_VERSION}"
  curl -fsSLo "${tarball}" "${base_url}/${asset}"
  curl -fsSLo "${checksum_file}" "${base_url}/gitleaks_${GITLEAKS_VERSION}_checksums.txt"

  expected_sha="$(awk -v asset="${asset}" '$0 ~ asset"$" {gsub(/^sha256:/,"",$1); print $1}' "${checksum_file}")"
  actual_sha="$(sha256sum "${tarball}" | awk '{print $1}')"

  [ -n "${expected_sha}" ] || die "checksum not found for ${asset}"
  [ "${expected_sha}" = "${actual_sha}" ] || die "checksum mismatch for ${asset}"

  tar -xzf "${tarball}" -C "${TEMP_DIR}"
  install -m 0755 "${TEMP_DIR}/gitleaks" /usr/local/bin/gitleaks
  gitleaks version
}

run_gitleaks() {
  local report="${repo_root}/gitleaks.sarif"
  local config_path=""

  if config_path="$(resolve_gitleaks_config)"; then
    log "Running Gitleaks with config: ${config_path}"
    gitleaks detect \
      --source . \
      --config "${config_path}" \
      --redact \
      --no-banner \
      --report-format sarif \
      --report-path "${report}" \
      --exit-code 1
  else
    warn "No Gitleaks config found; running with default rules"
    gitleaks detect \
      --source . \
      --redact \
      --no-banner \
      --report-format sarif \
      --report-path "${report}" \
      --exit-code 1
  fi
}

run_trivy_fs() {
  local report="${repo_root}/trivy-fs.sarif"

  log "Running Trivy filesystem scan"
  docker run --rm \
    -v "${repo_root}:/repo:ro" \
    -w /repo \
    "${TRIVY_IMAGE}" \
    fs \
    --scanners "${TRIVY_SCANNERS}" \
    --severity "${TRIVY_SEVERITY}" \
    $( [ "${TRIVY_IGNORE_UNFIXED}" = "true" ] && printf '%s ' '--ignore-unfixed' ) \
    --timeout "${TRIVY_TIMEOUT}" \
    --exit-code 1 \
    --format sarif \
    --output "${report}" \
    "${BUILD_CONTEXT}"
}

build_and_scan_platform() {
  local platform="$1"
  local safe_platform temp_tag image_report

  safe_platform="${platform//\//_}"
  safe_platform="${safe_platform//,/__}"
  temp_tag="${IMAGE_NAME}:${IMAGE_TAG}-${safe_platform}"
  image_report="${repo_root}/trivy-image-${safe_platform}.sarif"

  log "Building local image for scan: ${temp_tag} (${platform})"
  docker buildx build \
    --builder "${BUILDER_NAME}" \
    --platform "${platform}" \
    --file "${DOCKERFILE_PATH}" \
    --tag "${temp_tag}" \
    --load \
    --pull \
    --provenance=false \
    --sbom=false \
    "${BUILD_CONTEXT}"

  log "Running Trivy image scan: ${temp_tag}"
  docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    -v "${repo_root}:/repo" \
    -w /repo \
    "${TRIVY_IMAGE}" \
    image \
    --scanners "${TRIVY_SCANNERS}" \
    --severity "${TRIVY_SEVERITY}" \
    $( [ "${TRIVY_IGNORE_UNFIXED}" = "true" ] && printf '%s ' '--ignore-unfixed' ) \
    --timeout "${TRIVY_TIMEOUT}" \
    --exit-code 1 \
    --format sarif \
    --output "${image_report}" \
    "${temp_tag}"
}

login_ghcr() {
  [ -n "${GIT_PAT}" ] || die "GIT_PAT is required for GHCR push"
  [ -n "${GHCR_USERNAME}" ] || die "GHCR_USERNAME is required for GHCR push"
  log "Logging in to GHCR"
  printf '%s\n' "${GIT_PAT}" | docker login ghcr.io -u "${GHCR_USERNAME}" --password-stdin >/dev/null
}

push_multiarch_manifest() {
  local image_repo
  image_repo="$(registry_repo)"

  log "Pushing multi-arch image: ${image_repo}:${IMAGE_TAG}"
  docker buildx build \
    --builder "${BUILDER_NAME}" \
    --platform "${PLATFORMS}" \
    --file "${DOCKERFILE_PATH}" \
    --tag "${image_repo}:${IMAGE_TAG}" \
    --provenance=true \
    --sbom=true \
    --push \
    "${BUILD_CONTEXT}"
}

main() {
  [ -d "${BUILD_CONTEXT}" ] || die "build context not found: ${BUILD_CONTEXT}"
  [ -f "${DOCKERFILE_PATH}" ] || die "dockerfile not found: ${DOCKERFILE_PATH}"

  log "IMAGE_NAME=${IMAGE_NAME}"
  log "IMAGE_TAG=${IMAGE_TAG}"
  log "BUILD_CONTEXT=${BUILD_CONTEXT}"
  log "DOCKERFILE_PATH=${DOCKERFILE_PATH}"
  log "PLATFORMS=${PLATFORMS}"
  log "REGISTRY_TYPE=${REGISTRY_TYPE}"
  log "PUSH_IMAGE=${PUSH_IMAGE}"

  install_gitleaks
  docker buildx create --name "${BUILDER_NAME}" --use >/dev/null
  docker buildx inspect --bootstrap >/dev/null

  run_gitleaks
  run_trivy_fs

  IFS=',' read -r -a platform_list <<< "${PLATFORMS}"
  for platform in "${platform_list[@]}"; do
    build_and_scan_platform "${platform}"
  done

  if [ "${PUSH_IMAGE}" = "true" ]; then
    case "${REGISTRY_TYPE}" in
      ghcr)
        login_ghcr
        ;;
      ecr)
        warn "ECR login is expected from the workflow job before this script runs"
        ;;
    esac
    push_multiarch_manifest
  else
    warn "PUSH_IMAGE=false; skipping registry push"
  fi

  log "Spark CI complete"
}

main "$@"