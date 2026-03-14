#!/usr/bin/env bash
set -euo pipefail

SPARK_VERSION="${SPARK_VERSION:-4.0.2}"
GITHUB_USER="${GITHUB_USER:-${GITHUB_REPOSITORY_OWNER:-athithya-sakthivel}}"
REGISTRY_TYPE="${REGISTRY_TYPE:-ghcr}" # or ECR
REGISTRY="${REGISTRY:-ghcr.io}"
IMAGE_REPO="${IMAGE_REPO:-spark}"
IMAGE_TAG="${IMAGE_TAG:-${SPARK_VERSION}-multiarch}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
TRIVY_IMAGE="${TRIVY_IMAGE:-aquasec/trivy@sha256:3d1f862cb6c4fe13c1506f96f816096030d8d5ccdb2380a3069f7bf07daa86aa}"
TRIVY_SEVERITY="${TRIVY_SEVERITY:-CRITICAL}"
BUILD_CONTEXT="${BUILD_CONTEXT:-src/infra/spark}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-${BUILD_CONTEXT}/Dockerfile}"
PUSH="true"
BUILDER="spark-builder-$$"
LOCAL_PLATFORM="linux/amd64"
ECR_REPO="${ECR_REPO:-}"
AWS_REGION="${AWS_REGION:-ap-south-1}"
FORCE_REBUILD="${FORCE_REBUILD:-false}"

log(){ printf '[%s] [spark-ci] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"; }
fatal(){ printf '[%s] [spark-ci][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; exit 1; }
require(){ command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"; }

trap 'rc=$?; log "exiting rc=${rc}"; docker buildx rm "${BUILDER}" >/dev/null 2>&1 || true; exit $rc' EXIT

require docker
require git
if ! docker buildx version >/dev/null 2>&1; then log "docker buildx not available, continuing"; fi
[ -f "${DOCKERFILE_PATH}" ] || fatal "Dockerfile missing at ${DOCKERFILE_PATH}"
[ -d "${BUILD_CONTEXT}" ] || fatal "build context missing at ${BUILD_CONTEXT}"

if [ "${REGISTRY_TYPE}" = "ecr" ]; then
  [ -n "${ECR_REPO}" ] || fatal "REGISTRY_TYPE=ecr requires ECR_REPO"
  IMAGE_REF_BASE="${ECR_REPO}"
  PROVIDER="ECR"
else
  IMAGE_REF_BASE="ghcr.io/${GITHUB_USER}/${IMAGE_REPO}"
  PROVIDER="GHCR"
fi

repo_root="$(git rev-parse --show-toplevel)"
context_files="$(git -C "${repo_root}" ls-files -s "${BUILD_CONTEXT}" | sha1sum | awk '{print $1}')"
context_hash="${context_files}"
content_tag="${IMAGE_TAG}-${context_hash:0:12}"
image_ref_content="${IMAGE_REF_BASE}:${content_tag}"
image_ref_version="${IMAGE_REF_BASE}:${IMAGE_TAG}"

log "provider: ${PROVIDER}"
log "image content tag: ${image_ref_content}"
log "image version tag: ${image_ref_version}"
log "build context: ${BUILD_CONTEXT}"
log "dockerfile: ${DOCKERFILE_PATH}"

if [ "${FORCE_REBUILD}" != "true" ]; then
  if [ "${REGISTRY_TYPE}" = "ecr" ]; then
    repo_name="$(basename "${ECR_REPO}")"
    if aws ecr describe-images --repository-name "${repo_name}" --image-ids imageTag="${content_tag}" --region "${AWS_REGION}" >/dev/null 2>&1; then
      log "remote image with identical content exists: ${image_ref_content}"
      if ! aws ecr describe-images --repository-name "${repo_name}" --image-ids imageTag="${IMAGE_TAG}" --region "${AWS_REGION}" >/dev/null 2>&1; then
        log "version tag missing, retagging remote image by pulling and pushing version tag"
        ECR_HOST="$(printf '%s' "${ECR_REPO}" | cut -d'/' -f1)"
        aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ECR_HOST}" >/dev/null 2>&1 || fatal "ECR login failed"
        docker pull "${image_ref_content}" >/dev/null || fatal "failed to pull ${image_ref_content}"
        docker tag "${image_ref_content}" "${image_ref_version}"
        docker push "${image_ref_version}" >/dev/null || fatal "failed to push ${image_ref_version}"
      fi
      log "skip build/push because identical image already present"
      exit 0
    fi
  else
    if [ -n "${GITHUB_TOKEN:-}" ]; then
      http_code="$(curl -s -o /dev/null -w '%{http_code}' -I -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' -u "${GITHUB_USER}:${GITHUB_TOKEN}" "https://ghcr.io/v2/${GITHUB_USER}/${IMAGE_REPO}/manifests/${content_tag}")"
      if [ "${http_code}" = "200" ]; then
        log "remote image with identical content exists: ${image_ref_content}"
        http_code_version="$(curl -s -o /dev/null -w '%{http_code}' -I -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' -u "${GITHUB_USER}:${GITHUB_TOKEN}" "https://ghcr.io/v2/${GITHUB_USER}/${IMAGE_REPO}/manifests/${IMAGE_TAG}")"
        if [ "${http_code_version}" != "200" ]; then
          log "version tag missing, retagging by pulling and pushing version tag"
          echo "${GITHUB_TOKEN}" | docker login ghcr.io -u "${GITHUB_USER}" --password-stdin >/dev/null 2>&1 || fatal "ghcr login failed"
          docker pull "${image_ref_content}" >/dev/null || fatal "failed to pull ${image_ref_content}"
          docker tag "${image_ref_content}" "${image_ref_version}"
          docker push "${image_ref_version}" >/dev/null || fatal "failed to push ${image_ref_version}"
        fi
        log "skip build/push because identical image already present"
        exit 0
      fi
    else
      log "GITHUB_TOKEN not provided; cannot check remote GHCR manifest; proceeding with build"
    fi
  fi
else
  log "force rebuild requested; proceeding with build"
fi

if [ "${REGISTRY_TYPE}" = "ecr" ]; then
  ECR_HOST="$(printf '%s' "${ECR_REPO}" | cut -d'/' -f1)"
  aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ECR_HOST}" >/dev/null 2>&1 || fatal "ECR login failed"
else
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    echo "${GITHUB_TOKEN}" | docker login ghcr.io -u "${GITHUB_USER}" --password-stdin >/dev/null 2>&1 || fatal "ghcr login failed"
  else
    log "GITHUB_TOKEN not provided; assuming Docker already authenticated for GHCR"
  fi
fi

log "performing local single-arch build for scan"
docker buildx build --platform "${LOCAL_PLATFORM}" --tag "${image_ref_content}" --file "${DOCKERFILE_PATH}" --load "${BUILD_CONTEXT}" >/dev/null || fatal "local build failed"

log "running Trivy scan (severity=${TRIVY_SEVERITY})"
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock:ro "${TRIVY_IMAGE}" image --exit-code 1 --severity "${TRIVY_SEVERITY}" --no-progress "${image_ref_content}" || {
  log "trivy scan failed; printing findings"
  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock:ro "${TRIVY_IMAGE}" image --severity "${TRIVY_SEVERITY}" --format table "${image_ref_content}" >&2
  fatal "vulnerabilities exceed threshold ${TRIVY_SEVERITY}"
}

log "creating buildx builder ${BUILDER}"
docker buildx create --name "${BUILDER}" --driver docker-container --use --bootstrap >/dev/null || fatal "buildx create failed"

CACHE_FROM="type=gha,scope=spark-${IMAGE_REPO}-${IMAGE_TAG}"
CACHE_TO="type=gha,scope=spark-${IMAGE_REPO}-${IMAGE_TAG},mode=max"

log "building and pushing multi-arch image ${IMAGE_REF_BASE} with tags ${content_tag} and ${IMAGE_TAG}"
docker buildx build --builder "${BUILDER}" --platform "${PLATFORMS}" --tag "${image_ref_content}" --tag "${image_ref_version}" --file "${DOCKERFILE_PATH}" --push --cache-from "${CACHE_FROM}" --cache-to "${CACHE_TO}" "${BUILD_CONTEXT}" || fatal "multi-arch build/push failed"

log "verifying remote image is pullable"
docker pull "${image_ref_version}" >/dev/null || fatal "failed to pull pushed image ${image_ref_version}"

log "completed build, scan, and push: ${image_ref_version}"