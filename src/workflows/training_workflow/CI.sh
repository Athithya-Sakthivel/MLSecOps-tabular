#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
if [ -z "$MODE" ]; then
  printf '\033[0;31m[CI-ERR]\033[0m %s\n' "usage: $0 --localhost|--prod" >&2
  exit 2
fi

BUILD_CONTEXT="${BUILD_CONTEXT:-src/workflows/training_workflow}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-${BUILD_CONTEXT}/Dockerfile.task_image}"
REGISTRY_TYPE="${REGISTRY_TYPE:-dockerhub}"
PLATFORMS="${PLATFORMS:-linux/amd64}"
PUSH="${PUSH:-true}"
TRIVY_IMAGE="${TRIVY_IMAGE:-aquasec/trivy:latest}"
TRIVY_SEVERITY="${TRIVY_SEVERITY:-CRITICAL}"
AWS_REGION="${AWS_REGION:-ap-south-1}"

log(){ printf '\033[0;34m[CI]\033[0m %s\n' "$*"; }
err(){ printf '\033[0;31m[CI-ERR]\033[0m %s\n' "$*"; exit 1; }

[ -f "${DOCKERFILE_PATH}" ] || err "Dockerfile not found: ${DOCKERFILE_PATH}"
[ -f "${BUILD_CONTEXT}/requirements.txt" ] || err "requirements.txt missing in ${BUILD_CONTEXT}"

DEPS_SHA=$(sha256sum "${BUILD_CONTEXT}/requirements.txt" | cut -d' ' -f1)
IMAGE_NAME="${IMAGE_NAME:-training-runtime}"
IMAGE_TAG="${IMAGE_TAG:-deps-${DEPS_SHA}}"

if [ "${MODE}" = "--localhost" ]; then
  IMAGE_REF="${IMAGE_REF:-localhost:30000/${IMAGE_NAME}:${IMAGE_TAG}}"
elif [ "${MODE}" = "--prod" ]; then
  if [ "${REGISTRY_TYPE}" = "ecr" ]; then
    [ -n "${ECR_REPO:-}" ] || err "ECR_REPO required for ecr in prod mode"
    IMAGE_REF="${IMAGE_REF:-${ECR_REPO}:${IMAGE_TAG}}"
  else
    [ -n "${DOCKER_USERNAME:-}" ] || err "DOCKER_USERNAME required for dockerhub in prod mode"
    IMAGE_REF="${IMAGE_REF:-${DOCKER_USERNAME}/${IMAGE_NAME}:${IMAGE_TAG}}"
  fi
else
  err "unknown mode: ${MODE}; use --localhost or --prod"
fi

log "MODE=${MODE}"
log "IMAGE_REF=${IMAGE_REF}"
log "BUILD_CONTEXT=${BUILD_CONTEXT}"
log "DOCKERFILE=${DOCKERFILE_PATH}"
log "PLATFORMS=${PLATFORMS}"
log "PUSH=${PUSH}"

# if exact image already exists remotely (or locally), skip build/push
if docker pull "${IMAGE_REF}" >/dev/null 2>&1; then
  log "Image ${IMAGE_REF} already exists; skipping build and push"
  echo "${IMAGE_REF}"
  exit 0
fi

BUILD_PLATFORM="${PLATFORMS%%,*}"

log "Building local image for scan (platform=${BUILD_PLATFORM})"
docker buildx build --platform "${BUILD_PLATFORM}" --tag "${IMAGE_REF}" --file "${DOCKERFILE_PATH}" --load "${BUILD_CONTEXT}" || err "Local build failed"

log "Running Trivy scan (severity threshold=${TRIVY_SEVERITY})"
if ! docker run --rm -v /var/run/docker.sock:/var/run/docker.sock:ro "${TRIVY_IMAGE}" image --exit-code 1 --severity "${TRIVY_SEVERITY}" --no-progress "${IMAGE_REF}"; then
  log "Trivy scan reported issues; printing summary"
  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock:ro "${TRIVY_IMAGE}" image --severity "${TRIVY_SEVERITY}" --format table "${IMAGE_REF}" || true
  err "Trivy scan failed threshold ${TRIVY_SEVERITY}"
fi

if [ "${PUSH}" = "true" ]; then
  if [ "${MODE}" = "--localhost" ]; then
    log "Pushing to local sandbox registry ${IMAGE_REF}"
    docker tag "${IMAGE_REF}" "${IMAGE_REF}" || true
    docker push "${IMAGE_REF}" || err "Push to localhost registry failed"
    log "Pushed ${IMAGE_REF} to localhost registry"
  else
    if [ "${REGISTRY_TYPE}" = "ecr" ]; then
      log "Authenticating to ECR and pushing ${IMAGE_REF}"
      if command -v aws >/dev/null 2>&1; then
        aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "$(echo "${ECR_REPO}" | cut -d'/' -f1)" || err "ECR login failed"
      fi
      docker buildx build --builder default --platform "${PLATFORMS}" --tag "${IMAGE_REF}" --file "${DOCKERFILE_PATH}" --push "${BUILD_CONTEXT}" || err "Multi-platform push to ECR failed"
      log "Pushed ${IMAGE_REF} to ECR"
    else
      log "Authenticating to Docker registry and pushing ${IMAGE_REF}"
      printf '%s\n' "${DOCKER_PASSWORD:-}" | docker login -u "${DOCKER_USERNAME}" --password-stdin || err "Docker login failed"
      docker buildx build --builder default --platform "${PLATFORMS}" --tag "${IMAGE_REF}" --file "${DOCKERFILE_PATH}" --push "${BUILD_CONTEXT}" || err "Multi-platform push to Docker registry failed"
      log "Pushed ${IMAGE_REF} to registry"
    fi
  fi
else
  log "PUSH=false, skipping remote push"
fi

log "CI image build/scan/push completed. IMAGE=${IMAGE_REF}"
echo "${IMAGE_REF}"