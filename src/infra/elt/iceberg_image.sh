#!/usr/bin/env bash
set -euo pipefail

: "${GIT_PAT:?}"

GHCR_USER="${GHCR_USER:-athithya-sakthivel}"
IMAGE_NAME="${IMAGE_NAME:-iceberg-rest}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-src/infra/elt/Dockerfile.iceberg}"
IMAGE_TAG="${IMAGE_TAG:-$(TZ=Asia/Kolkata date +%Y-%m-%d-%H-%M)--$(git rev-parse --short HEAD)}"
FULL_IMAGE="ghcr.io/${GHCR_USER}/${IMAGE_NAME}:${IMAGE_TAG}"

DOCKER_CONFIG="$(mktemp -d)"
export DOCKER_CONFIG

printf '%s' "${GIT_PAT}" | docker login ghcr.io -u "${GHCR_USER}" --password-stdin >/dev/null

docker build \
  --no-cache \
  --pull \
  -t "${FULL_IMAGE}" \
  -f "${DOCKERFILE_PATH}" \
  .

docker run --rm --entrypoint java "${FULL_IMAGE}" -version >/dev/null

docker push "${FULL_IMAGE}"

echo "${FULL_IMAGE}"