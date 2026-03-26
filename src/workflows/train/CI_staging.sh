#!/usr/bin/env bash
set -euo pipefail

: "${GIT_PAT:?GIT_PAT is required}"

GHCR_USER="${GHCR_USER:-athithya-sakthivel}"
IMAGE_TAG="${IMAGE_TAG:-$(date -u +%Y-%m-%d-%H-%M)-$(git rev-parse --short HEAD)}"

PUSH_IMAGE="${PUSH_IMAGE:-true}"

TRAIN_TASK_IMAGE="ghcr.io/${GHCR_USER}/flyte-train-task:${IMAGE_TAG}"

echo "${GIT_PAT}" | docker login ghcr.io -u "${GHCR_USER}" --password-stdin

docker build \
  -t "${TRAIN_TASK_IMAGE}" \
  -f src/workflows/train/Dockerfile.task_image \
  .

if [ "${PUSH_IMAGE}" = "true" ]; then
  docker push "${TRAIN_TASK_IMAGE}"
  echo "Pushed:"
  echo "  ${TRAIN_TASK_IMAGE}"
else
  echo "Build complete (no push)"
fi