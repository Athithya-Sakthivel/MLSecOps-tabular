#!/usr/bin/env bash
set -euo pipefail

: "${GIT_PAT:?GIT_PAT is required}"

GHCR_USER="${GHCR_USER:-athithya-sakthivel}"
IMAGE_TAG="${IMAGE_TAG:-$(date -u +%Y-%m-%d-%H-%M)-$(git rev-parse --short HEAD)}"
ELT_TASK_IMAGE="${ELT_TASK_IMAGE:-ghcr.io/${GHCR_USER}/flyte-elt-task:${IMAGE_TAG}}"

echo "${GIT_PAT}" | docker login ghcr.io -u "${GHCR_USER}" --password-stdin

docker build \
  -t "${ELT_TASK_IMAGE}" \
  -f src/workflows/ELT/Dockerfile.flyte_task \
  .

docker push "${ELT_TASK_IMAGE}"

echo "Pushed:"
echo "  ${ELT_TASK_IMAGE}"





  