#!/usr/bin/env bash
set -euo pipefail

IMAGE_REF="${IMAGE_REF:-}"
if [ -z "$IMAGE_REF" ]; then
  IMAGE_REF="$(bash "$(dirname "$0")/CI.sh" --localhost)"
fi

flytectl sandbox status >/dev/null 2>&1 || flytectl demo start

PROJECT="${FLYTE_PROJECT:-mlops}"
DOMAIN="${FLYTE_DOMAIN:-development}"

pyflyte run --remote --project "$PROJECT" --domain "$DOMAIN" --image "$IMAGE_REF" src/workflows/training_workflow/flyte_training_workflow.py training_workflow