#!/usr/bin/env bash
# works in CI only not in devcontianer
set -euo pipefail

IMAGE="ghcr.io/athithya-sakthivel/trivy-0.69.3-gitleaks-8.30.1-opengrep-1.16.5@sha256:2d19bd7b418692a20ae57b24620da3af648878a66d582022c0721512fff1b5e2"
REPO_ROOT="$(git rev-parse --show-toplevel)"
RULE_FILE="$REPO_ROOT/src/opengrep/mlops_sast_rules.yaml"

if [[ ! -s "$RULE_FILE" ]]; then
  echo "Missing or empty rules file: $RULE_FILE" >&2
  ls -la "$REPO_ROOT/src/opengrep" >&2 || true
  exit 1
fi

docker run --rm \
  --entrypoint sh \
  -v "$REPO_ROOT:/workspace" \
  -w /workspace \
  "$IMAGE" \
  -c '
    set -euo pipefail

    echo "=== OpenGrep (SAST) ==="
    PYTHONWARNINGS=ignore opengrep scan \
      --config /workspace/src/opengrep/mlops_sast_rules.yaml \
      --error \
      .

    echo "=== Gitleaks (Secrets from all commits) ==="
    gitleaks git \
      --log-opts="--all" \
      --no-banner \
      --redact \
      --exit-code 1

    echo "=== Trivy (Filesystem) ==="
    trivy fs \
      --scanners vuln,misconfig \
      --severity HIGH,CRITICAL \
      --ignore-unfixed \
      --skip-dirs .git \
      --skip-dirs src/opengrep \
      --exit-code 1 \
      .
  '