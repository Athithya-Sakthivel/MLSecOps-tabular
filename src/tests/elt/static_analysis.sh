#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "::error::$*" >&2
  exit 1
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

find_repo_root() {
  local dir="$1"
  while [[ "$dir" != "/" ]]; do
    if [[ -d "$dir/static_analysis/elt" && -d "$dir/workflows/ELT" ]]; then
      printf '%s\n' "$dir"
      return 0
    fi
    if [[ -d "$dir/src/static_analysis/elt" && -d "$dir/src/workflows/ELT" ]]; then
      printf '%s\n' "$dir"
      return 0
    fi
    dir="$(dirname "$dir")"
  done
  return 1
}

repo_root="$(find_repo_root "$script_dir")" || die "Could not locate repo root from: $script_dir"

if [[ -d "$repo_root/static_analysis/elt" ]]; then
  container_rule_dir="/workspace/static_analysis/elt"
  container_target_dir="/workspace/workflows/ELT"
  container_scan_root="/workspace"
  host_rule_dir="$repo_root/static_analysis/elt"
else
  container_rule_dir="/workspace/src/static_analysis/elt"
  container_target_dir="/workspace/src/workflows/ELT"
  container_scan_root="/workspace/src"
  host_rule_dir="$repo_root/src/static_analysis/elt"
fi

rules=(
  security.yaml
  data_quality.yaml
  reliability.yaml
  workflow_reliability.yaml
  spark_performance.yaml
  iceberg_safety.yaml
)

for rule in "${rules[@]}"; do
  [[ -s "$host_rule_dir/$rule" ]] || die "Missing or empty ELT rule file: $host_rule_dir/$rule"
done

scanner_image="ghcr.io/athithya-sakthivel/trivy-0.69.3-gitleaks-8.30.1-opengrep-1.16.5@sha256:2d19bd7b418692a20ae57b24620da3af648878a66d582022c0721512fff1b5e2"
max_target_bytes=2000000

common_docker_args=(
  --rm
  -e HOME=/home/appuser
  -e XDG_CACHE_HOME=/home/appuser/.cache
  -e PYTHONWARNINGS=ignore
  -v "$repo_root:/workspace"
  -w /workspace
  --entrypoint /bin/sh
  "$scanner_image"
)

echo "=== Standard SAST (Prebuilt Rules) ==="
docker run "${common_docker_args[@]}" -ec "
  set -eu

  opengrep scan \
    --verbose \
    --config p/owasp-top-ten \
    --config p/python \
    --config p/secrets \
    --config p/dockerfile \
    --config p/docker-compose \
    --config p/kubernetes \
    --error \
    --no-git-ignore \
    --max-target-bytes $max_target_bytes \
    --exclude .spark-ci-artifacts \
    --exclude .venv \
    --exclude .venv_elt \
    --exclude .venv_deploy \
    --exclude .venv_train \
    --exclude .repos \
    --exclude __pycache__ \
    --exclude .pytest_cache \
    --exclude .mypy_cache \
    --exclude node_modules \
    $container_scan_root
"

echo "=== ELT Pipeline Scan (Custom Rules) ==="
docker run "${common_docker_args[@]}" -ec "
  set -eu

  test -d '$container_rule_dir' || { echo 'Missing rules dir: $container_rule_dir' >&2; exit 1; }
  test -d '$container_target_dir' || { echo 'Missing target dir: $container_target_dir' >&2; exit 1; }

  opengrep scan \
    --verbose \
    --config '$container_rule_dir/security.yaml' \
    --config '$container_rule_dir/data_quality.yaml' \
    --config '$container_rule_dir/reliability.yaml' \
    --config '$container_rule_dir/workflow_reliability.yaml' \
    --config '$container_rule_dir/spark_performance.yaml' \
    --config '$container_rule_dir/iceberg_safety.yaml' \
    --error \
    --no-git-ignore \
    --max-target-bytes $max_target_bytes \
    --exclude .spark-ci-artifacts \
    --exclude .venv \
    --exclude .venv_elt \
    --exclude .venv_deploy \
    --exclude .venv_train \
    --exclude .repos \
    --exclude __pycache__ \
    --exclude .pytest_cache \
    --exclude .mypy_cache \
    --exclude node_modules \
    $container_target_dir
"