#!/usr/bin/env bash
# Bootstraps a fresh kind cluster and installs all required infrastructure components
# Deploys KubeRay operator, SigNoz observability stack, and the inference service
# Configures model, runtime, and OpenTelemetry environment variables for the service
# Waits for Ray cluster readiness, exposes the service via port-forward, and validates health
# Generates realistic traffic patterns (normal, spike, error) to test inference and observability

IFS=$'\n\t'
umask 077

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
readonly KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-local-cluster}"
readonly INFERENCE_NAMESPACE="${INFERENCE_NAMESPACE:-inference}"
readonly SIGNOZ_NAMESPACE="${SIGNOZ_NAMESPACE:-signoz}"
readonly PORT_FORWARD_LOCAL_PORT="${PORT_FORWARD_LOCAL_PORT:-8000}"
readonly KEEP_ALIVE_SECONDS="${KEEP_ALIVE_SECONDS:-1800}"
readonly STEP_TIMEOUT="${STEP_TIMEOUT:-30m}"
readonly BOOTSTRAP_TIMEOUT="${BOOTSTRAP_TIMEOUT:-20m}"


export MODEL_URI="${MODEL_URI:-s3://e2e-mlops-data-681802563986/model-artifacts/trip_eta_lgbm_v1/bceb2eb9-e373-4c44-91e5-abde147fec8b/2025-01-06}"
export MODEL_VERSION="${MODEL_VERSION:-v1}"
export MODEL_SHA256="${MODEL_SHA256:-29505278adb825a2f79812221b5d3a245145e140973d0354b74e278b50811976}"
export MODEL_INPUT_NAME="${MODEL_INPUT_NAME:-input}"
export MODEL_OUTPUT_NAMES="${MODEL_OUTPUT_NAMES:-variable}"
export FEATURE_ORDER="${FEATURE_ORDER:-pickup_hour,pickup_dow,pickup_month,pickup_is_weekend,pickup_borough_id,pickup_zone_id,pickup_service_zone_id,dropoff_borough_id,dropoff_zone_id,dropoff_service_zone_id,route_pair_id,avg_duration_7d_zone_hour,avg_fare_30d_zone,trip_count_90d_zone_hour}"
export ALLOW_EXTRA_FEATURES="${ALLOW_EXTRA_FEATURES:-false}"
export MODEL_CACHE_DIR="${MODEL_CACHE_DIR:-/mlsecops/model-cache}"
export RAY_IMAGE="${RAY_IMAGE:-ghcr.io/athithya-sakthivel/tabular-inference-service:2026-04-15-06-12--b2617e3@sha256:3615562548755e906ad17945518267cfadbac7bdaacc5a76fc6ebd4dfbcfd6f4}"
export USE_IAM="${USE_IAM:-false}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://signoz-otel-collector.signoz.svc.cluster.local:4317}"


# temprory dev override to get adequate telemtry
export SLOW_REQUEST_MS="${SLOW_REQUEST_MS:-100}"
export OTEL_TRACES_SAMPLER=parentbased_traceidratio
export OTEL_TRACES_SAMPLER_ARG=0.7
export LOG_LEVEL="${LOG_LEVEL:-INFO}"


readonly DO_CLUSTER_BOOTSTRAP="${DO_CLUSTER_BOOTSTRAP:-1}"
readonly DO_ROLLOUT="${DO_ROLLOUT:-1}"
readonly DO_TRAFFIC="${DO_TRAFFIC:-1}"

TMP_DIR="$(mktemp -d)"
PORT_FORWARD_LOG="${TMP_DIR}/port-forward.log"
PORT_FORWARD_PID=""
RAY_CLUSTER_NAME=""

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

fatal() {
  log "FATAL: $*"
  exit 1
}

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"
}

is_true() {
  case "${1:-}" in
    1|true|TRUE|True|yes|YES|Yes|on|ON|On) return 0 ;;
    *) return 1 ;;
  esac
}

cleanup() {
  if [[ -n "${PORT_FORWARD_PID}" ]] && kill -0 "${PORT_FORWARD_PID}" >/dev/null 2>&1; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi
  rm -rf -- "${TMP_DIR}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

run_step() {
  local label="$1"
  shift
  log "${label}"
  timeout --kill-after=30s "${STEP_TIMEOUT}" "$@"
}

validate_env() {
  if [[ "${OTEL_TRACES_SAMPLER}" == "always_on" ]]; then
    unset OTEL_TRACES_SAMPLER_ARG || true
  else
    [[ "${OTEL_TRACES_SAMPLER_ARG}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fatal "OTEL_TRACES_SAMPLER_ARG must be numeric in [0.0, 1.0] or use OTEL_TRACES_SAMPLER=always_on"
    python3 - <<PY
from __future__ import annotations
v = float("${OTEL_TRACES_SAMPLER_ARG}")
if not 0.0 <= v <= 1.0:
    raise SystemExit("OTEL_TRACES_SAMPLER_ARG must be in [0.0, 1.0]")
PY
  fi

  if is_true "${DO_ROLLOUT}" && [[ "${USE_IAM}" != "true" ]]; then
    [[ -n "${AWS_ACCESS_KEY_ID:-}" ]] || fatal "AWS_ACCESS_KEY_ID is required when USE_IAM=false"
    [[ -n "${AWS_SECRET_ACCESS_KEY:-}" ]] || fatal "AWS_SECRET_ACCESS_KEY is required when USE_IAM=false"
  fi
}

bootstrap_kind_cluster() {
  log "deleting kind cluster ${KIND_CLUSTER_NAME}"
  kind delete cluster --name "${KIND_CLUSTER_NAME}" || true

  log "creating kind cluster ${KIND_CLUSTER_NAME}"
  timeout --kill-after=30s "${BOOTSTRAP_TIMEOUT}" kind create cluster --name "${KIND_CLUSTER_NAME}"

  kubectl cluster-info >/dev/null
}

install_default_storage_class() {
  run_step "installing default StorageClass" bash "${REPO_ROOT}/src/infra/core/default_storage_class.sh" --setup
}

install_kuberay_operator() {
  run_step "installing KubeRay operator" bash "${REPO_ROOT}/src/infra/deploy/kuberay_operator.sh" --rollout
}

rollout_signoz() {
  run_step "rolling out SigNoz" bash "${REPO_ROOT}/src/infra/observability/signoz.sh" --rollout
}

rollout_inference() {
  run_step "rolling out inference service" python3 "${REPO_ROOT}/src/infra/deploy/inference_service.py" --rollout
}

wait_for_selector_ready() {
  local namespace="$1"
  local selector="$2"
  local timeout_s="${3:-900}"

  log "waiting for pods in ${namespace} with selector [${selector}] to become Ready"
  kubectl wait --for=condition=Ready pod -n "${namespace}" -l "${selector}" --timeout="${timeout_s}s" >/dev/null
}

detect_ray_cluster_name() {
  python3 - <<'PY'
from __future__ import annotations

import json
import subprocess
import time

deadline = time.time() + 300.0
while time.time() < deadline:
    try:
        data = subprocess.check_output(["kubectl", "get", "pods", "-n", "inference", "-o", "json"], text=True)
    except subprocess.CalledProcessError:
        time.sleep(2)
        continue

    obj = json.loads(data)
    for item in obj.get("items", []):
        labels = item.get("metadata", {}).get("labels", {}) or {}
        if labels.get("ray.io/node-type") == "head":
            cluster = labels.get("ray.io/cluster")
            if cluster:
                print(cluster)
                raise SystemExit(0)

    time.sleep(2)

raise SystemExit("could not detect Ray cluster name")
PY
}

resolve_head_service() {
  local cluster="$1"
  printf '%s-head-svc' "${cluster}"
}

wait_for_service_endpoints() {
  local namespace="$1"
  local service="$2"
  local timeout_s="${3:-600}"
  local elapsed=0

  while (( elapsed < timeout_s )); do
    if kubectl -n "${namespace}" get endpoints "${service}" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | grep -q '.'; then
      log "service ${namespace}/${service} has ready endpoints"
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done

  fatal "timed out waiting for endpoints on ${namespace}/${service}"
}

start_port_forward() {
  local namespace="$1"
  local service="$2"

  log "starting port-forward svc/${service} -> 127.0.0.1:${PORT_FORWARD_LOCAL_PORT}"
  kubectl -n "${namespace}" port-forward "svc/${service}" "${PORT_FORWARD_LOCAL_PORT}:8000" >"${PORT_FORWARD_LOG}" 2>&1 &
  PORT_FORWARD_PID="$!"

  local elapsed=0
  while (( elapsed < 180 )); do
    if curl -fsS --max-time 5 "http://127.0.0.1:${PORT_FORWARD_LOCAL_PORT}/-/healthz" >/dev/null 2>&1; then
      log "port-forward is ready"
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done

  sed -n '1,200p' "${PORT_FORWARD_LOG}" >&2 || true
  fatal "port-forward did not become ready"
}

write_payload_file() {
  local out_file="$1"
  local count="$2"
  python3 - "$out_file" "$count" <<'PY'
from __future__ import annotations
import copy
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
count = int(sys.argv[2])

base = {
    "pickup_hour": 9,
    "pickup_dow": 2,
    "pickup_month": 4,
    "pickup_is_weekend": 0,
    "pickup_borough_id": 1,
    "pickup_zone_id": 15,
    "pickup_service_zone_id": 2,
    "dropoff_borough_id": 1,
    "dropoff_zone_id": 30,
    "dropoff_service_zone_id": 2,
    "route_pair_id": 123,
    "avg_duration_7d_zone_hour": 500.0,
    "avg_fare_30d_zone": 18.0,
    "trip_count_90d_zone_hour": 60,
}
payload = {"instances": [copy.deepcopy(base) for _ in range(count)]}
path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
}

write_invalid_json() {
  local out_file="$1"
  cat >"${out_file}" <<'EOF'
{
  "instances": [
EOF
}

write_empty_instances() {
  local out_file="$1"
  cat >"${out_file}" <<'EOF'
{
  "instances": []
}
EOF
}

write_oversize_payload() {
  local out_file="$1"
  python3 - "$out_file" <<'PY'
from __future__ import annotations
import copy
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])

base = {
    "pickup_hour": 9,
    "pickup_dow": 2,
    "pickup_month": 4,
    "pickup_is_weekend": 0,
    "pickup_borough_id": 1,
    "pickup_zone_id": 15,
    "pickup_service_zone_id": 2,
    "dropoff_borough_id": 1,
    "dropoff_zone_id": 30,
    "dropoff_service_zone_id": 2,
    "route_pair_id": 123,
    "avg_duration_7d_zone_hour": 500.0,
    "avg_fare_30d_zone": 18.0,
    "trip_count_90d_zone_hour": 60,
}
payload = {"instances": [copy.deepcopy(base) for _ in range(257)]}
path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
}

run_health_phase() {
  local total="${1:-20}"
  local interval_s="${2:-3}"
  log "health phase: ${total} requests @ ${interval_s}s"

  local i=1
  while (( i <= total )); do
    local body
    body="$(curl -fsS --max-time 10 "http://127.0.0.1:${PORT_FORWARD_LOCAL_PORT}/-/healthz")"
    [[ "${body}" == "success" ]] || fatal "unexpected /-/healthz body: ${body}"
    sleep "${interval_s}"
    i=$((i + 1))
  done
}

run_predict_phase() {
  local phase_name="$1"
  local total="$2"
  local interval_s="$3"
  local payload_file="$4"

  log "predict phase: ${phase_name} (${total} req, interval=${interval_s}s, payload=${payload_file##*/})"

  local -a pids=()
  local i=1
  while (( i <= total )); do
    curl --silent --show-error --fail \
      --connect-timeout 5 \
      --max-time 60 \
      -X POST "http://127.0.0.1:${PORT_FORWARD_LOCAL_PORT}/predict" \
      -H 'Content-Type: application/json' \
      --data-binary @"${payload_file}" \
      >/dev/null &
    pids+=("$!")
    sleep "${interval_s}"
    i=$((i + 1))
  done

  local pid
  for pid in "${pids[@]}"; do
    wait "${pid}"
  done
}

run_expected_error_phase() {
  local malformed="$1"
  local empty="$2"
  local oversize="$3"

  log "error phase"

  local status
  status="$(curl --silent --show-error --max-time 20 -o /dev/null -w '%{http_code}' \
    -X POST "http://127.0.0.1:${PORT_FORWARD_LOCAL_PORT}/predict" \
    -H 'Content-Type: application/json' \
    --data-binary @"${malformed}" || true)"
  [[ "${status}" == "400" ]] || fatal "malformed JSON returned ${status}, expected 400"

  status="$(curl --silent --show-error --max-time 20 -o /dev/null -w '%{http_code}' \
    -X POST "http://127.0.0.1:${PORT_FORWARD_LOCAL_PORT}/predict" \
    -H 'Content-Type: application/json' \
    --data-binary @"${empty}" || true)"
  [[ "${status}" == "422" ]] || fatal "empty instances returned ${status}, expected 422"

  status="$(curl --silent --show-error --max-time 20 -o /dev/null -w '%{http_code}' \
    -X POST "http://127.0.0.1:${PORT_FORWARD_LOCAL_PORT}/predict" \
    -H 'Content-Type: application/json' \
    --data-binary @"${oversize}" || true)"
  [[ "${status}" == "422" ]] || fatal "oversize payload returned ${status}, expected 422"
}

main() {
  require_bin kind
  require_bin kubectl
  require_bin curl
  require_bin python3
  require_bin timeout

  validate_env

  log "starting fresh-cluster battle test"
  log "cluster=${KIND_CLUSTER_NAME}, inference_ns=${INFERENCE_NAMESPACE}, signoz_ns=${SIGNOZ_NAMESPACE}"
  log "sampler=${OTEL_TRACES_SAMPLER}, sampler_arg=${OTEL_TRACES_SAMPLER_ARG:-<unset>}, log_level=${LOG_LEVEL}, slow_ms=${SLOW_REQUEST_MS}"

  if is_true "${DO_CLUSTER_BOOTSTRAP}"; then
    bootstrap_kind_cluster
    install_default_storage_class
    install_kuberay_operator
    rollout_inference
    rollout_signoz
  fi

  # SigNoz and inference rollouts should already have created their pods.
  wait_for_selector_ready "${SIGNOZ_NAMESPACE}" 'app.kubernetes.io/instance=signoz' 900

  RAY_CLUSTER_NAME="$(detect_ray_cluster_name)"
  wait_for_selector_ready "${INFERENCE_NAMESPACE}" "ray.io/cluster=${RAY_CLUSTER_NAME}" 900

  local head_svc
  head_svc="$(resolve_head_service "${RAY_CLUSTER_NAME}")"

  wait_for_service_endpoints "${INFERENCE_NAMESPACE}" "${head_svc}" 600
  wait_for_service_endpoints "${SIGNOZ_NAMESPACE}" "signoz" 600
  wait_for_service_endpoints "${SIGNOZ_NAMESPACE}" "signoz-otel-collector" 600

  start_port_forward "${INFERENCE_NAMESPACE}" "${head_svc}"

  local payload_1 payload_4 payload_16 malformed empty oversize
  payload_1="${TMP_DIR}/payload_1.json"
  payload_4="${TMP_DIR}/payload_4.json"
  payload_16="${TMP_DIR}/payload_16.json"
  malformed="${TMP_DIR}/malformed.json"
  empty="${TMP_DIR}/empty.json"
  oversize="${TMP_DIR}/oversize.json"

  write_payload_file "${payload_1}" 1
  write_payload_file "${payload_4}" 4
  write_payload_file "${payload_16}" 16
  write_invalid_json "${malformed}"
  write_empty_instances "${empty}"
  write_oversize_payload "${oversize}"

  if is_true "${DO_TRAFFIC}"; then
    run_health_phase 20 3
    run_predict_phase "baseline single" 60 0.50 "${payload_1}"
    run_predict_phase "small batch" 30 0.20 "${payload_4}"
    run_predict_phase "spike batch" 40 0.10 "${payload_16}"
    run_expected_error_phase "${malformed}" "${empty}" "${oversize}"
    run_predict_phase "recovery single" 40 1.00 "${payload_1}"
    run_health_phase 10 2
  else
    log "skipping traffic phase because DO_TRAFFIC=0"
  fi

  log "port-forward log: ${PORT_FORWARD_LOG}"
  log "test complete"
  sleep "${KEEP_ALIVE_SECONDS}"
}

main "$@"