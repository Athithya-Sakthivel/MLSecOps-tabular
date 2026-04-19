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
export RAY_IMAGE="${RAY_IMAGE:-ghcr.io/athithya-sakthivel/tabular-inference-service:2026-04-19-06-22--5f4dbcd@sha256:24c4b9a477c80700c79da2fcac9b265e3df6aeddfd64226ec6c74bbc29540c5a}"
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
    sleep 1600
  fi

  local head_svc
  head_svc="$(resolve_head_service "${RAY_CLUSTER_NAME}")"
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





root ➜ /workspace (main) $ cat src/terraform/aws/modules/ecr/main.tf
// src/terraform/modules/ecr/main.tf
// ECR repositories for AgentOps-ServiceAutomation
// Compatible with OpenTofu v1.11.5 and hashicorp/aws v6.x (resource/argument names conform to provider v6.x docs).
// Creates explicit repositories (no placeholders) and lifecycle policies to retain a bounded number of images.

variable "tags" {
  description = "Tags applied to all ECR repositories created by this module."
  type        = map(string)
  default     = {}
}

locals {
  merged_tags = merge({ ManagedBy = "agentops-serviceautomation" }, var.tags)
}

############################
# agentops-frontend
############################
resource "aws_ecr_repository" "agentops_frontend" {
  name                 = "agentops-frontend"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = local.merged_tags
}

resource "aws_ecr_lifecycle_policy" "agentops_frontend" {
  repository = aws_ecr_repository.agentops_frontend.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 30 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 30
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

############################
# agentops-inference
############################
resource "aws_ecr_repository" "agentops_inference" {
  name                 = "agentops-inference"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = local.merged_tags
}

resource "aws_ecr_lifecycle_policy" "agentops_inference" {
  repository = aws_ecr_repository.agentops_inference.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 30 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 30
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

############################
# agentops-auth
############################
resource "aws_ecr_repository" "agentops_auth" {
  name                 = "agentops-auth"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = local.merged_tags
}

resource "aws_ecr_lifecycle_policy" "agentops_auth" {
  repository = aws_ecr_repository.agentops_auth.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 30 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 30
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

############################
# agentops-cloudnativepg
############################
resource "aws_ecr_repository" "agentops_cloudnativepg" {
  name                 = "agentops-cloudnativepg"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = local.merged_tags
}

resource "aws_ecr_lifecycle_policy" "agentops_cloudnativepg" {
  repository = aws_ecr_repository.agentops_cloudnativepg.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 30 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 30
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

############################
# agentops-postgresql
############################
resource "aws_ecr_repository" "agentops_postgresql" {
  name                 = "agentops-postgresql"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = local.merged_tags
}

resource "aws_ecr_lifecycle_policy" "agentops_postgresql" {
  repository = aws_ecr_repository.agentops_postgresql.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 30 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 30
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

############################
# agentops-cloudflared
############################
resource "aws_ecr_repository" "agentops_cloudflared" {
  name                 = "agentops-cloudflared"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = local.merged_tags
}

resource "aws_ecr_lifecycle_policy" "agentops_cloudflared" {
  repository = aws_ecr_repository.agentops_cloudflared.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 30 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 30
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

############################
# Outputs
############################
output "repository_url_map" {
  description = "Map of repository name -> repository URL (https://<account>.dkr.ecr.<region>.amazonaws.com/<name>)"
  value = {
    "agentops-frontend"      = aws_ecr_repository.agentops_frontend.repository_url
    "agentops-inference"     = aws_ecr_repository.agentops_inference.repository_url
    "agentops-auth"          = aws_ecr_repository.agentops_auth.repository_url
    "agentops-cloudnativepg" = aws_ecr_repository.agentops_cloudnativepg.repository_url
    "agentops-postgresql"    = aws_ecr_repository.agentops_postgresql.repository_url
    "agentops-cloudflared"   = aws_ecr_repository.agentops_cloudflared.repository_url
  }
}

output "repository_arn_map" {
  description = "Map of repository name -> repository ARN"
  value = {
    "agentops-frontend"      = aws_ecr_repository.agentops_frontend.arn
    "agentops-inference"     = aws_ecr_repository.agentops_inference.arn
    "agentops-auth"          = aws_ecr_repository.agentops_auth.arn
    "agentops-cloudnativepg" = aws_ecr_repository.agentops_cloudnativepg.arn
    "agentops-postgresql"    = aws_ecr_repository.agentops_postgresql.arn
    "agentops-cloudflared"   = aws_ecr_repository.agentops_cloudflared.arn
  }
}root ➜ /workspace (main) $ catsrc/terraform/aws/modules/vpc/main.tff
// src/terraform/modules/vpc/main.tf
// Secure VPC module (AWS provider 6.x)
// - No implicit public exposure
// - Explicit routing for internet access
// - Private subnets remain isolated behind NAT

variable "vpc_cidr" {
  type = string
}

variable "private_subnet_cidrs" {
  type = list(string)

  validation {
    condition     = length(var.private_subnet_cidrs) == 2
    error_message = "private_subnet_cidrs must contain exactly 2 CIDRs."
  }
}

variable "tags" {
  type    = map(string)
  default = {}
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs         = slice(data.aws_availability_zones.available.names, 0, 2)
  env_tag     = lookup(var.tags, "Environment", "prod")
  common_tags = merge(
    { Name = "agentops-vpc", Environment = local.env_tag },
    var.tags
  )
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = local.common_tags
}

# -------------------------
# PUBLIC SUBNET (explicitly controlled)
# -------------------------
resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.this.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 250)
  availability_zone = local.azs[0]

  # SECURITY FIX:
  # Do NOT auto-assign public IPs
  map_public_ip_on_launch = false

  tags = merge(local.common_tags, {
    Name = "agentops-public-${local.azs[0]}"
    Tier = "public"
  })
}

# -------------------------
# PRIVATE SUBNETS (unchanged, already secure)
# -------------------------
resource "aws_subnet" "private" {
  count = 2

  vpc_id            = aws_vpc.this.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = local.azs[count.index]

  map_public_ip_on_launch = false

  tags = merge(local.common_tags, {
    Name = "agentops-private-${local.azs[count.index]}"
    Tier = "private"
  })
}

# -------------------------
# INTERNET GATEWAY
# -------------------------
resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = merge(local.common_tags, {
    Name = "agentops-igw"
  })
}

# -------------------------
# PUBLIC ROUTING (controlled exposure)
# -------------------------
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  tags = merge(local.common_tags, {
    Name = "agentops-public-rt"
  })
}

resource "aws_route" "public_default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public_assoc" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# -------------------------
# NAT (single, cost-optimized)
# -------------------------
resource "aws_eip" "nat_allocation" {
  domain = "vpc"

  tags = merge(local.common_tags, {
    Name = "agentops-nat-eip"
  })
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat_allocation.id
  subnet_id     = aws_subnet.public.id

  tags = merge(local.common_tags, {
    Name = "agentops-natgw"
  })
}

# -------------------------
# PRIVATE ROUTING (secure egress only)
# -------------------------
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id

  tags = merge(local.common_tags, {
    Name = "agentops-private-rt"
  })
}

resource "aws_route" "private_default" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this.id

  depends_on = [aws_nat_gateway.this]
}

resource "aws_route_table_association" "private_assoc" {
  count = 2

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# -------------------------
# OUTPUTS
# -------------------------
output "vpc_id" {
  value = aws_vpc.this.id
}

output "public_subnet_id" {
  value = aws_subnet.public.id
}

output "private_subnet_ids" {
  value = [for s in aws_subnet.private : s.id]
}

output "private_route_table_ids" {
  value = [aws_route_table.private.id]
}

output "main_route_table_id" {
  value = aws_vpc.this.main_route_table_id
}

output "nat_gateway_id" {
  value = aws_nat_gateway.this.id
}root ➜ /workspace (main) $ catsrc/terraform/aws/modules/security/main.tff
// src/terraform/modules/security/main.tf
// Security module (OpenTofu v1.11.5 + aws provider v6.x).
// Creates worker node SG and an interface-endpoints SG.

variable "vpc_id" {
  description = "VPC ID where security groups will be created."
  type        = string
}

variable "vpc_cidr" {
  description = "Primary IPv4 CIDR block for the VPC (used for internal allow rules)."
  type        = string
}

variable "ipv6_cidr_block" {
  description = "VPC IPv6 CIDR block (if assigned by VPC module). Provide empty string if not available."
  type        = string
  default     = ""
}

variable "enable_ipv6" {
  description = "Controls IPv6 rule population."
  type        = bool
  default     = true
}

variable "name_prefix" {
  description = "Name prefix for security groups."
  type        = string
  default     = "agentops"
}

variable "tags" {
  description = "Tags applied to all security groups created by this module."
  type        = map(string)
  default     = {}
}

locals {
  env_tag     = lookup(var.tags, "Environment", "prod")
  merged_tags = merge(
    {
      "Name"        = var.name_prefix
      "Environment" = local.env_tag
      "ManagedBy"   = "agentops-serviceautomation"
    },
    var.tags
  )

  ipv6_blocks = (var.enable_ipv6 && var.ipv6_cidr_block != "") ? [var.ipv6_cidr_block] : []
}

########################
# Worker node security group
########################
resource "aws_security_group" "node" {
  name        = "${var.name_prefix}-nodes-sg"
  description = "Worker node security group (allows intra-VPC traffic)."
  vpc_id      = var.vpc_id
  tags        = local.merged_tags

  # Allow full intra-VPC communication for node agents / kubelet / container networking.
  ingress {
    description      = "Allow all traffic within VPC CIDR"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = [var.vpc_cidr]
    ipv6_cidr_blocks = local.ipv6_blocks
  }

  # Restrict egress to the VPC only. This keeps private cluster access working
  # while removing unrestricted internet egress.
  egress {
    description      = "Allow outbound traffic only within the VPC CIDR"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = [var.vpc_cidr]
    ipv6_cidr_blocks = local.ipv6_blocks
  }
}

########################
# VPC interface endpoints SG (attach to interface endpoints ENIs)
########################
resource "aws_security_group" "vpc_endpoints" {
  name        = "${var.name_prefix}-endpoints-sg"
  description = "SG attached to VPC interface endpoints (ECR, STS, SSM...). Allows HTTPS from worker nodes."
  vpc_id      = var.vpc_id
  tags        = merge(local.merged_tags, { "role" = "vpc-endpoints" })

  # Allow worker nodes to reach endpoint ENIs on 443
  ingress {
    description     = "Allow HTTPS (443) from worker nodes"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.node.id]
  }

  # Optional: allow Kubelet port if operators choose to access nodes via endpoints (kept permissive from VPC CIDR)
  ingress {
    description      = "Optional: allow kubelet port (10250) from VPC CIDR"
    from_port        = 10250
    to_port          = 10250
    protocol         = "tcp"
    cidr_blocks      = [var.vpc_cidr]
    ipv6_cidr_blocks = local.ipv6_blocks
  }

  # Restrict egress to the VPC only; no unrestricted public egress.
  egress {
    description      = "Allow outbound traffic only within the VPC CIDR"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = [var.vpc_cidr]
    ipv6_cidr_blocks = local.ipv6_blocks
  }
}

########################
# Outputs
########################
output "node_security_group_id" {
  description = "Security Group ID for worker nodes."
  value       = aws_security_group.node.id
}

output "vpc_endpoints_security_group_id" {
  description = "Security Group ID to attach to VPC Interface Endpoints (ECR API/DKR, STS, SSM, etc)."
  value       = aws_security_group.vpc_endpoints.id
}root ➜ /workspace (main) $ catsrc/terraform/aws/modules/iam_pre_eks/main.tff
// src/terraform/modules/iam_pre_eks/main.tf
// Pre-EKS IAM: cluster & node roles, cluster-autoscaler policy, CI ECR push policy.
// Stable, deterministic outputs consumed by eks and iam_post_eks.

variable "name_prefix" {
  type    = string
  default = "agentops"
}

variable "tags" {
  type    = map(string)
  default = {}
}

locals {
  name_prefix = var.name_prefix
  common_tags = merge({ ManagedBy = "agentops-serviceautomation" }, var.tags)
}

resource "aws_iam_role" "cluster" {
  name = "${local.name_prefix}-eks-cluster-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "eks.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "cluster_attach" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_role" "node" {
  name = "${local.name_prefix}-eks-node-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "node_attach_eks_worker" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "node_attach_ecr_readonly" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy_attachment" "node_attach_cni" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_policy" "cluster_autoscaler" {
  name = "${local.name_prefix}-cluster-autoscaler-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:DescribeAutoScalingInstances",
          "autoscaling:DescribeLaunchConfigurations",
          "autoscaling:DescribeScalingActivities",
          "ec2:DescribeImages",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeLaunchTemplateVersions",
          "ec2:GetInstanceTypesFromInstanceRequirements",
          "eks:DescribeNodegroup"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "autoscaling:SetDesiredCapacity",
          "autoscaling:TerminateInstanceInAutoScalingGroup"
        ]
        Resource = ["*"]
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_policy" "ci_ecr_push" {
  name = "${local.name_prefix}-ci-ecr-push-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage",
          "ecr:BatchGetImage",
          "ecr:DescribeRepositories"
        ]
        Resource = ["*"]
      }
    ]
  })

  tags = local.common_tags
}

output "cluster_role_arn" {
  description = "ARN of EKS cluster IAM role"
  value       = aws_iam_role.cluster.arn
}

output "node_role_arn" {
  description = "ARN of EC2 node IAM role"
  value       = aws_iam_role.node.arn
}

output "cluster_autoscaler_policy_arn" {
  description = "ARN of the Cluster Autoscaler policy"
  value       = aws_iam_policy.cluster_autoscaler.arn
}

output "ci_ecr_push_policy_arn" {
  description = "ARN of the CI ECR push policy"
  value       = aws_iam_policy.ci_ecr_push.arn
}

output "ebs_csi_managed_policy_arn" {
  description = "Recommended managed policy ARN for EBS CSI"
  value       = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}root ➜ /workspace (main) $ catsrc/terraform/aws/modules/iam_post_eks/main.tff
// src/terraform/modules/iam_post_eks/main.tf
// Post-EKS IAM + optional control-plane <-> node SG rules.
// IRSA roles are created unconditionally; SG rules are created only when
// the operator explicitly enables them via module variable `create_sg_rules = true`.
// This avoids accidental duplicate-rule attempts and plan-time unknown count issues.

variable "name_prefix" {
  type    = string
  default = "agentops"
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "oidc_provider_arn" {
  type    = string
  default = ""
}

variable "oidc_provider_issuer" {
  type        = string
  description = "OIDC issuer host/path (without https://)"
  default     = ""
}

variable "ebs_csi_policy_arn" {
  type    = string
  default = ""
}

variable "cluster_autoscaler_policy_arn" {
  type    = string
  default = ""
}

variable "ebs_sa_namespace" {
  type    = string
  default = "kube-system"
}

variable "ebs_sa_name" {
  type    = string
  default = "ebs-csi-controller-sa"
}

variable "autoscaler_sa_namespace" {
  type    = string
  default = "kube-system"
}

variable "autoscaler_sa_name" {
  type    = string
  default = "cluster-autoscaler"
}

variable "node_security_group_id" {
  description = "Worker node security group id (from modules/security)."
  type        = string
  default     = ""
}

variable "cluster_security_group_id" {
  description = "EKS control-plane security group id (from modules/eks)."
  type        = string
  default     = ""
}

# Explicit operator-controlled flag. Default false to avoid accidental duplicate SG-rule creation.
variable "create_sg_rules" {
  description = "When true, the module will create control-plane <-> node SG rules. Set to true only after cluster and node SGs exist and you have confirmed rules are not already present."
  type        = bool
  default     = false
}

locals {
  name_prefix = var.name_prefix
  common_tags = merge({ ManagedBy = "agentops-serviceautomation" }, var.tags)
}

########################
# IRSA Roles (EBS CSI, Cluster Autoscaler)
########################
resource "aws_iam_role" "ebs_csi_irsa" {
  name = "${local.name_prefix}-ebs-csi-irsa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = var.oidc_provider_arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            ("${var.oidc_provider_issuer}:sub") = "system:serviceaccount:${var.ebs_sa_namespace}:${var.ebs_sa_name}"
            ("${var.oidc_provider_issuer}:aud") = "sts.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ebs_csi_attach" {
  role       = aws_iam_role.ebs_csi_irsa.name
  policy_arn = var.ebs_csi_policy_arn
}

resource "aws_iam_role" "cluster_autoscaler_irsa" {
  name = "${local.name_prefix}-cluster-autoscaler-irsa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = var.oidc_provider_arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            ("${var.oidc_provider_issuer}:sub") = "system:serviceaccount:${var.autoscaler_sa_namespace}:${var.autoscaler_sa_name}"
            ("${var.oidc_provider_issuer}:aud") = "sts.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "cluster_autoscaler_attach" {
  role       = aws_iam_role.cluster_autoscaler_irsa.name
  policy_arn = var.cluster_autoscaler_policy_arn
}

########################
# Control-plane <-> Nodes SG rules (explicit)
# - Created only if create_sg_rules = true.
# - Operator must ensure the SG IDs are correct and that identical rules don't already exist.
########################

resource "aws_security_group_rule" "nodes_to_api_https" {
  count = var.create_sg_rules ? 1 : 0

  security_group_id        = var.cluster_security_group_id
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  source_security_group_id = var.node_security_group_id
  description              = "Allow worker nodes to call EKS API server (TCP/443)"
}

resource "aws_security_group_rule" "api_to_kubelet" {
  count = var.create_sg_rules ? 1 : 0

  security_group_id        = var.node_security_group_id
  type                     = "ingress"
  from_port                = 10250
  to_port                  = 10250
  protocol                 = "tcp"
  source_security_group_id = var.cluster_security_group_id
  description              = "Allow EKS control plane to reach kubelet on workers (TCP/10250)"
}

########################
# Outputs
########################

output "ebs_csi_irsa_role_arn" {
  value       = aws_iam_role.ebs_csi_irsa.arn
  description = "ARN of the EBS CSI IRSA role"
}

output "cluster_autoscaler_irsa_role_arn" {
  value       = aws_iam_role.cluster_autoscaler_irsa.arn
  description = "ARN of the Cluster Autoscaler IRSA role"
}

output "cluster_node_sg_rules_applied" {
  description = "True if create_sg_rules was true (operator intent)."
  value       = var.create_sg_rules
}root ➜ /workspace (main) $ catsrc/terraform/aws/modules/eks/main.tff
// src/terraform/modules/eks/main.tf
// EKS cluster + managed nodegroups.
// Compatible with OpenTofu/Terraform >=1.11.5 and hashicorp/aws 6.x.
//
// Notes:
// - The module requires the caller to provide a node_security_group_id (SG attached to worker nodes).
//   The module creates the control-plane ingress rule that allows worker-node SG -> control-plane:443
//   so kubelet on nodes can register the node during bootstrap.
// - If you use a custom EC2 Launch Template for node bootstrap, supply launch_template_id / version.
// - This module intentionally does NOT attempt to pre-validate computed module outputs (they may be unknown
//   during plan). Ensure your root module wires and depends_on modules correctly.

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-south-1"
}

variable "vpc_id" {
  description = "VPC id"
  type        = string
}

variable "subnet_ids" {
  description = "List of private subnet IDs (two AZs)."
  type        = list(string)
}

variable "node_security_group_id" {
  description = "Security group ID used by worker nodes (required). Pass module.security.node_security_group_id from root."
  type        = string

  validation {
    condition     = var.node_security_group_id != ""
    error_message = "node_security_group_id must be provided to this module (pass module.security.node_security_group_id from root)."
  }
}

variable "endpoint_security_group_id" {
  description = "Optional SG used for VPC interface endpoints (passed for reference)."
  type        = string
  default     = ""
}

variable "cluster_role_arn" {
  description = "IAM role ARN for the EKS control plane (from iam_pre_eks)"
  type        = string
}

variable "node_role_arn" {
  description = "IAM role ARN for EC2 nodegroups (from iam_pre_eks)"
  type        = string
}

variable "ebs_csi_policy_arn" {
  description = "ARN of the EBS CSI managed policy (from iam_pre_eks)."
  type        = string
  default     = ""
}

variable "cluster_autoscaler_policy_arn" {
  description = "ARN of the Cluster Autoscaler policy (from iam_pre_eks)."
  type        = string
  default     = ""
}

variable "ecr_repository_urls" {
  description = "Map of ECR logical name -> repo URL (convenience)."
  type        = map(string)
  default     = {}
}

variable "system_nodegroup" {
  description = "System nodegroup sizing object."
  type = object({
    instance_type = string
    min_size      = number
    desired_size  = number
    max_size      = number
  })
}

variable "inference_nodegroup" {
  description = "Inference nodegroup sizing object."
  type = object({
    instance_type = string
    min_size      = number
    desired_size  = number
    max_size      = number
  })
}

variable "system_node_taints" {
  description = "List of taints for system nodegroup (structured: key,value,effect)."
  type = list(object({
    key    = string
    value  = string
    effect = string
  }))
  default = [{ key = "node-role", value = "system", effect = "NO_SCHEDULE" }]

  validation {
    condition     = alltrue([for t in var.system_node_taints : contains(["NO_SCHEDULE", "NO_EXECUTE", "PREFER_NO_SCHEDULE"], t.effect)])
    error_message = "Each system_node_taints[].effect must be one of: NO_SCHEDULE, NO_EXECUTE, PREFER_NO_SCHEDULE"
  }
}

variable "inference_node_labels" {
  description = "Labels for inference nodegroup"
  type        = map(string)
  default     = {}
}

variable "enabled_cluster_log_types" {
  description = "Control-plane log types to enable."
  type        = list(string)
  default     = ["api", "audit", "authenticator"]
}

variable "tags" {
  description = "Tags applied to resources"
  type        = map(string)
  default     = {}
}

# Optional: support a custom launch template for nodegroups (reduces failure modes when you manage AMI/user-data centrally)
variable "launch_template_id" {
  description = "Optional EC2 Launch Template ID for managed nodegroups (leave empty to let EKS create/choose instances)."
  type        = string
  default     = ""
}

variable "launch_template_version" {
  description = "Optional Launch Template version (string). Use empty string to let AWS default ($Latest)."
  type        = string
  default     = ""
}

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}

locals {
  merged_tags = merge(
    {
      ManagedBy   = "agentops-serviceautomation"
      Name        = var.cluster_name
      Environment = lookup(var.tags, "Environment", "")
    },
    var.tags
  )

  root_principal_arn = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"
}

data "aws_iam_policy_document" "eks_secrets_encryption" {
  statement {
    sid     = "EnableAccountRootPermissions"
    effect  = "Allow"
    actions = ["kms:*"]

    principals {
      type        = "AWS"
      identifiers = [local.root_principal_arn]
    }

    resources = ["*"]
  }

  statement {
    sid    = "AllowEKSClusterRoleToUseKey"
    effect = "Allow"

    actions = [
      "kms:DescribeKey",
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*"
    ]

    principals {
      type        = "AWS"
      identifiers = [var.cluster_role_arn]
    }

    resources = ["*"]
  }

  statement {
    sid    = "AllowEKSClusterRoleToManageGrants"
    effect = "Allow"

    actions = [
      "kms:CreateGrant",
      "kms:ListGrants",
      "kms:RevokeGrant"
    ]

    principals {
      type        = "AWS"
      identifiers = [var.cluster_role_arn]
    }

    resources = ["*"]

    condition {
      test     = "Bool"
      variable = "kms:GrantIsForAWSResource"
      values   = ["true"]
    }
  }
}

resource "aws_kms_key" "eks_secrets" {
  description             = "EKS secrets encryption key for ${var.cluster_name}"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.eks_secrets_encryption.json

  tags = local.merged_tags
}

resource "aws_kms_alias" "eks_secrets" {
  name          = "alias/${var.cluster_name}-eks-secrets"
  target_key_id  = aws_kms_key.eks_secrets.key_id
}

#########################
# EKS cluster (private)
#########################
resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  role_arn = var.cluster_role_arn

  vpc_config {
    subnet_ids = var.subnet_ids

    endpoint_public_access  = false
    endpoint_private_access = true
  }

  encryption_config {
    resources = ["secrets"]

    provider {
      key_arn = aws_kms_key.eks_secrets.arn
    }
  }

  enabled_cluster_log_types = var.enabled_cluster_log_types

  tags = local.merged_tags
}

# Expose cluster security group id (control plane SG) for use by other modules/root.
output "cluster_security_group_id" {
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
  description = "Control-plane security group id created/managed by the EKS control plane"
}

# Allow worker nodes to reach control plane on TCP/443.
# This rule must exist before nodegroups attempt to bootstrap and register.
resource "aws_security_group_rule" "allow_nodes_to_control_plane" {
  description              = "Allow worker nodes to contact control plane (kube-apiserver) on TCP/443"
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
  source_security_group_id = var.node_security_group_id
}

# Retrieve TLS certificate for issuer and compute SHA1 fingerprint for the OIDC provider.
data "tls_certificate" "eks_oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "this" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer

  client_id_list = ["sts.amazonaws.com"]

  thumbprint_list = [
    data.tls_certificate.eks_oidc.certificates[0].sha1_fingerprint
  ]

  tags = local.merged_tags
}

#########################
# Node group: system (stateful workloads)
# depends on the control-plane SG rule to exist first
#########################
resource "aws_eks_node_group" "system" {
  depends_on = [aws_security_group_rule.allow_nodes_to_control_plane]

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-system"
  node_role_arn   = var.node_role_arn
  subnet_ids      = var.subnet_ids

  scaling_config {
    desired_size = var.system_nodegroup.desired_size
    min_size     = var.system_nodegroup.min_size
    max_size     = var.system_nodegroup.max_size
  }

  instance_types = [var.system_nodegroup.instance_type]

  dynamic "taint" {
    for_each = var.system_node_taints
    content {
      key    = taint.value.key
      value  = taint.value.value
      effect = taint.value.effect
    }
  }

  dynamic "launch_template" {
    for_each = var.launch_template_id != "" ? [1] : []
    content {
      id      = var.launch_template_id
      version = var.launch_template_version != "" ? var.launch_template_version : "$Latest"
    }
  }

  tags = local.merged_tags
}

#########################
# Node group: inference (stateless inference + auth)
# depends on the control-plane SG rule to exist first
#########################
resource "aws_eks_node_group" "inference" {
  depends_on = [aws_security_group_rule.allow_nodes_to_control_plane]

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-inference"
  node_role_arn   = var.node_role_arn
  subnet_ids      = var.subnet_ids

  scaling_config {
    desired_size = var.inference_nodegroup.desired_size
    min_size     = var.inference_nodegroup.min_size
    max_size     = var.inference_nodegroup.max_size
  }

  instance_types = [var.inference_nodegroup.instance_type]

  labels = var.inference_node_labels

  dynamic "launch_template" {
    for_each = var.launch_template_id != "" ? [1] : []
    content {
      id      = var.launch_template_id
      version = var.launch_template_version != "" ? var.launch_template_version : "$Latest"
    }
  }

  tags = local.merged_tags
}

#########################
# Outputs (cluster info)
#########################
output "cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  description = "EKS cluster API server endpoint"
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_ca_data" {
  description = "Base64-encoded certificate authority data for the cluster"
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "oidc_provider_arn" {
  description = "ARN of the aws_iam_openid_connect_provider"
  value       = aws_iam_openid_connect_provider.this.arn
}

output "oidc_provider_issuer" {
  description = "OIDC issuer host/path without https://"
  value       = replace(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")
}

output "secrets_encryption_kms_key_arn" {
  description = "KMS key ARN used for EKS secrets encryption"
  value       = aws_kms_key.eks_secrets.arn
}root ➜ /workspace (main) $ 