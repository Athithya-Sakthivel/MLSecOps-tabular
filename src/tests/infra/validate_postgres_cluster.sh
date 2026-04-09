#!/usr/bin/env bash
# Standalone validation script for CloudNativePG deployment.
# Runs CRUD tests against the deployed pooler to verify connectivity, auth, and storage.

set -euo pipefail

# --- Configuration ---
TARGET_NS="${TARGET_NS:-default}"
CLUSTER_NAME="${CLUSTER_NAME:-postgres-cluster}"
POOLER_NAME="${POOLER_NAME:-postgres-pooler}"
INITDB_DB="${INITDB_DB:-flyte_admin}"
ADDITIONAL_DBS=(datacatalog mlflow iceberg)
ALL_DBS=("${INITDB_DB}" "${ADDITIONAL_DBS[@]}")

TEST_IMAGE="${TEST_IMAGE:-ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie}"
TEST_TIMEOUT="${TEST_TIMEOUT:-120}"

# --- Counters ---
passed=0
failed=0

# --- Logging (clean, minimal) ---
info()  { echo "[INFO] $*"; }
warn()  { echo "[WARN] $*" >&2; }
error() { echo "[ERROR] $*" >&2; }

# --- Get credentials from CNPG secret ---
get_credentials() {
  local secret
  secret=$(kubectl -n "${TARGET_NS}" get secret \
    -l "cnpg.io/cluster=${CLUSTER_NAME},cnpg.io/userType=app" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  
  [[ -n "${secret}" ]] || { error "App secret not found"; return 1; }
  
  export PGUSER=$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.username}' | base64 -d)
  export PGPASSWORD=$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.password}' | base64 -d)
  export PGPORT=$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.port}' | base64 -d)
  export PGHOST="${POOLER_NAME}.${TARGET_NS}.svc.cluster.local"
  
  info "Using credentials for user '${PGUSER}' at ${PGHOST}:${PGPORT}"
}

# --- Run CRUD test for a single database ---
test_database() {
  local db="$1"
  local pod="test-${db//_/-}-$RANDOM"
  local elapsed=0
  
  info "Testing database: ${db}"
  
  # Create test pod - simple, single-line SQL to avoid escaping issues
  kubectl run "${pod}" -n "${TARGET_NS}" --restart=Never --image="${TEST_IMAGE}" \
    --env="PGUSER=${PGUSER}" --env="PGPASSWORD=${PGPASSWORD}" \
    --env="PGHOST=${PGHOST}" --env="PGPORT=${PGPORT}" --env="PGDATABASE=${db}" \
    --command -- psql -v ON_ERROR_STOP=1 -c "
      CREATE TABLE IF NOT EXISTS validate (id serial primary key, t text);
      INSERT INTO validate(t) VALUES ('ok');
      SELECT COUNT(*) FROM validate WHERE t='ok';
      UPDATE validate SET t='done' WHERE t='ok';
      DELETE FROM validate WHERE t='done';
    " >/dev/null 2>&1 || {
      warn "Failed to create pod for ${db}"
      ((failed++))
      return 1
    }
  
  # Wait for pod to complete
  while [[ $elapsed -lt ${TEST_TIMEOUT} ]]; do
    local phase
    phase=$(kubectl -n "${TARGET_NS}" get pod "${pod}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    
    case "${phase}" in
      Succeeded)
        info "✓ ${db}: PASSED"
        ((passed++))
        kubectl -n "${TARGET_NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
        return 0
        ;;
      Failed|Error)
        warn "✗ ${db}: FAILED"
        kubectl -n "${TARGET_NS}" logs "${pod}" --tail=20 2>/dev/null || true
        ((failed++))
        kubectl -n "${TARGET_NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
        return 1
        ;;
    esac
    
    sleep 2
    ((elapsed+=2))
  done
  
  # Timeout
  warn "✗ ${db}: TIMEOUT"
  kubectl -n "${TARGET_NS}" logs "${pod}" --tail=20 2>/dev/null || true
  kubectl -n "${TARGET_NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  ((failed++))
  return 1
}

# --- Print summary ---
print_summary() {
  echo ""
  echo "Results: ${passed} passed, ${failed} failed"
  if [[ ${failed} -eq 0 ]]; then
    echo "✓ All CRUD tests passed"
    return 0
  else
    echo "✗ Some tests failed"
    return 1
  fi
}

# --- Main ---
main() {
  command -v kubectl >/dev/null || { error "kubectl not found"; exit 1; }
  
  # Verify cluster access
  kubectl cluster-info >/dev/null 2>&1 || { error "Cannot reach Kubernetes cluster"; exit 1; }
  
  # Verify pooler service exists
  kubectl -n "${TARGET_NS}" get svc "${POOLER_NAME}" >/dev/null 2>&1 || {
    error "Pooler service '${POOLER_NAME}' not found"
    exit 1
  }
  
  info "Starting validation: ${CLUSTER_NAME} (${TARGET_NS})"
  
  get_credentials || exit 1
  
  for db in "${ALL_DBS[@]}"; do
    test_database "${db}" || true
  done
  
  print_summary
}

case "${1:-}" in
  --run) main ;;
  --help|-h) 
    echo "Usage: $0 [--run]"
    echo "Env vars: TARGET_NS, CLUSTER_NAME, POOLER_NAME, TEST_TIMEOUT"
    exit 0 ;;
  *) main ;;
esac