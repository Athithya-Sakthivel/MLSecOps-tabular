#!/usr/bin/env bash
set -euo pipefail

K8S_CLUSTER="${K8S_CLUSTER:-kind}"
TARGET_NS="${TARGET_NS:-default}"
MANIFEST_DIR="${MANIFEST_DIR:-src/manifests/iceberg}"
IMAGE="${IMAGE:-ghcr.io/athithya-sakthivel/iceberg-rest-postgres@sha256:55250a42cd067c92a27559a5bebab570586b34b057d7ca9adf42f45a8ebab1a4}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-iceberg-rest-sa}"
SERVICE_NAME="${SERVICE_NAME:-iceberg-rest}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-iceberg-rest}"
CONFIGMAP_NAME="${CONFIGMAP_NAME:-iceberg-rest-conf}"
SECRET_NAME="${SECRET_NAME:-iceberg-storage-credentials}"
REST_SECRET_NAME="${REST_SECRET_NAME:-iceberg-rest-auth}"
ANNOTATION_KEY="${ANNOTATION_KEY:-mlsecops.iceberg.checksum}"
PORT="${PORT:-9001}"

STORAGE_PROVIDER="${STORAGE_PROVIDER:-aws}"
USE_IAM="${USE_IAM:-false}"

AWS_REGION="${AWS_REGION:-ap-south-1}"
S3_BUCKET="${S3_BUCKET:-e2e-mlops-data-681802563986}"
S3_PREFIX="${S3_PREFIX:-iceberg/warehouse}"
S3_ENDPOINT="${S3_ENDPOINT:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_ROLE_ARN="${AWS_ROLE_ARN:-}"

# REST auth (if provided via env, script will create or update k8s secret REST_SECRET_NAME)
ICEBERG_REST_AUTH_TYPE="${ICEBERG_REST_AUTH_TYPE:-}"
ICEBERG_REST_USER="${ICEBERG_REST_USER:-}"
ICEBERG_REST_PASSWORD="${ICEBERG_REST_PASSWORD:-}"

GCP_PROJECT="${GCP_PROJECT:-}"
GCP_BUCKET="${GCP_BUCKET:-mlops_iceberg_warehouse}"
GCP_SA_KEY_JSON_B64="${GCP_SA_KEY_JSON_B64:-}"
GCP_SA_EMAIL="${GCP_SA_EMAIL:-}"

AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-}"
AZURE_STORAGE_KEY="${AZURE_STORAGE_KEY:-}"
AZURE_CONTAINER="${AZURE_CONTAINER:-iceberg}"
AZURE_CLIENT_ID="${AZURE_CLIENT_ID:-}"
AZURE_TENANT_ID="${AZURE_TENANT_ID:-}"

PG_POOLER_HOST="${PG_POOLER_HOST:-postgres-pooler.${TARGET_NS}.svc.cluster.local}"
PG_POOLER_PORT="${PG_POOLER_PORT:-5432}"
PG_DB_OVERRIDE="${PG_DB_OVERRIDE:-}"
PG_APP_SECRET_LABEL="${PG_APP_SECRET_LABEL:-cnpg.io/cluster=postgres-cluster,cnpg.io/userType=app}"
FALLBACK_PG_SECRET="${FALLBACK_PG_SECRET:-postgres-cluster-app}"

REPLICAS_KIND="${REPLICAS_KIND:-1}"
REPLICAS_CLOUD="${REPLICAS_CLOUD:-2}"
REPLICAS="${REPLICAS:-${REPLICAS_CLOUD}}"
if [[ "${K8S_CLUSTER}" == "kind" ]]; then REPLICAS="${REPLICAS_KIND}"; fi

CPU_REQUEST_KIND="${CPU_REQUEST_KIND:-250m}"
CPU_LIMIT_KIND="${CPU_LIMIT_KIND:-1000m}"
MEM_REQUEST_KIND="${MEM_REQUEST_KIND:-512Mi}"
MEM_LIMIT_KIND="${MEM_LIMIT_KIND:-1Gi}"

CPU_REQUEST_CLOUD="${CPU_REQUEST_CLOUD:-500m}"
CPU_LIMIT_CLOUD="${CPU_LIMIT_CLOUD:-2000m}"
MEM_REQUEST_CLOUD="${MEM_REQUEST_CLOUD:-1Gi}"
MEM_LIMIT_CLOUD="${MEM_LIMIT_CLOUD:-4Gi}"

GRAVITINO_VERSION="${GRAVITINO_VERSION:-1.1.0}"

OP_TIMEOUT="${OP_TIMEOUT:-300}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"
RETRY_SLEEP="${RETRY_SLEEP:-3}"
RETRIES="${RETRIES:-6}"

mkdir -p "${MANIFEST_DIR}"

log(){ printf '[%s] [iceberg] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
fatal(){ printf '[%s] [iceberg][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; exit 1; }
require_bin(){ command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"; }

trap 'rc=$?; echo; echo "[DIAG] exit_code=$rc"; echo "[DIAG] kubectl context: $(kubectl config current-context 2>/dev/null || true)"; echo "[DIAG] pods in ns ${TARGET_NS}:"; kubectl -n "${TARGET_NS}" get pods -o wide || true; echo "[DIAG] svc in ns ${TARGET_NS}:"; kubectl -n "${TARGET_NS}" get svc -o wide || true; echo "[DIAG] events (last 200):"; kubectl get events -A --sort-by=.lastTimestamp | tail -n 200 || true; exit $rc' ERR

require_prereqs(){
  require_bin kubectl
  require_bin curl
  require_bin jq
  require_bin sha256sum
  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach cluster"
}

pg_detect_secret(){
  local s
  s="$(kubectl -n "${TARGET_NS}" get secret -l "${PG_APP_SECRET_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${s}" ]]; then
    FALLBACK_PG_SECRET="${s}"
    log "adopting CNPG app secret -> ${FALLBACK_PG_SECRET}"
  else
    log "CNPG app secret not found; using FALLBACK_PG_SECRET=${FALLBACK_PG_SECRET}"
  fi
}

find_pg_secret_name(){
  local s
  s=$(kubectl -n "${TARGET_NS}" get secret -l "${PG_APP_SECRET_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "${s}" ]]; then printf '%s' "${s}"; return 0; fi
  if kubectl -n "${TARGET_NS}" get secret "${FALLBACK_PG_SECRET}" >/dev/null 2>&1; then printf '%s' "${FALLBACK_PG_SECRET}"; return 0; fi
  printf ''
}

pg_detect_db(){
  if [[ -n "${PG_DB_OVERRIDE}" ]]; then
    log "PG_DB_OVERRIDE present; using PG_DB=${PG_DB_OVERRIDE}"
    printf '%s' "${PG_DB_OVERRIDE}"
    return 0
  fi
  local s db
  s="$(find_pg_secret_name)"
  if [[ -n "${s}" ]]; then
    db="$(kubectl -n "${TARGET_NS}" get secret "${s}" -o jsonpath='{.data.dbname}' 2>/dev/null || true)"
    if [[ -n "${db}" ]]; then
      db="$(printf '%s' "${db}" | base64 -d)"
      log "CNPG secret dbname detected -> ${db}"
      printf '%s' "${db}"
      return 0
    fi
  fi
  log "no dbname found in CNPG secret; falling back to iceberg_catalogue_metadata"
  printf '%s' "iceberg_catalogue_metadata"
}

derive_io_and_warehouse(){
  if [[ "${STORAGE_PROVIDER}" == "aws" ]]; then
    IO_IMPL="org.apache.iceberg.aws.s3.S3FileIO"
    if [[ -n "${S3_PREFIX}" ]]; then WAREHOUSE="s3://${S3_BUCKET}/${S3_PREFIX}/"; else WAREHOUSE="s3://${S3_BUCKET}/"; fi
  elif [[ "${STORAGE_PROVIDER}" == "gcp" ]]; then
    IO_IMPL="org.apache.iceberg.gcp.gcs.GCSFileIO"
    if [[ -n "${S3_PREFIX}" ]]; then WAREHOUSE="gs://${GCP_BUCKET}/${S3_PREFIX}/"; else WAREHOUSE="gs://${GCP_BUCKET}/"; fi
  elif [[ "${STORAGE_PROVIDER}" == "azure" ]]; then
    IO_IMPL="org.apache.iceberg.azure.adlsv2.ADLSFileIO"
    if [[ -n "${S3_PREFIX}" ]]; then WAREHOUSE="abfs://${AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/${S3_PREFIX}/"; else WAREHOUSE="abfs://${AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/"; fi
  else
    fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
  fi
  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    CPU_REQUEST="${CPU_REQUEST_KIND}"; CPU_LIMIT="${CPU_LIMIT_KIND}"; MEM_REQUEST="${MEM_REQUEST_KIND}"; MEM_LIMIT="${MEM_LIMIT_KIND}"
    GRAVITINO_MEM="-Xms512m -Xmx512m"
  else
    CPU_REQUEST="${CPU_REQUEST_CLOUD}"; CPU_LIMIT="${CPU_LIMIT_CLOUD}"; MEM_REQUEST="${MEM_REQUEST_CLOUD}"; MEM_LIMIT="${MEM_LIMIT_CLOUD}"
    GRAVITINO_MEM="-Xms2g -Xmx2g"
  fi
  log "derived IO_IMPL=${IO_IMPL}"
  log "derived WAREHOUSE path used in config"
}

render_configmap(){
  local out="${MANIFEST_DIR}/configmap.yaml"
  local sname user pw
  sname="$(find_pg_secret_name)"
  if [[ -n "${sname}" ]]; then
    user="$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.username}' 2>/dev/null | base64 -d || true)"
    pw="$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
  else
    user=""
    pw=""
  fi
  cat > "${out}" <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${CONFIGMAP_NAME}
  namespace: ${TARGET_NS}
data:
  gravitino-iceberg-rest-server.conf: |
    gravitino.iceberg-rest.host=0.0.0.0
    gravitino.iceberg-rest.httpPort=${PORT}
    gravitino.iceberg-rest.catalog-backend=jdbc
    gravitino.iceberg-rest.uri=jdbc:postgresql://${PG_POOLER_HOST}:${PG_POOLER_PORT}/${PG_DB}
    gravitino.iceberg-rest.warehouse=${WAREHOUSE}
    gravitino.iceberg-rest.io-impl=${IO_IMPL}
    gravitino.iceberg-rest.jdbc-driver=org.postgresql.Driver
    gravitino.iceberg-rest.jdbc-max-connections=20
    gravitino.iceberg-rest.jdbc-user=${user}
    gravitino.iceberg-rest.jdbc-password=${pw}
    gravitino.iceberg-rest.jdbc-initialize=true
    gravitino.iceberg-rest.jdbc.schema-version=V1
EOF
  log "rendered configmap -> ${out}"
}

render_serviceaccount_rbac(){
  local sa_out="${MANIFEST_DIR}/serviceaccount.yaml"
  local role_out="${MANIFEST_DIR}/role.yaml"
  local rb_out="${MANIFEST_DIR}/rolebinding.yaml"
  if [[ "${USE_IAM}" == "true" ]]; then
    if [[ "${STORAGE_PROVIDER}" == "aws" ]]; then
      if [[ -z "${AWS_ROLE_ARN}" ]]; then fatal "AWS_ROLE_ARN required for IRSA mode"; fi
      cat > "${sa_out}" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
  annotations:
    eks.amazonaws.com/role-arn: ${AWS_ROLE_ARN}
  labels:
    app.kubernetes.io/name: iceberg-rest
automountServiceAccountToken: true
EOF
    elif [[ "${STORAGE_PROVIDER}" == "gcp" ]]; then
      if [[ -z "${GCP_PROJECT}" || -z "${GCP_SA_EMAIL}" ]]; then fatal "GCP_PROJECT and GCP_SA_EMAIL required for Workload Identity"; fi
      cat > "${sa_out}" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
  annotations:
    iam.gke.io/gcp-service-account: ${GCP_SA_EMAIL}
  labels:
    app.kubernetes.io/name: iceberg-rest
automountServiceAccountToken: true
EOF
    elif [[ "${STORAGE_PROVIDER}" == "azure" ]]; then
      if [[ -z "${AZURE_CLIENT_ID}" ]]; then fatal "AZURE_CLIENT_ID required for Azure Workload Identity"; fi
      cat > "${sa_out}" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
  annotations:
    azure.workload.identity/client-id: ${AZURE_CLIENT_ID}
  labels:
    app.kubernetes.io/name: iceberg-rest
automountServiceAccountToken: true
EOF
    else
      fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
    fi
  else
    cat > "${sa_out}" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
  labels:
    app.kubernetes.io/name: iceberg-rest
automountServiceAccountToken: true
EOF
  fi
  cat > "${role_out}" <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: iceberg-rest-role
  namespace: ${TARGET_NS}
rules:
- apiGroups: [""]
  resources: ["configmaps","secrets"]
  verbs: ["get","list"]
EOF
  cat > "${rb_out}" <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: iceberg-rest-rb
  namespace: ${TARGET_NS}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: iceberg-rest-role
subjects:
- kind: ServiceAccount
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
EOF
  log "rendered serviceaccount and rbac"
}

render_deploy_svc_hpa_pdb(){
  local out="${MANIFEST_DIR}/deployment.yaml"
  local svc="${MANIFEST_DIR}/service.yaml"
  local hpa="${MANIFEST_DIR}/hpa.yaml"
  local pdb="${MANIFEST_DIR}/pdb.yaml"
  local extra_env=""
  if [[ "${USE_IAM}" != "true" ]]; then
    if [[ "${STORAGE_PROVIDER}" == "aws" ]]; then
      extra_env=$(cat <<EOT
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AWS_ACCESS_KEY_ID
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AWS_SECRET_ACCESS_KEY
        - name: AWS_REGION
          value: "${AWS_REGION}"
EOT
)
    elif [[ "${STORAGE_PROVIDER}" == "gcp" ]]; then
      extra_env=$(cat <<EOT
        - name: GCP_SA_KEY_JSON_B64
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: GCP_SA_KEY_JSON_B64
EOT
)
    elif [[ "${STORAGE_PROVIDER}" == "azure" ]]; then
      extra_env=$(cat <<EOT
        - name: AZURE_STORAGE_ACCOUNT
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AZURE_STORAGE_ACCOUNT
        - name: AZURE_STORAGE_KEY
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AZURE_STORAGE_KEY
EOT
)
    fi
  fi

  # Add REST basic auth env injection: read from REST_SECRET_NAME (keys: user,password)
  local rest_env_block
  rest_env_block=$(cat <<'EOT'
        - name: ICEBERG_REST_AUTH_TYPE
          value: "basic"
        - name: ICEBERG_REST_USER
          valueFrom:
            secretKeyRef:
              name: REST_SECRET
              key: user
        - name: ICEBERG_REST_PASSWORD
          valueFrom:
            secretKeyRef:
              name: REST_SECRET
              key: password
EOT
)
  rest_env_block="${rest_env_block//REST_SECRET/${REST_SECRET_NAME}}"

  cat > "${out}" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEPLOYMENT_NAME}
  namespace: ${TARGET_NS}
spec:
  replicas: ${REPLICAS}
  selector:
    matchLabels:
      app: ${DEPLOYMENT_NAME}
  template:
    metadata:
      labels:
        app: ${DEPLOYMENT_NAME}
    spec:
      serviceAccountName: ${SERVICE_ACCOUNT_NAME}
      initContainers:
      - name: copy-config
        image: busybox:1.36
        command:
        - sh
        - -c
        - cp -a /config/. /conf/ || true
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
        - name: conf
          mountPath: /conf
      containers:
      - name: grav
        image: ${IMAGE}
        ports:
        - containerPort: ${PORT}
        env:
        - name: GRAVITINO_VERSION
          value: "${GRAVITINO_VERSION}"
        - name: GRAVITINO_MEM
          value: "${GRAVITINO_MEM}"
        - name: GRAVITINO_ICEBERG_REST_IO_IMPL
          value: "${IO_IMPL}"
        - name: GRAVITINO_ICEBERG_REST_JDBC_USER
          valueFrom:
            secretKeyRef:
              name: ${FALLBACK_PG_SECRET}
              key: username
        - name: GRAVITINO_ICEBERG_REST_JDBC_PASSWORD
          valueFrom:
            secretKeyRef:
              name: ${FALLBACK_PG_SECRET}
              key: password
        - name: GRAVITINO_ICEBERG_REST_S3_ENDPOINT
          value: "${S3_ENDPOINT}"
        - name: GRAVITINO_ICEBERG_REST_JDBC_MAX_CONNECTIONS
          value: "20"
${extra_env}
${rest_env_block}
        resources:
          requests:
            cpu: "${CPU_REQUEST}"
            memory: "${MEM_REQUEST}"
          limits:
            cpu: "${CPU_LIMIT}"
            memory: "${MEM_LIMIT}"
        volumeMounts:
        - name: conf
          mountPath: /root/gravitino-iceberg-rest-server/conf
          readOnly: false
      volumes:
      - name: config
        configMap:
          name: ${CONFIGMAP_NAME}
      - name: conf
        emptyDir: {}
EOF

  cat > "${svc}" <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${SERVICE_NAME}
  namespace: ${TARGET_NS}
spec:
  type: ClusterIP
  selector:
    app: ${DEPLOYMENT_NAME}
  ports:
  - name: http
    port: ${PORT}
    targetPort: ${PORT}
EOF

  cat > "${hpa}" <<EOF
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ${DEPLOYMENT_NAME}-hpa
  namespace: ${TARGET_NS}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ${DEPLOYMENT_NAME}
  minReplicas: 1
  maxReplicas: 3
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 60
EOF

  cat > "${pdb}" <<EOF
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ${DEPLOYMENT_NAME}-pdb
  namespace: ${TARGET_NS}
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: ${DEPLOYMENT_NAME}
EOF

  log "rendered deployment, service, hpa, pdb"
}

create_or_update_storage_secret(){
  if [[ "${USE_IAM}" == "true" ]]; then
    log "USE_IAM=true; skipping static storage secret creation"
    return 0
  fi
  case "${STORAGE_PROVIDER}" in
    aws)
      if [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" ]]; then log "AWS creds not provided in env; will skip static secret creation"; return 0; fi
      kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
      kubectl -n "${TARGET_NS}" create secret generic "${SECRET_NAME}" --from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" >/dev/null
      log "created aws secret ${SECRET_NAME}"
      ;;

    gcp)
      if [[ -z "${GCP_SA_KEY_JSON_B64}" ]]; then log "GCP SA key not provided; skipping static secret creation"; return 0; fi
      kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
      kubectl -n "${TARGET_NS}" create secret generic "${SECRET_NAME}" --from-literal=GCP_SA_KEY_JSON_B64="${GCP_SA_KEY_JSON_B64}" >/dev/null
      log "created gcp secret ${SECRET_NAME}"
      ;;

    azure)
      if [[ -z "${AZURE_STORAGE_ACCOUNT}" || -z "${AZURE_STORAGE_KEY}" ]]; then log "Azure creds not provided; skipping static secret creation"; return 0; fi
      kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
      kubectl -n "${TARGET_NS}" create secret generic "${SECRET_NAME}" --from-literal=AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT}" --from-literal=AZURE_STORAGE_KEY="${AZURE_STORAGE_KEY}" >/dev/null
      log "created azure secret ${SECRET_NAME}"
      ;;

    *)
      fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
      ;;

  esac
}

create_or_update_rest_auth_secret(){
  if [[ -z "${ICEBERG_REST_USER}" || -z "${ICEBERG_REST_PASSWORD}" ]]; then
    log "ICEBERG_REST_USER/ICEBERG_REST_PASSWORD not provided; skipping REST auth secret creation (server will remain anonymous if not required)"
    return 0
  fi
  kubectl -n "${TARGET_NS}" delete secret "${REST_SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" create secret generic "${REST_SECRET_NAME}" --from-literal=user="${ICEBERG_REST_USER}" --from-literal=password="${ICEBERG_REST_PASSWORD}" >/dev/null
  log "created/updated REST auth secret ${REST_SECRET_NAME}"
}

compute_manifests_hash(){
  local tmp files f
  tmp=$(mktemp)
  files=(serviceaccount.yaml role.yaml rolebinding.yaml configmap.yaml deployment.yaml service.yaml hpa.yaml pdb.yaml)
  for f in "${files[@]}"; do
    if [[ -f "${MANIFEST_DIR}/${f}" ]]; then
      cat "${MANIFEST_DIR}/${f}" >> "${tmp}"
    fi
  done
  sha256sum "${tmp}" | awk '{print $1}'
  rm -f "${tmp}"
}

annotate_with_hash(){
  local h="$1"
  kubectl -n "${TARGET_NS}" patch deployment "${DEPLOYMENT_NAME}" --type=merge -p "{\"metadata\":{\"annotations\":{\"${ANNOTATION_KEY}\":\"${h}\"}}}" >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" patch configmap "${CONFIGMAP_NAME}" --type=merge -p "{\"metadata\":{\"annotations\":{\"${ANNOTATION_KEY}\":\"${h}\"}}}" >/dev/null 2>&1 || true
}

kubectl_diff_apply(){
  local file="$1"
  if kubectl diff --server-side -f "${file}" >/dev/null 2>&1; then
    log "no diff for ${file}; skipping server-side apply"
  else
    kubectl apply --server-side -f "${file}"
    log "applied ${file}"
  fi
}

apply_manifests_idempotent(){
  local hash
  hash=$(compute_manifests_hash)
  local existing
  existing=$(kubectl -n "${TARGET_NS}" get deployment "${DEPLOYMENT_NAME}" -o "jsonpath={.metadata.annotations['${ANNOTATION_KEY}']}" 2>/dev/null || true)
  if [[ "${existing}" == "${hash}" ]]; then
    log "manifests unchanged (hash match); skipping heavy apply"
    return 0
  fi
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/serviceaccount.yaml" || true
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/role.yaml" || true
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/rolebinding.yaml" || true
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/configmap.yaml" || true
  kubectl_diff_apply "${MANIFEST_DIR}/deployment.yaml"
  kubectl_diff_apply "${MANIFEST_DIR}/service.yaml"
  kubectl_diff_apply "${MANIFEST_DIR}/hpa.yaml"
  kubectl_diff_apply "${MANIFEST_DIR}/pdb.yaml"
  annotate_with_hash "${hash}"
  log "applied manifests and wrote annotation"
}

wait_for_deployment_ready(){
  local start now elapsed ready desired
  start=$(date +%s)
  while true; do
    now=$(date +%s); elapsed=$((now-start))
    if [[ "${elapsed}" -ge "${READY_TIMEOUT}" ]]; then fatal "timeout waiting for deployment readiness"; fi
    ready=$(kubectl -n "${TARGET_NS}" get deploy "${DEPLOYMENT_NAME}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
    desired=$(kubectl -n "${TARGET_NS}" get deploy "${DEPLOYMENT_NAME}" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "${REPLICAS}")
    if [[ -n "${ready}" && -n "${desired}" && "${ready}" -ge "${desired}" ]]; then
      log "deployment ready ${ready}/${desired}"
      return 0
    fi
    sleep 2
  done
}

wait_for_service(){
  local start now elapsed
  start=$(date +%s)
  while true; do
    now=$(date +%s); elapsed=$((now-start))
    if kubectl -n "${TARGET_NS}" get svc "${SERVICE_NAME}" >/dev/null 2>&1; then
      log "service ${SERVICE_NAME} present"
      return 0
    fi
    if [[ "${elapsed}" -gt 60 ]]; then fatal "service not created in 60s"; fi
    sleep 1
  done
}

rest_local_exec(){
  local pod body code
  pod=$(kubectl -n "${TARGET_NS}" get pod -l "app=${DEPLOYMENT_NAME}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -z "${pod}" ]]; then fatal "no iceberg pod found for REST smoke"; fi
  body=$(kubectl -n "${TARGET_NS}" exec -c grav "${pod}" -- curl -sS -f http://127.0.0.1:${PORT}/iceberg/v1/config 2>/dev/null || true)
  if [[ -z "${body}" ]]; then fatal "REST /v1/config no response from local pod ${pod}"; fi
  echo "${body}" | jq -e . >/dev/null 2>&1 || fatal "/v1/config returned non-json or error: ${body}"
  log "/v1/config ok (local pod ${pod})"
  printf '%s' "${pod}"
}

run_rest_smoke(){
  local pod head_status create_status
  log "REST smoke test starting"
  pod=$(rest_local_exec) || return 1

  head_status=$(kubectl -n "${TARGET_NS}" exec -c grav "${pod}" -- \
    curl -s -o /dev/null -w "%{http_code}" \
    -I "http://127.0.0.1:${PORT}/iceberg/v1/namespaces/mlsecops_smoke" || true)

  if [[ "${head_status}" == "200" || "${head_status}" == "204" ]]; then
    log "namespace already exists (HEAD=${head_status})"
    return 0
  fi

  log "namespace not present, creating"

  create_status=$(kubectl -n "${TARGET_NS}" exec -c grav "${pod}" -- \
    curl -s -o /dev/null -w "%{http_code}" \
    -X POST -H "Content-Type: application/json" \
    --data '{"namespace":["mlsecops_smoke"]}' \
    "http://127.0.0.1:${PORT}/iceberg/v1/namespaces" || true)

  if [[ "${create_status}" != "200" && "${create_status}" != "201" && "${create_status}" != "204" ]]; then
    log "namespace create unexpected status ${create_status}; dumping logs"
    kubectl -n "${TARGET_NS}" logs deployment/"${DEPLOYMENT_NAME}" --tail=200 || true
    fatal "REST namespace create failed (status=${create_status})"
  fi

  log "namespace created (status=${create_status})"
}

run_postgres_smoke(){
  local sname user pw port host db out
  sname=$(find_pg_secret_name)
  if [[ -z "${sname}" ]]; then fatal "Postgres app secret not found"; fi
  user=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.username}' 2>/dev/null | base64 -d || true)
  pw=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)
  port=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.port}' 2>/dev/null | base64 -d || echo "${PG_POOLER_PORT}")
  host="${PG_POOLER_HOST}"
  db="${PG_DB}"
  log "testing psql via pooler -> host=${host} port=${port} db=${db} user=${user}"
  out=$(kubectl -n "${TARGET_NS}" run --rm -i --restart=Never pgtest --image=postgres:18 --env PGPASSWORD="${pw}" --env PGUSER="${user}" --command -- psql -h "${host}" -U "${user}" -p "${port}" -d "${db}" -c "select current_database();" -tA 2>/dev/null || true)
  if [[ -z "${out}" ]]; then
    kubectl -n "${TARGET_NS}" logs deployment/"${DEPLOYMENT_NAME}" --tail=200 || true
    fatal "psql connectivity test failed via ${host}:${port} (empty response)"
  fi
  log "psql connectivity passed; current_database=${out}"
}

run_objectstore_smoke(){
  case "${STORAGE_PROVIDER}" in
    aws)
      local sname akey skey
      sname="${SECRET_NAME}"
      akey=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' 2>/dev/null || true)
      skey=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' 2>/dev/null || true)
      if [[ -z "${akey}" || -z "${skey}" ]]; then
        log "AWS static credentials missing in secret ${sname}; skipping S3 smoke (WORKLOAD IDENTITY/IRSA might be used)"
        return 0
      fi
      local ACCESS_KEY SECRET_KEY
      ACCESS_KEY=$(printf '%s' "${akey}" | base64 -d)
      SECRET_KEY=$(printf '%s' "${skey}" | base64 -d)
      kubectl -n "${TARGET_NS}" run --rm -i --restart=Never s3test --image=amazon/aws-cli --env AWS_REGION="${AWS_REGION}" --env AWS_ACCESS_KEY_ID="${ACCESS_KEY}" --env AWS_SECRET_ACCESS_KEY="${SECRET_KEY}" --command -- sh -c "aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX} 2>/dev/null || (printf 'WRITE_TEST' >/tmp/t && aws s3 cp /tmp/t s3://${S3_BUCKET}/${S3_PREFIX%/}/mlsecops_smoke/test.txt && echo OK)" >/dev/null 2>&1 || fatal "S3 smoke test failed"
      log "S3 smoke test passed"
      ;;
    gcp)
      kubectl -n "${TARGET_NS}" run --rm -i --restart=Never gcstest --image=google/cloud-sdk:slim --command -- sh -c "gsutil ls ${WAREHOUSE} 2>/dev/null || (echo ok >/tmp/test && gsutil cp /tmp/test ${WAREHOUSE%/}/mlsecops_smoke/test.txt)" >/dev/null 2>&1 || fatal "GCS smoke test failed"
      log "GCS smoke test passed"
      ;;
    azure)
      kubectl -n "${TARGET_NS}" run --rm -i --restart=Never aztest --image=mcr.microsoft.com/azure-cli --command -- sh -c "az storage blob list --account-name ${AZURE_STORAGE_ACCOUNT} --container-name ${AZURE_CONTAINER} 2>/dev/null || az storage blob upload --account-name ${AZURE_STORAGE_ACCOUNT} --container-name ${AZURE_CONTAINER} --name mlsecops_smoke/test.txt --file /etc/hosts" >/dev/null 2>&1 || fatal "Azure storage smoke test failed"
      log "Azure storage smoke test passed"
      ;;
    *)
      fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
      ;;
  esac
}

delete_all(){
  kubectl -n "${TARGET_NS}" delete deployment "${DEPLOYMENT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete svc "${SERVICE_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete configmap "${CONFIGMAP_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete sa "${SERVICE_ACCOUNT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${REST_SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete role iceberg-rest-role --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete rolebinding iceberg-rest-rb --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete hpa "${DEPLOYMENT_NAME}-hpa" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete pdb "${DEPLOYMENT_NAME}-pdb" --ignore-not-found >/dev/null 2>&1 || true
  log "deleted k8s resources; preserved object-store data"
}

print_connection_details(){
  local s secret user pw db host port jdbc masked
  s=$(find_pg_secret_name)
  secret="${s:-${FALLBACK_PG_SECRET}}"
  user="$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.username}' 2>/dev/null | base64 -d || true)"
  pw="$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
  db="$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.dbname}' 2>/dev/null | base64 -d || echo "${PG_DB}")"
  host="${PG_POOLER_HOST}"
  port="${PG_POOLER_PORT}"
  jdbc="jdbc:postgresql://${host}:${port}/${db}"
  printf "\nConnection details (materialized):\n\n"
  printf "JDBC_URI=%s\n" "${jdbc}"
  printf "JDBC_USER=%s\n" "${user}"
  printf "JDBC_PASSWORD=%s\n" "${pw}"
  printf "POOLER_HOST=%s\n" "${host}"
  printf "POOLER_PORT=%s\n" "${port}"
  printf "DB=%s\n" "${db}"
  printf "WAREHOUSE=%s\n" "${WAREHOUSE}"
  printf "IO_IMPL=%s\n" "${IO_IMPL}"
  if [[ "${STORAGE_PROVIDER}" == "aws" ]]; then
    printf "S3_BUCKET=%s\n" "${S3_BUCKET}"
    printf "S3_PREFIX=%s\n" "${S3_PREFIX}"
    printf "AWS_REGION=%s\n" "${AWS_REGION}"
  fi
  echo
}

main_rollout(){
  require_prereqs
  pg_detect_secret
  PG_DB="$(pg_detect_db)"
  derive_io_and_warehouse
  log "starting rollout provider=${STORAGE_PROVIDER} cluster=${K8S_CLUSTER} pg_db=${PG_DB}"
  render_configmap
  render_serviceaccount_rbac
  render_deploy_svc_hpa_pdb
  kubectl create ns "${TARGET_NS}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null 2>&1 || true
  create_or_update_storage_secret
  create_or_update_rest_auth_secret
  apply_manifests_idempotent
  wait_for_deployment_ready
  wait_for_service
  run_rest_smoke
  run_postgres_smoke
  run_objectstore_smoke
  log "[SUCCESS] iceberg rollout complete"
  print_connection_details
  exit 0
}

case "${1:-}" in
  --rollout) main_rollout ;;
  --delete) delete_all ;;
  *) printf "Usage: %s [--rollout|--delete]\n" "$0"; exit 2 ;;
esac