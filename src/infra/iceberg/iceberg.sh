#!/usr/bin/env bash
# 1. Assumes CNPG and PgBouncer already exist; this script deploys only the Iceberg REST layer.
# 2. Uses the documented Gravitino env mappings for JDBC, object-store, and HTTP-port settings.
# 3. Keeps only file-only settings (static provider/back-end name) in the ConfigMap.
# 4. Waits for the service to serve /iceberg/v1/config before creating a test namespace.
# 5. Disables HPA by default to avoid metrics-server noise on kind clusters.

set -Eeuo pipefail

K8S_CLUSTER="${K8S_CLUSTER:-kind}"   # kind|cloud
TARGET_NS="${TARGET_NS:-default}"
MANIFEST_DIR="${MANIFEST_DIR:-src/manifests/iceberg}"

IMAGE="${IMAGE:-ghcr.io/athithya-sakthivel/iceberg-rest-gravitino-postgres@sha256:bb2ac1c0328a94b65834aa0bae9e863baf8aaee7557810e7b79c71f3537d13ea}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-iceberg-rest-sa}"
SERVICE_NAME="${SERVICE_NAME:-iceberg-rest}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-iceberg-rest}"
CONFIGMAP_NAME="${CONFIGMAP_NAME:-iceberg-rest-conf}"
SECRET_NAME="${SECRET_NAME:-iceberg-storage-credentials}"
REST_SECRET_NAME="${REST_SECRET_NAME:-iceberg-rest-auth}"
PORT="${PORT:-9001}"
ANNOTATION_KEY="${ANNOTATION_KEY:-mlsecops.iceberg.checksum}"
ENABLE_HPA="${ENABLE_HPA:-false}"

# Storage provider: aws|gcp|azure
STORAGE_PROVIDER="${STORAGE_PROVIDER:-aws}"
USE_IAM="${USE_IAM:-false}"

# AWS
AWS_REGION="${AWS_REGION:-ap-south-1}"
S3_BUCKET="${S3_BUCKET:-e2e-mlops-data-681802563986}"
S3_PREFIX="${S3_PREFIX:-iceberg/warehouse}"
S3_ENDPOINT="${S3_ENDPOINT:-}"
S3_PATH_STYLE_ACCESS="${S3_PATH_STYLE_ACCESS:-false}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_ROLE_ARN="${AWS_ROLE_ARN:-}"

# GCP
GCP_PROJECT="${GCP_PROJECT:-}"
GCP_BUCKET="${GCP_BUCKET:-mlops_iceberg_warehouse}"
GCP_SA_KEY_JSON_B64="${GCP_SA_KEY_JSON_B64:-}"
GCP_SA_EMAIL="${GCP_SA_EMAIL:-}"
GCP_SECRET_FILE_NAME="${GCP_SECRET_FILE_NAME:-service-account.json}"
GCP_MOUNT_DIR="${GCP_MOUNT_DIR:-/var/run/gravitino/gcp}"

# Azure
AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-}"
AZURE_STORAGE_KEY="${AZURE_STORAGE_KEY:-}"
AZURE_CONTAINER="${AZURE_CONTAINER:-iceberg}"
AZURE_CLIENT_ID="${AZURE_CLIENT_ID:-}"
AZURE_TENANT_ID="${AZURE_TENANT_ID:-}"

# CNPG / Postgres pooler assumptions
PG_POOLER_HOST="${PG_POOLER_HOST:-postgres-pooler.${TARGET_NS}.svc.cluster.local}"
PG_POOLER_PORT="${PG_POOLER_PORT:-5432}"
PG_DB_OVERRIDE="${PG_DB_OVERRIDE:-}"
PG_APP_SECRET_LABEL="${PG_APP_SECRET_LABEL:-cnpg.io/cluster=postgres-cluster,cnpg.io/userType=app}"
FALLBACK_PG_SECRET="${FALLBACK_PG_SECRET:-postgres-cluster-app}"

# JVM sizing
JVM_HEAP_MODE="${JVM_HEAP_MODE:-auto}"  # auto|fixed|percentage
GRAVITINO_MEM_FIXED_KIND="${GRAVITINO_MEM_FIXED_KIND:--Xms512m -Xmx512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200}"
GRAVITINO_MEM_FIXED_CLOUD="${GRAVITINO_MEM_FIXED_CLOUD:--Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200}"
GRAVITINO_MEM_PERCENTAGE="${GRAVITINO_MEM_PERCENTAGE:--XX:InitialRAMPercentage=25 -XX:MaxRAMPercentage=50 -XX:+UseG1GC -XX:MaxGCPauseMillis=200}"

# Resource defaults
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
READY_TIMEOUT="${READY_TIMEOUT:-300}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-180}"

mkdir -p "${MANIFEST_DIR}"

log(){ printf '[%s] [iceberg] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
fatal(){ printf '[%s] [iceberg][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; exit 1; }
require_bin(){ command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"; }

trap 'rc=$?; echo; echo "[DIAG] exit_code=$rc"; echo "[DIAG] kubectl context: $(kubectl config current-context 2>/dev/null || true)"; echo "[DIAG] pods in ns ${TARGET_NS}:"; kubectl -n "${TARGET_NS}" get pods -o wide || true; echo "[DIAG] svc in ns ${TARGET_NS}:"; kubectl -n "${TARGET_NS}" get svc -o wide || true; echo "[DIAG] events (last 200):"; kubectl get events -A --sort-by=.lastTimestamp | tail -n 200 || true; exit $rc' ERR

STORAGE_SECRET_CHANGED=0
REST_SECRET_CHANGED=0
REST_AUTH_USER=""
REST_AUTH_PASSWORD=""

require_prereqs(){
  require_bin kubectl
  require_bin curl
  require_bin jq
  require_bin sha256sum
  require_bin python3
  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach cluster"
}

ensure_namespace(){
  kubectl create ns "${TARGET_NS}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null 2>&1 || true
}

find_pg_secret_name(){
  local s
  s="$(kubectl -n "${TARGET_NS}" get secret -l "${PG_APP_SECRET_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${s}" ]]; then
    printf '%s' "${s}"
    return 0
  fi
  if kubectl -n "${TARGET_NS}" get secret "${FALLBACK_PG_SECRET}" >/dev/null 2>&1; then
    printf '%s' "${FALLBACK_PG_SECRET}"
    return 0
  fi
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

compute_gravitino_mem(){
  case "${JVM_HEAP_MODE}" in
    auto)
      if [[ "${K8S_CLUSTER}" == "kind" ]]; then
        GRAVITINO_MEM="${GRAVITINO_MEM_FIXED_KIND}"
      else
        GRAVITINO_MEM="${GRAVITINO_MEM_PERCENTAGE}"
      fi
      ;;
    fixed)
      if [[ "${K8S_CLUSTER}" == "kind" ]]; then
        GRAVITINO_MEM="${GRAVITINO_MEM_FIXED_KIND}"
      else
        GRAVITINO_MEM="${GRAVITINO_MEM_FIXED_CLOUD}"
      fi
      ;;
    percentage)
      GRAVITINO_MEM="${GRAVITINO_MEM_PERCENTAGE}"
      ;;
    *)
      fatal "unsupported JVM_HEAP_MODE=${JVM_HEAP_MODE} (use auto|fixed|percentage)"
      ;;
  esac
  GRAVITINO_MEM="$(printf '%s' "${GRAVITINO_MEM}" | awk '{$1=$1;print}')"
}

derive_io_and_warehouse(){
  case "${STORAGE_PROVIDER}" in
    aws)
      IO_IMPL="org.apache.iceberg.aws.s3.S3FileIO"
      if [[ -n "${S3_PREFIX}" ]]; then
        WAREHOUSE="s3://${S3_BUCKET}/${S3_PREFIX}/"
      else
        WAREHOUSE="s3://${S3_BUCKET}/"
      fi
      ;;
    gcp)
      IO_IMPL="org.apache.iceberg.gcp.gcs.GCSFileIO"
      if [[ -n "${S3_PREFIX}" ]]; then
        WAREHOUSE="gs://${GCP_BUCKET}/${S3_PREFIX}/"
      else
        WAREHOUSE="gs://${GCP_BUCKET}/"
      fi
      ;;
    azure)
      IO_IMPL="org.apache.iceberg.azure.adlsv2.ADLSFileIO"
      if [[ -n "${S3_PREFIX}" ]]; then
        WAREHOUSE="abfs://${AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/${S3_PREFIX}/"
      else
        WAREHOUSE="abfs://${AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/"
      fi
      ;;
    *)
      fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
      ;;
  esac

  if [[ "${K8S_CLUSTER}" == "kind" ]]; then
    CPU_REQUEST="${CPU_REQUEST_KIND}"
    CPU_LIMIT="${CPU_LIMIT_KIND}"
    MEM_REQUEST="${MEM_REQUEST_KIND}"
    MEM_LIMIT="${MEM_LIMIT_KIND}"
  else
    CPU_REQUEST="${CPU_REQUEST_CLOUD}"
    CPU_LIMIT="${CPU_LIMIT_CLOUD}"
    MEM_REQUEST="${MEM_REQUEST_CLOUD}"
    MEM_LIMIT="${MEM_LIMIT_CLOUD}"
  fi

  compute_gravitino_mem

  log "derived IO_IMPL=${IO_IMPL}"
  log "derived WAREHOUSE=${WAREHOUSE}"
  log "derived GRAVITINO_MEM=${GRAVITINO_MEM}"
}

create_or_update_storage_secret(){
  if [[ "${USE_IAM}" == "true" ]]; then
    log "USE_IAM=true; skipping static storage secret creation"
    return 0
  fi

  case "${STORAGE_PROVIDER}" in
    aws)
      [[ -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]] || fatal "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when USE_IAM=false"
      kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
      kubectl -n "${TARGET_NS}" create secret generic "${SECRET_NAME}" \
        --from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" >/dev/null
      STORAGE_SECRET_CHANGED=1
      log "created aws storage secret ${SECRET_NAME}"
      ;;
    gcp)
      [[ -n "${GCP_SA_KEY_JSON_B64}" ]] || fatal "GCP_SA_KEY_JSON_B64 required when USE_IAM=false"
      local tmp_json
      tmp_json="$(mktemp)"
      python3 - <<'PY' >"${tmp_json}"
import base64
import os
import sys
sys.stdout.buffer.write(base64.b64decode(os.environ["GCP_SA_KEY_JSON_B64"]))
PY
      kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
      kubectl -n "${TARGET_NS}" create secret generic "${SECRET_NAME}" \
        --from-file="${GCP_SECRET_FILE_NAME}=${tmp_json}" >/dev/null
      rm -f "${tmp_json}" >/dev/null 2>&1 || true
      STORAGE_SECRET_CHANGED=1
      log "created gcp storage secret ${SECRET_NAME}"
      ;;
    azure)
      [[ -n "${AZURE_STORAGE_ACCOUNT}" && -n "${AZURE_STORAGE_KEY}" ]] || fatal "AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY are required when USE_IAM=false"
      kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
      kubectl -n "${TARGET_NS}" create secret generic "${SECRET_NAME}" \
        --from-literal=AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT}" \
        --from-literal=AZURE_STORAGE_KEY="${AZURE_STORAGE_KEY}" >/dev/null
      STORAGE_SECRET_CHANGED=1
      log "created azure storage secret ${SECRET_NAME}"
      ;;
    *)
      fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
      ;;
  esac
}

random_string(){
  python3 - <<'PY'
import secrets
import string
alphabet = string.ascii_letters + string.digits
print(''.join(secrets.choice(alphabet) for _ in range(24)))
PY
}

create_or_update_rest_auth_secret(){
  local existing_user existing_password

  if kubectl -n "${TARGET_NS}" get secret "${REST_SECRET_NAME}" >/dev/null 2>&1; then
    existing_user="$(kubectl -n "${TARGET_NS}" get secret "${REST_SECRET_NAME}" -o jsonpath='{.data.user}' 2>/dev/null | base64 -d || true)"
    existing_password="$(kubectl -n "${TARGET_NS}" get secret "${REST_SECRET_NAME}" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
    if [[ -z "${ICEBERG_REST_USER:-}" && -z "${ICEBERG_REST_PASSWORD:-}" ]]; then
      REST_AUTH_USER="${existing_user}"
      REST_AUTH_PASSWORD="${existing_password}"
      log "reusing existing REST auth secret ${REST_SECRET_NAME}"
      return 0
    fi
  fi

  if [[ -z "${ICEBERG_REST_USER:-}" ]]; then
    ICEBERG_REST_USER="iceberg"
  fi
  if [[ -z "${ICEBERG_REST_PASSWORD:-}" ]]; then
    ICEBERG_REST_PASSWORD="$(random_string)"
  fi

  kubectl -n "${TARGET_NS}" delete secret "${REST_SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" create secret generic "${REST_SECRET_NAME}" \
    --from-literal=user="${ICEBERG_REST_USER}" \
    --from-literal=password="${ICEBERG_REST_PASSWORD}" >/dev/null
  REST_SECRET_CHANGED=1
  REST_AUTH_USER="${ICEBERG_REST_USER}"
  REST_AUTH_PASSWORD="${ICEBERG_REST_PASSWORD}"
  log "created/updated REST auth secret ${REST_SECRET_NAME}"
}

render_configmap(){
  local out="${MANIFEST_DIR}/configmap.yaml"
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
    gravitino.iceberg-rest.catalog-config-provider=static-config-provider
    gravitino.iceberg-rest.catalog-backend=jdbc
    gravitino.iceberg-rest.catalog-backend-name=jdbc
    gravitino.iceberg-rest.jdbc-initialize=true
    gravitino.iceberg-rest.jdbc.schema-version=V1
EOF
  log "rendered configmap -> ${out}"
}

render_serviceaccount(){
  local sa_out="${MANIFEST_DIR}/serviceaccount.yaml"

  if [[ "${USE_IAM}" == "true" ]]; then
    case "${STORAGE_PROVIDER}" in
      aws)
        [[ -n "${AWS_ROLE_ARN}" ]] || fatal "AWS_ROLE_ARN required for USE_IAM=true on aws"
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
        ;;
      gcp)
        [[ -n "${GCP_PROJECT}" && -n "${GCP_SA_EMAIL}" ]] || fatal "GCP_PROJECT and GCP_SA_EMAIL required for USE_IAM=true on gcp"
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
        ;;
      azure)
        [[ -n "${AZURE_CLIENT_ID}" ]] || fatal "AZURE_CLIENT_ID required for USE_IAM=true on azure"
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
        ;;
      *)
        fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
        ;;
    esac
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
  log "rendered serviceaccount -> ${sa_out}"
}

render_deployment_service_pdb(){
  local out="${MANIFEST_DIR}/deployment.yaml"
  local svc="${MANIFEST_DIR}/service.yaml"
  local pdb="${MANIFEST_DIR}/pdb.yaml"
  local extra_env=""
  local extra_volume_mounts=""
  local extra_volumes=""

  case "${STORAGE_PROVIDER}" in
    aws)
      if [[ "${USE_IAM}" != "true" ]]; then
        extra_env=$(cat <<EOT
        - name: GRAVITINO_S3_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AWS_ACCESS_KEY_ID
        - name: GRAVITINO_S3_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AWS_SECRET_ACCESS_KEY
        - name: GRAVITINO_S3_REGION
          value: "${AWS_REGION}"
        - name: GRAVITINO_S3_ENDPOINT
          value: "${S3_ENDPOINT}"
        - name: GRAVITINO_S3_PATH_STYLE_ACCESS
          value: "${S3_PATH_STYLE_ACCESS}"
EOT
)
      else
        extra_env=$(cat <<EOT
        - name: GRAVITINO_S3_REGION
          value: "${AWS_REGION}"
        - name: GRAVITINO_S3_ENDPOINT
          value: "${S3_ENDPOINT}"
        - name: GRAVITINO_S3_PATH_STYLE_ACCESS
          value: "${S3_PATH_STYLE_ACCESS}"
EOT
)
      fi
      ;;
    gcp)
      if [[ "${USE_IAM}" != "true" ]]; then
        extra_env=$(cat <<EOT
        - name: GRAVITINO_GCS_SERVICE_ACCOUNT_FILE
          value: "${GCP_MOUNT_DIR}/${GCP_SECRET_FILE_NAME}"
EOT
)
        extra_volume_mounts=$(cat <<EOF
        - name: gcp-sa
          mountPath: ${GCP_MOUNT_DIR}
          readOnly: true
EOF
)
        extra_volumes=$(cat <<EOF
      - name: gcp-sa
        secret:
          secretName: ${SECRET_NAME}
          items:
          - key: ${GCP_SECRET_FILE_NAME}
            path: ${GCP_SECRET_FILE_NAME}
EOF
)
      fi
      ;;
    azure)
      if [[ "${USE_IAM}" != "true" ]]; then
        extra_env=$(cat <<EOT
        - name: GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AZURE_STORAGE_ACCOUNT
        - name: GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY
          valueFrom:
            secretKeyRef:
              name: ${SECRET_NAME}
              key: AZURE_STORAGE_KEY
        - name: GRAVITINO_AZURE_TENANT_ID
          value: "${AZURE_TENANT_ID}"
        - name: GRAVITINO_AZURE_CLIENT_ID
          value: "${AZURE_CLIENT_ID}"
EOT
)
      else
        extra_env=$(cat <<EOT
        - name: GRAVITINO_AZURE_TENANT_ID
          value: "${AZURE_TENANT_ID}"
        - name: GRAVITINO_AZURE_CLIENT_ID
          value: "${AZURE_CLIENT_ID}"
EOT
)
      fi
      ;;
    *)
      fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
      ;;
  esac

  local pg_secret
  pg_secret="$(find_pg_secret_name)"
  [[ -n "${pg_secret}" ]] || fatal "CNPG app secret not found; expected label selector '${PG_APP_SECRET_LABEL}'"

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
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  template:
    metadata:
      labels:
        app: ${DEPLOYMENT_NAME}
    spec:
      serviceAccountName: ${SERVICE_ACCOUNT_NAME}
      terminationGracePeriodSeconds: 30
      securityContext:
        seccompProfile:
          type: RuntimeDefault
        fsGroup: 0
        fsGroupChangePolicy: OnRootMismatch
      initContainers:
      - name: copy-config
        image: busybox:1.36
        imagePullPolicy: IfNotPresent
        command:
        - sh
        - -c
        - cp -a /config/. /conf/ && chmod -R ug+rwX /conf || true
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
        - name: conf
          mountPath: /conf
      containers:
      - name: grav
        image: ${IMAGE}
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: ${PORT}
        env:
        - name: GRAVITINO_VERSION
          value: "${GRAVITINO_VERSION}"
        - name: GRAVITINO_ICEBERG_REST_HTTP_PORT
          value: "${PORT}"
        - name: GRAVITINO_IO_IMPL
          value: "${IO_IMPL}"
        - name: GRAVITINO_CATALOG_BACKEND
          value: "jdbc"
        - name: GRAVITINO_URI
          value: "jdbc:postgresql://${PG_POOLER_HOST}:${PG_POOLER_PORT}/${PG_DB}"
        - name: GRAVITINO_WAREHOUSE
          value: "${WAREHOUSE}"
        - name: GRAVITINO_JDBC_DRIVER
          value: "org.postgresql.Driver"
        - name: GRAVITINO_JDBC_USER
          valueFrom:
            secretKeyRef:
              name: ${pg_secret}
              key: username
        - name: GRAVITINO_JDBC_PASSWORD
          valueFrom:
            secretKeyRef:
              name: ${pg_secret}
              key: password
${extra_env}
        - name: ICEBERG_REST_AUTH_TYPE
          value: "basic"
        - name: ICEBERG_REST_USER
          valueFrom:
            secretKeyRef:
              name: ${REST_SECRET_NAME}
              key: user
        - name: ICEBERG_REST_PASSWORD
          valueFrom:
            secretKeyRef:
              name: ${REST_SECRET_NAME}
              key: password
        resources:
          requests:
            cpu: "${CPU_REQUEST}"
            memory: "${MEM_REQUEST}"
          limits:
            cpu: "${CPU_LIMIT}"
            memory: "${MEM_LIMIT}"
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          runAsUser: 1001
          runAsGroup: 0
          capabilities:
            drop: ["ALL"]
        startupProbe:
          tcpSocket:
            port: ${PORT}
          initialDelaySeconds: 15
          periodSeconds: 5
          failureThreshold: 120
        readinessProbe:
          tcpSocket:
            port: ${PORT}
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 2
          failureThreshold: 12
        livenessProbe:
          tcpSocket:
            port: ${PORT}
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 2
          failureThreshold: 6
        volumeMounts:
        - name: conf
          mountPath: /root/gravitino-iceberg-rest-server/conf
          readOnly: false
        - name: logs
          mountPath: /root/gravitino-iceberg-rest-server/logs
        - name: tmp
          mountPath: /tmp
${extra_volume_mounts}
      volumes:
      - name: config
        configMap:
          name: ${CONFIGMAP_NAME}
      - name: conf
        emptyDir: {}
      - name: logs
        emptyDir: {}
      - name: tmp
        emptyDir: {}
${extra_volumes}
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

  log "rendered deployment, service, pdb"
}

compute_manifests_hash(){
  local tmp files f
  tmp=$(mktemp)
  files=(serviceaccount.yaml configmap.yaml deployment.yaml service.yaml pdb.yaml)
  for f in "${files[@]}"; do
    [[ -f "${MANIFEST_DIR}/${f}" ]] && cat "${MANIFEST_DIR}/${f}" >> "${tmp}"
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
    log "no diff for ${file}; skipping apply"
  else
    kubectl apply --server-side -f "${file}"
    log "applied ${file}"
  fi
}

apply_manifests_idempotent(){
  local hash existing
  hash=$(compute_manifests_hash)
  existing=$(kubectl -n "${TARGET_NS}" get deployment "${DEPLOYMENT_NAME}" -o "jsonpath={.metadata.annotations['${ANNOTATION_KEY}']}" 2>/dev/null || true)
  if [[ "${existing}" == "${hash}" ]]; then
    log "manifests unchanged (hash match); skipping heavy apply"
    return 0
  fi

  kubectl -n "${TARGET_NS}" delete hpa "${DEPLOYMENT_NAME}-hpa" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/serviceaccount.yaml" || true
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/configmap.yaml" || true
  kubectl_diff_apply "${MANIFEST_DIR}/deployment.yaml"
  kubectl_diff_apply "${MANIFEST_DIR}/service.yaml"
  kubectl_diff_apply "${MANIFEST_DIR}/pdb.yaml"
  annotate_with_hash "${hash}"
  log "applied manifests and wrote annotation"
}

wait_for_deployment_ready(){
  kubectl -n "${TARGET_NS}" rollout status deployment/"${DEPLOYMENT_NAME}" --timeout="${READY_TIMEOUT}s" >/dev/null || fatal "timeout waiting for deployment readiness"
  log "deployment ready"
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
    if [[ "${elapsed}" -gt 60 ]]; then
      fatal "service not created in 60s"
    fi
    sleep 1
  done
}

get_deployment_pod(){
  kubectl -n "${TARGET_NS}" get pod -l "app=${DEPLOYMENT_NAME}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

get_rest_creds(){
  local user pass
  if [[ -n "${REST_AUTH_USER}" && -n "${REST_AUTH_PASSWORD}" ]]; then
    printf '%s\n%s' "${REST_AUTH_USER}" "${REST_AUTH_PASSWORD}"
    return 0
  fi

  user="$(kubectl -n "${TARGET_NS}" get secret "${REST_SECRET_NAME}" -o jsonpath='{.data.user}' 2>/dev/null | base64 -d || true)"
  pass="$(kubectl -n "${TARGET_NS}" get secret "${REST_SECRET_NAME}" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
  [[ -n "${user}" && -n "${pass}" ]] || fatal "REST auth secret missing or incomplete"
  REST_AUTH_USER="${user}"
  REST_AUTH_PASSWORD="${pass}"
  printf '%s\n%s' "${user}" "${pass}"
}

dump_iceberg_debug(){
  local pod
  pod="$(get_deployment_pod)"
  log "dumping iceberg logs"
  kubectl -n "${TARGET_NS}" logs deployment/"${DEPLOYMENT_NAME}" --tail=200 || true
  if [[ -n "${pod}" ]]; then
    kubectl -n "${TARGET_NS}" exec -c grav "${pod}" -- sh -lc '
      echo "--- /root/gravitino-iceberg-rest-server/conf ---"
      ls -la /root/gravitino-iceberg-rest-server/conf || true
      echo "--- server conf ---"
      sed -n "1,240p" /root/gravitino-iceberg-rest-server/conf/gravitino-iceberg-rest-server.conf || true
      echo "--- env snapshot ---"
      env | sort | grep -E "^(GRAVITINO_|ICEBERG_REST_|AWS_|GCP_|AZURE_)" || true
    ' || true
  fi
}

wait_for_rest_config(){
  local user pass pod attempt response code body
  read -r user pass < <(get_rest_creds)

  for attempt in $(seq 1 "${SMOKE_TIMEOUT}"); do
    pod="$(get_deployment_pod)"
    if [[ -n "${pod}" ]]; then
      response="$(kubectl -n "${TARGET_NS}" exec -c grav "${pod}" -- env REST_USER="${user}" REST_PASS="${pass}" sh -lc "curl -sS -w '\n%{http_code}' -u \"\$REST_USER:\$REST_PASS\" 'http://127.0.0.1:${PORT}/iceberg/v1/config'" 2>/dev/null || true)"
      if [[ -n "${response}" ]]; then
        code="${response##*$'\n'}"
        body="${response%$'\n'*}"
        if [[ "${code}" == "200" ]] && printf '%s' "${body}" | jq -e . >/dev/null 2>&1; then
          log "/v1/config ok"
          return 0
        fi
      fi
    fi
    sleep 2
  done

  dump_iceberg_debug
  fatal "REST /v1/config never became healthy"
}

run_rest_smoke(){
  local user pass pod head_status response code body
  log "REST smoke test starting"

  wait_for_rest_config

  read -r user pass < <(get_rest_creds)
  pod="$(get_deployment_pod)"
  [[ -n "${pod}" ]] || fatal "no iceberg pod found for REST smoke"

  head_status="$(kubectl -n "${TARGET_NS}" exec -c grav "${pod}" -- env REST_USER="${user}" REST_PASS="${pass}" sh -lc "curl -s -o /dev/null -w '%{http_code}' -u \"\$REST_USER:\$REST_PASS\" -I 'http://127.0.0.1:${PORT}/iceberg/v1/namespaces/mlsecops_smoke'" 2>/dev/null || true)"
  if [[ "${head_status}" == "200" || "${head_status}" == "204" ]]; then
    log "namespace already exists (HEAD=${head_status})"
    return 0
  fi

  if [[ "${head_status}" != "404" && "${head_status}" != "405" && "${head_status}" != "204" && "${head_status}" != "200" ]]; then
    log "namespace HEAD returned ${head_status}; attempting create anyway"
  fi

  log "namespace not present, creating"
  response="$(kubectl -n "${TARGET_NS}" exec -c grav "${pod}" -- env REST_USER="${user}" REST_PASS="${pass}" sh -lc "curl -sS -w '\n%{http_code}' -u \"\$REST_USER:\$REST_PASS\" -X POST -H 'Content-Type: application/json' --data '{\"namespace\":[\"mlsecops_smoke\"]}' 'http://127.0.0.1:${PORT}/iceberg/v1/namespaces'" 2>/dev/null || true)"
  if [[ -z "${response}" ]]; then
    dump_iceberg_debug
    fatal "REST namespace create returned no response"
  fi

  code="${response##*$'\n'}"
  body="${response%$'\n'*}"

  if [[ "${code}" != "200" && "${code}" != "201" && "${code}" != "204" && "${code}" != "409" ]]; then
    log "namespace create unexpected status ${code}; body=${body}"
    dump_iceberg_debug
    fatal "REST namespace create failed (status=${code})"
  fi

  log "namespace created (status=${code})"
}

run_postgres_smoke(){
  local sname user pw port host db out
  sname=$(find_pg_secret_name)
  [[ -n "${sname}" ]] || fatal "Postgres app secret not found"

  user=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.username}' 2>/dev/null | base64 -d || true)
  pw=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)
  port=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.port}' 2>/dev/null | base64 -d || echo "${PG_POOLER_PORT}")
  host="${PG_POOLER_HOST}"
  db="${PG_DB}"

  [[ -n "${user}" && -n "${pw}" ]] || fatal "CNPG app secret missing username/password"

  log "testing psql via pooler -> host=${host} port=${port} db=${db} user=${user}"
  out=$(kubectl -n "${TARGET_NS}" run --rm -i --restart=Never pgtest --image=postgres:18 \
    --env PGPASSWORD="${pw}" --env PGUSER="${user}" --command -- \
    psql -h "${host}" -U "${user}" -p "${port}" -d "${db}" -c "select current_database();" -tA 2>/dev/null || true)

  if [[ -z "${out}" ]]; then
    dump_iceberg_debug
    fatal "psql connectivity test failed via ${host}:${port} (empty response)"
  fi
  log "psql connectivity passed; current_database=${out}"
}

run_objectstore_smoke(){
  case "${STORAGE_PROVIDER}" in
    aws)
      local sname akey skey access_key secret_key
      sname="${SECRET_NAME}"
      if kubectl -n "${TARGET_NS}" get secret "${sname}" >/dev/null 2>&1; then
        akey=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' 2>/dev/null || true)
        skey=$(kubectl -n "${TARGET_NS}" get secret "${sname}" -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' 2>/dev/null || true)
        if [[ -z "${akey}" || -z "${skey}" ]]; then
          log "AWS static credentials missing in secret ${sname}; skipping S3 smoke"
          return 0
        fi
        access_key=$(printf '%s' "${akey}" | base64 -d)
        secret_key=$(printf '%s' "${skey}" | base64 -d)
        kubectl -n "${TARGET_NS}" run --rm -i --restart=Never s3test --image=amazon/aws-cli \
          --env AWS_REGION="${AWS_REGION}" \
          --env AWS_ACCESS_KEY_ID="${access_key}" \
          --env AWS_SECRET_ACCESS_KEY="${secret_key}" \
          --command -- sh -c "aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX} 2>/dev/null || (printf 'WRITE_TEST' >/tmp/t && aws s3 cp /tmp/t s3://${S3_BUCKET}/${S3_PREFIX%/}/mlsecops_smoke/test.txt && echo OK)" >/dev/null 2>&1 || fatal "S3 smoke test failed"
        log "S3 smoke test passed"
      else
        log "AWS secret ${sname} not present; skipping S3 smoke"
      fi
      ;;
    gcp)
      log "GCS smoke test skipped unless you wire a GCP-authenticated test pod"
      ;;
    azure)
      log "Azure smoke test skipped unless you wire an Azure-authenticated test pod"
      ;;
    *)
      fatal "unsupported STORAGE_PROVIDER=${STORAGE_PROVIDER}"
      ;;
  esac
}

mask_uri(){ echo "$1" | sed -E 's#(:)[^:@]+(@)#:\*\*\*\*\*@#'; }


print_connection_details(){
  local s secret user pw db host port jdbc pooler_svc local_pf_port
  s="$(find_pg_secret_name)"
  secret="${s:-${FALLBACK_PG_SECRET}}"

  user="$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.username}' 2>/dev/null | base64 -d || true)"
  pw="$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
  db="$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.dbname}' 2>/dev/null | base64 -d || echo "${PG_DB}")"
  port="$(kubectl -n "${TARGET_NS}" get secret "${secret}" -o jsonpath='{.data.port}' 2>/dev/null | base64 -d || echo "${PG_POOLER_PORT}")"

  host="${PG_POOLER_HOST}"
  jdbc="jdbc:postgresql://${host}:${port}/${db}"
  pooler_svc="${PG_POOLER_SERVICE_NAME:-${PG_POOLER_HOST%%.*}}"
  local_pf_port="${LOCAL_PG_PORT:-15432}"

  printf "\nConnection details (unmasked):\n\n"
  printf "JDBC_URI=%s\n" "${jdbc}"
  printf "JDBC_USER=%s\n" "${user}"
  printf "JDBC_PASSWORD=%s\n" "${pw}"
  printf "POOLER_HOST=%s\n" "${host}"
  printf "POOLER_SERVICE=%s\n" "${pooler_svc}"
  printf "POOLER_PORT=%s\n" "${port}"
  printf "DB=%s\n" "${db}"
  printf "WAREHOUSE=%s\n" "${WAREHOUSE}"
  printf "IO_IMPL=%s\n" "${IO_IMPL}"
  printf "REST_AUTH_SECRET=%s\n" "${REST_SECRET_NAME}"

  if [[ "${STORAGE_PROVIDER}" == "aws" ]]; then
    printf "S3_BUCKET=%s\n" "${S3_BUCKET}"
    printf "S3_PREFIX=%s\n" "${S3_PREFIX}"
    printf "AWS_REGION=%s\n" "${AWS_REGION}"
  fi

  printf "\nLocal access via kubectl port-forward:\n"
  printf "kubectl -n %s port-forward svc/%s %s:%s\n" "${TARGET_NS}" "${pooler_svc}" "${local_pf_port}" "${port}"
  printf "psql 'postgresql://%s:%s@127.0.0.1:%s/%s'\n" "${user}" "${pw}" "${local_pf_port}" "${db}"
  printf "JDBC_URI_LOCAL=jdbc:postgresql://127.0.0.1:%s/%s\n" "${local_pf_port}" "${db}"

  printf "\nIn-cluster access:\n"
  printf "Same namespace host: %s\n" "${pooler_svc}"
  printf "Cross-namespace host: %s.%s.svc.cluster.local\n" "${pooler_svc}" "${TARGET_NS}"
  printf "JDBC_URI_IN_CLUSTER=%s\n" "${jdbc}"
  printf "psql -h %s -p %s -U %s -d %s\n" "${host}" "${port}" "${user}" "${db}"

  printf "\nExample from inside the cluster:\n"
  printf "kubectl -n %s run --rm -i --restart=Never pgtest --image=postgres:18 --env PGPASSWORD='%s' --env PGUSER='%s' --command -- psql -h %s -p %s -d %s -c 'select current_database();'\n" \
    "${TARGET_NS}" "${pw}" "${user}" "${host}" "${port}" "${db}"

  echo
}

delete_all(){
  kubectl -n "${TARGET_NS}" delete deployment "${DEPLOYMENT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete svc "${SERVICE_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete configmap "${CONFIGMAP_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete sa "${SERVICE_ACCOUNT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${REST_SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete hpa "${DEPLOYMENT_NAME}-hpa" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete pdb "${DEPLOYMENT_NAME}-pdb" --ignore-not-found >/dev/null 2>&1 || true
  log "deleted iceberg resources; preserved object-store data"
}

main_rollout(){
  require_prereqs
  ensure_namespace

  PG_DB="$(pg_detect_db)"
  PG_SECRET_NAME="$(find_pg_secret_name)"
  [[ -n "${PG_SECRET_NAME}" ]] || fatal "CNPG app secret not found; expected label selector '${PG_APP_SECRET_LABEL}'"

  derive_io_and_warehouse

  log "starting iceberg rollout (provider=${K8S_CLUSTER}, ns=${TARGET_NS})"
  log "storage provider=${STORAGE_PROVIDER}, heap mode=${JVM_HEAP_MODE}"

  render_configmap
  render_serviceaccount
  render_deployment_service_pdb

  create_or_update_storage_secret
  create_or_update_rest_auth_secret

  apply_manifests_idempotent

  if [[ "${STORAGE_SECRET_CHANGED}" == "1" || "${REST_SECRET_CHANGED}" == "1" ]]; then
    kubectl -n "${TARGET_NS}" rollout restart deployment "${DEPLOYMENT_NAME}" >/dev/null || true
    log "rolled deployment to pick up secret changes"
  fi

  wait_for_deployment_ready
  wait_for_service

  run_rest_smoke
  run_postgres_smoke

  log "[SUCCESS] iceberg rollout complete"
  print_connection_details
}

case "${1:-}" in
  --rollout) main_rollout ;;
  --delete) delete_all ;;
  --help|-h) printf "Usage: %s [--rollout|--delete]\n" "$0"; exit 0 ;;
  *) main_rollout ;;
esac