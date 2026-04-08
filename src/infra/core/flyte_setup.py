from __future__ import annotations

import base64
import copy
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

ROOT_DIR = Path(__file__).resolve().parents[3]
MANIFEST_DIR = Path(os.environ.get("MANIFEST_DIR", "src/manifests/flyte"))
VALUES_FILE = Path(os.environ.get("VALUES_FILE", str(MANIFEST_DIR / "values.yaml")))

TARGET_NS = os.environ.get("TARGET_NS", "flyte")
POSTGRES_NS = os.environ.get("POSTGRES_NS", "default")
CNPG_CLUSTER = os.environ.get("CNPG_CLUSTER", "postgres-cluster")
POOLER_SERVICE = os.environ.get("POOLER_SERVICE", "postgres-pooler")
DB_ACCESS_MODE = os.environ.get("DB_ACCESS_MODE", "rw")
DB_HOST = os.environ.get("DB_HOST", "")
POOLER_PORT = int(os.environ.get("POOLER_PORT", "5432"))

DB_SECRET_NAME = os.environ.get("DB_SECRET_NAME", "db-pass")
AUTH_SECRET_NAME = os.environ.get("AUTH_SECRET_NAME", "flyte-secret-auth")

FLYTE_ADMIN_DB = os.environ.get("FLYTE_ADMIN_DB", "flyteadmin")
FLYTE_DATACATALOG_DB = os.environ.get("FLYTE_DATACATALOG_DB", "datacatalog")

FLYTE_OAUTH_CLIENT_ID = os.environ.get("FLYTE_OAUTH_CLIENT_ID", "flytepropeller")
FLYTE_OAUTH_CLIENT_SECRET = os.environ.get("FLYTE_OAUTH_CLIENT_SECRET", "flytepropeller-secret")

USE_IAM = os.environ.get("USE_IAM", "false").lower() in {"1", "true", "yes", "y", "on"}
FLYTE_STORAGE_TYPE = os.environ.get("FLYTE_STORAGE_TYPE", "s3").strip().lower()

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "e2e-mlops-data-681802563986")
S3_PREFIX = os.environ.get("S3_PREFIX", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "")
AWS_ROLE_ARN = os.environ.get("AWS_ROLE_ARN", "")

FLYTE_INGRESS_ENABLED = os.environ.get("FLYTE_INGRESS_ENABLED", "false")
FLYTE_INGRESS_CLASS_NAME = os.environ.get("FLYTE_INGRESS_CLASS_NAME", "")
FLYTE_SEPARATE_GRPC_INGRESS = os.environ.get("FLYTE_SEPARATE_GRPC_INGRESS", "false")

FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED = os.environ.get("FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED", "true")
FLYTE_WORKFLOW_SCHEDULER_ENABLED = os.environ.get("FLYTE_WORKFLOW_SCHEDULER_ENABLED", "false")
FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED = os.environ.get("FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED", "false")
FLYTE_EXTERNAL_EVENTS_ENABLED = os.environ.get("FLYTE_EXTERNAL_EVENTS_ENABLED", "false")
FLYTE_CLOUD_EVENTS_ENABLED = os.environ.get("FLYTE_CLOUD_EVENTS_ENABLED", "false")
FLYTE_CONNECTOR_ENABLED = os.environ.get("FLYTE_CONNECTOR_ENABLED", "false")
FLYTE_SPARK_OPERATOR_ENABLED = os.environ.get("FLYTE_SPARK_OPERATOR_ENABLED", "false")
FLYTE_DASK_OPERATOR_ENABLED = os.environ.get("FLYTE_DASK_OPERATOR_ENABLED", "false")
FLYTE_DATABRICKS_ENABLED = os.environ.get("FLYTE_DATABRICKS_ENABLED", "false")

CHART_REPO_NAME = os.environ.get("CHART_REPO_NAME", "flyteorg")
CHART_REPO_URL = os.environ.get("CHART_REPO_URL", "https://helm.flyte.org")
CHART_NAME = os.environ.get("CHART_NAME", "flyte-core")
CHART_VERSION = os.environ.get("CHART_VERSION", "1.16.4")
RELEASE_NAME = os.environ.get("RELEASE_NAME", "flyte")

READY_TIMEOUT = os.environ.get("READY_TIMEOUT", "1200")
ROLLOUT_TIMEOUT = os.environ.get("ROLLOUT_TIMEOUT", "1200s")
FLYTE_ATOMIC = os.environ.get("FLYTE_ATOMIC", "false").lower() in {"1", "true", "yes", "y", "on"}

FLYTE_TASK_NAMESPACES = os.environ.get("FLYTE_TASK_NAMESPACES", "flytesnacks-development")
TASK_AWS_SECRET_NAME = os.environ.get("TASK_AWS_SECRET_NAME", "flyte-aws-credentials")

APP_DB_USER = ""
APP_DB_PASSWORD = ""
CONTROL_PLANE_AWS_ANNOTATION_KEY = ""
CONTROL_PLANE_AWS_ANNOTATION_VALUE = ""


def ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(f"[{ts()}] [flyte] {msg}", file=sys.stderr, flush=True)


def fatal(msg: str) -> None:
    print(f"[{ts()}] [flyte][FATAL] {msg}", file=sys.stderr, flush=True)
    raise SystemExit(1)


def require_bin(name: str) -> None:
    from shutil import which

    if which(name) is None:
        fatal(f"{name} required in PATH")


def run(cmd: list[str], *, input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    cp = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if check and cp.returncode != 0:
        detail: list[str] = []
        if cp.stdout:
            detail.append(f"stdout:\n{cp.stdout.rstrip()}")
        if cp.stderr:
            detail.append(f"stderr:\n{cp.stderr.rstrip()}")
        suffix = f"\n{chr(10).join(detail)}" if detail else ""
        raise RuntimeError(f"command failed ({cp.returncode}): {' '.join(cmd)}{suffix}")
    return cp


def run_text(cmd: list[str], *, input_text: str | None = None, check: bool = True) -> str:
    return run(cmd, input_text=input_text, check=check).stdout.strip()


def yaml_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "y", "on"}


def split_namespaces(value: str) -> list[str]:
    out: list[str] = []
    for part in value.replace(",", " ").replace(";", " ").split():
        part = part.strip()
        if part:
            out.append(part)
    return out


def apply_manifest(manifest: dict[str, Any]) -> None:
    run(["kubectl", "apply", "-f", "-"], input_text=yaml.safe_dump(manifest, sort_keys=False))


def ensure_namespace(namespace: str) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": namespace,
                "labels": {"app.kubernetes.io/part-of": "flyte"},
            },
        }
    )


def ensure_serviceaccount(namespace: str, name: str, annotations: dict[str, str] | None = None) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "annotations": annotations or {},
            },
        }
    )


def ensure_secret(namespace: str, name: str, string_data: dict[str, str]) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": name,
                "namespace": namespace,
            },
            "type": "Opaque",
            "stringData": string_data,
        }
    )


def secret_value(namespace: str, secret_name: str, key: str) -> str:
    try:
        raw = run_text(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "secret",
                secret_name,
                "-o",
                f"jsonpath={{.data.{key}}}",
            ]
        )
    except subprocess.CalledProcessError:
        return ""

    if not raw:
        return ""
    try:
        return base64.b64decode(raw).decode("utf-8")
    except Exception:
        return ""


def find_app_secret_name() -> str:
    selector = f"cnpg.io/cluster={CNPG_CLUSTER},cnpg.io/userType=app"
    try:
        name = run_text(
            [
                "kubectl",
                "-n",
                POSTGRES_NS,
                "get",
                "secret",
                "-l",
                selector,
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ]
        )
        if name:
            return name
    except subprocess.CalledProcessError:
        pass

    fallback = f"{CNPG_CLUSTER}-app"
    try:
        run_text(["kubectl", "-n", POSTGRES_NS, "get", "secret", fallback])
        return fallback
    except subprocess.CalledProcessError:
        return ""


def get_primary_pod() -> str:
    selector = f"cnpg.io/cluster={CNPG_CLUSTER},cnpg.io/instanceRole=primary"
    try:
        return run_text(
            [
                "kubectl",
                "-n",
                POSTGRES_NS,
                "get",
                "pods",
                "-l",
                selector,
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ]
        )
    except subprocess.CalledProcessError:
        return ""


def resolve_db_host() -> str:
    global DB_HOST
    if DB_HOST:
        return DB_HOST
    if DB_ACCESS_MODE == "rw":
        DB_HOST = f"{CNPG_CLUSTER}-rw.{POSTGRES_NS}.svc.cluster.local"
    elif DB_ACCESS_MODE == "pooler":
        DB_HOST = f"{POOLER_SERVICE}.{POSTGRES_NS}.svc.cluster.local"
    else:
        fatal(f"unsupported DB_ACCESS_MODE={DB_ACCESS_MODE}")
    return DB_HOST


def ensure_database(db_name: str) -> None:
    primary = get_primary_pod()
    if not primary:
        fatal(f"CNPG primary pod not found for {CNPG_CLUSTER}")

    exists = run_text(
        [
            "kubectl",
            "-n",
            POSTGRES_NS,
            "exec",
            primary,
            "--",
            "sh",
            "-lc",
            f"psql -U postgres -tAc \"SELECT 1 FROM pg_database WHERE datname='{db_name}';\"",
        ],
        check=False,
    ).strip()

    if exists != "1":
        log(f"creating database {db_name}")
        run(
            [
                "kubectl",
                "-n",
                POSTGRES_NS,
                "exec",
                primary,
                "--",
                "sh",
                "-lc",
                f'psql -U postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE {db_name} OWNER \\"{APP_DB_USER}\\";"',
            ],
            check=False,
        )
    else:
        log(f"database {db_name} already exists")

    run(
        [
            "kubectl",
            "-n",
            POSTGRES_NS,
            "exec",
            primary,
            "--",
            "sh",
            "-lc",
            f'psql -U postgres -v ON_ERROR_STOP=1 -c "ALTER DATABASE {db_name} OWNER TO \\"{APP_DB_USER}\\";"',
        ],
        check=False,
    )
    run(
        [
            "kubectl",
            "-n",
            POSTGRES_NS,
            "exec",
            primary,
            "--",
            "sh",
            "-lc",
            f'psql -U postgres -d "{db_name}" -v ON_ERROR_STOP=1 -c "ALTER SCHEMA public OWNER TO \\"{APP_DB_USER}\\";"',
        ],
        check=False,
    )


def join_uri_prefix(scheme: str, bucket_or_container: str, prefix: str = "") -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"{scheme}://{bucket_or_container}/{prefix}/"
    return f"{scheme}://{bucket_or_container}/"


def validate_static_aws_credentials() -> None:
    if FLYTE_STORAGE_TYPE != "s3" or USE_IAM:
        return
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        fatal("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when FLYTE_STORAGE_TYPE=s3 and USE_IAM=false")


def aws_secret_data() -> dict[str, str]:
    data = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION": AWS_REGION,
        "AWS_REGION": AWS_REGION,
        "FLYTE_AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "FLYTE_AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "FLYTE_AWS_DEFAULT_REGION": AWS_REGION,
        "FLYTE_AWS_REGION": AWS_REGION,
    }
    if AWS_SESSION_TOKEN:
        data["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
        data["FLYTE_AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
    return data


def ensure_accesskey_secret(namespace: str) -> None:
    ensure_namespace(namespace)
    ensure_secret(namespace, TASK_AWS_SECRET_NAME, aws_secret_data())


def ensure_task_namespace_auth(namespace: str) -> None:
    ensure_namespace(namespace)
    if USE_IAM:
        if not AWS_ROLE_ARN:
            fatal("AWS_ROLE_ARN is required when USE_IAM=true")
        ensure_serviceaccount(namespace, "ray", {"eks.amazonaws.com/role-arn": AWS_ROLE_ARN})
    elif FLYTE_STORAGE_TYPE == "s3":
        ensure_accesskey_secret(namespace)
        ensure_serviceaccount(namespace, "ray")
    else:
        ensure_serviceaccount(namespace, "ray")


def ensure_control_plane_secret() -> None:
    if FLYTE_STORAGE_TYPE == "s3" and not USE_IAM:
        ensure_accesskey_secret(TARGET_NS)


def control_plane_serviceaccount_block() -> dict[str, Any]:
    block = {"create": True, "annotations": {}}
    if FLYTE_STORAGE_TYPE == "s3" and USE_IAM:
        if not AWS_ROLE_ARN:
            fatal("AWS_ROLE_ARN is required when USE_IAM=true")
        block["annotations"] = {"eks.amazonaws.com/role-arn": AWS_ROLE_ARN}
    return block


def build_storage_block() -> dict[str, Any]:
    if FLYTE_STORAGE_TYPE not in {"sandbox", "s3"}:
        fatal(f"unsupported FLYTE_STORAGE_TYPE={FLYTE_STORAGE_TYPE!r}; use sandbox or s3")

    if FLYTE_STORAGE_TYPE == "sandbox":
        return {
            "secretName": "",
            "type": "sandbox",
            "bucketName": S3_BUCKET,
            "s3": {
                "endpoint": "",
                "region": AWS_REGION,
                "authType": "iam",
                "accessKey": "",
                "secretKey": "",
            },
            "custom": {},
            "enableMultiContainer": False,
            "limits": {"maxDownloadMBs": 10},
            "cache": {"maxSizeMBs": 0, "targetGCPercent": 70},
        }

    auth_type = "iam" if USE_IAM else "accesskey"
    return {
        "secretName": "",
        "type": "s3",
        "bucketName": S3_BUCKET,
        "s3": {
            "endpoint": S3_ENDPOINT,
            "region": AWS_REGION,
            "authType": auth_type,
            "accessKey": AWS_ACCESS_KEY_ID if auth_type == "accesskey" else "",
            "secretKey": AWS_SECRET_ACCESS_KEY if auth_type == "accesskey" else "",
        },
        "custom": {},
        "enableMultiContainer": False,
        "limits": {"maxDownloadMBs": 10},
        "cache": {"maxSizeMBs": 0, "targetGCPercent": 70},
    }


def build_values() -> dict[str, Any]:
    storage_type = FLYTE_STORAGE_TYPE
    raw_prefix = os.environ.get("FLYTE_RAWOUTPUT_PREFIX", join_uri_prefix("s3", S3_BUCKET, S3_PREFIX))
    service_account = control_plane_serviceaccount_block()

    task_secret_names = [TASK_AWS_SECRET_NAME] if storage_type == "s3" and not USE_IAM else []

    values: dict[str, Any] = {
        "deployRedoc": False,
        "flyteadmin": {
            "enabled": True,
            "replicaCount": 1,
            "image": {
                "repository": "cr.flyte.org/flyteorg/flyteadmin-release",
                "tag": "v1.16.4",
                "pullPolicy": "IfNotPresent",
            },
            "env": [],
            "serviceAccount": copy.deepcopy(service_account),
            "service": {"type": "ClusterIP"},
            "initialProjects": ["flytesnacks", "flytetester", "flyteexamples"],
        },
        "flytescheduler": {
            "runPrecheck": True,
            "image": {
                "repository": "cr.flyte.org/flyteorg/flytescheduler-release",
                "tag": "v1.16.4",
                "pullPolicy": "IfNotPresent",
            },
            "podEnv": {},
            "serviceAccount": copy.deepcopy(service_account),
            "service": {"enabled": False},
        },
        "datacatalog": {
            "enabled": True,
            "replicaCount": 1,
            "image": {
                "repository": "cr.flyte.org/flyteorg/datacatalog-release",
                "tag": "v1.16.4",
                "pullPolicy": "IfNotPresent",
            },
            "podEnv": {},
            "serviceAccount": copy.deepcopy(service_account),
            "service": {"type": "ClusterIP"},
        },
        "flyteconnector": {"enabled": yaml_bool(FLYTE_CONNECTOR_ENABLED)},
        "flytepropeller": {
            "enabled": True,
            "manager": False,
            "createCRDs": True,
            "replicaCount": 1,
            "image": {
                "repository": "cr.flyte.org/flyteorg/flytepropeller-release",
                "tag": "v1.16.4",
                "pullPolicy": "IfNotPresent",
            },
            "podEnv": {},
            "serviceAccount": copy.deepcopy(service_account),
            "service": {"enabled": False},
        },
        "flyteconsole": {
            "enabled": True,
            "replicaCount": 1,
            "image": {
                "repository": "cr.flyte.org/flyteorg/flyteconsole-release",
                "tag": "v1.16.4",
                "pullPolicy": "IfNotPresent",
            },
            "podEnv": {},
            "serviceAccount": {"create": True, "annotations": {}},
            "service": {"type": "ClusterIP"},
        },
        "webhook": {
            "enabled": True,
            "serviceAccount": {"create": True, "annotations": {}},
            "service": {"type": "ClusterIP"},
            "podEnv": {},
        },
        "common": {
            "databaseSecret": {"name": DB_SECRET_NAME, "secretManifest": {}},
            "ingress": {
                "ingressClassName": FLYTE_INGRESS_CLASS_NAME,
                "enabled": yaml_bool(FLYTE_INGRESS_ENABLED),
                "webpackHMR": False,
                "separateGrpcIngress": yaml_bool(FLYTE_SEPARATE_GRPC_INGRESS),
                "separateGrpcIngressAnnotations": {},
                "annotations": {},
                "albSSLRedirect": False,
                "tls": {"enabled": False},
            },
            "flyteNamespaceTemplate": {"enabled": False},
        },
        "storage": build_storage_block(),
        "db": {
            "datacatalog": {
                "database": {
                    "port": POOLER_PORT,
                    "username": APP_DB_USER,
                    "host": DB_HOST,
                    "dbname": FLYTE_DATACATALOG_DB,
                    "passwordPath": "/etc/db/pass.txt",
                }
            },
            "admin": {
                "database": {
                    "port": POOLER_PORT,
                    "username": APP_DB_USER,
                    "host": DB_HOST,
                    "dbname": FLYTE_ADMIN_DB,
                    "passwordPath": "/etc/db/pass.txt",
                }
            },
        },
        "secrets": {
            "adminOauthClientCredentials": {
                "enabled": True,
                "clientSecret": FLYTE_OAUTH_CLIENT_SECRET,
                "clientId": FLYTE_OAUTH_CLIENT_ID,
                "secretName": AUTH_SECRET_NAME,
            }
        },
        "configmap": {
            "adminServer": {
                "server": {
                    "httpPort": 8088,
                    "grpc": {"port": 8089},
                    "security": {
                        "secure": False,
                        "useAuth": False,
                        "allowCors": True,
                        "allowedOrigins": ["*"],
                        "allowedHeaders": ["Content-Type", "flyte-authorization"],
                    },
                },
                "flyteadmin": {
                    "roleNameKey": "iam.amazonaws.com/role",
                    "profilerPort": 10254,
                    "metricsScope": "flyte:",
                    "metadataStoragePrefix": ["metadata", "admin"],
                    "eventVersion": 2,
                    "injectIdentityAnnotations": False,
                    "identityAnnotationPrefix": "flyte.org",
                    "identityAnnotationKeys": ["email", "sub"],
                    "testing": {"host": "http://flyteadmin"},
                },
                "auth": {
                    "authorizedUris": [
                        "https://localhost:30081",
                        "http://flyteadmin:80",
                        "http://flyteadmin.flyte.svc.cluster.local:80",
                    ]
                },
            },
            "datacatalogServer": {
                "datacatalog": {
                    "storage-prefix": "metadata/datacatalog",
                    "metrics-scope": "flyte",
                    "profiler-port": 10254,
                    "heartbeat-grace-period-multiplier": 3,
                    "max-reservation-heartbeat": "30s",
                },
                "application": {
                    "grpcPort": 8089,
                    "httpPort": 8080,
                    "grpcServerReflection": True,
                    "grpcMaxRecvMsgSizeMBs": 6,
                },
            },
            "task_resource_defaults": {
                "task_resources": {
                    "defaults": {"cpu": "500m", "memory": "1500Mi"},
                    "limits": {"cpu": "6", "memory": "6Gi", "gpu": 1},
                }
            },
            "admin": {
                "event": {"type": "admin", "rate": 500, "capacity": 1000},
                "admin": {
                    "endpoint": "flyteadmin:81",
                    "insecure": True,
                    "clientId": "{{ .Values.secrets.adminOauthClientCredentials.clientId }}",
                    "clientSecretLocation": "/etc/secrets/client_secret",
                },
            },
            "catalog": {
                "catalog-cache": {
                    "endpoint": "datacatalog:89",
                    "type": "datacatalog",
                    "insecure": True,
                }
            },
            "core": {
                "manager": {
                    "pod-application": "flytepropeller",
                    "pod-template-container-name": "flytepropeller",
                    "pod-template-name": "flytepropeller-template",
                },
                "propeller": {
                    "rawoutput-prefix": raw_prefix,
                    "metadata-prefix": "metadata/propeller",
                    "workers": 4,
                    "max-workflow-retries": 30,
                    "workflow-reeval-duration": "30s",
                    "downstream-eval-duration": "30s",
                    "limit-namespace": "all",
                    "prof-port": 10254,
                    "metrics-prefix": "flyte",
                    "enable-admin-launcher": True,
                    "leader-election": {
                        "lock-config-map": {"name": "propeller-leader", "namespace": TARGET_NS},
                        "enabled": True,
                        "lease-duration": "15s",
                        "renew-deadline": "10s",
                        "retry-period": "2s",
                    },
                },
                "webhook": {
                    "certDir": "/etc/webhook/certs",
                    "serviceName": "flyte-pod-webhook",
                },
            },
            "enabled_plugins": {
                "tasks": {
                    "task-plugins": {
                        "enabled-plugins": [
                            "container",
                            "sidecar",
                            "k8s-array",
                            "connector-service",
                            "echo",
                        ],
                        "default-for-task-types": {
                            "container": "container",
                            "sidecar": "sidecar",
                            "container_array": "k8s-array",
                        },
                    }
                }
            },
            "k8s": {
                "plugins": {
                    "k8s": {
                        "default-env-from-configmaps": [],
                        "default-env-from-secrets": task_secret_names,
                        "default-env-vars": {
                            "AWS_DEFAULT_REGION": AWS_REGION,
                            "AWS_REGION": AWS_REGION,
                        },
                        "default-cpus": "100m",
                        "default-memory": "200Mi",
                    }
                }
            },
            "remoteData": {
                "remoteData": {
                    "region": AWS_REGION,
                    "scheme": "s3" if storage_type == "s3" else "local",
                    "signedUrls": {"durationMinutes": 3},
                }
            },
            "resource_manager": {"propeller": {"resourcemanager": {"type": "noop"}}},
            "task_logs": {
                "plugins": {
                    "logs": {
                        "kubernetes-enabled": True,
                        "cloudwatch-enabled": False,
                    }
                }
            },
        },
        "workflow_scheduler": {"enabled": yaml_bool(FLYTE_WORKFLOW_SCHEDULER_ENABLED)},
        "workflow_notifications": {"enabled": yaml_bool(FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED)},
        "external_events": {"enable": yaml_bool(FLYTE_EXTERNAL_EVENTS_ENABLED)},
        "cloud_events": {"enable": yaml_bool(FLYTE_CLOUD_EVENTS_ENABLED)},
        "cluster_resource_manager": {"enabled": yaml_bool(FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED)},
        "sparkoperator": {"enabled": yaml_bool(FLYTE_SPARK_OPERATOR_ENABLED)},
        "daskoperator": {"enabled": yaml_bool(FLYTE_DASK_OPERATOR_ENABLED)},
        "databricks": {"enabled": yaml_bool(FLYTE_DATABRICKS_ENABLED)},
    }
    return values


def render_values_file(values: dict[str, Any]) -> None:
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    with VALUES_FILE.open("w", encoding="utf-8") as f:
        yaml.safe_dump(values, f, sort_keys=False, default_flow_style=False, width=120)


def validate_rendered_values() -> None:
    with VALUES_FILE.open("r", encoding="utf-8") as f:
        yaml.safe_load(f)
    run(
        [
            "helm",
            "template",
            RELEASE_NAME,
            f"{CHART_REPO_NAME}/{CHART_NAME}",
            "--version",
            CHART_VERSION,
            "-n",
            TARGET_NS,
            "-f",
            str(VALUES_FILE),
        ]
    )


def require_prereqs() -> None:
    require_bin("kubectl")
    require_bin("helm")
    require_bin("python3")
    run(["kubectl", "cluster-info"])


def ensure_release_namespace() -> None:
    ensure_namespace(TARGET_NS)


def ensure_task_namespaces_ready() -> None:
    for namespace in split_namespaces(FLYTE_TASK_NAMESPACES):
        ensure_task_namespace_auth(namespace)


def print_summary() -> None:
    log("helm release status")
    run(["helm", "status", RELEASE_NAME, "-n", TARGET_NS], check=False)
    log("workloads")
    run(["kubectl", "-n", TARGET_NS, "get", "deploy", "-o", "wide"], check=False)
    run(["kubectl", "-n", TARGET_NS, "get", "svc", "-o", "wide"], check=False)
    print()
    print(f"Flyte namespace: {TARGET_NS}")
    print(f"Postgres namespace: {POSTGRES_NS}")
    print(f"Flyte admin DB: {FLYTE_ADMIN_DB}")
    print(f"Datacatalog DB: {FLYTE_DATACATALOG_DB}")
    print(f"DB host: {DB_HOST}")
    print(f"DB access mode: {DB_ACCESS_MODE}")
    print(f"Rendered values file: {VALUES_FILE}")
    print(f"Task AWS secret name: {TASK_AWS_SECRET_NAME}")
    print(f"Task namespaces: {FLYTE_TASK_NAMESPACES}")
    print()
    print(f"Local access:\nkubectl -n {TARGET_NS} port-forward svc/flyteadmin 30080:80")


def dump_diagnostics() -> None:
    log(f"diagnostics namespace={TARGET_NS}")
    run(["kubectl", "-n", TARGET_NS, "get", "pods", "-o", "wide"], check=False)
    run(["kubectl", "-n", TARGET_NS, "get", "svc", "-o", "wide"], check=False)
    run(["kubectl", "-n", TARGET_NS, "get", "events", "--sort-by=.lastTimestamp"], check=False)


def wait_for_rollouts() -> None:
    try:
        deploys = run_text(
            [
                "kubectl",
                "-n",
                TARGET_NS,
                "get",
                "deploy",
                "-o",
                'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}',
            ]
        )
    except subprocess.CalledProcessError:
        deploys = ""

    for dep in [line.strip() for line in deploys.splitlines() if line.strip()]:
        log(f"waiting for deployment {dep}")
        run(
            [
                "kubectl",
                "-n",
                TARGET_NS,
                "rollout",
                "status",
                f"deployment/{dep}",
                f"--timeout={ROLLOUT_TIMEOUT}",
            ]
        )


def main() -> None:
    global APP_DB_USER, APP_DB_PASSWORD, DB_HOST, POOLER_PORT
    global CONTROL_PLANE_AWS_ANNOTATION_KEY, CONTROL_PLANE_AWS_ANNOTATION_VALUE

    require_prereqs()
    ensure_release_namespace()
    validate_static_aws_credentials()

    app_secret = find_app_secret_name()
    if not app_secret:
        fatal(f"CNPG app secret not found for cluster {CNPG_CLUSTER}")

    app_user = secret_value(POSTGRES_NS, app_secret, "username")
    app_password = secret_value(POSTGRES_NS, app_secret, "password")
    app_port = secret_value(POSTGRES_NS, app_secret, "port")

    if not app_user:
        fatal(f"username missing from CNPG app secret {app_secret}")
    if not app_password:
        fatal(f"password missing from CNPG app secret {app_secret}")

    APP_DB_USER = app_user
    APP_DB_PASSWORD = app_password
    if app_port:
        try:
            POOLER_PORT = int(app_port)
        except ValueError:
            pass

    resolve_db_host()

    if FLYTE_STORAGE_TYPE == "s3" and USE_IAM:
        if not AWS_ROLE_ARN:
            fatal("AWS_ROLE_ARN is required when USE_IAM=true")
        CONTROL_PLANE_AWS_ANNOTATION_KEY = "eks.amazonaws.com/role-arn"
        CONTROL_PLANE_AWS_ANNOTATION_VALUE = AWS_ROLE_ARN
    else:
        CONTROL_PLANE_AWS_ANNOTATION_KEY = ""
        CONTROL_PLANE_AWS_ANNOTATION_VALUE = ""

    ensure_database(FLYTE_ADMIN_DB)
    ensure_database(FLYTE_DATACATALOG_DB)

    ensure_secret(TARGET_NS, DB_SECRET_NAME, {"pass.txt": APP_DB_PASSWORD})

    if FLYTE_STORAGE_TYPE == "s3" and not USE_IAM:
        ensure_control_plane_secret()

    ensure_task_namespaces_ready()

    run(["helm", "repo", "add", CHART_REPO_NAME, CHART_REPO_URL], check=False)
    run(["helm", "repo", "update"])

    values = build_values()
    render_values_file(values)
    validate_rendered_values()

    helm_args = [
        "helm",
        "upgrade",
        "--install",
        RELEASE_NAME,
        f"{CHART_REPO_NAME}/{CHART_NAME}",
        "--version",
        CHART_VERSION,
        "-n",
        TARGET_NS,
        "-f",
        str(VALUES_FILE),
        "--wait",
        "--timeout",
        f"{READY_TIMEOUT}s",
    ]
    if FLYTE_ATOMIC:
        helm_args.append("--atomic")

    run(helm_args)
    wait_for_rollouts()
    print_summary()


def delete_all() -> None:
    run(["kubectl", "-n", TARGET_NS, "delete", "deployment", RELEASE_NAME, "--ignore-not-found"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "secret", DB_SECRET_NAME, "--ignore-not-found"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "secret", AUTH_SECRET_NAME, "--ignore-not-found"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "secret", TASK_AWS_SECRET_NAME, "--ignore-not-found"], check=False)
    for namespace in split_namespaces(FLYTE_TASK_NAMESPACES):
        run(["kubectl", "-n", namespace, "delete", "secret", TASK_AWS_SECRET_NAME, "--ignore-not-found"], check=False)
        run(["kubectl", "-n", namespace, "delete", "serviceaccount", "ray", "--ignore-not-found"], check=False)
    run(["helm", "uninstall", RELEASE_NAME, "-n", TARGET_NS], check=False)
    log("deleted Flyte release and secrets")


def cli() -> int:
    try:
        if len(sys.argv) == 1 or sys.argv[1] == "--rollout":
            main()
            return 0
        if sys.argv[1] == "--delete":
            require_prereqs()
            delete_all()
            return 0
        if sys.argv[1] in {"--help", "-h"}:
            print(
                "Usage: flyte_setup.py [--rollout|--delete]\n\n"
                "Environment variables:\n"
                "  DB_ACCESS_MODE=rw|pooler\n"
                "  USE_IAM=true|false\n"
                "  FLYTE_STORAGE_TYPE=sandbox|s3\n"
                "  AWS_REGION=ap-south-1\n"
                "  AWS_ACCESS_KEY_ID=...\n"
                "  AWS_SECRET_ACCESS_KEY=...\n"
                "  AWS_SESSION_TOKEN=...\n"
                "  AWS_ROLE_ARN=...\n"
                "  FLYTE_TASK_NAMESPACES=flytesnacks-development[,other-namespace]\n"
                "  TASK_AWS_SECRET_NAME=flyte-aws-credentials\n"
                "  FLYTE_SPARK_OPERATOR_ENABLED=true|false\n"
                "  FLYTE_ATOMIC=true|false\n"
            )
            return 0
        fatal(f"unknown option: {sys.argv[1]}")
    except subprocess.CalledProcessError as exc:
        print(f"[{ts()}] [flyte][FATAL] command failed: {exc.cmd}", file=sys.stderr, flush=True)
        try:
            dump_diagnostics()
        except Exception:
            pass
        return exc.returncode
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[{ts()}] [flyte][FATAL] {exc}", file=sys.stderr, flush=True)
        try:
            dump_diagnostics()
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())