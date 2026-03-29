#!/usr/bin/env python3
"""
Bootstraps Flyte on Kubernetes and renders the Helm values file with PyYAML.

Goals:
- No YAML string stitching.
- Correct chart structure.
- Correct Python types: maps stay maps, lists stay lists.
- Silent kubectl helpers: no manifest spam on stdout.
- CNPG secret values decoded correctly.
- Helm template validation before install.
"""

from __future__ import annotations

import base64
import copy
import os
import subprocess
import sys
from datetime import datetime, timezone
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

STORAGE_PROVIDER = os.environ.get("STORAGE_PROVIDER", "auto")
USE_IAM = os.environ.get("USE_IAM", "false").lower() in {"1", "true", "yes", "y", "on"}

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "e2e-mlops-data-681802563986")
S3_PREFIX = os.environ.get("S3_PREFIX", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "")
AWS_ROLE_ARN = os.environ.get("AWS_ROLE_ARN", "")

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
GCP_BUCKET = os.environ.get("GCP_BUCKET", "mlops_iceberg_warehouse")
GCP_SA_EMAIL = os.environ.get("GCP_SA_EMAIL", "")
GCP_SERVICE_ACCOUNT_KEY = os.environ.get("GCP_SERVICE_ACCOUNT_KEY", "")

AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "")
AZURE_CONTAINER = os.environ.get("AZURE_CONTAINER", "iceberg")
AZURE_CLIENT_ID = os.environ.get("AZURE_CLIENT_ID", "")
AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID", "")
FLYTE_STORAGE_CUSTOM_YAML = os.environ.get("FLYTE_STORAGE_CUSTOM_YAML", "")

FLYTE_INGRESS_ENABLED = os.environ.get("FLYTE_INGRESS_ENABLED", "false")
FLYTE_INGRESS_CLASS_NAME = os.environ.get("FLYTE_INGRESS_CLASS_NAME", "")
FLYTE_SEPARATE_GRPC_INGRESS = os.environ.get("FLYTE_SEPARATE_GRPC_INGRESS", "false")

FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED = os.environ.get("FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED", "true")
FLYTE_WORKFLOW_SCHEDULER_ENABLED = os.environ.get("FLYTE_WORKFLOW_SCHEDULER_ENABLED", "false")
FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED = os.environ.get("FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED", "false")
FLYTE_EXTERNAL_EVENTS_ENABLED = os.environ.get("FLYTE_EXTERNAL_EVENTS_ENABLED", "false")
FLYTE_CLOUD_EVENTS_ENABLED = os.environ.get("FLYTE_CLOUD_EVENTS_ENABLED", "false")
FLYTE_CONNECTOR_ENABLED = os.environ.get("FLYTE_CONNECTOR_ENABLED", "false")
FLYTE_SPARK_OPERATOR_ENABLED = os.environ.get("FLYTE_SPARK_OPERATOR_ENABLED", "true")
FLYTE_DASK_OPERATOR_ENABLED = os.environ.get("FLYTE_DASK_OPERATOR_ENABLED", "false")
FLYTE_DATABRICKS_ENABLED = os.environ.get("FLYTE_DATABRICKS_ENABLED", "false")

CHART_REPO_NAME = os.environ.get("CHART_REPO_NAME", "flyte")
CHART_REPO_URL = os.environ.get("CHART_REPO_URL", "https://flyteorg.github.io/flyte")
CHART_NAME = os.environ.get("CHART_NAME", "flyte-core")
CHART_VERSION = os.environ.get("CHART_VERSION", "1.16.4")
RELEASE_NAME = os.environ.get("RELEASE_NAME", "flyte")

READY_TIMEOUT = os.environ.get("READY_TIMEOUT", "1200")
ROLLOUT_TIMEOUT = os.environ.get("ROLLOUT_TIMEOUT", "1200s")
FLYTE_ATOMIC = os.environ.get("FLYTE_ATOMIC", "false").lower() in {"1", "true", "yes", "y", "on"}

FLYTE_TASK_NAMESPACES = os.environ.get("FLYTE_TASK_NAMESPACES", "flytesnacks-development")
TASK_AWS_SECRET_NAME = os.environ.get("TASK_AWS_SECRET_NAME", "flyte-aws-credentials")
STORAGE_SECRET_NAME = os.environ.get("STORAGE_SECRET_NAME", "flyte-storage-config")

APP_DB_USER = ""
APP_DB_PASSWORD = ""
SERVICE_ANNOTATION_KEY = ""
SERVICE_ANNOTATION_VALUE = ""


def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(f"[{ts()}] [flyte] {msg}", file=sys.stderr)


def fatal(msg: str) -> None:
    print(f"[{ts()}] [flyte][FATAL] {msg}", file=sys.stderr)
    raise SystemExit(1)


def require_bin(name: str) -> None:
    from shutil import which

    if which(name) is None:
        fatal(f"{name} required in PATH")


def run(cmd: list[str], *, input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        check=check,
    )


def run_text(cmd: list[str], *, input_text: str | None = None, check: bool = True) -> str:
    res = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        check=check,
    )
    return res.stdout.strip()


def yaml_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "y", "on"}


def detect_storage_provider() -> str:
    if STORAGE_PROVIDER in {"aws", "gcs", "azure"}:
        return STORAGE_PROVIDER
    if AZURE_STORAGE_ACCOUNT or AZURE_CLIENT_ID or AZURE_TENANT_ID:
        return "azure"
    if GCP_PROJECT or GCP_SA_EMAIL:
        return "gcs"
    return "aws"


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
    manifest = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {
                "app.kubernetes.io/part-of": "flyte",
            },
        },
    }
    apply_manifest(manifest)


def ensure_default_serviceaccount(namespace: str) -> None:
    manifest = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "default",
            "namespace": namespace,
        },
    }
    apply_manifest(manifest)


def patch_default_serviceaccount_annotation(namespace: str, key: str, value: str) -> None:
    if not key or not value:
        return
    ensure_default_serviceaccount(namespace)
    run(
        [
            "kubectl",
            "-n",
            namespace,
            "annotate",
            "sa",
            "default",
            f"{key}={value}",
            "--overwrite",
        ],
        check=True,
    )


def ensure_secret(namespace: str, name: str, string_data: dict[str, str]) -> None:
    manifest = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "type": "Opaque",
        "stringData": string_data,
    }
    apply_manifest(manifest)


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

    try:
        run_text(["kubectl", "-n", POSTGRES_NS, "get", "secret", f"{CNPG_CLUSTER}-app"])
        return f"{CNPG_CLUSTER}-app"
    except subprocess.CalledProcessError:
        return ""


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
        log(f"creating database {db_name} if not exist")
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
                f"psql -U postgres -v ON_ERROR_STOP=1 -c \"CREATE DATABASE {db_name} OWNER \\\"{APP_DB_USER}\\\";\"",
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
            f"psql -U postgres -v ON_ERROR_STOP=1 -c \"ALTER DATABASE {db_name} OWNER TO \\\"{APP_DB_USER}\\\";\"",
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
            f"psql -U postgres -d \"{db_name}\" -v ON_ERROR_STOP=1 -c \"ALTER SCHEMA public OWNER TO \\\"{APP_DB_USER}\\\";\"",
        ],
        check=False,
    )


def join_uri_prefix(scheme: str, bucket_or_container: str, prefix: str = "") -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"{scheme}://{bucket_or_container}/{prefix}/"
    return f"{scheme}://{bucket_or_container}/"


def identity_annotation(provider: str) -> tuple[str, str]:
    if provider == "aws" and USE_IAM:
        if not AWS_ROLE_ARN:
            fatal("AWS_ROLE_ARN is required when USE_IAM=true")
        return "eks.amazonaws.com/role-arn", AWS_ROLE_ARN
    if provider == "gcs" and GCP_SA_EMAIL:
        return "iam.gke.io/gcp-service-account", GCP_SA_EMAIL
    if provider == "azure" and AZURE_CLIENT_ID:
        return "azure.workload.identity/client-id", AZURE_CLIENT_ID
    return "", ""


def build_storage_block(provider: str) -> dict[str, Any]:
    if provider == "aws":
        auth_type = "iam" if USE_IAM else "accesskey"
        return {
            "secretName": "",
            "type": "s3",
            "bucketName": S3_BUCKET,
            "s3": {
                "endpoint": S3_ENDPOINT,
                "region": AWS_REGION,
                "authType": auth_type,
                "accessKey": AWS_ACCESS_KEY_ID if not USE_IAM else "",
                "secretKey": AWS_SECRET_ACCESS_KEY if not USE_IAM else "",
            },
            "gcs": {"projectId": ""},
            "custom": {},
            "enableMultiContainer": False,
            "limits": {"maxDownloadMBs": 10},
            "cache": {"maxSizeMBs": 0, "targetGCPercent": 70},
        }

    if provider == "gcs":
        return {
            "secretName": "",
            "type": "gcs",
            "bucketName": GCP_BUCKET,
            "s3": {
                "endpoint": "",
                "region": AWS_REGION,
                "authType": "iam",
                "accessKey": "",
                "secretKey": "",
            },
            "gcs": {
                "projectId": GCP_PROJECT,
                "serviceAccountKey": GCP_SERVICE_ACCOUNT_KEY,
            },
            "custom": {},
            "enableMultiContainer": False,
            "limits": {"maxDownloadMBs": 10},
            "cache": {"maxSizeMBs": 0, "targetGCPercent": 70},
        }

    if provider == "azure":
        if not FLYTE_STORAGE_CUSTOM_YAML:
            fatal("FLYTE_STORAGE_CUSTOM_YAML is required for azure")
        try:
            custom = yaml.safe_load(FLYTE_STORAGE_CUSTOM_YAML)
        except yaml.YAMLError as exc:
            fatal(f"FLYTE_STORAGE_CUSTOM_YAML is invalid YAML: {exc}")
        if custom is None:
            custom = {}
        if not isinstance(custom, dict):
            fatal("FLYTE_STORAGE_CUSTOM_YAML must parse to a mapping/object")
        return {
            "secretName": "",
            "type": "custom",
            "bucketName": AZURE_CONTAINER,
            "s3": {
                "endpoint": "",
                "region": AWS_REGION,
                "authType": "iam",
                "accessKey": "",
                "secretKey": "",
            },
            "gcs": {"projectId": ""},
            "custom": custom,
            "enableMultiContainer": False,
            "limits": {"maxDownloadMBs": 10},
            "cache": {"maxSizeMBs": 0, "targetGCPercent": 70},
        }

    fatal(f"unsupported storage provider {provider}")
    raise AssertionError("unreachable")


def build_k8s_block(provider: str) -> dict[str, Any]:
    default_env_vars: dict[str, str] = {}
    default_env_from_secrets: list[str] = []

    if provider == "aws" and not USE_IAM:
        default_env_from_secrets = [TASK_AWS_SECRET_NAME]
        default_env_vars = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "AWS_REGION": AWS_REGION,
            "FLYTE_AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "FLYTE_AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        }
        if AWS_SESSION_TOKEN:
            default_env_vars["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
        if S3_ENDPOINT:
            default_env_vars["FLYTE_AWS_ENDPOINT"] = S3_ENDPOINT

    return {
        "plugins": {
            "k8s": {
                "default-cpus": "100m",
                "default-memory": "200Mi",
                "default-env-from-configmaps": [],
                "default-env-from-secrets": default_env_from_secrets,
                "default-env-vars": default_env_vars,
            }
        }
    }


def build_values() -> dict[str, Any]:
    provider = detect_storage_provider()
    global SERVICE_ANNOTATION_KEY, SERVICE_ANNOTATION_VALUE
    SERVICE_ANNOTATION_KEY, SERVICE_ANNOTATION_VALUE = identity_annotation(provider)

    service_account = {
        "create": True,
        "annotations": {SERVICE_ANNOTATION_KEY: SERVICE_ANNOTATION_VALUE} if SERVICE_ANNOTATION_KEY and SERVICE_ANNOTATION_VALUE else {},
    }

    if provider == "aws":
        raw_prefix = os.environ.get("FLYTE_RAWOUTPUT_PREFIX", join_uri_prefix("s3", S3_BUCKET, S3_PREFIX))
        remote_scheme = "s3"
    elif provider == "gcs":
        raw_prefix = os.environ.get("FLYTE_RAWOUTPUT_PREFIX", join_uri_prefix("gs", GCP_BUCKET))
        remote_scheme = "gs"
    else:
        raw_prefix = os.environ.get(
            "FLYTE_RAWOUTPUT_PREFIX",
            join_uri_prefix("abfs", f"{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"),
        )
        remote_scheme = "abfs"

    values: dict[str, Any] = {
        "deployRedoc": False,
        "flyteadmin": {
            "enabled": True,
            "replicaCount": 1,
            "serviceAccount": copy.deepcopy(service_account),
            "service": {"type": "ClusterIP"},
        },
        "flytescheduler": {
            "runPrecheck": True,
            "serviceAccount": copy.deepcopy(service_account),
            "service": {"enabled": False},
        },
        "datacatalog": {
            "enabled": True,
            "replicaCount": 1,
            "serviceAccount": copy.deepcopy(service_account),
            "service": {"type": "ClusterIP"},
        },
        "flyteconnector": {"enabled": yaml_bool(FLYTE_CONNECTOR_ENABLED)},
        "flytepropeller": {
            "enabled": True,
            "manager": False,
            "createCRDs": True,
            "replicaCount": 1,
            "serviceAccount": copy.deepcopy(service_account),
            "service": {"enabled": False},
        },
        "flyteconsole": {
            "enabled": True,
            "replicaCount": 1,
            "serviceAccount": {
                "create": True,
                "annotations": {},
            },
            "service": {"type": "ClusterIP"},
        },
        "webhook": {
            "enabled": True,
            "serviceAccount": {
                "create": True,
                "annotations": {},
            },
            "service": {"type": "ClusterIP"},
        },
        "common": {
            "databaseSecret": {
                "name": DB_SECRET_NAME,
                "secretManifest": {},
            },
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
        "storage": build_storage_block(provider),
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
            "core": {
                "propeller": {
                    "rawoutput-prefix": raw_prefix,
                    "metadata-prefix": "metadata/propeller",
                    "workers": 4,
                    "max-workflow-retries": 30,
                    "workflow-reeval-duration": "30s",
                    "downstream-eval-duration": "30s",
                    "limit-namespace": "all",
                    "leader-election": {
                        "enabled": True,
                        "lock-config-map": {
                            "name": "propeller-leader",
                            "namespace": TARGET_NS,
                        },
                        "lease-duration": "15s",
                        "renew-deadline": "10s",
                        "retry-period": "2s",
                    },
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
                            "spark",
                        ],
                        "default-for-task-types": {
                            "container": "container",
                            "sidecar": "sidecar",
                            "container_array": "k8s-array",
                            "spark": "spark",
                        },
                    }
                }
            },
            "k8s": build_k8s_block(provider),
            "remoteData": {
                "remoteData": {
                    "region": AWS_REGION,
                    "scheme": remote_scheme,
                    "signedUrls": {"durationMinutes": 3},
                }
            },
            "resource_manager": {
                "propeller": {
                    "resourcemanager": {"type": "noop"}
                }
            },
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
        "sparkoperator": {
            "enabled": yaml_bool(FLYTE_SPARK_OPERATOR_ENABLED),
            "plugin_config": {
                "plugins": {
                    "spark": {
                        "spark-config-default": (
                            [
                                {"spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"},
                                {"spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"},
                                {"spark.kubernetes.allocation.batch.size": "50"},
                                {"spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl"},
                                {"spark.hadoop.fs.s3n.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"},
                                {"spark.hadoop.fs.AbstractFileSystem.s3n.impl": "org.apache.hadoop.fs.s3a.S3A"},
                                {"spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"},
                                {"spark.hadoop.fs.AbstractFileSystem.s3.impl": "org.apache.hadoop.fs.s3a.S3A"},
                                {"spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"},
                                {"spark.hadoop.fs.AbstractFileSystem.s3a.impl": "org.apache.hadoop.fs.s3a.S3A"},
                                {"spark.hadoop.fs.s3a.multipart.threshold": "536870912"},
                                {"spark.blacklist.enabled": "true"},
                                {"spark.blacklist.timeout": "5m"},
                                {"spark.task.maxfailures": "8"},
                            ]
                            if provider == "aws"
                            else []
                        )
                    }
                }
            },
        },
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
        ],
        check=True,
    )


def require_prereqs() -> None:
    require_bin("kubectl")
    require_bin("helm")
    require_bin("python3")
    run(["kubectl", "cluster-info"], check=True)


def ensure_release_namespace() -> None:
    ensure_namespace(TARGET_NS)


def ensure_task_aws_secret(namespace: str) -> None:
    if USE_IAM:
        return
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        fatal("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when USE_IAM=false")

    ensure_namespace(namespace)
    data = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION": AWS_REGION,
        "AWS_REGION": AWS_REGION,
        "FLYTE_AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "FLYTE_AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    }
    if AWS_SESSION_TOKEN:
        data["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
    if S3_ENDPOINT:
        data["FLYTE_AWS_ENDPOINT"] = S3_ENDPOINT
    ensure_secret(namespace, TASK_AWS_SECRET_NAME, data)


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
                "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
            ]
        )
    except subprocess.CalledProcessError:
        deploys = ""

    for dep in [line.strip() for line in deploys.splitlines() if line.strip()]:
        log(f"waiting for deployment {dep}")
        run(["kubectl", "-n", TARGET_NS, "rollout", "status", f"deployment/{dep}", f"--timeout={ROLLOUT_TIMEOUT}"], check=True)


def main() -> None:
    global APP_DB_USER, APP_DB_PASSWORD, DB_HOST, POOLER_PORT

    require_prereqs()
    ensure_release_namespace()

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

    provider = detect_storage_provider()

    if provider == "aws":
        if USE_IAM:
            if not AWS_ROLE_ARN:
                fatal("AWS_ROLE_ARN is required when USE_IAM=true")
            global SERVICE_ANNOTATION_KEY, SERVICE_ANNOTATION_VALUE
            SERVICE_ANNOTATION_KEY, SERVICE_ANNOTATION_VALUE = "eks.amazonaws.com/role-arn", AWS_ROLE_ARN
        else:
            if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
                fatal("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when USE_IAM=false")
            SERVICE_ANNOTATION_KEY = ""
            SERVICE_ANNOTATION_VALUE = ""
    elif provider == "gcs":
        if not GCP_PROJECT:
            fatal("GCP_PROJECT is required for gcs")
        if GCP_SA_EMAIL:
            SERVICE_ANNOTATION_KEY, SERVICE_ANNOTATION_VALUE = "iam.gke.io/gcp-service-account", GCP_SA_EMAIL
        else:
            SERVICE_ANNOTATION_KEY = ""
            SERVICE_ANNOTATION_VALUE = ""
    elif provider == "azure":
        if not AZURE_STORAGE_ACCOUNT:
            fatal("AZURE_STORAGE_ACCOUNT is required for azure")
        if not FLYTE_STORAGE_CUSTOM_YAML:
            fatal("FLYTE_STORAGE_CUSTOM_YAML is required for azure")
        if AZURE_CLIENT_ID:
            SERVICE_ANNOTATION_KEY, SERVICE_ANNOTATION_VALUE = "azure.workload.identity/client-id", AZURE_CLIENT_ID
        else:
            SERVICE_ANNOTATION_KEY = ""
            SERVICE_ANNOTATION_VALUE = ""
    else:
        fatal(f"unsupported storage provider {provider}")

    ensure_database(FLYTE_ADMIN_DB)
    ensure_database(FLYTE_DATACATALOG_DB)

    ensure_secret(TARGET_NS, DB_SECRET_NAME, {"pass.txt": APP_DB_PASSWORD})
    for namespace in split_namespaces(FLYTE_TASK_NAMESPACES):
        ensure_namespace(namespace)
        if provider == "aws" and USE_IAM and AWS_ROLE_ARN:
            patch_default_serviceaccount_annotation(namespace, "eks.amazonaws.com/role-arn", AWS_ROLE_ARN)
        elif provider == "gcs" and GCP_SA_EMAIL:
            patch_default_serviceaccount_annotation(namespace, "iam.gke.io/gcp-service-account", GCP_SA_EMAIL)
        elif provider == "azure" and AZURE_CLIENT_ID:
            patch_default_serviceaccount_annotation(namespace, "azure.workload.identity/client-id", AZURE_CLIENT_ID)

    run(["helm", "repo", "add", CHART_REPO_NAME, CHART_REPO_URL], check=False)
    run(["helm", "repo", "update"], check=True)

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

    run(helm_args, check=True)
    wait_for_rollouts()
    print_summary()


def delete_all() -> None:
    run(["kubectl", "-n", TARGET_NS, "delete", "deployment", RELEASE_NAME, "--ignore-not-found"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "secret", DB_SECRET_NAME, "--ignore-not-found"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "secret", AUTH_SECRET_NAME, "--ignore-not-found"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "secret", STORAGE_SECRET_NAME, "--ignore-not-found"], check=False)
    for namespace in split_namespaces(FLYTE_TASK_NAMESPACES):
        run(["kubectl", "-n", namespace, "delete", "secret", TASK_AWS_SECRET_NAME, "--ignore-not-found"], check=False)
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
                "  STORAGE_PROVIDER=auto|aws|gcs|azure\n"
                "  USE_IAM=true|false\n"
                "  FLYTE_TASK_NAMESPACES=flytesnacks-development[,other-namespace]\n"
                "  TASK_AWS_SECRET_NAME=flyte-aws-credentials\n"
                "  STORAGE_SECRET_NAME=flyte-storage-config\n"
                "  FLYTE_SPARK_OPERATOR_ENABLED=true|false\n"
                "  FLYTE_ATOMIC=true|false\n"
            )
            return 0
        fatal(f"unknown option: {sys.argv[1]}")
    except subprocess.CalledProcessError as exc:
        print(f"[{ts()}] [flyte][FATAL] command failed: {exc.cmd}", file=sys.stderr)
        try:
            dump_diagnostics()
        except Exception:
            pass
        return exc.returncode
    except SystemExit as exc:
        raise
    except Exception as exc:
        print(f"[{ts()}] [flyte][FATAL] {exc}", file=sys.stderr)
        try:
            dump_diagnostics()
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())