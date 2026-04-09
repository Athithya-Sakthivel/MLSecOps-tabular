from __future__ import annotations

# Single-source lifecycle tool for the RayService deployment.
# Renders manifests from env, computes a stable hash, and applies only when the
# effective configuration changes.
# Secrets are rendered and applied inline; no separate secrets.yaml is used.
import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

MANIFEST_DIR = Path("src/manifests/kuberay_rayservice")
STATE_DIR = MANIFEST_DIR / ".state"
RENDERED_PATH = STATE_DIR / "rendered.yaml"
HASH_PATH = STATE_DIR / "rendered.sha256"
MANIFEST_HASH_ANNOTATION = "inference-service.io/rendered-sha256"

# Deployment-level defaults. These are intentionally separate from the 32 app env vars.
DEPLOYMENT_DEFAULTS = {
    "NAMESPACE": "inference",
    "RAYSERVICE_NAME": "tabular-inference",
    "SERVICE_ACCOUNT_NAME": "ray-inference-sa",
    "AWS_SECRET_NAME": "aws-credentials",
    "RAY_IMAGE": "ghcr.io/YOUR_ORG/YOUR_IMAGE:YOUR_TAG",
    "RAY_VERSION": "2.54.1",
    "HEAD_CPU": "1",
    "HEAD_MEMORY": "2Gi",
    "WORKER_CPU": "1",
    "WORKER_MEMORY": "4Gi",
    "WORKER_REPLICAS": "1",
    "WORKER_MAX_REPLICAS": "8",
    "WORKER_RAY_NUM_CPUS": "1",
    "HEAD_RAY_NUM_CPUS": "0",
    "MODEL_CACHE_VOLUME": "/mlsecops",
    "MODEL_CACHE_DIR": "/mlsecops/model-cache",
    "RUN_AS_USER": "1000",
    "RUN_AS_GROUP": "1000",
    "FS_GROUP": "1000",
}

# The 32 finalized env vars from the app/runtime contract.
APP_ENV_DEFAULTS = {
    "DEPLOYMENT_PROFILE": "prod",
    "OTEL_SERVICE_NAME": "tabular-inference",
    "SERVICE_VERSION": "v1",
    "DEPLOYMENT_ENVIRONMENT": "prod",
    "K8S_CLUSTER_NAME": "production-cluster",
    "MODEL_URI": "s3://your-bucket/model-bundle/",
    "MODEL_VERSION": "v1",
    "MODEL_CACHE_DIR": "/mlsecops/model-cache",
    "FEATURE_ORDER": "feature1,feature2,feature3",
    "MODEL_OUTPUT_NAMES": "output",
    "MAX_INSTANCES_PER_REQUEST": "256",
    "SERVE_DEPLOYMENT_NAME": "tabular_inference",
    "SERVE_NUM_CPUS": "1.0",
    "SERVE_MIN_REPLICAS": "1",
    "SERVE_INITIAL_REPLICAS": "1",
    "SERVE_MAX_REPLICAS": "8",
    "SERVE_TARGET_ONGOING_REQUESTS": "2",
    "SERVE_MAX_ONGOING_REQUESTS": "3",
    "SERVE_UPSCALE_DELAY_S": "3.0",
    "SERVE_DOWNSCALE_DELAY_S": "60.0",
    "SERVE_BATCH_MAX_SIZE": "16",
    "SERVE_BATCH_WAIT_TIMEOUT_S": "0.005",
    "ORT_INTRA_OP_NUM_THREADS": "1",
    "ORT_INTER_OP_NUM_THREADS": "1",
    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://signoz-otel-collector.signoz.svc.cluster.local:4317",
    "OTEL_TRACES_SAMPLER": "parentbased_traceidratio",
    "OTEL_TRACES_SAMPLER_ARG": "0.10",
    "LOG_LEVEL": "WARNING",
    "SLOW_REQUEST_MS": "500.0",
    "AWS_ACCESS_KEY_ID": "",
    "AWS_SECRET_ACCESS_KEY": "",
    "USE_IAM": "true",
}

APP_ENV_ORDER = list(APP_ENV_DEFAULTS.keys())


@dataclass(frozen=True)
class DeploymentSettings:
    namespace: str
    rayservice_name: str
    service_account_name: str
    aws_secret_name: str
    ray_image: str
    ray_version: str
    head_cpu: str
    head_memory: str
    worker_cpu: str
    worker_memory: str
    worker_replicas: int
    worker_max_replicas: int
    worker_ray_num_cpus: str
    head_ray_num_cpus: str
    model_cache_volume: str
    model_cache_dir: str
    run_as_user: int
    run_as_group: int
    fs_group: int
    use_iam: bool
    irsa_role_arn: str | None


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value.strip()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_deployment_settings() -> DeploymentSettings:
    use_iam = _env_bool("USE_IAM", True)
    irsa_role_arn = os.getenv("KUBERAY_IAM_ROLE_ARN") if use_iam else None

    return DeploymentSettings(
        namespace=_env_str("NAMESPACE", DEPLOYMENT_DEFAULTS["NAMESPACE"]),
        rayservice_name=_env_str("RAYSERVICE_NAME", DEPLOYMENT_DEFAULTS["RAYSERVICE_NAME"]),
        service_account_name=_env_str(
            "SERVICE_ACCOUNT_NAME", DEPLOYMENT_DEFAULTS["SERVICE_ACCOUNT_NAME"]
        ),
        aws_secret_name=_env_str("AWS_SECRET_NAME", DEPLOYMENT_DEFAULTS["AWS_SECRET_NAME"]),
        ray_image=_env_str("RAY_IMAGE", DEPLOYMENT_DEFAULTS["RAY_IMAGE"]),
        ray_version=_env_str("RAY_VERSION", DEPLOYMENT_DEFAULTS["RAY_VERSION"]),
        head_cpu=_env_str("HEAD_CPU", DEPLOYMENT_DEFAULTS["HEAD_CPU"]),
        head_memory=_env_str("HEAD_MEMORY", DEPLOYMENT_DEFAULTS["HEAD_MEMORY"]),
        worker_cpu=_env_str("WORKER_CPU", DEPLOYMENT_DEFAULTS["WORKER_CPU"]),
        worker_memory=_env_str("WORKER_MEMORY", DEPLOYMENT_DEFAULTS["WORKER_MEMORY"]),
        worker_replicas=_env_int("WORKER_REPLICAS", int(DEPLOYMENT_DEFAULTS["WORKER_REPLICAS"])),
        worker_max_replicas=_env_int(
            "WORKER_MAX_REPLICAS", int(DEPLOYMENT_DEFAULTS["WORKER_MAX_REPLICAS"])
        ),
        worker_ray_num_cpus=_env_str(
            "WORKER_RAY_NUM_CPUS", DEPLOYMENT_DEFAULTS["WORKER_RAY_NUM_CPUS"]
        ),
        head_ray_num_cpus=_env_str(
            "HEAD_RAY_NUM_CPUS", DEPLOYMENT_DEFAULTS["HEAD_RAY_NUM_CPUS"]
        ),
        model_cache_volume=_env_str(
            "MODEL_CACHE_VOLUME", DEPLOYMENT_DEFAULTS["MODEL_CACHE_VOLUME"]
        ),
        model_cache_dir=_env_str("MODEL_CACHE_DIR", DEPLOYMENT_DEFAULTS["MODEL_CACHE_DIR"]),
        run_as_user=_env_int("RUN_AS_USER", int(DEPLOYMENT_DEFAULTS["RUN_AS_USER"])),
        run_as_group=_env_int("RUN_AS_GROUP", int(DEPLOYMENT_DEFAULTS["RUN_AS_GROUP"])),
        fs_group=_env_int("FS_GROUP", int(DEPLOYMENT_DEFAULTS["FS_GROUP"])),
        use_iam=use_iam,
        irsa_role_arn=irsa_role_arn,
    )


def load_app_env() -> dict[str, str]:
    return {name: _env_str(name, default) for name, default in APP_ENV_DEFAULTS.items()}


def _require_nonempty(value: str | None, message: str) -> str:
    if value is None or not value.strip():
        raise RuntimeError(message)
    return value.strip()


def _secret_data_from_env() -> dict[str, str]:
    access_key = _require_nonempty(
        os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_ACCESS_KEY_ID is required when USE_IAM=false",
    )
    secret_key = _require_nonempty(
        os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_SECRET_ACCESS_KEY is required when USE_IAM=false",
    )
    return {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
    }


def _base_labels(settings: DeploymentSettings) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": settings.rayservice_name,
        "app.kubernetes.io/managed-by": "inference-service",
        "app.kubernetes.io/component": "inference",
    }


def _secret_ref_env(name: str, secret_name: str) -> dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {
            "secretKeyRef": {
                "name": secret_name,
                "key": name,
            }
        },
    }


def _build_container_env(app_env: dict[str, str], settings: DeploymentSettings) -> list[dict[str, Any]]:
    env: list[dict[str, Any]] = []

    for name in APP_ENV_ORDER:
        if name in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}:
            if settings.use_iam:
                continue
            env.append(_secret_ref_env(name, settings.aws_secret_name))
            continue

        if name == "USE_IAM":
            env.append({"name": name, "value": "true" if settings.use_iam else "false"})
            continue

        env.append({"name": name, "value": app_env[name]})

    return env


def build_namespace_doc(settings: DeploymentSettings) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": settings.namespace,
            "labels": _base_labels(settings),
        },
    }


def build_service_account_doc(settings: DeploymentSettings) -> dict[str, Any] | None:
    if not settings.use_iam:
        return None

    role_arn = _require_nonempty(
        settings.irsa_role_arn,
        "KUBERAY_IAM_ROLE_ARN is required when USE_IAM=true",
    )
    return {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": settings.service_account_name,
            "namespace": settings.namespace,
            "labels": _base_labels(settings),
            "annotations": {
                "eks.amazonaws.com/role-arn": role_arn,
            },
        },
    }


def build_secret_doc(settings: DeploymentSettings) -> dict[str, Any] | None:
    if settings.use_iam:
        return None

    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": settings.aws_secret_name,
            "namespace": settings.namespace,
            "labels": _base_labels(settings),
        },
        "type": "Opaque",
        "stringData": _secret_data_from_env(),
    }


def build_rayservice_doc(settings: DeploymentSettings, app_env: dict[str, str]) -> dict[str, Any]:
    labels = _base_labels(settings)
    container_env = _build_container_env(app_env, settings)

    pod_template_common = {
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": settings.run_as_user,
            "runAsGroup": settings.run_as_group,
            "fsGroup": settings.fs_group,
        },
        "terminationGracePeriodSeconds": 30,
        "volumes": [
            {
                "name": "model-cache",
                "emptyDir": {},
            }
        ],
    }

    head_container = {
        "name": "ray-head",
        "image": settings.ray_image,
        "imagePullPolicy": "IfNotPresent",
        "resources": {
            "requests": {
                "cpu": settings.head_cpu,
                "memory": settings.head_memory,
            },
            "limits": {
                "cpu": settings.head_cpu,
                "memory": settings.head_memory,
            },
        },
        "volumeMounts": [
            {
                "name": "model-cache",
                "mountPath": settings.model_cache_volume,
            }
        ],
        "env": container_env,
    }

    worker_container = {
        "name": "ray-worker",
        "image": settings.ray_image,
        "imagePullPolicy": "IfNotPresent",
        "resources": {
            "requests": {
                "cpu": settings.worker_cpu,
                "memory": settings.worker_memory,
            },
            "limits": {
                "cpu": settings.worker_cpu,
                "memory": settings.worker_memory,
            },
        },
        "volumeMounts": [
            {
                "name": "model-cache",
                "mountPath": settings.model_cache_volume,
            }
        ],
        "env": container_env,
    }

    if settings.use_iam:
        head_template_spec = {
            **pod_template_common,
            "serviceAccountName": settings.service_account_name,
            "containers": [head_container],
        }
        worker_template_spec = {
            **pod_template_common,
            "serviceAccountName": settings.service_account_name,
            "containers": [worker_container],
        }
    else:
        head_template_spec = {
            **pod_template_common,
            "containers": [head_container],
        }
        worker_template_spec = {
            **pod_template_common,
            "containers": [worker_container],
        }

    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayService",
        "metadata": {
            "name": settings.rayservice_name,
            "namespace": settings.namespace,
            "labels": labels,
        },
        "spec": {
            "serviceUnhealthySecondThreshold": 300,
            "deploymentUnhealthySecondThreshold": 300,
            "rayClusterConfig": {
                "rayVersion": settings.ray_version,
                "enableInTreeAutoscaling": True,
                "autoscalerOptions": {
                    "version": "v2",
                    "idleTimeoutSeconds": 60,
                },
                "headGroupSpec": {
                    "serviceType": "ClusterIP",
                    "rayStartParams": {
                        "dashboard-host": "0.0.0.0",
                        "num-cpus": settings.head_ray_num_cpus,
                    },
                    "template": {
                        "spec": head_template_spec,
                    },
                },
                "workerGroupSpecs": [
                    {
                        "groupName": "inference-workers",
                        "replicas": settings.worker_replicas,
                        "minReplicas": 1,
                        "maxReplicas": settings.worker_max_replicas,
                        "rayStartParams": {
                            "num-cpus": settings.worker_ray_num_cpus,
                        },
                        "template": {
                            "spec": worker_template_spec,
                        },
                    }
                ],
            },
            "serveConfigV2": (
                "proxy_location: EveryNode\n"
                "http_options:\n"
                "  host: 0.0.0.0\n"
                "  port: 8000\n"
                "applications:\n"
                "  - name: tabular_inference\n"
                "    route_prefix: /\n"
                "    import_path: service:app\n"
            ),
        },
    }


def build_canonical_documents() -> list[dict[str, Any]]:
    settings = load_deployment_settings()
    app_env = load_app_env()

    docs: list[dict[str, Any]] = [build_namespace_doc(settings)]

    sa_doc = build_service_account_doc(settings)
    if sa_doc is not None:
        docs.append(sa_doc)

    secret_doc = build_secret_doc(settings)
    if secret_doc is not None:
        docs.append(secret_doc)

    docs.append(build_rayservice_doc(settings, app_env))
    return docs


def inject_manifest_hash(docs: list[dict[str, Any]], digest: str) -> list[dict[str, Any]]:
    cloned = copy.deepcopy(docs)
    for doc in cloned:
        if doc.get("kind") == "RayService":
            metadata = doc.setdefault("metadata", {})
            annotations = metadata.setdefault("annotations", {})
            annotations[MANIFEST_HASH_ANNOTATION] = digest
            break
    return cloned


def render_documents(docs: list[dict[str, Any]]) -> str:
    return yaml.safe_dump_all(
        docs,
        sort_keys=False,
        default_flow_style=False,
        explicit_start=True,
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def read_previous_hash() -> str | None:
    if not HASH_PATH.exists():
        return None
    value = HASH_PATH.read_text(encoding="utf-8").strip()
    return value or None


def write_state(rendered_yaml: str, digest: str) -> None:
    ensure_state_dir()
    RENDERED_PATH.write_text(rendered_yaml, encoding="utf-8")
    HASH_PATH.write_text(digest + "\n", encoding="utf-8")


def kubectl_apply(path: Path) -> None:
    subprocess.run(["kubectl", "apply", "-f", str(path)], check=True)


def kubectl_delete(path: Path) -> None:
    subprocess.run(["kubectl", "delete", "-f", str(path), "--ignore-not-found"], check=True)


def kubectl_get_rayservice_hash(namespace: str, name: str) -> str | None:
    try:
        result = subprocess.run(
            ["kubectl", "get", "rayservice", name, "-n", namespace, "-o", "json"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return None

    try:
        payload = json.loads(result.stdout)
        annotations = payload.get("metadata", {}).get("annotations", {})
        value = annotations.get(MANIFEST_HASH_ANNOTATION)
        return value.strip() if isinstance(value, str) and value.strip() else None
    except Exception:
        return None


def write_temp_yaml(text: str) -> Path:
    tmp = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    try:
        tmp.write(text)
        tmp.flush()
        return Path(tmp.name)
    finally:
        tmp.close()


def rollout() -> int:
    canonical_docs = build_canonical_documents()
    canonical_rendered = render_documents(canonical_docs)
    digest = sha256_text(canonical_rendered)

    settings = load_deployment_settings()
    live_hash = kubectl_get_rayservice_hash(settings.namespace, settings.rayservice_name)
    previous = read_previous_hash()

    if previous == digest and live_hash == digest and RENDERED_PATH.exists():
        print(f"[OK] Manifest unchanged; hash={digest}")
        return 0

    applied_docs = inject_manifest_hash(canonical_docs, digest)
    applied_rendered = render_documents(applied_docs)

    namespace_path = write_temp_yaml(render_documents([applied_docs[0]]))
    rest_path = write_temp_yaml(render_documents(applied_docs[1:]))

    try:
        kubectl_apply(namespace_path)
        kubectl_apply(rest_path)
        write_state(applied_rendered, digest)
        print(f"[OK] Rollout applied; hash={digest}")
        return 0
    finally:
        for path in (namespace_path, rest_path):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


def delete() -> int:
    if RENDERED_PATH.exists():
        rendered_yaml = RENDERED_PATH.read_text(encoding="utf-8")
        docs = list(yaml.safe_load_all(rendered_yaml))
    else:
        docs = build_canonical_documents()

    if not docs:
        print("[OK] Nothing to delete")
        return 0

    namespace_doc = [docs[0]]
    rest_docs = docs[1:]

    namespace_path = write_temp_yaml(render_documents(namespace_doc))
    rest_path = write_temp_yaml(render_documents(rest_docs))

    try:
        kubectl_delete(rest_path)
        kubectl_delete(namespace_path)

        for path in (RENDERED_PATH, HASH_PATH):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass

        print("[OK] Delete applied")
        return 0
    finally:
        for path in (namespace_path, rest_path):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="inference_service.py",
        description="Render and lifecycle-manage the KubeRay RayService deployment.",
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--rollout", action="store_true", help="Render and apply manifests")
    action.add_argument("--delete", action="store_true", help="Delete rendered manifests")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        if args.rollout:
            return rollout()
        if args.delete:
            return delete()
        return 1
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] kubectl failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode or 1
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())