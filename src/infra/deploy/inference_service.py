from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

MANIFEST_DIR = Path("src/manifests/kuberay_rayservice")
STATE_DIR = MANIFEST_DIR / ".state"
RENDERED_PATH = STATE_DIR / "rendered.yaml"
HASH_PATH = STATE_DIR / "rendered.sha256"

AUTH_MODE_ANNOTATION = "inference-service.io/auth-mode"

DEPLOYMENT_DEFAULTS: dict[str, str] = {
    "NAMESPACE": "inference",
    "RAYSERVICE_NAME": "tabular-inference",
    "SERVICE_ACCOUNT_NAME": "ray-inference-sa",
    "AWS_SECRET_NAME": "aws-credentials",
    "RAY_IMAGE": "ghcr.io/athithya-sakthivel/tabular-inference-service:2026-04-15-06-12--b2617e3@sha256:3615562548755e906ad17945518267cfadbac7bdaacc5a76fc6ebd4dfbcfd6f4",
    "RAY_VERSION": "2.54.1",
    "HEAD_CPU": "1",
    "HEAD_MEMORY": "4Gi",
    "WORKER_CPU": "1",
    "WORKER_MEMORY": "2536Mi",
    "WORKER_REPLICAS": "1",
    "WORKER_MAX_REPLICAS": "1",
    "WORKER_RAY_NUM_CPUS": "1",
    "HEAD_RAY_NUM_CPUS": "0",
    "MODEL_CACHE_VOLUME": "/mlsecops",
    "MODEL_CACHE_DIR": "/mlsecops/model-cache",
    "RUN_AS_USER": "1000",
    "RUN_AS_GROUP": "1000",
    "FS_GROUP": "1000",
}

APP_ENV_DEFAULTS: dict[str, str] = {
    "DEPLOYMENT_PROFILE": "prod",
    "OTEL_SERVICE_NAME": "tabular-inference",
    "SERVICE_VERSION": "v1",
    "DEPLOYMENT_ENVIRONMENT": "prod",
    "K8S_CLUSTER_NAME": "production-cluster",
    "MODEL_URI": "__REQUIRED_MODEL_URI__",
    "MODEL_VERSION": "__REQUIRED_MODEL_VERSION__",
    "MODEL_SHA256": "__REQUIRED_MODEL_SHA256__",
    "MODEL_INPUT_NAME": "__REQUIRED_MODEL_INPUT_NAME__",
    "MODEL_OUTPUT_NAMES": "__REQUIRED_MODEL_OUTPUT_NAMES__",
    "FEATURE_ORDER": "__REQUIRED_FEATURE_ORDER__",
    "ALLOW_EXTRA_FEATURES": "false",
    "MODEL_CACHE_DIR": "/mlsecops/model-cache",
    "MAX_INSTANCES_PER_REQUEST": "256",
    "SERVE_DEPLOYMENT_NAME": "tabular_inference",
    "SERVE_NUM_CPUS": "0.5",
    "SERVE_MIN_REPLICAS": "1",
    "SERVE_INITIAL_REPLICAS": "1",
    "SERVE_MAX_REPLICAS": "1",
    "SERVE_TARGET_ONGOING_REQUESTS": "1",
    "SERVE_MAX_ONGOING_REQUESTS": "2",
    "SERVE_UPSCALE_DELAY_S": "3.0",
    "SERVE_DOWNSCALE_DELAY_S": "60.0",
    "SERVE_BATCH_MAX_SIZE": "16",
    "SERVE_BATCH_WAIT_TIMEOUT_S": "0.005",
    "SERVE_HEALTH_CHECK_PERIOD_S": "20.0",
    "SERVE_HEALTH_CHECK_TIMEOUT_S": "40.0",
    "SERVE_GRACEFUL_SHUTDOWN_WAIT_LOOP_S": "2.0",
    "SERVE_GRACEFUL_SHUTDOWN_TIMEOUT_S": "20.0",
    "ORT_INTRA_OP_NUM_THREADS": "1",
    "ORT_INTER_OP_NUM_THREADS": "1",
    "ORT_LOG_SEVERITY_LEVEL": "3",
    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://signoz-otel-collector.signoz.svc.cluster.local:4317",
    "OTEL_EXPORTER_OTLP_TIMEOUT": "10000",
    "OTEL_METRIC_EXPORT_INTERVAL_MS": "15000",
    "OTEL_METRIC_EXPORT_TIMEOUT_MS": "10000",
    "LOG_LEVEL": "WARNING",
    "SLOW_REQUEST_MS": "100.0",
    "AWS_ACCESS_KEY_ID": "",
    "AWS_SECRET_ACCESS_KEY": "",
    "USE_IAM": "false",
}

APP_ENV_ORDER = list(APP_ENV_DEFAULTS.keys())
REQUIRED_APP_ENV_NAMES = {
    "MODEL_URI",
    "MODEL_VERSION",
    "MODEL_SHA256",
    "MODEL_INPUT_NAME",
    "MODEL_OUTPUT_NAMES",
    "FEATURE_ORDER",
}

PLACEHOLDER_VALUES = {key: APP_ENV_DEFAULTS[key] for key in REQUIRED_APP_ENV_NAMES}


@dataclass(frozen=True, slots=True)
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

    def __post_init__(self) -> None:
        if not self.namespace.strip():
            raise RuntimeError("NAMESPACE must not be empty")
        if not self.rayservice_name.strip():
            raise RuntimeError("RAYSERVICE_NAME must not be empty")
        if not self.aws_secret_name.strip():
            raise RuntimeError("AWS_SECRET_NAME must not be empty")
        if not self.ray_image.strip():
            raise RuntimeError("RAY_IMAGE must not be empty")
        if not self.ray_version.strip():
            raise RuntimeError("RAY_VERSION must not be empty")
        if not self.head_cpu.strip():
            raise RuntimeError("HEAD_CPU must not be empty")
        if not self.head_memory.strip():
            raise RuntimeError("HEAD_MEMORY must not be empty")
        if not self.worker_cpu.strip():
            raise RuntimeError("WORKER_CPU must not be empty")
        if not self.worker_memory.strip():
            raise RuntimeError("WORKER_MEMORY must not be empty")
        if not self.worker_ray_num_cpus.strip():
            raise RuntimeError("WORKER_RAY_NUM_CPUS must not be empty")
        if not self.head_ray_num_cpus.strip():
            raise RuntimeError("HEAD_RAY_NUM_CPUS must not be empty")
        if not self.model_cache_volume.strip():
            raise RuntimeError("MODEL_CACHE_VOLUME must not be empty")
        if not self.model_cache_dir.strip():
            raise RuntimeError("MODEL_CACHE_DIR must not be empty")
        if self.worker_replicas < 1:
            raise RuntimeError("WORKER_REPLICAS must be >= 1")
        if self.worker_max_replicas < self.worker_replicas:
            raise RuntimeError("WORKER_MAX_REPLICAS must be >= WORKER_REPLICAS")
        if self.run_as_user < 1:
            raise RuntimeError("RUN_AS_USER must be >= 1")
        if self.run_as_group < 1:
            raise RuntimeError("RUN_AS_GROUP must be >= 1")
        if self.fs_group < 1:
            raise RuntimeError("FS_GROUP must be >= 1")


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip() or default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _require_nonempty(value: str | None, message: str) -> str:
    if value is None or not value.strip():
        raise RuntimeError(message)
    return value.strip()


def _validate_log_level(value: str) -> str:
    normalized = value.strip().upper()
    if normalized == "WARN":
        normalized = "WARNING"
    allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
    if not normalized:
        return "WARNING"
    if normalized not in allowed:
        raise RuntimeError(f"LOG_LEVEL must be one of {sorted(allowed)}, got {value!r}")
    return normalized


def _profile() -> str:
    raw = os.getenv("DEPLOYMENT_PROFILE", "prod").strip().lower()
    if raw != "prod":
        raise RuntimeError("DEPLOYMENT_PROFILE is fixed to 'prod' in this build")
    return "prod"


def _validate_auth_mode() -> tuple[bool, str | None]:
    use_iam = _env_bool("USE_IAM", False)
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()

    if use_iam:
        role_arn = os.getenv("KUBERAY_IAM_ROLE_ARN", "").strip() or None
        return True, role_arn

    if not access_key or not secret_key:
        raise RuntimeError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when USE_IAM=false"
        )

    return False, None


def _static_credentials() -> dict[str, str]:
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


def load_deployment_settings() -> DeploymentSettings:
    use_iam, irsa_role_arn = _validate_auth_mode()

    return DeploymentSettings(
        namespace=_env_str("NAMESPACE", DEPLOYMENT_DEFAULTS["NAMESPACE"]),
        rayservice_name=_env_str("RAYSERVICE_NAME", DEPLOYMENT_DEFAULTS["RAYSERVICE_NAME"]),
        service_account_name=_env_str("SERVICE_ACCOUNT_NAME", DEPLOYMENT_DEFAULTS["SERVICE_ACCOUNT_NAME"]),
        aws_secret_name=_env_str("AWS_SECRET_NAME", DEPLOYMENT_DEFAULTS["AWS_SECRET_NAME"]),
        ray_image=_env_str("RAY_IMAGE", DEPLOYMENT_DEFAULTS["RAY_IMAGE"]),
        ray_version=_env_str("RAY_VERSION", DEPLOYMENT_DEFAULTS["RAY_VERSION"]),
        head_cpu=_env_str("HEAD_CPU", DEPLOYMENT_DEFAULTS["HEAD_CPU"]),
        head_memory=_env_str("HEAD_MEMORY", DEPLOYMENT_DEFAULTS["HEAD_MEMORY"]),
        worker_cpu=_env_str("WORKER_CPU", DEPLOYMENT_DEFAULTS["WORKER_CPU"]),
        worker_memory=_env_str("WORKER_MEMORY", DEPLOYMENT_DEFAULTS["WORKER_MEMORY"]),
        worker_replicas=_env_int("WORKER_REPLICAS", int(DEPLOYMENT_DEFAULTS["WORKER_REPLICAS"])),
        worker_max_replicas=_env_int("WORKER_MAX_REPLICAS", int(DEPLOYMENT_DEFAULTS["WORKER_MAX_REPLICAS"])),
        worker_ray_num_cpus=_env_str("WORKER_RAY_NUM_CPUS", DEPLOYMENT_DEFAULTS["WORKER_RAY_NUM_CPUS"]),
        head_ray_num_cpus=_env_str("HEAD_RAY_NUM_CPUS", DEPLOYMENT_DEFAULTS["HEAD_RAY_NUM_CPUS"]),
        model_cache_volume=_env_str("MODEL_CACHE_VOLUME", DEPLOYMENT_DEFAULTS["MODEL_CACHE_VOLUME"]),
        model_cache_dir=_env_str("MODEL_CACHE_DIR", DEPLOYMENT_DEFAULTS["MODEL_CACHE_DIR"]),
        run_as_user=_env_int("RUN_AS_USER", int(DEPLOYMENT_DEFAULTS["RUN_AS_USER"])),
        run_as_group=_env_int("RUN_AS_GROUP", int(DEPLOYMENT_DEFAULTS["RUN_AS_GROUP"])),
        fs_group=_env_int("FS_GROUP", int(DEPLOYMENT_DEFAULTS["FS_GROUP"])),
        use_iam=use_iam,
        irsa_role_arn=irsa_role_arn,
    )


def load_app_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for name in APP_ENV_ORDER:
        value = _env_str(name, APP_ENV_DEFAULTS[name])
        if name == "LOG_LEVEL":
            value = _validate_log_level(value)
        if name in REQUIRED_APP_ENV_NAMES and (not value or value == PLACEHOLDER_VALUES[name]):
            raise RuntimeError(f"{name} is required and must be set explicitly")
        env[name] = value
    return env


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


def _container_security_context() -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }


def _probe(command: str, timeout: int, period: int, failure_threshold: int) -> dict[str, Any]:
    return {
        "exec": {
            "command": ["/bin/bash", "-lc", command],
        },
        "initialDelaySeconds": 0,
        "timeoutSeconds": timeout,
        "periodSeconds": period,
        "failureThreshold": failure_threshold,
        "successThreshold": 1,
    }


def _python_http_json_probe(url: str, expected_key: str, expected_value: str, timeout: int = 10) -> str:
    return textwrap.dedent(
        f"""
        python3 - <<'PY'
        import json
        import urllib.request

        try:
            with urllib.request.urlopen({url!r}, timeout={timeout}) as resp:
                body = resp.read().decode("utf-8", "ignore")
        except Exception:
            raise SystemExit(1)

        try:
            payload = json.loads(body)
        except Exception:
            raise SystemExit(1)

        raise SystemExit(0 if payload.get({expected_key!r}) == {expected_value!r} else 1)
        PY
        """
    ).strip()


def _timeout_ray_health_cmd(address_expr: str, timeout_s: int = 10) -> str:
    return f'timeout {timeout_s}s ray health-check --address "{address_expr}" >/dev/null 2>&1'


def _head_probe_set() -> dict[str, Any]:
    cmd = _timeout_ray_health_cmd("${RAY_ADDRESS}", timeout_s=10)
    return {
        "startupProbe": _probe(cmd, timeout=12, period=5, failure_threshold=60),
        "readinessProbe": _probe(cmd, timeout=12, period=5, failure_threshold=12),
        "livenessProbe": _probe(cmd, timeout=12, period=10, failure_threshold=12),
    }


def _worker_probe_set() -> dict[str, Any]:
    ray_cmd = _timeout_ray_health_cmd("${RAY_ADDRESS}", timeout_s=10)
    app_cmd = _python_http_json_probe(
        "http://127.0.0.1:8000/healthz",
        expected_key="status",
        expected_value="ok",
        timeout=10,
    )
    return {
        "startupProbe": _probe(ray_cmd, timeout=12, period=5, failure_threshold=60),
        "readinessProbe": _probe(
            f"{ray_cmd} && {app_cmd}",
            timeout=20,
            period=5,
            failure_threshold=12,
        ),
        "livenessProbe": _probe(ray_cmd, timeout=12, period=10, failure_threshold=12),
    }


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


def build_secret_doc(settings: DeploymentSettings, secret_data: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": settings.aws_secret_name,
            "namespace": settings.namespace,
            "labels": _base_labels(settings),
        },
        "type": "Opaque",
        "stringData": secret_data,
    }


def build_secret_delete_doc(settings: DeploymentSettings) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": settings.aws_secret_name,
            "namespace": settings.namespace,
        },
    }


def _shared_volumes() -> list[dict[str, Any]]:
    return [
        {"name": "model-cache", "emptyDir": {}},
        {"name": "tmp", "emptyDir": {}},
    ]


def _shared_mounts(settings: DeploymentSettings) -> list[dict[str, Any]]:
    return [
        {"name": "model-cache", "mountPath": settings.model_cache_volume},
        {"name": "tmp", "mountPath": "/tmp"},
    ]


def build_rayservice_doc(settings: DeploymentSettings, app_env: dict[str, str]) -> dict[str, Any]:
    labels = _base_labels(settings)
    annotations = {
        AUTH_MODE_ANNOTATION: "iam" if settings.use_iam else "static",
    }
    container_env = _build_container_env(app_env, settings)

    pod_template_common = {
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": settings.run_as_user,
            "runAsGroup": settings.run_as_group,
            "fsGroup": settings.fs_group,
        },
        "terminationGracePeriodSeconds": 30,
        "volumes": _shared_volumes(),
    }

    head_container = {
        "name": "ray-head",
        "image": settings.ray_image,
        "imagePullPolicy": "IfNotPresent",
        "securityContext": _container_security_context(),
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
        "volumeMounts": _shared_mounts(settings),
        "env": container_env,
        **_head_probe_set(),
    }

    worker_container = {
        "name": "ray-worker",
        "image": settings.ray_image,
        "imagePullPolicy": "IfNotPresent",
        "securityContext": _container_security_context(),
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
        "volumeMounts": _shared_mounts(settings),
        "env": container_env,
        **_worker_probe_set(),
    }

    head_template_spec: dict[str, Any] = {
        **pod_template_common,
        "containers": [head_container],
    }
    worker_template_spec: dict[str, Any] = {
        **pod_template_common,
        "containers": [worker_container],
    }

    if settings.use_iam:
        head_template_spec["serviceAccountName"] = settings.service_account_name
        worker_template_spec["serviceAccountName"] = settings.service_account_name

    serve_config_v2 = textwrap.dedent(
        """
        proxy_location: EveryNode
        http_options:
          host: 0.0.0.0
          port: 8000
        applications:
          - name: tabular_inference
            route_prefix: /
            import_path: service:app
        """
    ).strip() + "\n"

    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayService",
        "metadata": {
            "name": settings.rayservice_name,
            "namespace": settings.namespace,
            "labels": labels,
            "annotations": annotations,
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
                        "include-dashboard": "true",
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
            "serveConfigV2": serve_config_v2,
        },
    }


def build_documents(settings: DeploymentSettings, app_env: dict[str, str]) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = [build_namespace_doc(settings)]
    sa_doc = build_service_account_doc(settings)
    if sa_doc is not None:
        docs.append(sa_doc)
    docs.append(build_rayservice_doc(settings, app_env))
    return docs


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


def write_state(rendered_yaml: str, digest: str) -> None:
    ensure_state_dir()
    RENDERED_PATH.write_text(rendered_yaml, encoding="utf-8")
    HASH_PATH.write_text(digest + "\n", encoding="utf-8")


def write_temp_yaml(text: str) -> Path:
    tmp = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    try:
        tmp.write(text)
        tmp.flush()
        return Path(tmp.name)
    finally:
        tmp.close()


def kubectl_apply(path: Path) -> None:
    subprocess.run(["kubectl", "apply", "-f", str(path)], check=True)


def kubectl_delete(path: Path) -> None:
    subprocess.run(["kubectl", "delete", "-f", str(path), "--ignore-not-found"], check=True)


def kubectl_apply_doc(doc: dict[str, Any]) -> None:
    path = write_temp_yaml(render_documents([doc]))
    try:
        kubectl_apply(path)
    finally:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass


def kubectl_delete_doc(doc: dict[str, Any]) -> None:
    path = write_temp_yaml(render_documents([doc]))
    try:
        kubectl_delete(path)
    finally:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass


def rollout() -> int:
    settings = load_deployment_settings()
    app_env = load_app_env()

    secret_doc: dict[str, Any] | None = None
    if not settings.use_iam:
        secret_doc = build_secret_doc(settings, _static_credentials())

    docs = build_documents(settings, app_env)
    rendered = render_documents(docs)
    digest = sha256_text(rendered)

    kubectl_apply_doc(docs[0])
    if secret_doc is not None:
        kubectl_apply_doc(secret_doc)
    if len(docs) > 1 and docs[1].get("kind") == "ServiceAccount":
        kubectl_apply_doc(docs[1])

    rayservice_doc = docs[-1]
    rayservice_path = write_temp_yaml(render_documents([rayservice_doc]))
    try:
        kubectl_apply(rayservice_path)
        write_state(rendered, digest)
        print(f"[OK] Rollout applied; hash={digest}")
        return 0
    finally:
        try:
            rayservice_path.unlink(missing_ok=True)
        except Exception:
            pass


def delete() -> int:
    settings = load_deployment_settings()
    namespace_doc = build_namespace_doc(settings)
    sa_doc = build_service_account_doc(settings)
    rayservice_doc: dict[str, Any] | None = None

    if RENDERED_PATH.exists():
        rendered_yaml = RENDERED_PATH.read_text(encoding="utf-8")
        docs = list(yaml.safe_load_all(rendered_yaml))
        for doc in docs:
            if isinstance(doc, dict) and doc.get("kind") == "RayService":
                rayservice_doc = doc
                break

    if rayservice_doc is None:
        rayservice_doc = build_rayservice_doc(settings, load_app_env())

    secret_doc = build_secret_delete_doc(settings) if not settings.use_iam else None

    kubectl_delete_doc(rayservice_doc)
    if sa_doc is not None:
        kubectl_delete_doc(sa_doc)
    if secret_doc is not None:
        kubectl_delete_doc(secret_doc)
    kubectl_delete_doc(namespace_doc)

    for path in (RENDERED_PATH, HASH_PATH):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass

    print("[OK] Delete applied")
    return 0


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