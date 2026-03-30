from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _env_list(name: str, default: list[str], sep: str = ",") -> list[str]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return list(default)
    return [part.strip() for part in raw.split(sep) if part.strip()]


def _env_tuple(name: str, default: tuple[str, ...], sep: str = ",") -> tuple[str, ...]:
    return tuple(_env_list(name, list(default), sep=sep))


@dataclass(frozen=True)
class Settings:
    # Identity
    service_name: str
    service_version: str
    deployment_environment: str
    cluster_name: str
    instance_id: str

    # Model
    model_uri: str
    model_version: str
    model_cache_dir: str
    model_sha256: str | None
    feature_order: tuple[str, ...]
    model_input_name: str | None
    model_output_names: tuple[str, ...]
    allow_extra_features: bool
    max_instances_per_request: int

    # Ray Serve
    serve_deployment_name: str
    replica_num_cpus: float
    min_replicas: int
    initial_replicas: int
    max_replicas: int
    target_ongoing_requests: int
    max_ongoing_requests: int
    upscale_delay_s: float
    downscale_delay_s: float
    batch_max_size: int
    batch_wait_timeout_s: float

    # ONNX Runtime
    ort_intra_op_num_threads: int
    ort_inter_op_num_threads: int
    ort_providers: tuple[str, ...]
    ort_log_severity_level: int

    # Telemetry
    otel_endpoint: str
    otel_timeout_seconds: float
    otel_metric_export_interval_ms: int
    otel_metric_export_timeout_ms: int
    trace_sample_ratio: float
    log_level: str

    # Operational
    slow_request_ms: float


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    feature_order = _env_list("FEATURE_ORDER", [])
    if not feature_order:
        raise RuntimeError("FEATURE_ORDER is required, for example: FEATURE_ORDER=age,income,score")

    model_uri = os.getenv("MODEL_URI", "").strip()
    if not model_uri:
        raise RuntimeError("MODEL_URI is required and should point to a bundle root or a model file")

    model_output_names = _env_tuple("MODEL_OUTPUT_NAMES", tuple())
    model_input_name = os.getenv("MODEL_INPUT_NAME", "").strip() or None

    service_name = os.getenv("OTEL_SERVICE_NAME", "inference-api").strip() or "inference-api"
    service_version = os.getenv("SERVICE_VERSION", "v1").strip() or "v1"
    deployment_environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "prod").strip() or "prod"
    cluster_name = os.getenv("K8S_CLUSTER_NAME", "prod-eks").strip() or "prod-eks"
    instance_id = os.getenv("POD_NAME", os.getenv("HOSTNAME", "local")).strip() or "local"

    model_version = os.getenv("MODEL_VERSION", "v1").strip() or "v1"
    model_cache_dir = os.getenv("MODEL_CACHE_DIR", "/tmp/model-cache").strip() or "/tmp/model-cache"
    model_sha256 = os.getenv("MODEL_SHA256", "").strip() or None
    allow_extra_features = _env_bool("ALLOW_EXTRA_FEATURES", False)
    max_instances_per_request = _env_int("MAX_INSTANCES_PER_REQUEST", 256)

    serve_deployment_name = os.getenv("SERVE_DEPLOYMENT_NAME", "tabular_inference").strip() or "tabular_inference"
    replica_num_cpus = _env_float("SERVE_NUM_CPUS", 1.0)
    min_replicas = _env_int("SERVE_MIN_REPLICAS", 1)
    initial_replicas = _env_int("SERVE_INITIAL_REPLICAS", min_replicas)
    max_replicas = _env_int("SERVE_MAX_REPLICAS", 8)
    target_ongoing_requests = _env_int("SERVE_TARGET_ONGOING_REQUESTS", 2)
    max_ongoing_requests = _env_int("SERVE_MAX_ONGOING_REQUESTS", max(4, target_ongoing_requests + 2))
    upscale_delay_s = _env_float("SERVE_UPSCALE_DELAY_S", 1.0)
    downscale_delay_s = _env_float("SERVE_DOWNSCALE_DELAY_S", 30.0)
    batch_max_size = _env_int("SERVE_BATCH_MAX_SIZE", 16)
    batch_wait_timeout_s = _env_float("SERVE_BATCH_WAIT_TIMEOUT_S", 0.005)

    ort_intra_op_num_threads = _env_int("ORT_INTRA_OP_NUM_THREADS", 0)
    ort_inter_op_num_threads = _env_int("ORT_INTER_OP_NUM_THREADS", 1)
    ort_providers = _env_tuple("ORT_PROVIDERS", ("CPUExecutionProvider",))
    ort_log_severity_level = _env_int("ORT_LOG_SEVERITY_LEVEL", 3)

    otel_endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://signoz-otel-collector.signoz.svc.cluster.local:4317",
    ).strip()
    otel_timeout_seconds = _env_float("OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS", 10.0)
    otel_metric_export_interval_ms = _env_int("OTEL_METRIC_EXPORT_INTERVAL_MS", 15000)
    otel_metric_export_timeout_ms = _env_int("OTEL_METRIC_EXPORT_TIMEOUT_MS", 10000)
    trace_sample_ratio = _env_float("OTEL_TRACES_SAMPLER_ARG", 0.1)
    log_level = os.getenv("LOG_LEVEL", "WARNING").strip().upper() or "WARNING"

    slow_request_ms = _env_float("SLOW_REQUEST_MS", 500.0)

    return Settings(
        service_name=service_name,
        service_version=service_version,
        deployment_environment=deployment_environment,
        cluster_name=cluster_name,
        instance_id=instance_id,
        model_uri=model_uri,
        model_version=model_version,
        model_cache_dir=model_cache_dir,
        model_sha256=model_sha256,
        feature_order=tuple(feature_order),
        model_input_name=model_input_name,
        model_output_names=tuple(model_output_names),
        allow_extra_features=allow_extra_features,
        max_instances_per_request=max_instances_per_request,
        serve_deployment_name=serve_deployment_name,
        replica_num_cpus=replica_num_cpus,
        min_replicas=min_replicas,
        initial_replicas=initial_replicas,
        max_replicas=max_replicas,
        target_ongoing_requests=target_ongoing_requests,
        max_ongoing_requests=max_ongoing_requests,
        upscale_delay_s=upscale_delay_s,
        downscale_delay_s=downscale_delay_s,
        batch_max_size=batch_max_size,
        batch_wait_timeout_s=batch_wait_timeout_s,
        ort_intra_op_num_threads=ort_intra_op_num_threads,
        ort_inter_op_num_threads=ort_inter_op_num_threads,
        ort_providers=ort_providers,
        ort_log_severity_level=ort_log_severity_level,
        otel_endpoint=otel_endpoint,
        otel_timeout_seconds=otel_timeout_seconds,
        otel_metric_export_interval_ms=otel_metric_export_interval_ms,
        otel_metric_export_timeout_ms=otel_metric_export_timeout_ms,
        trace_sample_ratio=trace_sample_ratio,
        log_level=log_level,
        slow_request_ms=slow_request_ms,
    )
