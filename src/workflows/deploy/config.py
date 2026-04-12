from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal

DeploymentProfile = Literal["prod"]

# Fixed production baseline.
# Keep these stable unless profiling or an incident specifically justifies a change.
PROD_DEFAULTS: dict[str, str] = {
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
    "SERVE_HEALTH_CHECK_PERIOD_S": "10.0",
    "SERVE_HEALTH_CHECK_TIMEOUT_S": "30.0",
    "SERVE_GRACEFUL_SHUTDOWN_WAIT_LOOP_S": "2.0",
    "SERVE_GRACEFUL_SHUTDOWN_TIMEOUT_S": "20.0",
    "ORT_INTRA_OP_NUM_THREADS": "1",
    "ORT_INTER_OP_NUM_THREADS": "1",
    "OTEL_TRACES_SAMPLER": "parentbased_traceidratio",
    "OTEL_TRACES_SAMPLER_ARG": "0.10",
    "LOG_LEVEL": "WARNING",
}


def _profile() -> DeploymentProfile:
    raw = os.getenv("DEPLOYMENT_PROFILE", "prod").strip().lower()
    if raw != "prod":
        raise RuntimeError("DEPLOYMENT_PROFILE is fixed to 'prod' in this build")
    return "prod"


def _profile_default(name: str, default: str) -> str:
    return os.getenv(name, PROD_DEFAULTS.get(name, default))


def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip()
    return value if value else default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(name: str, raw: str) -> int:
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return _parse_int(name, raw)


def _env_int_profile(name: str, default: int) -> int:
    raw = _profile_default(name, str(default))
    return _parse_int(name, raw)


def _env_int_any(names: tuple[str, ...], default: int) -> int:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip() != "":
            return _parse_int(name, raw)
    return default


def _parse_float(name: str, raw: str) -> float:
    try:
        return float(raw.strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a float, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return _parse_float(name, raw)


def _env_float_profile(name: str, default: float) -> float:
    raw = _profile_default(name, str(default))
    return _parse_float(name, raw)


def _env_float_any(names: tuple[str, ...], default: float) -> float:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip() != "":
            return _parse_float(name, raw)
    return default


def _env_ratio(name: str, default: float) -> float:
    value = _env_float(name, default)
    if not 0.0 <= value <= 1.0:
        raise RuntimeError(f"{name} must be in the range [0.0, 1.0], got {value!r}")
    return value


def _env_ratio_profile(name: str, default: float) -> float:
    value = _env_float_profile(name, default)
    if not 0.0 <= value <= 1.0:
        raise RuntimeError(f"{name} must be in the range [0.0, 1.0], got {value!r}")
    return value


def _env_list(name: str, default: list[str], sep: str = ",") -> list[str]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return list(default)
    return [part.strip() for part in raw.split(sep) if part.strip()]


def _env_tuple(name: str, default: tuple[str, ...], sep: str = ",") -> tuple[str, ...]:
    return tuple(_env_list(name, list(default), sep=sep))


def _env_otlp_timeout_seconds() -> float:
    """
    OpenTelemetry exporters commonly use OTEL_EXPORTER_OTLP_TIMEOUT in milliseconds.
    Keep OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS as a fallback for older deployments.
    """
    raw_ms = os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT")
    if raw_ms is not None and raw_ms.strip() != "":
        return _parse_float("OTEL_EXPORTER_OTLP_TIMEOUT", raw_ms) / 1000.0

    raw_seconds = os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS")
    if raw_seconds is not None and raw_seconds.strip() != "":
        return _parse_float("OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS", raw_seconds)

    return 10.0


def _validate_log_level(value: str) -> str:
    allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET", "WARN"}
    normalized = value.strip().upper()
    if not normalized:
        return "WARNING"
    if normalized not in allowed:
        raise RuntimeError(
            f"LOG_LEVEL must be one of {sorted(allowed)}, got {value!r}"
        )
    return "WARNING" if normalized == "WARN" else normalized


def _ensure_unique(names: tuple[str, ...], field_name: str) -> None:
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise RuntimeError(f"{field_name} contains duplicate name: {name}")
        seen.add(name)


@dataclass(frozen=True, slots=True)
class Settings:
    # Identity
    service_name: str
    service_version: str
    deployment_environment: str
    cluster_name: str
    instance_id: str
    deployment_profile: DeploymentProfile

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
    serve_health_check_period_s: float
    serve_health_check_timeout_s: float
    serve_graceful_shutdown_wait_loop_s: float
    serve_graceful_shutdown_timeout_s: float

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
    otel_traces_sampler: str
    trace_sample_ratio: float
    log_level: str

    # Operational
    slow_request_ms: float

    def __post_init__(self) -> None:
        if not self.service_name.strip():
            raise RuntimeError("service_name must not be empty")
        if not self.service_version.strip():
            raise RuntimeError("service_version must not be empty")
        if not self.deployment_environment.strip():
            raise RuntimeError("deployment_environment must not be empty")
        if not self.cluster_name.strip():
            raise RuntimeError("cluster_name must not be empty")
        if not self.instance_id.strip():
            raise RuntimeError("instance_id must not be empty")

        if not self.model_uri.strip():
            raise RuntimeError("MODEL_URI is required and cannot be empty")
        if not self.feature_order:
            raise RuntimeError("FEATURE_ORDER is required and cannot be empty")
        _ensure_unique(self.feature_order, "FEATURE_ORDER")

        if self.model_output_names:
            _ensure_unique(self.model_output_names, "MODEL_OUTPUT_NAMES")

        if self.min_replicas < 0:
            raise RuntimeError("SERVE_MIN_REPLICAS must be >= 0")
        if self.max_replicas < self.min_replicas:
            raise RuntimeError("SERVE_MAX_REPLICAS must be >= SERVE_MIN_REPLICAS")
        if not (self.min_replicas <= self.initial_replicas <= self.max_replicas):
            raise RuntimeError(
                "SERVE_INITIAL_REPLICAS must be between SERVE_MIN_REPLICAS and SERVE_MAX_REPLICAS"
            )

        if self.target_ongoing_requests < 1:
            raise RuntimeError("SERVE_TARGET_ONGOING_REQUESTS must be >= 1")
        if self.max_ongoing_requests < self.target_ongoing_requests:
            raise RuntimeError(
                "SERVE_MAX_ONGOING_REQUESTS must be >= SERVE_TARGET_ONGOING_REQUESTS"
            )

        if self.replica_num_cpus <= 0:
            raise RuntimeError("SERVE_NUM_CPUS must be > 0")
        if self.batch_max_size < 1:
            raise RuntimeError("SERVE_BATCH_MAX_SIZE must be >= 1")
        if self.batch_wait_timeout_s < 0:
            raise RuntimeError("SERVE_BATCH_WAIT_TIMEOUT_S must be >= 0")
        if self.upscale_delay_s < 0:
            raise RuntimeError("SERVE_UPSCALE_DELAY_S must be >= 0")
        if self.downscale_delay_s < 0:
            raise RuntimeError("SERVE_DOWNSCALE_DELAY_S must be >= 0")
        if self.serve_health_check_period_s <= 0:
            raise RuntimeError("SERVE_HEALTH_CHECK_PERIOD_S must be > 0")
        if self.serve_health_check_timeout_s <= 0:
            raise RuntimeError("SERVE_HEALTH_CHECK_TIMEOUT_S must be > 0")
        if self.serve_graceful_shutdown_wait_loop_s < 0:
            raise RuntimeError("SERVE_GRACEFUL_SHUTDOWN_WAIT_LOOP_S must be >= 0")
        if self.serve_graceful_shutdown_timeout_s < 0:
            raise RuntimeError("SERVE_GRACEFUL_SHUTDOWN_TIMEOUT_S must be >= 0")

        if self.ort_intra_op_num_threads < 0:
            raise RuntimeError("ORT_INTRA_OP_NUM_THREADS must be >= 0")
        if self.ort_inter_op_num_threads < 0:
            raise RuntimeError("ORT_INTER_OP_NUM_THREADS must be >= 0")
        if not self.ort_providers:
            raise RuntimeError("ORT_PROVIDERS must not be empty")

        if not self.otel_endpoint.strip():
            raise RuntimeError("OTEL_EXPORTER_OTLP_ENDPOINT must not be empty")
        if self.otel_timeout_seconds <= 0:
            raise RuntimeError("OTEL_EXPORTER_OTLP_TIMEOUT must be > 0")
        if self.otel_metric_export_interval_ms <= 0:
            raise RuntimeError("OTEL_METRIC_EXPORT_INTERVAL_MS must be > 0")
        if self.otel_metric_export_timeout_ms <= 0:
            raise RuntimeError("OTEL_METRIC_EXPORT_TIMEOUT_MS must be > 0")

        if not self.otel_traces_sampler.strip():
            raise RuntimeError("OTEL_TRACES_SAMPLER must not be empty")
        if not 0.0 <= self.trace_sample_ratio <= 1.0:
            raise RuntimeError("OTEL_TRACES_SAMPLER_ARG must be in [0.0, 1.0]")

        if self.max_instances_per_request < 1:
            raise RuntimeError("MAX_INSTANCES_PER_REQUEST must be >= 1")
        if self.slow_request_ms < 0:
            raise RuntimeError("SLOW_REQUEST_MS must be >= 0")

        if not self.log_level.strip():
            raise RuntimeError("LOG_LEVEL must not be empty")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    deployment_profile = _profile()

    feature_order = _env_list("FEATURE_ORDER", [])
    if not feature_order:
        raise RuntimeError(
            "FEATURE_ORDER is required, for example: FEATURE_ORDER=age,income,score"
        )

    model_uri = _env_str("MODEL_URI")
    if not model_uri:
        raise RuntimeError(
            "MODEL_URI is required and should point to a bundle root or a model file"
        )

    model_output_names = _env_tuple("MODEL_OUTPUT_NAMES", tuple())
    model_input_name = _env_str("MODEL_INPUT_NAME") or None

    service_name = _env_str("OTEL_SERVICE_NAME", "inference-api")
    service_version = _env_str("SERVICE_VERSION", "v1")
    deployment_environment = _env_str("DEPLOYMENT_ENVIRONMENT", deployment_profile)
    cluster_name = _env_str("K8S_CLUSTER_NAME", "production-cluster")
    instance_id = _env_str("POD_NAME", _env_str("HOSTNAME", "local"))

    model_version = _env_str("MODEL_VERSION", "v1")
    model_cache_dir = _env_str("MODEL_CACHE_DIR", "/tmp/model-cache")
    model_sha256 = _env_str("MODEL_SHA256") or None
    allow_extra_features = _env_bool("ALLOW_EXTRA_FEATURES", False)
    max_instances_per_request = _env_int("MAX_INSTANCES_PER_REQUEST", 256)

    serve_deployment_name = _env_str("SERVE_DEPLOYMENT_NAME", "tabular_inference")
    replica_num_cpus = _env_float_profile("SERVE_NUM_CPUS", 1.0)
    min_replicas = _env_int_profile("SERVE_MIN_REPLICAS", 1)
    initial_replicas = _env_int_profile("SERVE_INITIAL_REPLICAS", min_replicas)
    max_replicas = _env_int_profile("SERVE_MAX_REPLICAS", 8)
    target_ongoing_requests = _env_int_profile("SERVE_TARGET_ONGOING_REQUESTS", 2)
    max_ongoing_requests = _env_int_profile(
        "SERVE_MAX_ONGOING_REQUESTS",
        max(4, target_ongoing_requests + 1),
    )
    upscale_delay_s = _env_float_profile("SERVE_UPSCALE_DELAY_S", 3.0)
    downscale_delay_s = _env_float_profile("SERVE_DOWNSCALE_DELAY_S", 60.0)
    batch_max_size = _env_int_profile("SERVE_BATCH_MAX_SIZE", 16)
    batch_wait_timeout_s = _env_float_profile("SERVE_BATCH_WAIT_TIMEOUT_S", 0.005)
    serve_health_check_period_s = _env_float_profile("SERVE_HEALTH_CHECK_PERIOD_S", 10.0)
    serve_health_check_timeout_s = _env_float_profile("SERVE_HEALTH_CHECK_TIMEOUT_S", 30.0)
    serve_graceful_shutdown_wait_loop_s = _env_float_profile(
        "SERVE_GRACEFUL_SHUTDOWN_WAIT_LOOP_S", 2.0
    )
    serve_graceful_shutdown_timeout_s = _env_float_profile(
        "SERVE_GRACEFUL_SHUTDOWN_TIMEOUT_S", 20.0
    )

    ort_intra_op_num_threads = _env_int_profile("ORT_INTRA_OP_NUM_THREADS", 1)
    ort_inter_op_num_threads = _env_int_profile("ORT_INTER_OP_NUM_THREADS", 1)
    ort_providers = _env_tuple("ORT_PROVIDERS", ("CPUExecutionProvider",))
    ort_log_severity_level = _env_int("ORT_LOG_SEVERITY_LEVEL", 3)

    otel_endpoint = _env_str(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://signoz-otel-collector.signoz.svc.cluster.local:4317",
    )
    otel_timeout_seconds = _env_otlp_timeout_seconds()
    otel_metric_export_interval_ms = _env_int_any(
        ("OTEL_METRIC_EXPORT_INTERVAL", "OTEL_METRIC_EXPORT_INTERVAL_MS"),
        15000,
    )
    otel_metric_export_timeout_ms = _env_int_any(
        ("OTEL_METRIC_EXPORT_TIMEOUT", "OTEL_METRIC_EXPORT_TIMEOUT_MS"),
        10000,
    )

    otel_traces_sampler = _env_str(
        "OTEL_TRACES_SAMPLER",
        PROD_DEFAULTS["OTEL_TRACES_SAMPLER"],
    )
    trace_sample_ratio = _env_ratio_profile(
        "OTEL_TRACES_SAMPLER_ARG",
        float(PROD_DEFAULTS["OTEL_TRACES_SAMPLER_ARG"]),
    )

    log_level = _validate_log_level(
        _env_str("LOG_LEVEL", PROD_DEFAULTS["LOG_LEVEL"])
    )
    slow_request_ms = _env_float_profile("SLOW_REQUEST_MS", 500.0)

    return Settings(
        service_name=service_name,
        service_version=service_version,
        deployment_environment=deployment_environment,
        cluster_name=cluster_name,
        instance_id=instance_id,
        deployment_profile=deployment_profile,
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
        serve_health_check_period_s=serve_health_check_period_s,
        serve_health_check_timeout_s=serve_health_check_timeout_s,
        serve_graceful_shutdown_wait_loop_s=serve_graceful_shutdown_wait_loop_s,
        serve_graceful_shutdown_timeout_s=serve_graceful_shutdown_timeout_s,
        ort_intra_op_num_threads=ort_intra_op_num_threads,
        ort_inter_op_num_threads=ort_inter_op_num_threads,
        ort_providers=ort_providers,
        ort_log_severity_level=ort_log_severity_level,
        otel_endpoint=otel_endpoint,
        otel_timeout_seconds=otel_timeout_seconds,
        otel_metric_export_interval_ms=otel_metric_export_interval_ms,
        otel_metric_export_timeout_ms=otel_metric_export_timeout_ms,
        otel_traces_sampler=otel_traces_sampler,
        trace_sample_ratio=trace_sample_ratio,
        log_level=log_level,
        slow_request_ms=slow_request_ms,
    )