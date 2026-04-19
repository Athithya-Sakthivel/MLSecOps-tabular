from __future__ import annotations

import atexit
import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON, ParentBased, TraceIdRatioBased
from settings import Settings

_STATE_LOCK = threading.Lock()
_HANDLE: TelemetryHandle | None = None
_STATE_KEY: tuple[Any, ...] | None = None
_ATEEXIT_REGISTERED = False


def _clean_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _require_positive_number(name: str, value: object) -> float:
    try:
        num = float(value)
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"{name} must be numeric") from exc
    if num <= 0:
        raise ValueError(f"{name} must be > 0")
    return num


def _require_ratio(name: str, value: object) -> float:
    num = _require_positive_number(name, value)
    if not 0.0 <= num <= 1.0:
        raise ValueError(f"{name} must be between 0.0 and 1.0")
    return num


def _require_positive_int(name: str, value: object) -> int:
    try:
        num = int(value)
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"{name} must be an integer") from exc
    if num <= 0:
        raise ValueError(f"{name} must be > 0")
    return num


def _normalize_sampler_name(raw: str | None) -> str:
    return (_clean_str(raw) or "parentbased_traceidratio").strip().lower()


def _grpc_endpoint(endpoint: str) -> tuple[str, bool]:
    raw = endpoint.strip()
    if not raw:
        raise ValueError("otel_endpoint must be set")

    parsed = urlparse(raw if "://" in raw else f"//{raw}", scheme="http")

    if parsed.scheme not in ("", "http", "https"):
        raise ValueError(f"unsupported otel endpoint scheme: {parsed.scheme!r}")

    if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
        raise ValueError("otel_endpoint must point to OTLP/gRPC, not OTLP/HTTP")

    authority = (parsed.netloc or parsed.path).rstrip("/")
    if not authority:
        raise ValueError("otel_endpoint is invalid")

    insecure = parsed.scheme != "https"
    return authority, insecure


def _resource(settings: Settings) -> Resource:
    attrs = {
        "service.name": _clean_str(settings.service_name),
        "service.version": _clean_str(settings.service_version),
        "deployment.environment": _clean_str(settings.deployment_environment),
        "k8s.cluster.name": _clean_str(settings.cluster_name),
        "service.instance.id": _clean_str(settings.instance_id),
    }
    clean_attrs = {k: v for k, v in attrs.items() if v is not None}
    if not clean_attrs.get("service.name"):
        raise ValueError("service_name must be set")
    return Resource.create(clean_attrs)


def _build_sampler(settings: Settings):
    sampler = _normalize_sampler_name(settings.otel_traces_sampler)
    ratio = _require_ratio("trace_sample_ratio", settings.trace_sample_ratio)

    if sampler == "always_on":
        return ALWAYS_ON
    if sampler == "always_off":
        return ALWAYS_OFF
    if sampler == "traceidratio":
        return TraceIdRatioBased(ratio)
    if sampler == "parentbased_always_on":
        return ParentBased(root=ALWAYS_ON)
    if sampler == "parentbased_always_off":
        return ParentBased(root=ALWAYS_OFF)
    if sampler == "parentbased_traceidratio":
        return ParentBased(root=TraceIdRatioBased(ratio))

    raise ValueError(
        "unsupported OTEL_TRACES_SAMPLER value: "
        f"{settings.otel_traces_sampler!r}. Supported values: "
        "'always_on', 'always_off', 'traceidratio', "
        "'parentbased_always_on', 'parentbased_always_off', 'parentbased_traceidratio'"
    )


def _config_key(
    settings: Settings,
    endpoint: str,
    insecure: bool,
    resource_attrs: dict[str, str],
) -> tuple[Any, ...]:
    return (
        endpoint,
        insecure,
        tuple(sorted(resource_attrs.items())),
        _normalize_sampler_name(settings.otel_traces_sampler),
        _require_ratio("trace_sample_ratio", settings.trace_sample_ratio),
        _require_positive_number("otel_timeout_seconds", settings.otel_timeout_seconds),
        _require_positive_int("otel_metric_export_interval_ms", settings.otel_metric_export_interval_ms),
        _require_positive_int("otel_metric_export_timeout_ms", settings.otel_metric_export_timeout_ms),
    )


@dataclass
class TelemetryHandle:
    tracer_provider: TracerProvider
    endpoint: str
    insecure: bool
    sampler_name: str
    sample_ratio: float
    timeout_seconds: float
    _closed: bool = False

    def shutdown(self) -> None:
        global _HANDLE, _STATE_KEY

        with _STATE_LOCK:
            if self._closed:
                return
            self._closed = True

            try:
                self.tracer_provider.shutdown()
            except Exception:
                pass

            if _HANDLE is self:
                _HANDLE = None
                _STATE_KEY = None


def initialize_telemetry(settings: Settings) -> TelemetryHandle:
    global _HANDLE, _STATE_KEY, _ATEEXIT_REGISTERED

    with _STATE_LOCK:
        endpoint, insecure = _grpc_endpoint(settings.otel_endpoint)
        resource = _resource(settings)
        resource_attrs = {
            k: v for k, v in resource.attributes.items() if isinstance(k, str) and isinstance(v, str)
        }
        config_key = _config_key(settings, endpoint, insecure, resource_attrs)

        if _HANDLE is not None:
            if _STATE_KEY == config_key:
                return _HANDLE
            raise RuntimeError("telemetry was already initialized with a different configuration")

        timeout_seconds = _require_positive_number("otel_timeout_seconds", settings.otel_timeout_seconds)
        sampler_name = _normalize_sampler_name(settings.otel_traces_sampler)
        sample_ratio = _require_ratio("trace_sample_ratio", settings.trace_sample_ratio)

        tracer_provider = TracerProvider(resource=resource, sampler=_build_sampler(settings))
        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(
                    endpoint=endpoint,
                    insecure=insecure,
                    timeout=timeout_seconds,
                )
            )
        )
        trace.set_tracer_provider(tracer_provider)

        handle = TelemetryHandle(
            tracer_provider=tracer_provider,
            endpoint=endpoint,
            insecure=insecure,
            sampler_name=sampler_name,
            sample_ratio=sample_ratio,
            timeout_seconds=timeout_seconds,
        )
        _HANDLE = handle
        _STATE_KEY = config_key

        if not _ATEEXIT_REGISTERED:
            atexit.register(handle.shutdown)
            _ATEEXIT_REGISTERED = True

        return handle