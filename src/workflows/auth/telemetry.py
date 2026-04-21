from __future__ import annotations

import atexit
import logging
import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

try:
    from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON, ParentBased, TraceIdRatioBased
except ImportError:  # pragma: no cover
    from opentelemetry.sdk.trace.sampling import AlwaysOffSampler, AlwaysOnSampler, ParentBased, TraceIdRatioBased

    ALWAYS_ON = AlwaysOnSampler()
    ALWAYS_OFF = AlwaysOffSampler()

from settings import Settings

logger = logging.getLogger(__name__)

_STATE_LOCK = threading.Lock()
_HANDLE: TelemetryHandle | None = None
_STATE_KEY: tuple[Any, ...] | None = None
_ATEEXIT_REGISTERED = False


def _clean_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_sampler_name(raw: str | None) -> str:
    return (_clean_str(raw) or "parentbased_traceidratio").strip().lower()


def _require_positive_float(name: str, value: object | None, default: float) -> float:
    if value is None:
        return default
    try:
        num = float(value)
    except Exception:
        return default
    if num <= 0:
        return default
    return num


def _require_ratio(value: object | None, default: float) -> float:
    if value is None:
        return default
    try:
        num = float(value)
    except Exception:
        return default
    if not 0.0 <= num <= 1.0:
        return default
    return num


def _grpc_endpoint(endpoint: str | None) -> tuple[str | None, bool]:
    raw = _clean_str(endpoint)
    if not raw:
        return None, True

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
        clean_attrs["service.name"] = "auth-service"
    return Resource.create(clean_attrs)


def _build_sampler(settings: Settings):
    sampler = _normalize_sampler_name(getattr(settings, "otel_traces_sampler", None))
    ratio = _require_ratio(
        getattr(settings, "trace_sample_ratio", None),
        _require_ratio(getattr(settings, "otel_traces_sampler_arg", None), 0.1),
    )

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

    return ParentBased(root=TraceIdRatioBased(ratio))


def _config_key(
    settings: Settings,
    endpoint: str | None,
    insecure: bool,
    resource_attrs: dict[str, str],
) -> tuple[Any, ...]:
    return (
        endpoint,
        insecure,
        tuple(sorted(resource_attrs.items())),
        _normalize_sampler_name(getattr(settings, "otel_traces_sampler", None)),
        _require_ratio(
            getattr(settings, "trace_sample_ratio", None),
            _require_ratio(getattr(settings, "otel_traces_sampler_arg", None), 0.1),
        ),
        _require_positive_float("otel_timeout_seconds", getattr(settings, "otel_timeout_seconds", None), 10.0),
    )


@dataclass
class TelemetryHandle:
    tracer_provider: TracerProvider
    endpoint: str | None
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
                logger.exception("telemetry_shutdown_failed")

            if _HANDLE is self:
                _HANDLE = None
                _STATE_KEY = None


def initialize_telemetry(settings: Settings) -> TelemetryHandle | None:
    """
    Initialize OTEL tracing for the auth service.

    This is intentionally non-fatal:
    - If OTEL_EXPORTER_OTLP_ENDPOINT is missing, it returns None.
    - If exporter setup fails, it logs and returns None.
    - Repeated calls with the same configuration return the existing handle.
    """
    global _HANDLE, _STATE_KEY, _ATEEXIT_REGISTERED

    raw_endpoint = getattr(settings, "otel_endpoint", None)
    if not raw_endpoint:
        return None

    with _STATE_LOCK:
        try:
            endpoint, insecure = _grpc_endpoint(raw_endpoint)
            resource = _resource(settings)
            resource_attrs = {
                k: v for k, v in resource.attributes.items() if isinstance(k, str) and isinstance(v, str)
            }
            config_key = _config_key(settings, endpoint, insecure, resource_attrs)

            if _HANDLE is not None:
                if _STATE_KEY == config_key:
                    return _HANDLE
                return _HANDLE

            timeout_seconds = _require_positive_float(
                "otel_timeout_seconds",
                getattr(settings, "otel_timeout_seconds", None),
                10.0,
            )
            sampler_name = _normalize_sampler_name(getattr(settings, "otel_traces_sampler", None))
            sample_ratio = _require_ratio(
                getattr(settings, "trace_sample_ratio", None),
                _require_ratio(getattr(settings, "otel_traces_sampler_arg", None), 0.1),
            )

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

        except Exception as exc:
            logger.warning("telemetry initialization skipped: %s", exc, exc_info=True)
            return None