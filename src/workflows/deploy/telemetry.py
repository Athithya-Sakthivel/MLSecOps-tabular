from __future__ import annotations

import atexit
import json
import logging
import os
import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from config import Settings
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

try:
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
except ImportError:  # pragma: no cover
    from opentelemetry.exporter.otlp.proto.grpc.log_exporter import OTLPLogExporter  # type: ignore

from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

logger = logging.getLogger(__name__)
logger.propagate = True

_STATE_LOCK = threading.Lock()
_INITIALIZED = False
_STATE_KEY: tuple[Any, ...] | None = None
_HANDLE: TelemetryHandle | None = None
_ATEEXIT_REGISTERED = False


def _clean_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_level_name(raw: str | None, default: str = "INFO") -> str:
    level = (_clean_str(raw) or default).upper()
    aliases = {
        "WARN": "WARNING",
        "EXCEPTION": "ERROR",
    }
    level = aliases.get(level, level)
    valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    if level not in valid:
        raise ValueError(f"invalid LOG_LEVEL: {raw!r}")
    return level


def _level_to_int(level_name: str) -> int:
    return getattr(logging, level_name, logging.INFO)


def _current_span_fields() -> dict[str, str]:
    try:
        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx is None or not ctx.is_valid:
            return {}
        return {
            "trace_id": f"{ctx.trace_id:032x}",
            "span_id": f"{ctx.span_id:016x}",
        }
    except Exception:
        return {}


def _json_log(level_label: str, event: str, message: str, **fields: Any) -> str:
    payload: dict[str, Any] = {
        "component": "telemetry",
        "event": event,
        "level": level_label,
        "message": message,
        **{k: v for k, v in fields.items() if v is not None},
    }
    payload.update(_current_span_fields())
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _log_info(event: str, message: str, **fields: Any) -> None:
    logger.info(_json_log("INFO", event, message, **fields))


def _log_warn(event: str, message: str, **fields: Any) -> None:
    logger.warning(_json_log("WARN", event, message, **fields))


def _log_error(event: str, message: str, **fields: Any) -> None:
    logger.error(_json_log("ERROR", event, message, **fields))


def _log_exception(event: str, message: str, **fields: Any) -> None:
    logger.exception(_json_log("EXCEPTION", event, message, **fields))


def _grpc_endpoint(endpoint: str) -> tuple[str, bool]:
    """
    Normalize an OTLP/gRPC endpoint.

    Accepted forms:
      - collector:4317
      - http://collector:4317
      - https://collector:4317

    This module is intentionally gRPC-only. If a path is supplied, fail fast
    instead of silently guessing protocol semantics.
    """
    raw = endpoint.strip()
    if not raw:
        raise ValueError("otel_endpoint must be set")

    parsed = urlparse(raw if "://" in raw else f"//{raw}", scheme="http")

    if parsed.scheme not in ("", "http", "https"):
        raise ValueError(f"unsupported otel endpoint scheme: {parsed.scheme!r}")

    if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(
            "otel_endpoint looks like OTLP/HTTP; this module is configured for OTLP/gRPC only"
        )

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
    if num > 1.0:
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


def _effective_log_level(settings: Settings) -> str:
    return _normalize_level_name(
        os.getenv("LOG_LEVEL") or getattr(settings, "log_level", None) or "INFO"
    )


def _config_key(
    settings: Settings,
    endpoint: str,
    insecure: bool,
    resource_attrs: dict[str, str],
    log_level_name: str,
) -> tuple[Any, ...]:
    return (
        endpoint,
        insecure,
        tuple(sorted(resource_attrs.items())),
        _require_ratio("trace_sample_ratio", settings.trace_sample_ratio),
        _require_positive_number("otel_timeout_seconds", settings.otel_timeout_seconds),
        _require_positive_int("otel_metric_export_interval_ms", settings.otel_metric_export_interval_ms),
        _require_positive_int("otel_metric_export_timeout_ms", settings.otel_metric_export_timeout_ms),
        log_level_name,
    )


class _DropOpenTelemetryRecords(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not record.name.startswith("opentelemetry")


@dataclass
class TelemetryHandle:
    tracer_provider: TracerProvider
    meter_provider: MeterProvider
    logger_provider: LoggerProvider
    root_handler: logging.Handler
    log_level_name: str
    endpoint: str
    insecure: bool
    _closed: bool = False

    def shutdown(self) -> None:
        global _HANDLE, _STATE_KEY

        with _STATE_LOCK:
            if self._closed:
                return
            self._closed = True

            _log_info(
                event="telemetry.shutdown.start",
                message="shutting down telemetry providers",
                endpoint=self.endpoint,
                insecure=self.insecure,
            )

            root_logger = logging.getLogger()
            if self.root_handler in root_logger.handlers:
                root_logger.removeHandler(self.root_handler)

            try:
                self.root_handler.flush()
            except Exception:
                _log_warn(
                    event="telemetry.shutdown.flush_failed",
                    message="root handler flush failed",
                    endpoint=self.endpoint,
                    insecure=self.insecure,
                )

            try:
                self.root_handler.close()
            except Exception:
                _log_warn(
                    event="telemetry.shutdown.close_failed",
                    message="root handler close failed",
                    endpoint=self.endpoint,
                    insecure=self.insecure,
                )

            for provider_name, provider in (
                ("logger_provider", self.logger_provider),
                ("meter_provider", self.meter_provider),
                ("tracer_provider", self.tracer_provider),
            ):
                try:
                    provider.shutdown()
                    _log_info(
                        event="telemetry.shutdown.provider_complete",
                        message="provider shutdown complete",
                        provider=provider_name,
                        endpoint=self.endpoint,
                        insecure=self.insecure,
                    )
                except Exception:
                    _log_warn(
                        event="telemetry.shutdown.provider_failed",
                        message="provider shutdown failed",
                        provider=provider_name,
                        endpoint=self.endpoint,
                        insecure=self.insecure,
                    )

            if _HANDLE is self:
                _HANDLE = None
                _STATE_KEY = None

            _log_info(
                event="telemetry.shutdown.complete",
                message="telemetry shutdown complete",
                endpoint=self.endpoint,
                insecure=self.insecure,
            )


def initialize_telemetry(settings: Settings) -> TelemetryHandle:
    global _INITIALIZED, _HANDLE, _STATE_KEY, _ATEEXIT_REGISTERED

    with _STATE_LOCK:
        log_level_name = _effective_log_level(settings)
        endpoint, insecure = _grpc_endpoint(settings.otel_endpoint)
        resource = _resource(settings)
        resource_attrs = {
            k: v for k, v in resource.attributes.items() if isinstance(k, str) and isinstance(v, str)
        }
        config_key = _config_key(settings, endpoint, insecure, resource_attrs, log_level_name)

        if _HANDLE is not None:
            if _STATE_KEY == config_key:
                _log_info(
                    event="telemetry.initialize.idempotent_hit",
                    message="telemetry already initialized with identical config",
                    endpoint=endpoint,
                    insecure=insecure,
                    log_level=log_level_name,
                )
                return _HANDLE
            raise RuntimeError("telemetry was already initialized with a different configuration")

        partial: dict[str, Any] = {}
        _log_info(
            event="telemetry.initialize.start",
            message="starting telemetry initialization",
            service_name=resource_attrs.get("service.name"),
            service_version=resource_attrs.get("service.version"),
            deployment_environment=resource_attrs.get("deployment.environment"),
            cluster_name=resource_attrs.get("k8s.cluster.name"),
            instance_id=resource_attrs.get("service.instance.id"),
            endpoint=endpoint,
            insecure=insecure,
            log_level=log_level_name,
            trace_sample_ratio=float(settings.trace_sample_ratio),
            otel_timeout_seconds=float(settings.otel_timeout_seconds),
            otel_metric_export_interval_ms=int(settings.otel_metric_export_interval_ms),
            otel_metric_export_timeout_ms=int(settings.otel_metric_export_timeout_ms),
        )

        try:
            tracer_provider = TracerProvider(
                resource=resource,
                sampler=ParentBased(root=TraceIdRatioBased(_require_ratio("trace_sample_ratio", settings.trace_sample_ratio))),
            )
            tracer_provider.add_span_processor(
                BatchSpanProcessor(
                    OTLPSpanExporter(
                        endpoint=endpoint,
                        insecure=insecure,
                        timeout=_require_positive_number("otel_timeout_seconds", settings.otel_timeout_seconds),
                    )
                )
            )
            trace.set_tracer_provider(tracer_provider)
            partial["tracer_provider"] = tracer_provider
            _log_info(
                event="telemetry.traces.configured",
                message="tracing configured",
                endpoint=endpoint,
                insecure=insecure,
            )

            metric_exporter = OTLPMetricExporter(
                endpoint=endpoint,
                insecure=insecure,
                timeout=_require_positive_number("otel_timeout_seconds", settings.otel_timeout_seconds),
            )
            meter_provider = MeterProvider(
                resource=resource,
                metric_readers=[
                    PeriodicExportingMetricReader(
                        metric_exporter,
                        export_interval_millis=_require_positive_int(
                            "otel_metric_export_interval_ms", settings.otel_metric_export_interval_ms
                        ),
                        export_timeout_millis=_require_positive_int(
                            "otel_metric_export_timeout_ms", settings.otel_metric_export_timeout_ms
                        ),
                    )
                ],
            )
            metrics.set_meter_provider(meter_provider)
            partial["meter_provider"] = meter_provider
            _log_info(
                event="telemetry.metrics.configured",
                message="metrics configured",
                endpoint=endpoint,
                insecure=insecure,
            )

            logger_provider = LoggerProvider(resource=resource)
            logger_provider.add_log_record_processor(
                BatchLogRecordProcessor(
                    OTLPLogExporter(
                        endpoint=endpoint,
                        insecure=insecure,
                        timeout=_require_positive_number("otel_timeout_seconds", settings.otel_timeout_seconds),
                    )
                )
            )
            set_logger_provider(logger_provider)
            partial["logger_provider"] = logger_provider
            _log_info(
                event="telemetry.logs.configured",
                message="logs provider configured",
                endpoint=endpoint,
                insecure=insecure,
            )

            LoggingInstrumentor().instrument(set_logging_format=False)
            _log_info(
                event="telemetry.python_logging.instrumented",
                message="python logging instrumentor enabled",
                endpoint=endpoint,
                insecure=insecure,
            )

            root_logger = logging.getLogger()
            root_logger.setLevel(_level_to_int(log_level_name))
            root_logger.propagate = True

            existing_handler = next(
                (handler for handler in root_logger.handlers if getattr(handler, "_otel_handler", False)),
                None,
            )
            if existing_handler is None:
                otel_handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
                otel_handler._otel_handler = True  # sentinel for idempotency
                otel_handler.addFilter(_DropOpenTelemetryRecords())
                root_logger.addHandler(otel_handler)
                root_handler = otel_handler
                _log_info(
                    event="telemetry.root_handler.attached",
                    message="otel log handler attached to root logger",
                    endpoint=endpoint,
                    insecure=insecure,
                )
            else:
                root_handler = existing_handler
                _log_warn(
                    event="telemetry.root_handler.reused",
                    message="existing otel log handler found and reused",
                    endpoint=endpoint,
                    insecure=insecure,
                )

            handle = TelemetryHandle(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
                logger_provider=logger_provider,
                root_handler=root_handler,
                log_level_name=log_level_name,
                endpoint=endpoint,
                insecure=insecure,
            )

            _HANDLE = handle
            _STATE_KEY = config_key
            _INITIALIZED = True

            if not _ATEEXIT_REGISTERED:
                atexit.register(handle.shutdown)
                _ATEEXIT_REGISTERED = True

            _log_info(
                event="telemetry.initialize.complete",
                message="telemetry initialization complete",
                endpoint=endpoint,
                insecure=insecure,
                log_level=log_level_name,
            )
            return handle

        except Exception as exc:
            _log_exception(
                event="telemetry.initialize.failed",
                message="telemetry initialization failed",
                endpoint=endpoint,
                insecure=insecure,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            for provider in (
                partial.get("logger_provider"),
                partial.get("meter_provider"),
                partial.get("tracer_provider"),
            ):
                try:
                    if provider is not None:
                        provider.shutdown()
                except Exception:
                    pass
            raise