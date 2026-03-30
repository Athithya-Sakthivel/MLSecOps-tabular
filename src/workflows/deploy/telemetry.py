from __future__ import annotations

import atexit
import logging
from dataclasses import dataclass

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

try:
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
except ImportError:  # pragma: no cover
    from opentelemetry.exporter.otlp.proto.grpc.log_exporter import OTLPLogExporter  # type: ignore
from config import Settings
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler, set_logger_provider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased


def _grpc_endpoint(endpoint: str) -> str:
    endpoint = endpoint.strip()
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    endpoint = endpoint.rstrip("/")
    return endpoint


def _resource(settings: Settings) -> Resource:
    attrs = {
        "service.name": settings.service_name,
        "service.version": settings.service_version,
        "deployment.environment": settings.deployment_environment,
        "k8s.cluster.name": settings.cluster_name,
        "service.instance.id": settings.instance_id,
    }
    return Resource.create(attrs)


@dataclass
class TelemetryHandle:
    tracer_provider: TracerProvider
    meter_provider: MeterProvider
    logger_provider: LoggerProvider
    root_handler: LoggingHandler

    def shutdown(self) -> None:
        for provider in (self.logger_provider, self.meter_provider, self.tracer_provider):
            try:
                provider.shutdown()
            except Exception:
                pass


_INITIALIZED = False
_HANDLE: TelemetryHandle | None = None


def initialize_telemetry(settings: Settings) -> TelemetryHandle:
    global _INITIALIZED, _HANDLE

    if _INITIALIZED and _HANDLE is not None:
        return _HANDLE

    resource = _resource(settings)
    endpoint = _grpc_endpoint(settings.otel_endpoint)

    tracer_provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(root=TraceIdRatioBased(settings.trace_sample_ratio)),
    )
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=endpoint,
                insecure=True,
                timeout=settings.otel_timeout_seconds,
            )
        )
    )
    trace.set_tracer_provider(tracer_provider)

    metric_exporter = OTLPMetricExporter(
        endpoint=endpoint,
        insecure=True,
        timeout=settings.otel_timeout_seconds,
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                metric_exporter,
                export_interval_millis=settings.otel_metric_export_interval_ms,
                export_timeout_millis=settings.otel_metric_export_timeout_ms,
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(
            OTLPLogExporter(
                endpoint=endpoint,
                insecure=True,
                timeout=settings.otel_timeout_seconds,
            )
        )
    )
    set_logger_provider(logger_provider)

    LoggingInstrumentor().instrument(set_logging_format=False)

    root_logger = logging.getLogger()
    root_logger.setLevel(settings.log_level)
    if not any(getattr(handler, "_otel_handler", False) for handler in root_logger.handlers):
        otel_handler = LoggingHandler(level=root_logger.level, logger_provider=logger_provider)
        otel_handler._otel_handler = True
        root_logger.addHandler(otel_handler)

    _HANDLE = TelemetryHandle(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
        root_handler=next(handler for handler in root_logger.handlers if getattr(handler, "_otel_handler", False)),
    )
    _INITIALIZED = True
    atexit.register(_HANDLE.shutdown)
    return _HANDLE
