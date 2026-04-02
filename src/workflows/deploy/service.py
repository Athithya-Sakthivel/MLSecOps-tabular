from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from json import JSONDecodeError
from typing import Any, ClassVar

from config import Settings, get_settings
from fastapi import FastAPI, HTTPException, Request, Response
from model_store import LoadedModel, load_loaded_model
from opentelemetry import metrics, trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel, ConfigDict
from ray import serve
from schemas import build_feature_matrix, coerce_instances, split_model_outputs
from telemetry import initialize_telemetry

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()

_LEVEL_MAP: dict[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


def _resolve_log_level(level_name: str) -> int:
    return _LEVEL_MAP.get(level_name, logging.INFO)


class JsonFormatter(logging.Formatter):
    _standard_attrs: ClassVar[set[str]] = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key in self._standard_attrs or key.startswith("_"):
                continue
            if value is None:
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(_resolve_log_level(LOG_LEVEL))

    formatter = JsonFormatter()

    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root.addHandler(handler)
    else:
        for handler in root.handlers:
            handler.setFormatter(formatter)

    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)


configure_logging()

SETTINGS = get_settings()
initialize_telemetry(SETTINGS)

BASE_LOG_FIELDS: dict[str, Any] = {
    "service_name": SETTINGS.service_name,
    "service_version": SETTINGS.service_version,
    "deployment": SETTINGS.serve_deployment_name,
    "model_uri": SETTINGS.model_uri,
}


def _log(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger.log(level, event, extra={**BASE_LOG_FIELDS, **fields})


def _log_exception(logger: logging.Logger, event: str, **fields: Any) -> None:
    logger.exception(event, extra={**BASE_LOG_FIELDS, **fields})


api = FastAPI(title="tabular-inference-api", version=SETTINGS.service_version)

if not getattr(api.state, "otel_instrumented", False):
    FastAPIInstrumentor.instrument_app(api)
    api.state.otel_instrumented = True


class PredictResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model_version: str
    n_instances: int
    predictions: list[dict[str, Any]]


def _serve_autoscaling_config(settings: Settings) -> dict[str, Any]:
    return {
        "min_replicas": settings.min_replicas,
        "initial_replicas": settings.initial_replicas,
        "max_replicas": settings.max_replicas,
        "target_ongoing_requests": settings.target_ongoing_requests,
        "upscale_delay_s": settings.upscale_delay_s,
        "downscale_delay_s": settings.downscale_delay_s,
    }


@api.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    request.state.request_id = request_id

    route = request.url.path
    method = request.method
    client_ip = request.client.host if request.client else None
    start = time.perf_counter()
    should_log = route not in {"/healthz", "/readyz"}

    if should_log:
        _log(
            logging.getLogger("tabular-inference"),
            logging.INFO,
            "http_request_started",
            request_id=request_id,
            route=route,
            method=method,
            client_ip=client_ip,
            user_agent=request.headers.get("user-agent"),
            content_type=request.headers.get("content-type"),
        )

    try:
        response = await call_next(request)
    except Exception:
        if should_log:
            _log_exception(
                logging.getLogger("tabular-inference"),
                "http_request_failed",
                request_id=request_id,
                route=route,
                method=method,
                client_ip=client_ip,
                elapsed_ms=round((time.perf_counter() - start) * 1000.0, 3),
            )
        raise

    if should_log:
        _log(
            logging.getLogger("tabular-inference"),
            logging.INFO,
            "http_request_completed",
            request_id=request_id,
            route=route,
            method=method,
            client_ip=client_ip,
            status_code=response.status_code,
            elapsed_ms=round((time.perf_counter() - start) * 1000.0, 3),
        )

    response.headers["X-Request-ID"] = request_id
    return response


@serve.deployment(
    name=SETTINGS.serve_deployment_name,
    num_replicas="auto",
    autoscaling_config=_serve_autoscaling_config(SETTINGS),
    ray_actor_options={"num_cpus": SETTINGS.replica_num_cpus},
    max_ongoing_requests=SETTINGS.max_ongoing_requests,
    health_check_period_s=getattr(SETTINGS, "serve_health_check_period_s", 10.0),
    health_check_timeout_s=getattr(SETTINGS, "serve_health_check_timeout_s", 30.0),
    graceful_shutdown_wait_loop_s=getattr(
        SETTINGS, "serve_graceful_shutdown_wait_loop_s", 2.0
    ),
    graceful_shutdown_timeout_s=getattr(
        SETTINGS, "serve_graceful_shutdown_timeout_s", 20.0
    ),
)
@serve.ingress(api)
class TabularInferenceService:
    def __init__(self) -> None:
        self.settings = SETTINGS
        self.logger = logging.getLogger("tabular-inference")
        self.tracer = trace.get_tracer("tabular-inference")
        self.meter = metrics.get_meter("tabular-inference")

        self.request_counter = self.meter.create_counter(
            name="http_requests_total",
            description="Total inference requests",
            unit="1",
        )
        self.error_counter = self.meter.create_counter(
            name="http_errors_total",
            description="Total inference errors",
            unit="1",
        )
        self.latency_histogram = self.meter.create_histogram(
            name="inference_latency_ms",
            description="End-to-end inference latency",
            unit="ms",
        )
        self.model_latency_histogram = self.meter.create_histogram(
            name="model_inference_latency_ms",
            description="Model execution latency for a batch",
            unit="ms",
        )
        self.batch_size_histogram = self.meter.create_histogram(
            name="prediction_batch_size",
            description="Number of rows processed per model batch",
            unit="1",
        )
        self.active_requests = self.meter.create_up_down_counter(
            name="active_requests",
            description="Number of in-flight HTTP requests",
            unit="1",
        )

        self.loaded_model: LoadedModel = load_loaded_model(self.settings)
        self.session = self.loaded_model.session
        self.schema = self.loaded_model.schema
        self.metadata = self.loaded_model.metadata
        self.effective_model_version = (
            self.metadata.model_version or self.settings.model_version
        )
        self.input_name = self.loaded_model.input_name
        self.output_names = tuple(self.loaded_model.output_names)
        self.feature_order = tuple(self.schema.feature_order)
        self.ort_providers = tuple(self.settings.ort_providers)

        _log(
            self.logger,
            logging.INFO,
            "model_loaded",
            model_version=self.effective_model_version,
            model_path=str(self.loaded_model.model_path),
            input_name=self.input_name,
            output_names=list(self.output_names),
            feature_count=len(self.feature_order),
            ort_providers=list(self.ort_providers),
        )

    def check_health(self) -> None:
        if self.session is None:
            raise RuntimeError("model session is not initialized")
        if not self.input_name:
            raise RuntimeError("model input name is missing")
        if not self.output_names:
            raise RuntimeError("model output names are missing")
        if not self.feature_order:
            raise RuntimeError("feature order is missing")

    @api.get("/healthz")
    async def healthz(self) -> dict[str, str]:
        return {"status": "ok"}

    @api.get("/readyz")
    async def readyz(self) -> dict[str, Any]:
        self.check_health()
        return {
            "status": "ok",
            "service.name": self.settings.service_name,
            "model.version": self.effective_model_version,
            "model.uri": self.settings.model_uri,
            "model.path": str(self.loaded_model.model_path),
        }

    @serve.batch(
        max_batch_size=SETTINGS.batch_max_size,
        batch_wait_timeout_s=SETTINGS.batch_wait_timeout_s,
    )
    async def _predict_row_batch(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        batch_start = time.perf_counter()
        batch_id = uuid.uuid4().hex

        _log(
            self.logger,
            logging.INFO,
            "model_batch_started",
            batch_id=batch_id,
            batch_size=len(rows),
            model_version=self.effective_model_version,
        )

        with self.tracer.start_as_current_span("prepare_input") as span:
            feature_matrix = build_feature_matrix(
                rows,
                self.feature_order,
                allow_extra_features=self.schema.allow_extra_features,
            )
            span.set_attribute("batch.size", len(rows))
            span.set_attribute("model.version", self.effective_model_version)
            span.set_attribute("feature.count", len(self.feature_order))

        with self.tracer.start_as_current_span("onnx_inference") as span:
            outputs = self.session.run(self.output_names, {self.input_name: feature_matrix})
            span.set_attribute("batch.size", len(rows))
            span.set_attribute("onnx.input.name", self.input_name)
            span.set_attribute("onnx.provider", ",".join(self.ort_providers))

        batch_ms = (time.perf_counter() - batch_start) * 1000.0
        self.model_latency_histogram.record(
            batch_ms,
            attributes={
                "model": self.effective_model_version,
                "batch_size": len(rows),
            },
        )
        self.batch_size_histogram.record(
            len(rows),
            attributes={"model": self.effective_model_version},
        )

        _log(
            self.logger,
            logging.INFO,
            "model_batch_completed",
            batch_id=batch_id,
            batch_size=len(rows),
            model_version=self.effective_model_version,
            latency_ms=round(batch_ms, 3),
        )

        return split_model_outputs(outputs, self.output_names, row_count=len(rows))

    @api.post("/predict", response_model=PredictResponse)
    async def predict(self, request: Request, response: Response) -> PredictResponse:
        route = "/predict"
        start = time.perf_counter()
        request_id = getattr(request.state, "request_id", None) or uuid.uuid4().hex

        self.active_requests.add(1, attributes={"route": route})
        self.request_counter.add(1, attributes={"route": route, "method": "POST"})

        status = "ok"
        error_type = "none"
        n_instances = 0

        try:
            payload = await request.json()

            _log(
                self.logger,
                logging.INFO,
                "predict_payload_received",
                request_id=request_id,
                route=route,
                payload_type=type(payload).__name__,
            )

            rows = coerce_instances(payload)
            n_instances = len(rows)

            _log(
                self.logger,
                logging.INFO,
                "predict_payload_coerced",
                request_id=request_id,
                route=route,
                n_instances=n_instances,
                max_instances_per_request=self.settings.max_instances_per_request,
            )

            if n_instances > self.settings.max_instances_per_request:
                status = "error"
                error_type = "validation_error"
                _log(
                    self.logger,
                    logging.WARN,
                    "predict_rejected_too_many_instances",
                    request_id=request_id,
                    route=route,
                    n_instances=n_instances,
                    max_instances_per_request=self.settings.max_instances_per_request,
                )
                raise ValueError(
                    f"Too many instances: {n_instances} > "
                    f"{self.settings.max_instances_per_request}"
                )

            _log(
                self.logger,
                logging.INFO,
                "predict_batch_submission_started",
                request_id=request_id,
                route=route,
                row_count=n_instances,
            )

            predictions = await asyncio.gather(
                *(self._predict_row_batch(row) for row in rows)
            )

            total_ms = (time.perf_counter() - start) * 1000.0
            if total_ms >= self.settings.slow_request_ms:
                _log(
                    self.logger,
                    logging.WARN,
                    "predict_slow_request",
                    request_id=request_id,
                    route=route,
                    model_version=self.effective_model_version,
                    n_instances=n_instances,
                    latency_ms=round(total_ms, 3),
                    slow_request_threshold_ms=self.settings.slow_request_ms,
                )

            _log(
                self.logger,
                logging.INFO,
                "predict_completed",
                request_id=request_id,
                route=route,
                model_version=self.effective_model_version,
                n_instances=n_instances,
                latency_ms=round(total_ms, 3),
            )

            response.headers["X-Request-ID"] = request_id
            return PredictResponse(
                model_version=self.effective_model_version,
                n_instances=n_instances,
                predictions=predictions,
            )

        except JSONDecodeError as exc:
            status = "error"
            error_type = "invalid_json"
            self.error_counter.add(
                1,
                attributes={"route": route, "error_type": error_type},
            )
            _log(
                self.logger,
                logging.WARN,
                "predict_invalid_json",
                request_id=request_id,
                route=route,
                error_type=error_type,
            )
            raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

        except HTTPException:
            status = "error"
            error_type = "http_exception"
            self.error_counter.add(
                1,
                attributes={"route": route, "error_type": error_type},
            )
            _log(
                self.logger,
                logging.ERROR,
                "predict_http_exception",
                request_id=request_id,
                route=route,
                model_version=self.effective_model_version,
            )
            raise

        except (ValueError, TypeError) as exc:
            status = "error"
            error_type = "validation_error"
            self.error_counter.add(
                1,
                attributes={"route": route, "error_type": error_type},
            )
            _log(
                self.logger,
                logging.WARN,
                "predict_validation_failed",
                request_id=request_id,
                route=route,
                model_version=self.effective_model_version,
                error_message=str(exc),
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        except Exception as exc:
            status = "error"
            error_type = "internal_error"
            self.error_counter.add(
                1,
                attributes={"route": route, "error_type": error_type},
            )
            _log_exception(
                self.logger,
                "predict_internal_failure",
                request_id=request_id,
                route=route,
                model_version=self.effective_model_version,
                error_type=exc.__class__.__name__,
            )
            raise HTTPException(status_code=500, detail="Inference failed") from exc

        finally:
            total_ms = (time.perf_counter() - start) * 1000.0
            self.latency_histogram.record(
                total_ms,
                attributes={
                    "route": route,
                    "model": self.effective_model_version,
                    "status": status,
                    "error_type": error_type,
                },
            )
            self.active_requests.add(-1, attributes={"route": route})


app = TabularInferenceService.bind()


if __name__ == "__main__":
    serve.run(app)