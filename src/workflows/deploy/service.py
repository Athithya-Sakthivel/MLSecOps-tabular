from __future__ import annotations

import asyncio
import json
import logging
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

logger = logging.getLogger("tabular-inference")
SETTINGS = get_settings()


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


def _resolve_log_level(level_name: str) -> int:
    level = level_name.strip().upper()
    mapping = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
    }
    return mapping.get(level, logging.INFO)


def configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(_resolve_log_level(SETTINGS.log_level))

    formatter = JsonFormatter()

    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root.addHandler(handler)
    else:
        for handler in root.handlers:
            try:
                handler.setFormatter(formatter)
            except Exception:
                continue

    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


configure_logging()
initialize_telemetry(SETTINGS)

BASE_LOG_FIELDS: dict[str, Any] = {
    "service_name": SETTINGS.service_name,
    "service_version": SETTINGS.service_version,
    "deployment": SETTINGS.serve_deployment_name,
    "model_uri": SETTINGS.model_uri,
}


def _log(logger_obj: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger_obj.log(level, event, extra={**BASE_LOG_FIELDS, **fields})


def _log_exception(logger_obj: logging.Logger, event: str, **fields: Any) -> None:
    logger_obj.exception(event, extra={**BASE_LOG_FIELDS, **fields})


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


def _get_service_instance(request: Request) -> TabularInferenceService | None:
    return getattr(request.app.state, "service_instance", None)


@api.middleware("http")
async def request_metrics_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    request.state.request_id = request_id

    route = request.url.path
    method = request.method
    start = time.perf_counter()
    status_code = 500
    response: Response | None = None

    service = _get_service_instance(request)
    if service is not None:
        service.active_requests.add(1, attributes={"route": route})
        service.http_request_counter.add(
            1,
            attributes={"route": route, "method": method},
        )

    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Request-ID"] = request_id
        return response
    except Exception:
        status_code = 500
        raise
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        if service is not None:
            service.http_duration_histogram.record(
                elapsed_ms,
                attributes={
                    "route": route,
                    "method": method,
                    "status_code": status_code,
                },
            )
            if status_code >= 400:
                service.http_error_counter.add(
                    1,
                    attributes={
                        "route": route,
                        "method": method,
                        "status_code": status_code,
                    },
                )
            service.active_requests.add(-1, attributes={"route": route})


@serve.deployment(
    name=SETTINGS.serve_deployment_name,
    num_replicas="auto",
    autoscaling_config=_serve_autoscaling_config(SETTINGS),
    ray_actor_options={"num_cpus": SETTINGS.replica_num_cpus},
    max_ongoing_requests=SETTINGS.max_ongoing_requests,
    health_check_period_s=SETTINGS.serve_health_check_period_s,
    health_check_timeout_s=SETTINGS.serve_health_check_timeout_s,
    graceful_shutdown_wait_loop_s=SETTINGS.serve_graceful_shutdown_wait_loop_s,
    graceful_shutdown_timeout_s=SETTINGS.serve_graceful_shutdown_timeout_s,
)
@serve.ingress(api)
class TabularInferenceService:
    def __init__(self) -> None:
        self.settings = SETTINGS
        self.logger = logger
        self.tracer = trace.get_tracer("tabular-inference")
        self.meter = metrics.get_meter("tabular-inference")

        self.http_request_counter = self.meter.create_counter(
            name="http.server.request_count",
            description="Total HTTP requests",
            unit="1",
        )
        self.http_error_counter = self.meter.create_counter(
            name="http.server.errors",
            description="Total failed HTTP requests",
            unit="1",
        )
        self.http_duration_histogram = self.meter.create_histogram(
            name="http.server.duration",
            description="HTTP request latency",
            unit="ms",
        )

        self.inference_request_counter = self.meter.create_counter(
            name="inference.requests",
            description="Total inference requests",
            unit="1",
        )
        self.inference_error_counter = self.meter.create_counter(
            name="inference.errors",
            description="Total inference failures",
            unit="1",
        )
        self.inference_duration_histogram = self.meter.create_histogram(
            name="inference.duration",
            description="Model execution latency",
            unit="ms",
        )
        self.inference_batch_size_histogram = self.meter.create_histogram(
            name="inference.batch_size",
            description="Number of instances per inference request",
            unit="1",
        )
        self.active_requests = self.meter.create_up_down_counter(
            name="active.requests",
            description="Number of in-flight HTTP requests",
            unit="1",
        )

        self.loaded_model: LoadedModel = load_loaded_model(self.settings)
        self.session = self.loaded_model.session
        self.schema = self.loaded_model.schema
        self.metadata = self.loaded_model.metadata
        self.effective_model_version = self.metadata.model_version or self.settings.model_version
        self.model_name = self.metadata.model_name or "model"
        self.input_name = self.loaded_model.input_name
        self.output_names = tuple(self.loaded_model.output_names)
        self.feature_order = tuple(self.schema.feature_order)
        self.ort_providers = tuple(self.settings.ort_providers)

        api.state.service_instance = self

        _log(
            self.logger,
            logging.INFO,
            "model_loaded",
            model_name=self.model_name,
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
        try:
            self.check_health()
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        return {
            "status": "ok",
            "service.name": self.settings.service_name,
            "model.name": self.model_name,
            "model.version": self.effective_model_version,
            "model.uri": self.settings.model_uri,
            "model.path": str(self.loaded_model.model_path),
        }

    @serve.batch(
        max_batch_size=SETTINGS.batch_max_size,
        batch_wait_timeout_s=SETTINGS.batch_wait_timeout_s,
    )
    async def _batched_predict(
        self,
        rows_batch: list[list[dict[str, Any]]],
    ) -> list[list[dict[str, Any]]]:
        if not rows_batch:
            return []

        batch_start = time.perf_counter()
        request_count = len(rows_batch)
        batch_instance_count = sum(len(rows) for rows in rows_batch)

        if batch_instance_count == 0:
            return [[] for _ in rows_batch]

        flattened_rows: list[dict[str, Any]] = [row for rows in rows_batch for row in rows]

        with self.tracer.start_as_current_span("onnx_inference") as span:
            span.set_attribute("batch.request_count", request_count)
            span.set_attribute("batch.size", batch_instance_count)
            span.set_attribute("model.name", self.model_name)
            span.set_attribute("model.version", self.effective_model_version)
            span.set_attribute("onnx.input.name", self.input_name)
            span.set_attribute("onnx.provider", ",".join(self.ort_providers))

            feature_matrix = build_feature_matrix(
                flattened_rows,
                self.feature_order,
                allow_extra_features=self.schema.allow_extra_features,
            )

            outputs = await asyncio.to_thread(
                self.session.run,
                self.output_names,
                {self.input_name: feature_matrix},
            )

        inference_ms = (time.perf_counter() - batch_start) * 1000.0
        self.inference_duration_histogram.record(
            inference_ms,
            attributes={
                "model.name": self.model_name,
                "model.version": self.effective_model_version,
            },
        )
        self.inference_batch_size_histogram.record(
            batch_instance_count,
            attributes={
                "model.name": self.model_name,
                "model.version": self.effective_model_version,
            },
        )

        all_predictions = split_model_outputs(
            outputs,
            self.output_names,
            row_count=batch_instance_count,
        )

        partitioned: list[list[dict[str, Any]]] = []
        offset = 0
        for rows in rows_batch:
            row_count = len(rows)
            partitioned.append(all_predictions[offset : offset + row_count])
            offset += row_count

        return partitioned

    @api.post("/predict", response_model=PredictResponse)
    async def predict(self, request: Request) -> PredictResponse:
        route = "/predict"
        start = time.perf_counter()
        n_instances = 0
        inference_started = False

        try:
            payload = await request.json()
            rows = coerce_instances(payload)
            n_instances = len(rows)

            if n_instances < 1:
                raise ValueError("At least one instance is required")
            if n_instances > self.settings.max_instances_per_request:
                raise ValueError(
                    f"Too many instances: {n_instances} > {self.settings.max_instances_per_request}"
                )

            with self.tracer.start_as_current_span("prepare_input") as span:
                span.set_attribute("batch.size", n_instances)
                span.set_attribute("model.name", self.model_name)
                span.set_attribute("model.version", self.effective_model_version)
                span.set_attribute("feature.count", len(self.feature_order))

            self.inference_request_counter.add(
                1,
                attributes={
                    "model.name": self.model_name,
                    "model.version": self.effective_model_version,
                },
            )
            inference_started = True

            predictions_batch = await self._batched_predict(rows)
            predictions = predictions_batch[0] if predictions_batch else []

            total_ms = (time.perf_counter() - start) * 1000.0
            if total_ms >= self.settings.slow_request_ms:
                _log(
                    self.logger,
                    logging.WARNING,
                    "predict_slow_request",
                    route=route,
                    model_name=self.model_name,
                    model_version=self.effective_model_version,
                    n_instances=n_instances,
                    latency_ms=round(total_ms, 3),
                    slow_request_threshold_ms=self.settings.slow_request_ms,
                )

            return PredictResponse(
                model_version=self.effective_model_version,
                n_instances=n_instances,
                predictions=predictions,
            )

        except JSONDecodeError as exc:
            _log(
                self.logger,
                logging.WARNING,
                "predict_invalid_json",
                route=route,
                error_type="invalid_json",
            )
            raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

        except (ValueError, TypeError) as exc:
            if inference_started:
                self.inference_error_counter.add(
                    1,
                    attributes={
                        "model.name": self.model_name,
                        "model.version": self.effective_model_version,
                    },
                )
            _log(
                self.logger,
                logging.WARNING,
                "predict_validation_failed",
                route=route,
                model_name=self.model_name,
                model_version=self.effective_model_version,
                error_type="validation_error",
                error_message=str(exc),
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        except HTTPException:
            raise

        except Exception as exc:
            if inference_started:
                self.inference_error_counter.add(
                    1,
                    attributes={
                        "model.name": self.model_name,
                        "model.version": self.effective_model_version,
                    },
                )
            _log_exception(
                self.logger,
                "predict_internal_failure",
                route=route,
                model_name=self.model_name,
                model_version=self.effective_model_version,
                error_type=exc.__class__.__name__,
            )
            raise HTTPException(status_code=500, detail="Inference failed") from exc


app = TabularInferenceService.bind()


if __name__ == "__main__":
    serve.run(app)
