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

import numpy as np
from config import get_settings
from model_store import LoadedModel, load_loaded_model
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind, Status, StatusCode
from ray import serve
from ray.serve.handle import DeploymentHandle
from schemas import build_feature_matrix, coerce_instances, split_model_outputs
from starlette.requests import Request
from starlette.responses import JSONResponse
from telemetry import initialize_telemetry

SETTINGS = get_settings()

BACKEND_DEPLOYMENT_NAME = f"{SETTINGS.serve_deployment_name}_backend"
INGRESS_DEPLOYMENT_NAME = SETTINGS.serve_deployment_name

# The backend needs CPU for ONNX execution. The ingress is an HTTP shim and
# should not consume scarce CPU on a tiny cluster.
BACKEND_NUM_CPUS = float(SETTINGS.replica_num_cpus)
INGRESS_NUM_CPUS = 0.0

INGRESS_MAX_ONGOING_REQUESTS = max(SETTINGS.max_ongoing_requests, 32)
BACKEND_MAX_ONGOING_REQUESTS = max(SETTINGS.max_ongoing_requests, SETTINGS.batch_max_size)

REQUEST_ID_HEADER = "X-Request-Id"

logger = logging.getLogger("tabular-inference")
BASE_LOG_FIELDS: dict[str, Any] = {
    "service_name": SETTINGS.service_name,
    "service_version": SETTINGS.service_version,
    "deployment": SETTINGS.serve_deployment_name,
    "model_uri": SETTINGS.model_uri,
}


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
        "NOTSET": logging.NOTSET,
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


def _log(logger_obj: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger_obj.log(level, event, extra={**BASE_LOG_FIELDS, **fields})


def _log_exception(logger_obj: logging.Logger, event: str, **fields: Any) -> None:
    logger_obj.exception(event, extra={**BASE_LOG_FIELDS, **fields})


def _json_response(
    payload: Any,
    status_code: int,
    request_id: str | None = None,
) -> JSONResponse:
    headers = {REQUEST_ID_HEADER: request_id} if request_id else None
    return JSONResponse(payload, status_code=status_code, headers=headers)


def _route_key(path: str) -> str:
    cleaned = path.rstrip("/")
    return cleaned if cleaned else "/"


def _autoscaling_config() -> dict[str, Any]:
    return {
        "min_replicas": SETTINGS.min_replicas,
        "initial_replicas": SETTINGS.initial_replicas,
        "max_replicas": SETTINGS.max_replicas,
        "target_ongoing_requests": SETTINGS.target_ongoing_requests,
        "upscale_delay_s": SETTINGS.upscale_delay_s,
        "downscale_delay_s": SETTINGS.downscale_delay_s,
    }


class InferenceBackend:
    def __init__(self) -> None:
        self.settings = SETTINGS
        self.logger = logging.getLogger("tabular-inference.backend")
        self.tracer = trace.get_tracer("tabular-inference-backend")
        self.meter = metrics.get_meter("tabular-inference-backend")
        self._telemetry = initialize_telemetry(self.settings)

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

        self.loaded_model: LoadedModel = load_loaded_model(self.settings)
        self.session = self.loaded_model.session
        self.schema = self.loaded_model.schema
        self.metadata = self.loaded_model.metadata
        self.manifest = self.loaded_model.manifest

        self.effective_model_version = self.metadata.model_version or self.settings.model_version
        self.model_name = self.metadata.model_name or "model"
        self.input_name = self.loaded_model.input_name
        self.output_names = tuple(self.loaded_model.output_names)
        self.feature_order = tuple(self.schema.feature_order)
        self.allow_extra_features = bool(self.schema.allow_extra_features)
        self.ort_providers = tuple(self.settings.ort_providers)
        self.prediction_cap_seconds = min(float(self.metadata.label_cap_seconds), 24.0 * 3600.0)

        _log(
            self.logger,
            logging.INFO,
            "model_loaded",
            model_name=self.model_name,
            model_version=self.effective_model_version,
            schema_version=self.schema.schema_version,
            feature_version=self.schema.feature_version,
            model_path=str(self.loaded_model.model_path),
            input_name=self.input_name,
            output_names=list(self.output_names),
            feature_count=len(self.feature_order),
            ort_providers=list(self.ort_providers),
        )

    async def check_health(self) -> None:
        if self.session is None:
            raise RuntimeError("model session is not initialized")
        if not self.input_name:
            raise RuntimeError("model input name is missing")
        if not self.output_names:
            raise RuntimeError("model output names are missing")
        if not self.feature_order:
            raise RuntimeError("feature order is missing")

    async def ready_summary(self) -> dict[str, Any]:
        await self.check_health()
        return {
            "status": "ok",
            "service_name": self.settings.service_name,
            "model_name": self.model_name,
            "model_version": self.effective_model_version,
            "schema_version": self.schema.schema_version,
            "feature_version": self.schema.feature_version,
            "model_uri": self.settings.model_uri,
            "model_path": str(self.loaded_model.model_path),
            "cache_dir": str(self.loaded_model.cache_dir),
            "feature_order": list(self.feature_order),
            "allow_extra_features": self.allow_extra_features,
            "prediction_cap_seconds": self.prediction_cap_seconds,
            "manifest_model_sha256": self.manifest.model_sha256,
        }

    @serve.batch(
        max_batch_size=SETTINGS.batch_max_size,
        batch_wait_timeout_s=SETTINGS.batch_wait_timeout_s,
    )
    async def predict_batch(self, feature_matrices: list[np.ndarray]) -> list[list[dict[str, Any]]]:
        if not feature_matrices:
            return []

        batch_start = time.perf_counter()
        request_count = len(feature_matrices)
        feature_count = len(self.feature_order)

        matrices: list[np.ndarray] = []
        row_counts: list[int] = []

        for idx, matrix in enumerate(feature_matrices):
            arr = np.asarray(matrix, dtype=np.float32)
            if arr.ndim != 2:
                raise ValueError(f"feature matrix at index {idx} must be 2D")
            if arr.shape[1] != feature_count:
                raise ValueError(
                    f"feature matrix at index {idx} has {arr.shape[1]} features, expected {feature_count}"
                )
            if arr.shape[0] < 1:
                raise ValueError(f"feature matrix at index {idx} must contain at least one row")

            matrices.append(arr)
            row_counts.append(int(arr.shape[0]))

        batch_instance_count = sum(row_counts)
        if batch_instance_count == 0:
            return [[] for _ in feature_matrices]

        combined = np.concatenate(matrices, axis=0)

        with self.tracer.start_as_current_span("onnx_inference") as span:
            span.set_attribute("batch.request_count", request_count)
            span.set_attribute("batch.size", batch_instance_count)
            span.set_attribute("model.name", self.model_name)
            span.set_attribute("model.version", self.effective_model_version)
            span.set_attribute("onnx.input.name", self.input_name)
            span.set_attribute("onnx.provider", ",".join(self.ort_providers))

            try:
                outputs = await asyncio.to_thread(
                    self.session.run,
                    self.output_names,
                    {self.input_name: combined},
                )
            except Exception as exc:
                self.inference_error_counter.add(
                    request_count,
                    attributes={
                        "model.name": self.model_name,
                        "model.version": self.effective_model_version,
                    },
                )
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR))
                _log_exception(
                    self.logger,
                    "predict_batch_failed",
                    model_name=self.model_name,
                    model_version=self.effective_model_version,
                    batch_requests=request_count,
                    batch_size=batch_instance_count,
                    error_type=exc.__class__.__name__,
                )
                raise

        elapsed_ms = (time.perf_counter() - batch_start) * 1000.0
        self.inference_request_counter.add(
            request_count,
            attributes={
                "model.name": self.model_name,
                "model.version": self.effective_model_version,
            },
        )
        self.inference_duration_histogram.record(
            elapsed_ms,
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
        for row_count in row_counts:
            partitioned.append(all_predictions[offset : offset + row_count])
            offset += row_count

        return partitioned


class TabularInferenceApp:
    def __init__(self, backend: DeploymentHandle) -> None:
        self.settings = SETTINGS
        self.backend = backend
        self.logger = logger
        self.tracer = trace.get_tracer("tabular-inference-http")
        self.meter = metrics.get_meter("tabular-inference-http")
        self._telemetry = initialize_telemetry(self.settings)

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
        self.http_active_requests = self.meter.create_up_down_counter(
            name="active.requests",
            description="Number of in-flight HTTP requests",
            unit="1",
        )

    async def _get_backend_info(self) -> dict[str, Any]:
        raw = await self.backend.ready_summary.remote()
        if not isinstance(raw, dict):
            raise RuntimeError("backend ready_summary returned an invalid payload")
        return raw

    async def _handle_ready(self, request_id: str) -> JSONResponse:
        try:
            info = await self._get_backend_info()
        except Exception as exc:
            return _json_response(
                {"detail": f"Backend unavailable: {exc}"},
                503,
                request_id=request_id,
            )

        payload = {
            "status": "ok",
            "service_name": self.settings.service_name,
            "service_version": self.settings.service_version,
            "deployment": self.settings.serve_deployment_name,
            "model_name": info["model_name"],
            "model_version": info["model_version"],
            "schema_version": info["schema_version"],
            "feature_version": info["feature_version"],
            "model_uri": info["model_uri"],
            "model_path": info["model_path"],
            "feature_order": info["feature_order"],
            "allow_extra_features": info["allow_extra_features"],
            "prediction_cap_seconds": info["prediction_cap_seconds"],
        }
        return _json_response(payload, 200, request_id=request_id)

    async def _handle_predict(
        self,
        request: Request,
        request_id: str,
        request_start: float,
    ) -> JSONResponse:
        try:
            payload = await request.json()
        except (JSONDecodeError, ValueError):
            return _json_response({"detail": "Invalid JSON body"}, 400, request_id=request_id)

        try:
            rows = coerce_instances(payload)
            n_instances = len(rows)

            if n_instances < 1:
                raise ValueError("At least one instance is required")
            if n_instances > self.settings.max_instances_per_request:
                raise ValueError(
                    f"Too many instances: {n_instances} > {self.settings.max_instances_per_request}"
                )

            try:
                backend_info = await self._get_backend_info()
            except Exception as exc:
                return _json_response(
                    {"detail": f"Backend unavailable: {exc}"},
                    503,
                    request_id=request_id,
                )

            with self.tracer.start_as_current_span("prepare_input") as span:
                span.set_attribute("batch.size", n_instances)
                span.set_attribute("model.name", backend_info["model_name"])
                span.set_attribute("model.version", backend_info["model_version"])
                span.set_attribute("feature.count", len(backend_info["feature_order"]))
                span.set_attribute("request.id", request_id)

            feature_matrix = build_feature_matrix(
                rows,
                backend_info["feature_order"],
                allow_extra_features=bool(backend_info["allow_extra_features"]),
            )

            try:
                predictions = await self.backend.predict_batch.remote(feature_matrix)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log_exception(
                    self.logger,
                    "predict_backend_failed",
                    route="/predict",
                    request_id=request_id,
                    model_name=backend_info["model_name"],
                    model_version=backend_info["model_version"],
                    error_type=exc.__class__.__name__,
                )
                return _json_response(
                    {"detail": "Inference failed"},
                    500,
                    request_id=request_id,
                )

            total_ms = (time.perf_counter() - request_start) * 1000.0
            if total_ms >= self.settings.slow_request_ms:
                _log(
                    self.logger,
                    logging.WARNING,
                    "predict_slow_request",
                    route="/predict",
                    request_id=request_id,
                    model_name=backend_info["model_name"],
                    model_version=backend_info["model_version"],
                    n_instances=n_instances,
                    latency_ms=round(total_ms, 3),
                    slow_request_threshold_ms=self.settings.slow_request_ms,
                )

            return _json_response(
                {
                    "model_version": backend_info["model_version"],
                    "n_instances": n_instances,
                    "predictions": predictions,
                },
                200,
                request_id=request_id,
            )

        except ValueError as exc:
            backend_info = None
            try:
                backend_info = await self._get_backend_info()
            except Exception:
                backend_info = None

            model_name = backend_info["model_name"] if backend_info else "model"
            model_version = (
                backend_info["model_version"] if backend_info else self.settings.model_version
            )

            _log(
                self.logger,
                logging.WARNING,
                "predict_validation_failed",
                route="/predict",
                request_id=request_id,
                model_name=model_name,
                model_version=model_version,
                error_type="validation_error",
                error_message=str(exc),
            )
            return _json_response(
                {"detail": str(exc)},
                422,
                request_id=request_id,
            )

        except Exception as exc:
            _log_exception(
                self.logger,
                "predict_internal_failure",
                route="/predict",
                request_id=request_id,
                error_type=exc.__class__.__name__,
            )
            return _json_response(
                {"detail": "Inference failed"},
                500,
                request_id=request_id,
            )

    async def __call__(self, request: Request) -> JSONResponse:
        path = _route_key(request.url.path)
        method = request.method.upper()
        request_id = (request.headers.get("x-request-id") or uuid.uuid4().hex).strip()
        request_start = time.perf_counter()
        status_code = 500

        ctx = extract(dict(request.headers))
        self.http_active_requests.add(1, attributes={"route": path})
        self.http_request_counter.add(1, attributes={"route": path, "method": method})

        with self.tracer.start_as_current_span(
            f"HTTP {method} {path}",
            context=ctx,
            kind=SpanKind.SERVER,
        ) as span:
            span.set_attribute("http.method", method)
            span.set_attribute("http.route", path)
            span.set_attribute("http.request_id", request_id)
            span.set_attribute("service.name", self.settings.service_name)
            span.set_attribute("service.version", self.settings.service_version)
            span.set_attribute("deployment.environment", self.settings.deployment_environment)
            span.set_attribute("k8s.cluster.name", self.settings.cluster_name)
            span.set_attribute("service.instance.id", self.settings.instance_id)

            try:
                if method in {"GET", "HEAD"} and path in {"/", "/readyz", "/-/healthz"}:
                    response = await self._handle_ready(request_id=request_id)
                elif method in {"GET", "HEAD"} and path == "/healthz":
                    response = _json_response({"status": "ok"}, 200, request_id=request_id)
                elif method == "POST" and path in {"/", "/predict"}:
                    response = await self._handle_predict(request, request_id, request_start)
                else:
                    response = _json_response(
                        {"detail": "Not found"},
                        404,
                        request_id=request_id,
                    )

                status_code = response.status_code
                span.set_attribute("http.status_code", status_code)
                if status_code >= 500:
                    span.set_status(Status(StatusCode.ERROR))
                return response

            except asyncio.CancelledError:
                span.set_status(Status(StatusCode.ERROR))
                raise

            except Exception as exc:
                status_code = 500
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR))
                _log_exception(
                    self.logger,
                    "request_failed",
                    route=path,
                    request_id=request_id,
                    error_type=exc.__class__.__name__,
                )
                return _json_response(
                    {"detail": "Internal server error"},
                    500,
                    request_id=request_id,
                )

            finally:
                elapsed_ms = (time.perf_counter() - request_start) * 1000.0
                self.http_duration_histogram.record(
                    elapsed_ms,
                    attributes={
                        "route": path,
                        "method": method,
                        "status_code": status_code,
                    },
                )
                if status_code >= 400:
                    self.http_error_counter.add(
                        1,
                        attributes={
                            "route": path,
                            "method": method,
                            "status_code": status_code,
                        },
                    )
                self.http_active_requests.add(-1, attributes={"route": path})


@serve.deployment(
    name=BACKEND_DEPLOYMENT_NAME,
    num_replicas="auto",
    autoscaling_config=_autoscaling_config(),
    ray_actor_options={"num_cpus": BACKEND_NUM_CPUS},
    max_ongoing_requests=BACKEND_MAX_ONGOING_REQUESTS,
    health_check_period_s=SETTINGS.serve_health_check_period_s,
    health_check_timeout_s=SETTINGS.serve_health_check_timeout_s,
    graceful_shutdown_wait_loop_s=SETTINGS.serve_graceful_shutdown_wait_loop_s,
    graceful_shutdown_timeout_s=SETTINGS.serve_graceful_shutdown_timeout_s,
)
class InferenceBackendDeployment(InferenceBackend):
    pass


@serve.deployment(
    name=INGRESS_DEPLOYMENT_NAME,
    num_replicas="auto",
    autoscaling_config=_autoscaling_config(),
    ray_actor_options={"num_cpus": INGRESS_NUM_CPUS},
    max_ongoing_requests=INGRESS_MAX_ONGOING_REQUESTS,
    health_check_period_s=SETTINGS.serve_health_check_period_s,
    health_check_timeout_s=SETTINGS.serve_health_check_timeout_s,
    graceful_shutdown_wait_loop_s=SETTINGS.serve_graceful_shutdown_wait_loop_s,
    graceful_shutdown_timeout_s=SETTINGS.serve_graceful_shutdown_timeout_s,
)
class TabularInferenceDeployment(TabularInferenceApp):
    pass


app = TabularInferenceDeployment.bind(InferenceBackendDeployment.bind())

if __name__ == "__main__":
    serve.run(app, route_prefix="/")