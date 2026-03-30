from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from config import Settings, get_settings
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from model_store import LoadedModel, load_loaded_model
from opentelemetry import metrics, trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from ray import serve
from schemas import build_feature_matrix, coerce_instances, split_model_outputs
from telemetry import initialize_telemetry

SETTINGS = get_settings()
initialize_telemetry(SETTINGS)

api = FastAPI(title="tabular-inference-api", version=SETTINGS.service_version)
if not getattr(api.state, "otel_instrumented", False):
    FastAPIInstrumentor.instrument_app(api)
    api.state.otel_instrumented = True


def _serve_autoscaling_config(settings: Settings) -> dict[str, Any]:
    return {
        "min_replicas": settings.min_replicas,
        "initial_replicas": settings.initial_replicas,
        "max_replicas": settings.max_replicas,
        "target_ongoing_requests": settings.target_ongoing_requests,
        "upscale_delay_s": settings.upscale_delay_s,
        "downscale_delay_s": settings.downscale_delay_s,
    }


@serve.deployment(
    name=SETTINGS.serve_deployment_name,
    num_replicas="auto",
    autoscaling_config=_serve_autoscaling_config(SETTINGS),
    ray_actor_options={"num_cpus": SETTINGS.replica_num_cpus},
    max_ongoing_requests=SETTINGS.max_ongoing_requests,
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
        self.effective_model_version = self.metadata.model_version or self.settings.model_version
        self.input_name = self.loaded_model.input_name
        self.output_names = self.loaded_model.output_names

        self.logger.warning(
            "model ready: uri=%s version=%s input=%s outputs=%s",
            self.settings.model_uri,
            self.effective_model_version,
            self.input_name,
            ",".join(self.output_names),
        )

    @api.get("/healthz")
    async def healthz(self) -> dict[str, str]:
        return {"status": "ok"}

    @api.get("/readyz")
    async def readyz(self) -> dict[str, Any]:
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

        with self.tracer.start_as_current_span("prepare_input") as span:
            feature_matrix = build_feature_matrix(
                rows,
                self.schema.feature_order,
                allow_extra_features=self.schema.allow_extra_features,
            )
            span.set_attribute("batch.size", len(rows))
            span.set_attribute("model.version", self.effective_model_version)
            span.set_attribute("feature.count", len(self.schema.feature_order))

        with self.tracer.start_as_current_span("onnx_inference") as span:
            outputs = self.session.run(self.output_names, {self.input_name: feature_matrix})
            span.set_attribute("batch.size", len(rows))
            span.set_attribute("onnx.input.name", self.input_name)
            span.set_attribute("onnx.provider", ",".join(self.settings.ort_providers))

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
            attributes={
                "model": self.effective_model_version,
            },
        )
        return split_model_outputs(outputs, self.output_names, row_count=len(rows))

    @api.post("/predict")
    async def predict(self, request: Request) -> JSONResponse:
        route = "/predict"
        start = time.perf_counter()
        self.active_requests.add(1, attributes={"route": route})
        self.request_counter.add(1, attributes={"route": route, "method": "POST"})

        try:
            payload = await request.json()
            rows = coerce_instances(payload)
            if len(rows) > self.settings.max_instances_per_request:
                raise ValueError(f"Too many instances: {len(rows)} > {self.settings.max_instances_per_request}")

            # Submit each row independently so Serve can batch across concurrent requests.
            predictions = await asyncio.gather(*(self._predict_row_batch(row) for row in rows))

            response = {
                "model_version": self.effective_model_version,
                "n_instances": len(rows),
                "predictions": predictions,
            }

            total_ms = (time.perf_counter() - start) * 1000.0
            self.latency_histogram.record(
                total_ms,
                attributes={
                    "route": route,
                    "model": self.effective_model_version,
                },
            )
            if total_ms >= self.settings.slow_request_ms:
                self.logger.warning(
                    "slow inference request: route=%s model=%s latency_ms=%.3f n_instances=%d",
                    route,
                    self.effective_model_version,
                    total_ms,
                    len(rows),
                )

            return JSONResponse(response)

        except HTTPException:
            self.error_counter.add(
                1,
                attributes={"route": route, "error_type": "http_exception"},
            )
            raise
        except ValueError as exc:
            self.error_counter.add(
                1,
                attributes={"route": route, "error_type": "validation_error"},
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            self.error_counter.add(
                1,
                attributes={"route": route, "error_type": exc.__class__.__name__},
            )
            self.logger.exception(
                "inference failed: route=%s model=%s",
                route,
                self.effective_model_version,
            )
            raise HTTPException(status_code=500, detail="Inference failed") from exc
        finally:
            self.active_requests.add(-1, attributes={"route": route})


app = TabularInferenceService.bind()


if __name__ == "__main__":
    # Local development only. Production should be RayService / Serve config.
    serve.run(app)
