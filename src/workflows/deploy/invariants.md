# End-to-End Production Invariants

This document is the single production contract for the full stack: Kubernetes, KubeRay, FastAPI, Ray Serve, ONNX Runtime, OpenTelemetry, k8s-infra, and SigNoz.

The system is production-correct only if all invariants below hold together.

---

## 1) Deployment / runtime

- Production runs on **KubeRay `RayService`**.
- The **only RayService** for this application lives in the **inference namespace**.
- The KubeRay operator runs in **`ray-system`** and does not host the application workload.
- The application code defines the service; Kubernetes deploys it.
- `serve.run(...)` is not the production deployment mechanism.
- Ray Serve autoscaling is enabled.
- Fixed replica counts are only the bootstrap state.
- A custom production image is preferred over runtime package setup.
- Helm chart versions are **always pinned**. No `latest`.

---

## 2) Namespace boundaries

- `ray-system` contains only the KubeRay operator and its control-plane resources.
- `inference-ns` contains the model-serving RayService and its runtime pods.
- No other Ray application shares the inference namespace.
- The observability scope for application telemetry is the inference namespace only.
- App logs, metrics, and traces are for the inference service only; Ray control-plane noise is not part of the application contract.

---

## 3) Serving path

- One FastAPI ingress is the HTTP entrypoint.
- One trace root exists per request.
- Ray Serve handles batching and replica scaling.
- The app code does not implement ad hoc request queueing or batching logic.
- Dynamic batching is allowed, but only if load tests show measurable benefit.
- Request handling remains synchronous from the API contract point of view even if model execution is offloaded to worker threads.

---

## 4) Model loading

- The model URI is versioned and immutable.
- The model bundle is a directory root, not a mutable single object key.
- Required bundle contents:
  - `model.onnx`
  - `schema.json`
  - optional `metadata.json`
- A checksum is required before serving.
- The model loads once per Ray replica and is cached locally.
- One model version is served at a time.
- Artifact mismatch is a hard error.
- Readiness is only true after the ONNX session is fully created and validated.

---

## 5) ONNX Runtime

- Threading is bound to the Ray replica CPU budget.
- Thread counts are set in code, not by ad hoc process environment variables.
- Default stance:
  - sequential execution
  - explicit intra-op and inter-op sizing
- No uncontrolled thread oversubscription.
- ONNX Runtime uses CPU execution unless the deployment is explicitly changed.
- The ONNX session is created once at replica startup.
- The ONNX session is not recreated per request.

---

## 6) Request schema

- Feature order is fixed and explicit.
- Unknown features are rejected.
- Missing features are rejected.
- Input schema is strict.
- Tabular coercion is numeric and finite only.
- Boolean, null, NaN, and infinite values are rejected.
- Maximum instances per request is bounded.
- One request body format is preferred and documented.
- Schema validation happens before model execution.

---

## 7) Tracing

- FastAPI request span is the root.
- Model preparation and ONNX execution are child spans.
- Trace context survives across service boundaries.
- Ray execution does not fragment traces.
- The app uses head-based sampling only.
- Supported root sampling policies:
  - `TraceIdRatioBased`
  - `ParentBased(TraceIdRatioBased)`
- Tail sampling belongs in the Collector or backend, not in the app.
- Stable resource attributes are required:
  - `service.name`
  - `service.version`
  - `deployment.environment.name`
  - `k8s.cluster.name`
  - `service.instance.id`
- Spans describe semantically meaningful work, not trivial micro-steps.
- Child spans should represent:
  - input preparation
  - model execution
  - downstream calls, if any
- Logs emitted during traced work must be linkable via `trace_id` and `span_id`.

---

## 8) Metrics

- Metrics are minimal and app-level.
- Required metrics:
  - request count
  - error count
  - inference latency histogram
  - active requests
  - batch size
- Latency uses a histogram, not a counter.
- High-cardinality labels are avoided.
- Metrics are for system health, not debugging.
- App metrics and infra metrics are separate planes.
- Allowed labels are fixed and low-cardinality.
- No per-request labels.
- No user-level dimensions.
- Histogram buckets are stable and explicit.
- Metrics must remain aggregatable over time.

---

## 9) Logs

- Logs are sparse and structured.
- Production log level is `WARNING`.
- Development log level may be `DEBUG`.
- Production allows only:
  - `WARNING`
  - `ERROR`
  - `CRITICAL`
- Only important paths log:
  - startup
  - errors
  - slow requests
- Logs are trace-correlated.
- No payload dumps.
- No noisy per-request success logs.
- No request lifecycle spam.
- No large blobs or objects in logs.
- Logs exist to explain deviations from expected behavior.
- Normal execution visibility comes from traces and metrics.

---

## 10) Telemetry pipeline

- The app exports via OTLP to the Collector.
- The Collector mediates batching, enrichment, and export.
- Self-hosted SigNoz means ClickHouse exporters, not cloud ingestion.
- App telemetry and infra telemetry are separate planes.
- OTLP transport is gRPC only.
- No silent fallback to OTLP/HTTP.
- Export failures must not block request success.
- Export buffering and batching are handled by the SDK and Collector, not by request handlers.

---

## 11) Infrastructure telemetry

- K8s infra metrics come from k8s-infra / Collector receivers.
- Node, pod, container, and cluster metrics are collected outside app code.
- Kubernetes metadata enrichment is required for correlation.
- Do not scrape infra in the service module.
- The inference namespace is the only namespace included for app-facing observability by default.
- Use include/whitelist rules rather than broad cluster-wide collection.
- Exclude rules are optional and only used for narrow cleanup of included scope.
- Collector and backend resources are explicitly sized.
- Retention is short by default.

---

## 12) Environment

- `LOG_LEVEL` controls app logs.
- `OTEL_LOG_LEVEL` controls OpenTelemetry internal logging.
- OTel env vars are minimal and deliberate.
- Service identity is fixed by env/resource attributes.
- Environment defaults must be explicit and fail-fast.
- No generated placeholders are allowed in production bootstrap.
- Missing required environment variables are a startup error.

---

## 13) File layout

The production app code is exactly five Python files in one directory:

- `config.py`
- `telemetry.py`
- `model_store.py`
- `schemas.py`
- `service.py`

No monolithic serving script is allowed in production.

---

## 14) Failure handling

- Startup fails fast if required config is missing.
- Readiness is only true after model/session load.
- Artifact mismatch is a hard error.
- Request validation errors are explicit and bounded.
- Telemetry shutdown is deterministic.
- Telemetry initialization is idempotent only for identical configuration.
- Any mismatch in telemetry configuration is a hard failure.
- The service must not become healthy unless the model bundle and ONNX session are valid.

---

## 15) Production defaults

- Self-hosted SigNoz is the backend.
- No Prometheus storage is required.
- k8s-infra handles infra observability.
- Ray autoscaling plus optional batching plus strict schema plus checksummed model bundle is the production baseline.
- Chart versions are pinned.
- Secrets are required explicitly.
- No silent fallback values are allowed for production-critical settings.
- Insecure OTLP inside the cluster is acceptable only while the cluster network policy and service mesh provide the intended security boundary.

---

## 16) Final end-to-end invariant

One request enters FastAPI, is traced end-to-end through Ray Serve and ONNX execution, emits sparse production logs and minimal metrics, and lands in SigNoz with stable identity, namespace context, and Kubernetes metadata.

If any part of that chain is missing, noisy, or ambiguous, the deployment is not production-complete.