# Inference Service End-to-End Invariants

This document is the production contract for the tabular inference system. It defines the required behavior across:

- Kubernetes and KubeRay
- RayService and Ray Serve
- FastAPI ingress
- ONNX Runtime execution
- telemetry and logging
- AWS object storage access
- lifecycle automation

The system is production-correct only when all invariants in this document hold simultaneously.

---

## 1) Scope and governing principles

1. The application is a **single-service inference system**.
2. The production deployment uses **KubeRay RayService**.
3. The application is **defined in code** and **deployed by Kubernetes**.
4. Production behavior must be **fail-fast**, **deterministic**, and **idempotent**.
5. There is **one production baseline**. No kind/EKS behavior split exists in application logic.
6. Environment-specific variation is limited to **deployment capacity**, **auth mode**, and **cluster wiring**.
7. Runtime defaults must be explicit. Silent fallback behavior is not allowed for production-critical settings.
8. All configuration changes must be explainable from one of the following layers:
   - app/runtime environment
   - RayService manifest
   - container image
   - model bundle

---

## 2) Source of truth boundaries

### Kubernetes / RayService
Kubernetes owns:
- namespace creation
- RayService creation
- worker scale envelope
- image selection
- pod security context
- volumes and mounts
- AWS auth object wiring
- cluster-level resource allocation

### Application code
The Python app owns:
- request validation
- schema enforcement
- model load semantics
- batching behavior
- request tracing
- inference execution
- app-level metrics and logs
- readiness and health semantics
- model/version reporting

### Model bundle
The model bundle owns:
- model artifact contents
- model version identity
- model checksum identity
- schema identity

---

## 3) Required Kubernetes objects

The minimal production multi-doc YAML must contain:

1. `Namespace`
2. `RayService`
3. Exactly one AWS auth path:
   - `ServiceAccount` for IRSA on EKS, or
   - `Secret` containing `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for static credentials

No additional application routing Service is required for the standard path. KubeRay/RayService creates the required service wiring for the Serve deployment.

Optional objects are allowed only when justified:
- `PDB` for voluntary disruption policy
- additional `Service` only for custom exposure or routing
- `Role` / `RoleBinding` only if the app itself talks to the Kubernetes API
- `ConfigMap` only if a non-env config distribution path is required

---

## 4) Namespace boundaries

1. The inference workload runs in the `inference` namespace.
2. The Ray operator runs in `ray-system`.
3. The application namespace contains only the inference service and its direct runtime objects.
4. No unrelated Ray application shares the inference namespace.
5. Observability scope for the app is limited to the inference service and its replicas.
6. Control-plane noise from the Ray operator is not part of the app contract.

---

## 5) Image and runtime immutability

1. The production image is pinned by tag or digest.
2. The image contains:
   - application code
   - Python dependencies
   - ONNX Runtime dependencies
   - telemetry dependencies
3. The container runs as a **non-root user**.
4. The UID/GID are deterministic and aligned with Kubernetes security context.
5. The image must support ephemeral per-replica cache writes under `/mlsecops/model-cache`.
6. Runtime package installation is not allowed in production.
7. `python3.13.12` is the declared base runtime for the deployment image.
8. The build context is the `deploy/` directory for the deployment image build.

---

## 6) Security and filesystem invariants

1. Containers must not run as root.
2. The pod security context must declare:
   - `runAsNonRoot: true`
   - `runAsUser`
   - `runAsGroup`
   - `fsGroup`
3. The model cache must use `emptyDir`.
4. The model cache path must be mounted at `/mlsecops`.
5. `MODEL_CACHE_DIR` must resolve to `/mlsecops/model-cache`.
6. Cache storage is ephemeral per replica.
7. Cache storage must not survive pod deletion.
8. Files written to cache must be writable by the non-root container user.

---

## 7) AWS authentication invariants

Exactly one AWS auth mode must be active.

### IRSA mode
- `USE_IAM=true`
- `KUBERAY_IAM_ROLE_ARN` is set
- a `ServiceAccount` is attached to both Ray head and worker pods
- no static `AWS_*` secrets are used

### Static credential mode
- `USE_IAM=false`
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are injected from a Kubernetes `Secret`
- no IRSA annotation is used
- the app must not silently mix static env credentials with IAM mode

### Prohibited mixed mode
- `USE_IAM=true` plus `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in the same pod runtime is not permitted as a production state

---

## 8) RayService and cluster invariants

1. Production uses `RayService`.
2. `enableInTreeAutoscaling: true` must be enabled.
3. The Ray version is pinned.
4. The Ray image must match the pinned Ray version line.
5. The head pod must not consume inference CPU resources.
6. The worker group defines the inference capacity envelope.
7. Worker autoscaling bounds are controlled by:
   - `replicas`
   - `minReplicas`
   - `maxReplicas`
8. Head and worker images must be the same unless a specific operational reason exists.
9. Worker CPU requests/limits must match the `rayStartParams.num-cpus` budget.
10. The worker pool must be large enough to satisfy the Serve replica ceiling under peak load.

---

## 9) Serve deployment invariants

1. The production Serve deployment uses `num_replicas="auto"`.
2. Request-aware autoscaling is controlled by the Serve autoscaling policy.
3. `SERVE_MIN_REPLICAS` must be at least 1 in production.
4. `SERVE_INITIAL_REPLICAS` must be within the min/max envelope.
5. `SERVE_MAX_REPLICAS` must not exceed the practical worker capacity envelope.
6. `SERVE_TARGET_ONGOING_REQUESTS` and `SERVE_MAX_ONGOING_REQUESTS` must be consistent.
7. `SERVE_MAX_ONGOING_REQUESTS` must be greater than or equal to `SERVE_TARGET_ONGOING_REQUESTS`.
8. `SERVE_UPSCALE_DELAY_S` and `SERVE_DOWNSCALE_DELAY_S` define the control loop timing.
9. `SERVE_UPSCALE_DELAY_S` must be shorter than `SERVE_DOWNSCALE_DELAY_S`.
10. Batching is an application feature, not a Kubernetes feature.
11. Serve batching requires:
    - `@serve.batch`
    - an `async def` batched method
    - list-in/list-out semantics

---

## 10) Batching invariants

1. Dynamic batching must be implemented in application code.
2. Batching must be opt-in and deterministic.
3. `SERVE_BATCH_MAX_SIZE` and `SERVE_BATCH_WAIT_TIMEOUT_S` define the batching policy.
4. Batching must preserve response ordering.
5. Batching must not drop or duplicate rows.
6. Batching must preserve the number of outputs per input row.
7. Batching must not bypass schema validation.
8. Batching must not bypass trace generation.
9. Batching must not bypass error metrics.
10. Batching must remain compatible with `num_replicas="auto"`.

---

## 11) ONNX Runtime invariants

1. The ONNX session is created once per replica.
2. The ONNX session is not recreated per request.
3. Threading is explicit and bounded.
4. `ORT_INTRA_OP_NUM_THREADS` and `ORT_INTER_OP_NUM_THREADS` must be set deliberately.
5. The default stance is CPU execution.
6. ONNX thread usage must not exceed the replica CPU budget.
7. The ONNX runtime must operate under the same non-root filesystem constraints as the app.
8. A failed session creation is a startup failure.
9. A missing or invalid ONNX model session is a readiness failure.

---

## 12) Model bundle invariants

1. The model URI is versioned and immutable.
2. The model bundle root is a directory, not a mutable single object key.
3. Required bundle contents:
   - `model.onnx`
   - `schema.json`
4. Optional bundle contents:
   - `metadata.json`
5. A checksum is required before serving.
6. Artifact mismatch is a hard error.
7. One model version is served at a time.
8. Model metadata determines runtime identity only when present and valid.
9. Model version and bundle checksum must be consistent with deployment expectations.
10. Model load must be complete before readiness becomes true.

---

## 13) Request schema invariants

1. Input schema is strict.
2. Feature order is fixed and explicit.
3. Missing features are rejected.
4. Unknown features are rejected unless the schema explicitly allows them.
5. Input coercion must be numeric and finite only.
6. Boolean, null, NaN, and infinite values are invalid unless the schema explicitly allows them.
7. Maximum instances per request is bounded.
8. Schema validation must happen before model execution.
9. Validation failure must return a bounded, explicit client error.
10. Request bodies must not be accepted in multiple undocumented formats.

---

## 14) FastAPI and request path invariants

1. FastAPI is the HTTP ingress.
2. One request enters the app through FastAPI.
3. The request path remains synchronous from the API contract point of view.
4. Internal execution may offload work to worker threads or batchers.
5. Request IDs must be present and propagated in response headers.
6. Request middleware must always record duration and outcome.
7. HTTP response codes must be deterministic and bounded.
8. Internal exceptions must not leak raw stack traces to clients.
9. Request handling must not block the event loop on ONNX execution.

---

## 15) Tracing invariants

1. Each request has one trace root.
2. FastAPI request handling is the trace root.
3. Model preparation and ONNX execution are child spans.
4. Trace context must survive across service boundaries.
5. Ray execution must not fragment traces.
6. Sampling must be head-based only.
7. Tail sampling belongs in the collector or backend.
8. Stable resource attributes are required.
9. Child spans must represent meaningful work, not trivial micro-steps.
10. Logs emitted during traced work must be correlatable through trace identifiers.

---

## 16) Metrics invariants

1. Metrics are minimal and app-level.
2. Required app metrics include:
   - request count
   - error count
   - inference latency histogram
   - active requests
   - batch size
3. Histogram use is mandatory for latency.
4. High-cardinality labels are prohibited.
5. No per-request labels.
6. No user-level labels.
7. Metrics must remain aggregatable over time.
8. App metrics and infrastructure metrics are separate planes.
9. Metrics are for health and service behavior, not debugging payloads.
10. Metric names and label sets must remain stable across releases.

---

## 17) Logging invariants

1. Production logs are structured.
2. Production log level is `WARNING` by default.
3. Allowed production levels are:
   - `WARNING`
   - `ERROR`
   - `CRITICAL`
4. Startup, errors, and slow requests are the only expected log paths.
5. Normal successful requests must not generate noisy logs.
6. Payload dumps are forbidden.
7. Large object dumps are forbidden.
8. Logs must be trace-correlated when emitted during traced work.
9. Log content must explain deviations, not duplicate telemetry.
10. Logging configuration must be deterministic and explicit.

---

## 18) Telemetry pipeline invariants

1. The app exports telemetry via OTLP.
2. The collector mediates batching, enrichment, and export.
3. OTLP transport is gRPC only unless the deployment is explicitly changed.
4. No silent fallback to an alternate transport is allowed.
5. Export failures must not block request success.
6. Telemetry buffering and batching are handled by the SDK and collector, not by request handlers.
7. Self-hosted SigNoz is the production backend.
8. App telemetry and infrastructure telemetry are separate planes.
9. Export configuration must be explicit and fail-fast.
10. OTel configuration conflicts must fail early.

---

## 19) Infrastructure telemetry invariants

1. Node, pod, container, and cluster telemetry comes from infrastructure receivers.
2. Infrastructure telemetry is not scraped from the service module.
3. Kubernetes metadata enrichment is required.
4. The inference namespace is the default app-facing scope.
5. Include/whitelist rules are preferred over broad cluster-wide collection.
6. Collector and backend resources must be explicitly sized.
7. Retention is intentionally short by default.
8. Infrastructure telemetry must not corrupt app telemetry semantics.

---

## 20) Environment invariants

1. All required environment variables must be explicit.
2. Missing required values are startup errors.
3. No generated placeholders are allowed in production bootstrap.
4. `DEPLOYMENT_PROFILE` is fixed to the production baseline.
5. Identity, model, telemetry, Serve, and operational env vars must remain stable unless a deliberate release changes them.
6. `MODEL_CACHE_DIR` must match the mounted cache path.
7. `LOG_LEVEL` controls application logging.
8. `OTEL_LOG_LEVEL` controls OpenTelemetry internal logging if used.
9. The environment must not infer behavior from cluster flavor.
10. The environment must not silently patch missing config.

---

## 21) Lifecycle automation invariants

1. Deployment manifests are rendered from code, not manually maintained.
2. Rendering must be idempotent.
3. Rendering must be hash-gated.
4. Re-applying an unchanged render must be a no-op.
5. The rendered manifest hash must be stable for identical effective input.
6. The rollout command must not mutate state unless the rendered content changes.
7. The delete command must remove rendered resources and local state.
8. The deployment utility must be deterministic.
9. The deployment utility must not require manual YAML editing for routine changes.
10. Secrets must be applied directly into the cluster as secret objects or secret references, not managed as a separate checked-in secrets manifest.

---

## 22) Storage and caching invariants

1. Per-replica ONNX caching uses `emptyDir`.
2. Cache data is ephemeral.
3. Cache data is not shared across replicas.
4. Cache data is not shared across pods.
5. Cache directory writes must be constrained to the mounted volume.
6. Cache invalidation occurs naturally on pod replacement.
7. Cache content must not be treated as durable state.

---

## 23) Failure handling invariants

1. Startup fails fast on missing config.
2. Startup fails fast on invalid model bundle state.
3. Startup fails fast on invalid telemetry configuration.
4. Startup fails fast on invalid auth configuration.
5. Readiness is only true after model/session validation.
6. Artifact mismatch is a hard error.
7. Validation failures return explicit client errors.
8. Internal inference failures return bounded server errors.
9. Telemetry export failures must not break request handling.
10. Deployment reconciliation failures must surface clearly in logs and controller status.

---

## 24) Production defaults

1. One warm replica is the baseline.
2. Autoscaling is enabled.
3. Batching is enabled only if it is coded and validated.
4. CPU execution is the default ONNX path.
5. The model cache is ephemeral per replica.
6. The app runs non-root.
7. The cluster uses a fixed namespace.
8. The image is pinned.
9. Configuration is minimal and explicit.
10. No environment-specific behavior split exists in application logic.

---

## 25) Final end-to-end invariant

One request enters FastAPI, is validated against a strict schema, is traced end-to-end through Ray Serve and ONNX execution, optionally participates in dynamic batching, emits sparse structured logs and minimal metrics, uses the correct AWS auth path, writes only to ephemeral per-replica cache storage, and is exported to SigNoz with stable service identity and Kubernetes metadata.

If any part of that chain is missing, ambiguous, noisy, or inconsistent, the deployment is not production-complete.

---

## 26) Non-goals

This system does not require:
- manual YAML edits for every rollout
- per-environment application code branches
- ad hoc request queueing in the app
- payload logging
- cluster-wide observability by default
- static shared model cache between replicas
- root containers
- mixed IAM and static AWS credential modes

---

## 27) Change control rule

Any change to:
- env vars
- model bundle format
- ONNX session setup
- batching parameters
- Serve autoscaling policy
- telemetry export path
- auth mode
- pod security context
- rollout automation

must preserve every invariant in this document unless the document is intentionally revised first.