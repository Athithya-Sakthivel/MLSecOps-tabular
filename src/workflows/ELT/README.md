## End-to-end runtime model (control plane vs data plane)

Think in two strictly separated paths:

```text
Control plane (Flyte) ───────► schedules + submits
Data plane (Spark)   ───────► executes + writes
Storage (Iceberg)    ───────► governs state
```

---

# 1) Trigger → workflow start

* A schedule or manual trigger starts `elt_main_workflow`.
* Flyte Admin records execution metadata.
* Flyte Propeller begins orchestrating tasks.

**Nothing data-heavy happens here.**

---

# 2) Task execution lifecycle (per task)

For each of the 4 tasks:

```text
Flyte → K8s Pod (your base image) → Spark submission → exit
```

### Inside the Flyte task pod

1. Container starts (`flyte-elt-spark-base`)
2. Python loads your task
3. `SparkTask` constructs a Spark job spec
4. Submits a **SparkApplication CR** to Kubernetes

**At this point, the task container is done with data responsibilities.**

---

# 3) Spark operator takes over

* Spark Operator detects the `SparkApplication`
* Creates:

  * Driver pod
  * Executor pods

```text
Spark Driver → coordinates
Executors → process data
```

---

# 4) Actual data processing (data plane)

Inside Spark:

### Bronze

```text
Read → S3 raw
Transform (minimal)
Write → Iceberg (append/partition overwrite)
```

### Silver

```text
Read → Iceberg bronze
MERGE → Iceberg silver
```

### Gold

```text
Read → Iceberg silver
Aggregate
Write → Iceberg gold
```

### Maintenance

```text
Expire snapshots
Compact files
Rewrite manifests
```

---

# 5) Iceberg interaction model

Every Spark write:

```text
Executor writes files → S3
Driver commits → Iceberg REST
REST updates → Postgres metadata
```

### Critical sequence

1. Data files written to S3
2. Manifest files generated
3. REST catalog commit
4. Snapshot becomes visible

**Atomic from reader perspective**

---

# 6) Metadata flow

```text
Spark → Iceberg REST → Postgres
```

* Postgres stores:

  * snapshots
  * manifests
  * table metadata

* S3 stores:

  * parquet files
  * manifests

---

# 7) Task completion

* Spark job finishes
* Driver exits
* SparkApplication marked complete
* Flyte task marked success

---

# 8) Retry behavior (key property)

If a task fails:

* Flyte retries the task
* Spark job re-runs
* Idempotency ensures no corruption:

| Layer  | Safety mechanism             |
| ------ | ---------------------------- |
| Bronze | partition overwrite / dedupe |
| Silver | MERGE                        |
| Gold   | overwrite                    |
| Maint  | safe re-run                  |

---

# 9) End-to-end flow (compressed)

```text
Trigger
  ↓
Flyte Workflow
  ↓
Task Pod (control)
  ↓
SparkApplication (CRD)
  ↓
Spark Driver + Executors
  ↓
S3 (data files)
  ↓
Iceberg REST
  ↓
Postgres (metadata)
  ↓
Next task
```

---

# 10) Where your Docker image fits

Your image is used only here:

```text
Flyte Task Pod → builds + submits Spark job
```

It does NOT:

* process data
* read/write S3
* interact with Iceberg directly

---

# 11) Failure boundaries

| Layer          | Failure impact              |
| -------------- | --------------------------- |
| Flyte pod      | job not submitted           |
| Spark driver   | task fails                  |
| Executor       | retried internally          |
| Iceberg commit | atomic failure (safe retry) |

---

# 12) Key invariants (must hold)

* Only Spark touches data
* Only Iceberg manages table state
* Flyte never touches storage
* Postgres never accessed directly

---

# Final mental model

> Flyte is a scheduler.
> Spark is the worker.
> Iceberg is the transaction layer.
> S3 is the storage.
> Postgres is the metadata log.

---