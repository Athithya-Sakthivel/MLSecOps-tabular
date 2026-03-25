## End-to-end runtime model (control plane vs data plane)
Below is the rewritten plan for the 4-file Flyte ELT setup.

```text
Control plane (Flyte) ───────► orchestration only
Data plane (Spark)   ───────► all data movement + joins + maintenance
Storage (Iceberg)    ───────► table state, snapshots, transactions
Object store (S3)    ───────► immutable data files
Catalog DB            ───────► Iceberg metadata/state
```

Flyte tasks run in their own Kubernetes pods, and the Spark task plugin is what gives the task access to a Spark session and the Spark infrastructure it needs. `pyflyte run --remote` is the supported way to package the workflow code, register it, and launch it on the remote Flyte backend. Tasks are versioned and are typically aligned with the git SHA, which matches your lineage model. ([Flyte][1])

## 1) Core contracts

**Single runtime image**

* One shared runtime image for all 4 files.
* It contains the Python runtime, Flytekit, Spark, Iceberg jars, Hadoop S3 support, and any Python libraries needed by the bronze extractor.
* It does not contain application source code.

**Code delivery**

* Workflow code is shipped by Flyte at execution time, not baked into the image.
* The image is runtime; the repo snapshot is code.

**Versioning**

* Image version is separate from code version.
* Execution name deterministcally derieved using git SHA (7 chars) and current timestamp

This matches Flyte’s model of registered workflows and remote execution, while keeping task code and runtime concerns separate. ([Flyte][2])

Implementation note: because `bronze_ingest.py` now uses `datasets`, the runtime image must be rebuilt once to include `datasets` and its direct runtime dependencies. The other three files can then reuse that same image.

## 2) Final file-by-file plan

| File                      | Responsibility                                                                         | Inputs                                 | Outputs                        | Bottleneck rule                                             |
| ------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------- | ------------------------------ | ----------------------------------------------------------- |
| `bronze_ingest.py`        | Pull the two validated sources, normalize minimally, land raw Bronze tables in Iceberg | remote trip dataset + taxi zone lookup | `iceberg.bronze.*` tables      | This is the only stage that touches external datasets       |
| `silver_transform.py`     | Join, clean, and feature-engineer model-ready data                                     | Bronze tables                          | `iceberg.silver.trip_features` | All enrichment happens in Spark, not in Flyte control logic |
| `maintenance_optimize.py` | Compact files, expire snapshots, remove orphan files                                   | Bronze + Silver Iceberg tables         | no business-data output        | Runs on its own cadence; never blocks ingestion             |
| `elt_workflow.py`         | Orchestrate the 3 tasks                                                                | task outputs                           | execution result               | Keep it thin; no transformation logic here                  |

## 3) Data contract for the two source datasets

Your validation test already proved the datasets are joinable:

* trips table contains `PULocationID` and `DOLocationID`
* taxi zone lookup contains `LocationID`

That means the silver layer should treat them as a fact/dimension pair:

* `trips.PULocationID -> zones.LocationID`
* `trips.DOLocationID -> zones.LocationID`

So the silver table should be a trip-level feature table, not an analytics aggregate. Keep pickup/dropoff zone fields explicit and deterministic.

## 4) Exact behavior of each task

### `bronze_ingest.py`

Purpose: land raw data with minimal transformation.

Rules:

* validate the source URIs before Spark work begins
* extract the two datasets once
* write them to Iceberg Bronze tables with idempotent semantics
* preserve source metadata such as `run_id`, `ingestion_ts`, `source_uri`, and `source_file`
* only normalize column names and do the smallest required type cleanup

This task should fail fast if the raw source is inaccessible. It should not contain joins, feature logic, or maintenance logic.

### `silver_transform.py`

Purpose: produce the model-ready dataset.

Rules:

* read only from Bronze Iceberg tables
* join the trips table to the taxi zone lookup twice:

  * pickup enrichment via `PULocationID`
  * dropoff enrichment via `DOLocationID`
* produce stable feature columns such as:

  * trip duration
  * pickup hour
  * day-of-week
  * distance
  * fare/tip/total amounts
  * pickup/dropoff borough and zone fields
* write deterministically so retries do not duplicate or corrupt the table

This is the main MLOps dataset. It should stay narrow, reproducible, and schema-stable.

### `maintenance_optimize.py`

Purpose: keep Iceberg healthy.

Iceberg snapshots accumulate with each write/update/delete/compaction. The current Iceberg docs recommend expiring old snapshots regularly because it removes unneeded data files and keeps metadata small. Iceberg also documents `deleteOrphanFiles` for cleaning files left behind by task/job failures, and `rewrite_data_files` for compacting small files into larger ones. `deleteOrphanFiles` can be expensive and should be run periodically; `rewrite_data_files` is the Spark procedure that combines small files and can also remove dangling deletes. ([Apache Iceberg][3])

Use these maintenance actions:

* `CALL ... system.expire_snapshots(...)`
* `CALL ... system.rewrite_data_files(...)`
* `CALL ... system.delete_orphan_files(...)`

Iceberg’s Spark procedures are exposed through the `system` namespace when Iceberg SQL extensions are enabled. ([Apache Iceberg][4])

### `elt_workflow.py`

Purpose: be the thin orchestration layer.

Recommended flow:

1. `bronze_ingest`
2. `silver_transform`
3. `maintenance_optimize`

If you want to avoid maintenance becoming an ingestion bottleneck, keep it logically separate in cadence even if it lives in the same repo. The workflow file should only wire task dependencies and inputs/outputs.

## 5) Runtime and execution model

Use the Spark task pattern Flyte documents: task config carries Spark settings, and `hadoop_conf` is the place to inject S3 access details for Spark-side I/O. Flyte’s Spark plugin is explicitly designed to provide the Spark infrastructure the task needs by the time the task function runs. ([Flyte][5])

That gives you this runtime shape:

```text
Flyte workflow
  -> Bronze task pod
  -> Spark work for bronze
  -> Silver task pod
  -> Spark work for silver
  -> Maintenance task pod
  -> Spark work for lifecycle ops
```

## 6) Bottleneck rules

* Only `bronze_ingest.py` talks to external raw datasets.
* Only `silver_transform.py` does dataset joins and feature engineering.
* Only `maintenance_optimize.py` does table hygiene.
* The workflow file must stay thin.
* Rebuild the shared image only when task dependencies change, not for workflow wiring changes.
* Keep Spark jobs bounded and task-scoped; do not move long-running table cleanup into the critical ingestion path.

## 7) Final invariants

These should always hold:

* Flyte orchestrates.
* Spark executes.
* Iceberg owns table state.
* S3 stores immutable files.
* Bronze is raw landing.
* Silver is model-ready curation.
* Maintenance is file/snapshot hygiene.
* The runtime image is shared.
* Application code is external and versioned by git SHA.

If you want the next step, I can turn this into a concrete per-file contract with table names, function names, Spark write modes, and the exact Iceberg procedures to call in `maintenance_optimize.py`.

[1]: https://docs-legacy.flyte.org/en/v1.12.0/user_guide/basics/tasks.html "Tasks"
[2]: https://docs-legacy.flyte.org/en/v1.15.0/user_guide/getting_started_with_workflow_development/running_a_workflow_locally.html "Running a workflow locally"
[3]: https://iceberg.apache.org/docs/latest/maintenance/ "Maintenance - Apache Iceberg™"
[4]: https://iceberg.apache.org/docs/latest/spark-procedures/ "Procedures - Apache Iceberg™"
[5]: https://docs-legacy.flyte.org/en/latest/flytesnacks/examples/k8s_spark_plugin/pyspark_pi.html "Running a Spark Task"
