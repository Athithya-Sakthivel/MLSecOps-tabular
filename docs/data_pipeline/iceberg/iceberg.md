# 1) System Overview

## Purpose

Deploy and operate a **production-grade Apache Iceberg data platform** consisting of:

* A **REST-based Iceberg catalog (Gravitino)**
* A **PostgreSQL-backed catalog state store**
* An **object store-backed data + metadata layer**
* A **distributed ingestion pipeline (Ray + pyiceberg)**

---

# 2) Architecture

## Logical Architecture

```text
Client (pyiceberg / Ray / Spark)
        ↓ HTTP (Iceberg REST API)
Gravitino Iceberg REST Server (Kubernetes)
        ↓ JDBC
PostgreSQL (catalog backend)
        ↓
Object Store (GCS / S3 / ADLS)
        ├── metadata/
        └── data/
```

---

## Responsibilities

| Component    | Responsibility                                     |
| ------------ | -------------------------------------------------- |
| Gravitino    | REST API, commit coordination, catalog abstraction |
| Postgres     | table registry, atomic commit pointer              |
| Object Store | Iceberg metadata files + data files                |
| Ray job      | distributed ingestion + writes                     |

---

# 3) Deployment Specification (Kubernetes)

## Core Resources

* **Deployment**: `iceberg-rest`
* **Service**: `iceberg-rest` (ClusterIP)
* **ConfigMap**: `iceberg-rest-conf`
* **ServiceAccount + RBAC**
* **Secrets**:

  * storage credentials
  * REST auth (optional)
  * Postgres credentials (CNPG)
* **HPA**: CPU-based autoscaling
* **PDB**: availability guarantee

---

## Runtime Configuration (ConfigMap)

```properties
gravitino.iceberg-rest.host=0.0.0.0
gravitino.iceberg-rest.httpPort=9001

# Catalog backend
gravitino.iceberg-rest.catalog-backend=jdbc
gravitino.iceberg-rest.uri=jdbc:postgresql://<pooler>:5432/<db>

# Storage
gravitino.iceberg-rest.warehouse=gs://<bucket>/iceberg/warehouse/
gravitino.iceberg-rest.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO

# JDBC tuning
gravitino.iceberg-rest.jdbc-max-connections=20
```

---

## Identity & Access

### Mode 1 — Workload Identity (recommended)

* GKE ServiceAccount annotated with:

```yaml
iam.gke.io/gcp-service-account: <GCP_SA_EMAIL>
```

### Mode 2 — Static credentials

* Stored in Kubernetes Secret
* Injected as env vars

---

# 4) Storage Layout (Object Store)

## Warehouse root

```text
gs://<bucket>/iceberg/warehouse/
```

---

## Table structure

```text
warehouse/
└── <namespace>/
    └── <table>/
        ├── metadata/
        │   ├── v1.metadata.json
        │   ├── v2.metadata.json
        │   ├── manifest-list-*.avro
        │   └── manifest-*.avro
        │
        └── data/
            ├── *.parquet
            └── ...
```

---

## Data model

| Layer         | Description                    |
| ------------- | ------------------------------ |
| metadata.json | table state + snapshot pointer |
| manifest list | snapshot grouping              |
| manifest      | file-level metadata            |
| data files    | actual data                    |

---

# 5) Catalog Backend (Postgres)

## Role

* Stores **table registry**
* Maintains **current metadata pointer**
* Provides **transactional guarantees**

---

## Key responsibilities

* Namespace management
* Table resolution:

  ```
  namespace.table → warehouse path
  ```
* Atomic commit coordination
* Multi-writer concurrency control

---

# 6) Iceberg REST API (Gravitino)

## Base URI

```text
http://<service>:9001/iceberg
```

---

## Key endpoints

| Endpoint                       | Purpose          |
| ------------------------------ | ---------------- |
| `GET /v1/config`               | server config    |
| `GET /v1/namespaces`           | list namespaces  |
| `POST /v1/namespaces`          | create namespace |
| `GET /v1/tables/{ns}.{table}`  | load table       |
| `POST /v1/tables/{ns}.{table}` | create table     |
| `POST /v1/transactions/commit` | commit metadata  |

---

## Auth (optional)

* Basic auth via:

  * `ICEBERG_REST_USER`
  * `ICEBERG_REST_PASSWORD`

---

# 7) Ingestion Pipeline (Ray + pyiceberg)

## Input

* Source: Parquet dataset

```text
s3://.../raw/transactions/
```

---

## Execution Steps

### 1. Initialize Ray

* Local or cluster mode

---

### 2. Read dataset

```python
ds = ray.data.read_parquet(source_uri)
```

---

### 3. Infer schema

* Sample dataset
* Convert → PyArrow schema

---

### 4. Load catalog

```python
catalog = load_catalog(
    "rest",
    uri=ICEBERG_REST_URI,
    warehouse=ICEBERG_WAREHOUSE,
    auth=...
)
```

---

### 5. Load table

```python
table = catalog.load_table("default.transactions")
```

---

### 6. Write data

* Repartition dataset
* Iterate batches
* Convert → `pa.Table`
* Append:

```python
table.append(pa_table)
```

---

### 7. Commit flow

Handled by Gravitino:

1. Validate current metadata version
2. Write new metadata.json
3. Update pointer in Postgres
4. Create snapshot

---

# 8) Failure Handling

## Fallback mechanism

If catalog is unavailable:

```python
write_parquet("s3://.../raw_fallback/")
```

### Guarantees

* No data loss
* Deferred ingestion possible

---

# 9) End-to-End Write Flow

```text
Ray Job
  ↓
pyiceberg (REST client)
  ↓
Gravitino
  ↓
(Postgres: validate + commit pointer)
  ↓
(Object store: write metadata + data)
```

---

## Detailed sequence

1. Client reads current metadata
2. Writes Parquet files → object store
3. Writes manifest + metadata.json
4. Calls commit API
5. Gravitino:

   * checks version
   * updates pointer (atomic)
6. Snapshot becomes visible

---

# 10) Read Flow

```text
Client → REST → metadata.json → manifests → data files
```

Steps:

1. Resolve table via catalog
2. Fetch latest metadata.json
3. Load manifests
4. Read only required files

---

# 11) Observability & Validation

## Built-in smoke tests

* REST health (`/v1/config`)
* Namespace creation
* Postgres connectivity (psql)
* Object store read/write

---

## Logs

* Structured JSON logs from ingestion
* Kubernetes logs for REST server

---

# 12) Scaling Characteristics

## Horizontal scaling

* REST server: stateless → scale replicas
* HPA: CPU-based

---

## Data scaling

* Unlimited via object store
* Parallel writes via Ray

---

## Bottlenecks

* Postgres → commit coordination
* Small file problem → requires compaction (not yet implemented)

---

# 13) Security Model

* IAM / Workload Identity preferred
* Secrets stored in Kubernetes
* Optional REST auth
* RBAC for service account

---

# 14) Key Guarantees

| Property           | Mechanism               |
| ------------------ | ----------------------- |
| Atomic commits     | Postgres + REST catalog |
| Snapshot isolation | Iceberg metadata        |
| Schema evolution   | metadata.json           |
| Time travel        | snapshot history        |
| Fault tolerance    | fallback writes         |

---

# 15) Final Mental Model

```text
Control Plane:
  Gravitino + Postgres

Data Plane:
  Object Store (metadata + data)

Compute:
  Ray / pyiceberg
```

---

# 16) One-line Summary

> A Kubernetes-deployed Iceberg REST catalog (Gravitino) coordinates transactional metadata updates via Postgres while storing all data and metadata in object storage, with Ray-based distributed ingestion writing data through the REST interface.

---

