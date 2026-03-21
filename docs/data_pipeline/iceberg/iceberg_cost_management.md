# Apache Iceberg Cost Management Guide

## 1. Objective

This document defines the mechanisms, risk factors, and operational controls required to manage storage and compute costs in deployments using Apache Iceberg on object storage such as Amazon Web Services S3.

---

## 2. Cost Drivers in Iceberg

### 2.1 Storage Costs

Primary contributors:

* Data files (Parquet/ORC/Avro)
* Metadata files (manifests, manifest lists, metadata.json)
* Orphaned files
* Historical snapshots

### 2.2 Compute Costs

* Query execution (scan + planning)
* Compaction jobs
* Metadata operations

### 2.3 Request Costs (Object Storage)

* GET (reads during query planning and execution)
* LIST (reduced in Iceberg but still present)
* PUT (writes during ingestion and compaction)

---

## 3. Storage Cost Amplifiers

### 3.1 Snapshot Retention

Each write creates a new snapshot:

* Retains references to older data files
* Prevents deletion of obsolete files

Impact:

* Linear or exponential storage growth depending on ingestion rate

---

### 3.2 Small File Problem

Frequent small writes result in:

* Increased number of files
* Higher metadata overhead
* More GET requests per query

---

### 3.3 Metadata Growth

Manifest and metadata files accumulate:

* Increased planning time
* Additional storage consumption

---

### 3.4 Orphan Files

Files not referenced by any snapshot:

* Persist indefinitely without cleanup
* Invisible to queries but billable

---

### 3.5 Object Store Versioning

If enabled at the storage layer:

* Deleted files are retained as previous versions
* Storage usage increases without visibility at the Iceberg layer

---

## 4. Cost Control Mechanisms

### 4.1 Snapshot Expiration

Removes older snapshots and dereferences unused data files.

**Policy Recommendations:**

* Retain 7–30 days of snapshots
* Retain minimum number required for rollback/debugging

**Execution (example):**

```sql
CALL system.expire_snapshots(
  table => 'db.table',
  older_than => TIMESTAMP 'YYYY-MM-DD HH:MM:SS'
);
```

---

### 4.2 Orphan File Removal

Deletes files not referenced by any metadata.

```sql
CALL system.remove_orphan_files(
  table => 'db.table'
);
```

**Frequency:** Daily

---

### 4.3 File Compaction

Merges small files into larger ones to reduce overhead.

```sql
CALL system.rewrite_data_files(
  table => 'db.table'
);
```

**Target file size:**

* 128 MB – 512 MB (Parquet)

**Frequency:**

* Streaming ingestion: hourly/daily
* Batch ingestion: as needed

---

### 4.4 Manifest Optimization

Reduces metadata overhead and improves query planning.

```sql
CALL system.rewrite_manifests('db.table');
```

---

### 4.5 Partition Strategy Optimization

* Avoid over-partitioning
* Use partition evolution when needed
* Ensure partitions align with query patterns

---

## 5. Object Storage Configuration

### 5.1 Versioning Strategy

For Iceberg-managed data:

* **Recommended:** Disable versioning on data buckets
* If required (compliance):

  * Apply lifecycle rules:

    * Expire non-current versions (7–30 days)
    * Remove delete markers

---

### 5.2 Lifecycle Policies

Define automated cleanup:

* Expire incomplete uploads
* Delete temporary files
* Remove aged data outside retention policies

---

## 6. Ingestion Best Practices

### 6.1 File Size Control

* Avoid very small batches
* Configure writers to produce optimal file sizes

### 6.2 Batching Strategy

* Use micro-batching instead of per-record writes
* Avoid forced single partition writes unless necessary

### 6.3 Schema Stability

* Frequent schema changes increase metadata churn

---

## 7. Query Cost Optimization

### 7.1 Predicate Pushdown

* Ensure queries filter on partitioned columns

### 7.2 Column Pruning

* Select only required columns

### 7.3 Metadata Pruning

* Maintained through manifest optimization

---

## 8. Monitoring and Observability

Track the following metrics:

### 8.1 Storage Metrics

* Total table size
* Snapshot count
* Metadata size
* Orphan file volume

### 8.2 File Metrics

* Average file size
* File count per partition

### 8.3 Query Metrics

* Data scanned per query
* Query latency

### 8.4 Cost Indicators

* Storage growth rate
* Object store request rates (GET, PUT)

---

## 9. Operational Scheduling

| Operation           | Frequency      |
| ------------------- | -------------- |
| Snapshot expiration | Daily          |
| Orphan cleanup      | Daily          |
| Compaction          | Hourly / Daily |
| Manifest rewrite    | Daily / Weekly |

---

## 10. Recommended Architecture

* Storage: S3 (versioning disabled or lifecycle-managed)
* Table format: Iceberg
* Compute: Apache Spark / Trino
* Catalog: REST / Hive Metastore / Glue
* Orchestration: Scheduled workflows (e.g., Flyte, Airflow)

---

## 11. Risk Summary

| Risk                  | Impact              | Mitigation                    |
| --------------------- | ------------------- | ----------------------------- |
| Snapshot accumulation | Storage growth      | Expire snapshots              |
| Small files           | Query inefficiency  | Compaction                    |
| Orphan files          | Hidden storage cost | Orphan cleanup                |
| Metadata bloat        | Planning latency    | Manifest rewrite              |
| S3 versioning enabled | Storage duplication | Disable or lifecycle policies |

---

## 12. Baseline Configuration Checklist

* Snapshot retention policy defined
* Daily cleanup jobs scheduled
* Compaction configured
* File size targets enforced
* Object storage lifecycle rules applied
* Monitoring dashboards in place

---

## 13. Conclusion

Cost efficiency in Iceberg systems is achieved through:

* Controlled snapshot retention
* Continuous file and metadata optimization
* Explicit lifecycle management

Unmanaged deployments will exhibit:

* Unbounded storage growth
* Increasing query latency
* Escalating operational costs

A disciplined operational model is required to maintain predictable cost and performance characteristics.
