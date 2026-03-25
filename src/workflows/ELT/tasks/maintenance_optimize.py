from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from flytekit import Resources, task
from flytekitplugins.spark import Spark
from pyspark.sql import SparkSession

from workflows.ELT.tasks.bronze_ingest import (
    BRONZE_NAMESPACE,
    CATALOG_NAME,
    ICEBERG_EXPIRE_DAYS,
    ICEBERG_ORPHAN_DAYS,
    ICEBERG_RETAIN_LAST,
    MAINTENANCE_REWRITE_DAYS,
    SILVER_NAMESPACE,
    TASK_IMAGE,
    BronzeIngestResult,
    build_hadoop_conf,
    build_spark_conf,
    ensure_namespace,
    get_spark_session,
    log_json,
    qualify_table_id,
    table_exists,
)
from workflows.ELT.tasks.silver_transform import SilverTransformResult

LOG = logging.getLogger("elt_maintenance_optimize")
LOG.setLevel(logging.INFO)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_handler]
LOG.propagate = False

ENABLE_MAINTENANCE = os.environ.get("ENABLE_MAINTENANCE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
FAIL_OPEN_MAINTENANCE = os.environ.get("FAIL_OPEN_MAINTENANCE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


@dataclass(frozen=True)
class MaintenanceResult:
    run_id: str
    status: str
    managed_tables: str
    rewritten_table: str
    expire_days: int
    orphan_days: int
    errors: str = ""


def utc_cutoff_string(days: int) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def execute_sql_action(spark: SparkSession, statement: str) -> None:
    spark.sql(statement).rdd.foreachPartition(lambda _: None)


def expire_snapshots_call(table_id: str, older_than: str) -> str:
    return (
        f"CALL {CATALOG_NAME}.system.expire_snapshots("
        f"table => '{table_id}', "
        f"older_than => TIMESTAMP '{older_than}', "
        f"retain_last => {ICEBERG_RETAIN_LAST}, "
        f"clean_expired_metadata => true, "
        f"stream_results => true)"
    )


def remove_orphan_files_call(table_id: str, older_than: str) -> str:
    return (
        f"CALL {CATALOG_NAME}.system.remove_orphan_files("
        f"table => '{table_id}', "
        f"older_than => TIMESTAMP '{older_than}', "
        f"dry_run => false, "
        f"prefix_mismatch_mode => 'IGNORE')"
    )


def rewrite_data_files_call(table_id: str) -> str:
    rewrite_where = os.environ.get(
        "MAINTENANCE_REWRITE_WHERE",
        f"pickup_date >= date_sub(current_date(), {MAINTENANCE_REWRITE_DAYS})",
    )
    return (
        f"CALL {CATALOG_NAME}.system.rewrite_data_files("
        f"table => '{table_id}', "
        f"where => '{rewrite_where}', "
        f"options => map('min-input-files', '2', 'remove-dangling-deletes', 'true'))"
    )


@task(
    task_config=Spark(
        spark_conf=build_spark_conf(),
        hadoop_conf=build_hadoop_conf(),
        executor_path="/opt/venv/bin/python",
    ),
    container_image=TASK_IMAGE,
    retries=1,
    limits=Resources(mem="2500M"),
)
def maintenance_optimize(
    bronze: BronzeIngestResult,
    silver: SilverTransformResult,
) -> MaintenanceResult:
    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    ensure_namespace(spark, CATALOG_NAME, BRONZE_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, SILVER_NAMESPACE)

    managed_tables = [
        qualify_table_id(bronze.trips_table),
        qualify_table_id(bronze.taxi_zone_table),
        qualify_table_id(silver.silver_table),
    ]

    if not ENABLE_MAINTENANCE:
        result = MaintenanceResult(
            run_id=bronze.run_id,
            status="skipped",
            managed_tables=",".join(managed_tables),
            rewritten_table="",
            expire_days=ICEBERG_EXPIRE_DAYS,
            orphan_days=ICEBERG_ORPHAN_DAYS,
            errors="maintenance disabled by ENABLE_MAINTENANCE=false",
        )
        log_json(msg="maintenance_skipped", **result.__dict__)
        return result

    log_json(
        msg="maintenance_start",
        run_id=bronze.run_id,
        tables=managed_tables,
        expire_days=ICEBERG_EXPIRE_DAYS,
        orphan_days=ICEBERG_ORPHAN_DAYS,
    )

    rewritten_table = ""
    errors: list[str] = []

    def run_best_effort(label: str, statement: str) -> None:
        try:
            execute_sql_action(spark, statement)
        except Exception as exc:
            msg = f"{label}: {exc}"
            errors.append(msg)
            log_json(msg="maintenance_error", label=label, error=str(exc))
            if not FAIL_OPEN_MAINTENANCE:
                raise

    if table_exists(spark, silver.silver_table):
        silver_table = qualify_table_id(silver.silver_table)
        log_json(msg="maintenance_rewrite_start", table=silver_table)
        run_best_effort("rewrite_data_files", rewrite_data_files_call(silver_table))
        rewritten_table = silver_table
        log_json(msg="maintenance_rewrite_done", table=silver_table)

    for table_id in managed_tables:
        if not table_exists(spark, table_id):
            log_json(msg="maintenance_skip_missing_table", table=table_id)
            continue

        expire_before = utc_cutoff_string(ICEBERG_EXPIRE_DAYS)
        orphan_before = utc_cutoff_string(ICEBERG_ORPHAN_DAYS)

        log_json(msg="maintenance_expire_start", table=table_id, older_than=expire_before)
        run_best_effort("expire_snapshots", expire_snapshots_call(table_id, expire_before))
        log_json(msg="maintenance_expire_done", table=table_id)

        log_json(msg="maintenance_orphan_cleanup_start", table=table_id, older_than=orphan_before)
        run_best_effort("remove_orphan_files", remove_orphan_files_call(table_id, orphan_before))
        log_json(msg="maintenance_orphan_cleanup_done", table=table_id)

    status = "ok" if not errors else "degraded"
    result = MaintenanceResult(
        run_id=bronze.run_id,
        status=status,
        managed_tables=",".join(managed_tables),
        rewritten_table=rewritten_table,
        expire_days=ICEBERG_EXPIRE_DAYS,
        orphan_days=ICEBERG_ORPHAN_DAYS,
        errors="; ".join(errors),
    )
    log_json(msg="maintenance_success", **result.__dict__)
    return result