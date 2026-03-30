from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from flytekit import Resources, task
from flytekitplugins.spark import Spark
from pyspark.sql import SparkSession

from src.workflows.ELT.tasks.bronze_ingest import (
    BRONZE_TAXI_ZONE_TABLE,
    BRONZE_TRIPS_TABLE,
    CATALOG_NAME,
    GOLD_CONTRACT_TABLE,
    GOLD_TRAINING_TABLE,
    SILVER_TRIPS_TABLE,
    TASK_IMAGE,
    build_hadoop_conf,
    build_spark_conf,
    build_task_environment,
    ensure_namespace,
    get_spark_session,
    log_json,
    parse_table_id,
    qualify_table_id,
    table_exists,
    validate_iceberg_catalog,
)

LOG = logging.getLogger("elt_maintenance_optimize")
LOG.setLevel(logging.INFO)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_handler]
LOG.propagate = False

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()
ELT_PROFILE = os.environ.get(
    "ELT_PROFILE",
    "dev" if K8S_CLUSTER in {"kind", "minikube", "docker-desktop", "local"} else "prod",
).strip().lower()

ICEBERG_EXPIRE_DAYS = int(os.environ.get("ICEBERG_EXPIRE_DAYS", "7"))
ICEBERG_ORPHAN_DAYS = int(os.environ.get("ICEBERG_ORPHAN_DAYS", "3"))
ICEBERG_RETAIN_LAST = max(1, int(os.environ.get("ICEBERG_RETAIN_LAST", "2")))

DEFAULT_REWRITE_WHERE = "as_of_date >= date_sub(current_date(), 30)"


@dataclass(frozen=True)
class MaintenanceResult:
    run_id: str
    status: str
    expired_tables: str
    rewritten_tables: str
    skipped_tables: str
    failed_tables: str
    table_results_json: str
    expire_days: int
    orphan_days: int


def parse_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def utc_cutoff_string(days: int) -> str:
    if days < 0:
        raise RuntimeError(f"days must be non-negative, got {days}")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def execute_sql_action(spark: SparkSession, statement: str) -> None:
    spark.sql(statement).collect()


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


def rewrite_data_files_call(table_id: str, where_clause: str) -> str:
    escaped_where = where_clause.replace("'", "''")
    return (
        f"CALL {CATALOG_NAME}.system.rewrite_data_files("
        f"table => '{table_id}', "
        f"where => '{escaped_where}', "
        "options => map("
        "'min-input-files', '2', "
        "'remove-dangling-deletes', 'true'))"
    )


def parse_table_list(env_name: str, default_value: str) -> list[str]:
    raw = os.environ.get(env_name, default_value).strip()
    if not raw:
        return []
    return dedupe_preserve_order([qualify_table_id(item) for item in split_csv(raw)])


def parse_table_predicate_map(env_name: str) -> dict[str, str]:
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{env_name} must be valid JSON") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError(f"{env_name} must be a JSON object mapping table ids to predicates")

    out: dict[str, str] = {}
    for key, value in parsed.items():
        table_id = qualify_table_id(str(key).strip())
        predicate = str(value).strip()
        if predicate:
            out[table_id] = predicate
    return out


def table_has_column(spark: SparkSession, table_id: str, column: str) -> bool:
    return column in spark.table(table_id).columns


def table_op_result(table_id: str, operation: str, status: str, message: str = "") -> dict[str, Any]:
    return {
        "table_id": table_id,
        "operation": operation,
        "status": status,
        "message": message,
    }


def default_rewrite_predicates() -> dict[str, str]:
    return {qualify_table_id(GOLD_TRAINING_TABLE): DEFAULT_REWRITE_WHERE}


def maintenance_spark_conf() -> dict[str, str]:
    if ELT_PROFILE == "prod":
        spark_driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "1g")
        spark_executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY", "1g")
        spark_driver_memory_overhead = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "256m")
        spark_executor_memory_overhead = os.environ.get("SPARK_EXECUTOR_MEMORY_OVERHEAD", "256m")
        spark_executor_cores = os.environ.get("SPARK_EXECUTOR_CORES", "1")
        spark_executor_instances = os.environ.get("SPARK_EXECUTOR_INSTANCES", "1")
        spark_driver_cores = os.environ.get("SPARK_DRIVER_CORES", "1")
        spark_shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")
        spark_max_partition_bytes = os.environ.get("SPARK_MAX_PARTITION_BYTES", "134217728")
        spark_max_result_size = os.environ.get("SPARK_MAX_RESULT_SIZE", "256m")
    else:
        spark_driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "768m")
        spark_executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY", "512m")
        spark_driver_memory_overhead = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "256m")
        spark_executor_memory_overhead = os.environ.get("SPARK_EXECUTOR_MEMORY_OVERHEAD", "256m")
        spark_executor_cores = os.environ.get("SPARK_EXECUTOR_CORES", "1")
        spark_executor_instances = os.environ.get("SPARK_EXECUTOR_INSTANCES", "1")
        spark_driver_cores = os.environ.get("SPARK_DRIVER_CORES", "1")
        spark_shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")
        spark_max_partition_bytes = os.environ.get("SPARK_MAX_PARTITION_BYTES", "67108864")
        spark_max_result_size = os.environ.get("SPARK_MAX_RESULT_SIZE", "128m")

    return build_spark_conf(
        spark_driver_memory=spark_driver_memory,
        spark_executor_memory=spark_executor_memory,
        spark_driver_memory_overhead=spark_driver_memory_overhead,
        spark_executor_memory_overhead=spark_executor_memory_overhead,
        spark_executor_cores=spark_executor_cores,
        spark_executor_instances=spark_executor_instances,
        spark_driver_cores=spark_driver_cores,
        spark_shuffle_partitions=spark_shuffle_partitions,
        spark_max_partition_bytes=spark_max_partition_bytes,
        spark_max_result_size=spark_max_result_size,
    )


@task(
    task_config=Spark(
        spark_conf=maintenance_spark_conf(),
        hadoop_conf=build_hadoop_conf(),
        executor_path="/opt/venv/bin/python",
    ),
    container_image=TASK_IMAGE,
    environment=build_task_environment(),
    retries=int(os.environ.get("MAINTENANCE_TASK_RETRIES", "1")),
    limits=Resources(
        cpu="1000m" if ELT_PROFILE == "prod" else "500m",
        mem="768Mi" if ELT_PROFILE != "prod" else "1024Mi",
    ),
)
def maintenance_optimize() -> MaintenanceResult:
    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    validate_iceberg_catalog(spark)

    run_id = (
        os.environ.get("RUN_ID")
        or os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
        or uuid.uuid4().hex
    )
    continue_on_error = parse_bool(
        os.environ.get("MAINTENANCE_CONTINUE_ON_ERROR"),
        default=True,
    )

    expire_tables = parse_table_list(
        "ICEBERG_MAINTENANCE_EXPIRE_TABLES",
        ",".join(
            [
                BRONZE_TRIPS_TABLE,
                BRONZE_TAXI_ZONE_TABLE,
                SILVER_TRIPS_TABLE,
                GOLD_TRAINING_TABLE,
                GOLD_CONTRACT_TABLE,
            ]
        ),
    )

    orphan_tables = parse_table_list(
        "ICEBERG_MAINTENANCE_ORPHAN_TABLES",
        ",".join(expire_tables),
    )

    rewrite_tables = parse_table_list(
        "ICEBERG_MAINTENANCE_REWRITE_TABLES",
        GOLD_TRAINING_TABLE,
    )

    rewrite_where_by_table = parse_table_predicate_map(
        "ICEBERG_MAINTENANCE_REWRITE_WHERE_BY_TABLE_JSON"
    )
    if not rewrite_where_by_table:
        rewrite_where_by_table = default_rewrite_predicates()

    for table_id in dedupe_preserve_order(expire_tables + orphan_tables + rewrite_tables):
        catalog_name, namespace, _ = parse_table_id(table_id)
        ensure_namespace(spark, catalog_name, namespace)

    log_json(
        msg="maintenance_start",
        run_id=run_id,
        profile=ELT_PROFILE,
        k8s_cluster=K8S_CLUSTER,
        expire_tables=expire_tables,
        orphan_tables=orphan_tables,
        rewrite_tables=rewrite_tables,
        expire_days=ICEBERG_EXPIRE_DAYS,
        orphan_days=ICEBERG_ORPHAN_DAYS,
        retain_last=ICEBERG_RETAIN_LAST,
        continue_on_error=continue_on_error,
        rewrite_predicates=rewrite_where_by_table,
    )

    expired_done: list[str] = []
    rewritten_done: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []
    table_results: list[dict[str, Any]] = []

    expire_before = utc_cutoff_string(ICEBERG_EXPIRE_DAYS)
    orphan_before = utc_cutoff_string(ICEBERG_ORPHAN_DAYS)

    for table_id in expire_tables:
        if not table_exists(spark, table_id):
            skipped.append(table_id)
            table_results.append(
                table_op_result(
                    table_id=table_id,
                    operation="expire_orphan",
                    status="skipped",
                    message="table_missing",
                )
            )
            log_json(msg="maintenance_skip_missing_table", table=table_id, phase="expire_orphan")
            continue

        try:
            log_json(msg="maintenance_expire_start", table=table_id, older_than=expire_before)
            execute_sql_action(spark, expire_snapshots_call(table_id, expire_before))
            log_json(msg="maintenance_expire_done", table=table_id)

            if table_id in orphan_tables:
                log_json(
                    msg="maintenance_orphan_cleanup_start",
                    table=table_id,
                    older_than=orphan_before,
                )
                execute_sql_action(spark, remove_orphan_files_call(table_id, orphan_before))
                log_json(msg="maintenance_orphan_cleanup_done", table=table_id)

            expired_done.append(table_id)
            table_results.append(
                table_op_result(
                    table_id=table_id,
                    operation="expire_orphan",
                    status="ok",
                    message="completed",
                )
            )
        except Exception as exc:
            failed.append(table_id)
            table_results.append(
                table_op_result(
                    table_id=table_id,
                    operation="expire_orphan",
                    status="failed",
                    message=str(exc),
                )
            )
            log_json(
                msg="maintenance_expire_orphan_failed",
                table=table_id,
                error=str(exc),
            )
            if not continue_on_error:
                raise

    for table_id in rewrite_tables:
        if not table_exists(spark, table_id):
            skipped.append(table_id)
            table_results.append(
                table_op_result(
                    table_id=table_id,
                    operation="rewrite",
                    status="skipped",
                    message="table_missing",
                )
            )
            log_json(msg="maintenance_skip_missing_table", table=table_id, phase="rewrite")
            continue

        where_clause = rewrite_where_by_table.get(table_id, "").strip()
        if not where_clause:
            skipped.append(table_id)
            table_results.append(
                table_op_result(
                    table_id=table_id,
                    operation="rewrite",
                    status="skipped",
                    message="no_rewrite_predicate_configured",
                )
            )
            log_json(msg="maintenance_skip_rewrite_no_predicate", table=table_id)
            continue

        try:
            if "as_of_date" in where_clause and not table_has_column(
                spark, table_id, "as_of_date"
            ):
                skipped.append(table_id)
                table_results.append(
                    table_op_result(
                        table_id=table_id,
                        operation="rewrite",
                        status="skipped",
                        message="missing_as_of_date_column",
                    )
                )
                log_json(
                    msg="maintenance_skip_rewrite_missing_column",
                    table=table_id,
                    column="as_of_date",
                )
                continue

            log_json(msg="maintenance_rewrite_start", table=table_id, where=where_clause)
            execute_sql_action(spark, rewrite_data_files_call(table_id, where_clause))
            log_json(msg="maintenance_rewrite_done", table=table_id)

            rewritten_done.append(table_id)
            table_results.append(
                table_op_result(
                    table_id=table_id,
                    operation="rewrite",
                    status="ok",
                    message="completed",
                )
            )
        except Exception as exc:
            failed.append(table_id)
            table_results.append(
                table_op_result(
                    table_id=table_id,
                    operation="rewrite",
                    status="failed",
                    message=str(exc),
                )
            )
            log_json(
                msg="maintenance_rewrite_failed",
                table=table_id,
                error=str(exc),
            )
            if not continue_on_error:
                raise

    if failed:
        raise RuntimeError(
            "Iceberg maintenance completed with failures: "
            + json.dumps(
                {
                    "run_id": run_id,
                    "failed_tables": failed,
                    "table_results": table_results,
                },
                sort_keys=True,
            )
        )

    result = MaintenanceResult(
        run_id=run_id,
        status="ok",
        expired_tables=",".join(expired_done),
        rewritten_tables=",".join(rewritten_done),
        skipped_tables=",".join(skipped),
        failed_tables=",".join(failed),
        table_results_json=json.dumps(table_results, sort_keys=True),
        expire_days=ICEBERG_EXPIRE_DAYS,
        orphan_days=ICEBERG_ORPHAN_DAYS,
    )
    log_json(msg="maintenance_success", **result.__dict__)
    return result