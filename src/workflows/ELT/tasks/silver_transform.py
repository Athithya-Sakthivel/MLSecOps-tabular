from __future__ import annotations

import logging
import math
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass

from flytekit import Resources, task
from flytekitplugins.spark import Spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from src.workflows.ELT.tasks.bronze_ingest import (
    BRONZE_NAMESPACE,
    CATALOG_NAME,
    GOLD_NAMESPACE,
    SILVER_NAMESPACE,
    SILVER_TRIPS_TABLE,
    TASK_IMAGE,
    BronzeIngestResult,
    build_hadoop_conf,
    build_spark_conf,
    build_task_environment,
    ensure_namespace,
    get_spark_session,
    log_json,
    qualify_table_id,
    table_exists,
    validate_iceberg_catalog,
)

LOG = logging.getLogger("elt_silver_transform")
LOG.setLevel(logging.INFO)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_handler]
LOG.propagate = False

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()
ELT_PROFILE = (
    os.environ.get(
        "ELT_PROFILE",
        "dev" if K8S_CLUSTER in {"kind", "minikube", "docker-desktop", "local"} else "prod",
    )
    .strip()
    .lower()
)

if ELT_PROFILE == "prod":
    TASK_LIMITS = Resources(cpu="1000m", mem="1024Mi")
    SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")
    SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "512m")
    SPARK_EXECUTOR_MEMORY_OVERHEAD = os.environ.get("SPARK_EXECUTOR_MEMORY_OVERHEAD", "512m")
    SPARK_EXECUTOR_CORES = os.environ.get("SPARK_EXECUTOR_CORES", "1")
    SPARK_EXECUTOR_INSTANCES = os.environ.get("SPARK_EXECUTOR_INSTANCES", "1")
    SPARK_DRIVER_CORES = os.environ.get("SPARK_DRIVER_CORES", "1")
    SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")
    SPARK_MAX_PARTITION_BYTES = os.environ.get("SPARK_MAX_PARTITION_BYTES", "134217728")
    SPARK_MAX_RESULT_SIZE = os.environ.get("SPARK_MAX_RESULT_SIZE", "256m")
    TASK_RETRIES = int(os.environ.get("SILVER_TASK_RETRIES", "1"))
    SILVER_ROWS_PER_PARTITION = int(os.environ.get("SILVER_ROWS_PER_PARTITION", "100000"))
else:
    TASK_LIMITS = Resources(cpu="500m", mem="768Mi")
    SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1g")
    SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "1g")
    SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "256m")
    SPARK_EXECUTOR_MEMORY_OVERHEAD = os.environ.get("SPARK_EXECUTOR_MEMORY_OVERHEAD", "256m")
    SPARK_EXECUTOR_CORES = os.environ.get("SPARK_EXECUTOR_CORES", "1")
    SPARK_EXECUTOR_INSTANCES = os.environ.get("SPARK_EXECUTOR_INSTANCES", "1")
    SPARK_DRIVER_CORES = os.environ.get("SPARK_DRIVER_CORES", "1")
    SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")
    SPARK_MAX_PARTITION_BYTES = os.environ.get("SPARK_MAX_PARTITION_BYTES", "67108864")
    SPARK_MAX_RESULT_SIZE = os.environ.get("SPARK_MAX_RESULT_SIZE", "128m")
    TASK_RETRIES = int(os.environ.get("SILVER_TASK_RETRIES", "1"))
    SILVER_ROWS_PER_PARTITION = int(os.environ.get("SILVER_ROWS_PER_PARTITION", "50000"))


@dataclass(frozen=True)
class SilverTransformResult:
    run_id: str
    silver_table: str
    source_trips_table: str
    source_taxi_zone_table: str
    write_mode: str
    status: str


def require_columns(df: DataFrame, required: Sequence[str], label: str) -> None:
    missing = set(required) - set(df.columns)
    if missing:
        raise RuntimeError(f"{label} is missing required columns: {sorted(missing)}")


def ensure_column(df: DataFrame, column: str, spark_type: str) -> DataFrame:
    if column in df.columns:
        return df.withColumn(column, F.col(column).cast(spark_type))
    return df.withColumn(column, F.lit(None).cast(spark_type))


def ensure_trips_schema(df: DataFrame) -> DataFrame:
    required = (
        "pickup_ts",
        "dropoff_ts",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "run_id",
        "source_revision",
        "source_file",
        "source_uri",
        "ingestion_ts",
    )
    require_columns(df, required, "bronze trips table")

    cast_map = {
        "pickup_location_id": "long",
        "dropoff_location_id": "long",
        "trip_distance": "double",
        "fare_amount": "double",
        "tip_amount": "double",
        "total_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "congestion_surcharge": "double",
        "cbd_congestion_fee": "double",
        "vendor_id": "long",
        "ratecode_id": "long",
        "passenger_count": "long",
        "payment_type": "long",
        "trip_type": "long",
        "store_and_fwd_flag": "string",
    }

    normalized = df
    for col_name, spark_type in cast_map.items():
        normalized = ensure_column(normalized, col_name, spark_type)

    return normalized


def ensure_zone_schema(df: DataFrame) -> DataFrame:
    required = ("location_id", "borough", "zone", "service_zone")
    require_columns(df, required, "bronze taxi zone table")

    return df.select(
        F.col("location_id").cast("long").alias("location_id"),
        F.col("borough").cast("string").alias("borough"),
        F.col("zone").cast("string").alias("zone"),
        F.col("service_zone").cast("string").alias("service_zone"),
    ).dropDuplicates(["location_id"])


def stable_trip_id_expr() -> F.Column:
    return F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("t.pickup_ts").cast("string"), F.lit("")),
            F.coalesce(F.col("t.dropoff_ts").cast("string"), F.lit("")),
            F.coalesce(F.col("t.pickup_location_id").cast("string"), F.lit("")),
            F.coalesce(F.col("t.dropoff_location_id").cast("string"), F.lit("")),
            F.coalesce(F.col("t.vendor_id").cast("string"), F.lit("")),
            F.coalesce(F.col("t.ratecode_id").cast("string"), F.lit("")),
            F.coalesce(F.col("t.passenger_count").cast("string"), F.lit("")),
            F.coalesce(F.col("t.payment_type").cast("string"), F.lit("")),
            F.coalesce(F.col("t.trip_distance").cast("string"), F.lit("")),
            F.coalesce(F.col("t.fare_amount").cast("string"), F.lit("")),
            F.coalesce(F.col("t.total_amount").cast("string"), F.lit("")),
            F.coalesce(F.col("t.source_revision").cast("string"), F.lit("")),
            F.coalesce(F.col("t.source_file").cast("string"), F.lit("")),
        ),
        256,
    )


def write_partitioned_iceberg_table(df: DataFrame, table_id: str, partition_column: str) -> str:
    table_id = qualify_table_id(table_id)
    if table_exists(df.sparkSession, table_id):
        df.writeTo(table_id).overwritePartitions()
        return "overwrite_partitions"

    (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", "268435456")
        .partitionedBy(F.col(partition_column))
        .create()
    )
    return "create"


def build_canonical_frame(trips_df: DataFrame, zones_df: DataFrame, run_id: str) -> DataFrame:
    trips_df = ensure_trips_schema(trips_df)
    zones_df = ensure_zone_schema(zones_df)

    pickup_lookup = broadcast(
        zones_df.select(
            F.col("location_id").alias("pickup_location_id_join"),
            F.col("borough").alias("pickup_borough"),
            F.col("zone").alias("pickup_zone"),
            F.col("service_zone").alias("pickup_service_zone"),
        )
    )
    dropoff_lookup = broadcast(
        zones_df.select(
            F.col("location_id").alias("dropoff_location_id_join"),
            F.col("borough").alias("dropoff_borough"),
            F.col("zone").alias("dropoff_zone"),
            F.col("service_zone").alias("dropoff_service_zone"),
        )
    )

    joined = (
        trips_df.alias("t")
        .join(
            pickup_lookup,
            F.col("t.pickup_location_id") == F.col("pickup_location_id_join"),
            "left",
        )
        .join(
            dropoff_lookup,
            F.col("t.dropoff_location_id") == F.col("dropoff_location_id_join"),
            "left",
        )
    )

    canonical = joined.select(
        F.col("t.run_id").alias("bronze_run_id"),
        F.col("t.source_uri"),
        F.col("t.source_revision"),
        F.col("t.source_file"),
        F.col("t.source_kind"),
        F.col("t.ingestion_ts"),
        F.col("t.pickup_ts"),
        F.col("t.dropoff_ts"),
        F.col("t.pickup_location_id"),
        F.col("t.dropoff_location_id"),
        F.col("t.vendor_id"),
        F.col("t.ratecode_id"),
        F.col("t.passenger_count"),
        F.col("t.payment_type"),
        F.col("t.trip_type"),
        F.col("t.store_and_fwd_flag"),
        F.col("t.trip_distance"),
        F.col("t.fare_amount"),
        F.col("t.tip_amount"),
        F.col("t.total_amount"),
        F.col("t.extra"),
        F.col("t.mta_tax"),
        F.col("t.tolls_amount"),
        F.col("t.improvement_surcharge"),
        F.col("t.congestion_surcharge"),
        F.col("t.cbd_congestion_fee"),
        F.col("pickup_borough"),
        F.col("pickup_zone"),
        F.col("pickup_service_zone"),
        F.col("dropoff_borough"),
        F.col("dropoff_zone"),
        F.col("dropoff_service_zone"),
    ).withColumn("trip_id", stable_trip_id_expr())

    canonical = (
        canonical.withColumn("pickup_date", F.to_date(F.col("pickup_ts")))
        .withColumn("pickup_hour", F.hour(F.col("pickup_ts")).cast("int"))
        .withColumn("pickup_dow", F.dayofweek(F.col("pickup_ts")).cast("int"))
        .withColumn("pickup_month", F.month(F.col("pickup_ts")).cast("int"))
        .withColumn(
            "pickup_is_weekend",
            F.when(F.dayofweek(F.col("pickup_ts")).isin(1, 7), F.lit(1)).otherwise(F.lit(0)).cast("int"),
        )
        .withColumn(
            "trip_duration_seconds",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")).cast("long"),
        )
        .withColumn(
            "trip_duration_minutes",
            F.round(F.col("trip_duration_seconds") / F.lit(60.0), 3),
        )
        .withColumn("silver_run_id", F.lit(run_id))
    )

    canonical = canonical.select(
        "trip_id",
        "pickup_date",
        "pickup_ts",
        "dropoff_ts",
        "pickup_hour",
        "pickup_dow",
        "pickup_month",
        "pickup_is_weekend",
        "trip_duration_seconds",
        "trip_duration_minutes",
        "pickup_location_id",
        "dropoff_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "dropoff_borough",
        "dropoff_zone",
        "dropoff_service_zone",
        "vendor_id",
        "ratecode_id",
        "passenger_count",
        "payment_type",
        "trip_type",
        "store_and_fwd_flag",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "extra",
        "mta_tax",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "cbd_congestion_fee",
        "source_uri",
        "source_revision",
        "source_file",
        "source_kind",
        "ingestion_ts",
        "bronze_run_id",
        "silver_run_id",
    )

    required = {
        "trip_id",
        "pickup_date",
        "pickup_ts",
        "dropoff_ts",
        "pickup_location_id",
        "dropoff_location_id",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
        "trip_duration_seconds",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "source_uri",
        "source_revision",
        "source_file",
    }
    missing = required - set(canonical.columns)
    if missing:
        raise RuntimeError(f"silver canonical dataframe is missing required columns: {sorted(missing)}")

    return canonical.filter(
        F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & F.col("pickup_location_id").isNotNull()
        & F.col("dropoff_location_id").isNotNull()
        & (F.col("trip_duration_seconds") > 0)
        & (F.col("trip_distance") >= 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
    )


def silver_spark_conf() -> dict[str, str]:
    return build_spark_conf(
        spark_driver_memory=SPARK_DRIVER_MEMORY,
        spark_executor_memory=SPARK_EXECUTOR_MEMORY,
        spark_driver_memory_overhead=SPARK_DRIVER_MEMORY_OVERHEAD,
        spark_executor_memory_overhead=SPARK_EXECUTOR_MEMORY_OVERHEAD,
        spark_executor_cores=SPARK_EXECUTOR_CORES,
        spark_executor_instances=SPARK_EXECUTOR_INSTANCES,
        spark_driver_cores=SPARK_DRIVER_CORES,
        spark_shuffle_partitions=SPARK_SHUFFLE_PARTITIONS,
        spark_max_partition_bytes=SPARK_MAX_PARTITION_BYTES,
        spark_max_result_size=SPARK_MAX_RESULT_SIZE,
    )


@task(
    task_config=Spark(
        spark_conf=silver_spark_conf(),
        hadoop_conf=build_hadoop_conf(),
        executor_path="/opt/venv/bin/python",
    ),
    container_image=TASK_IMAGE,
    environment=build_task_environment(),
    retries=TASK_RETRIES,
    limits=TASK_LIMITS,
)
def silver_transform(bronze: BronzeIngestResult) -> SilverTransformResult:
    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    validate_iceberg_catalog(spark)

    bronze_trips_table = qualify_table_id(bronze.trips_table)
    bronze_taxi_zone_table = qualify_table_id(bronze.taxi_zone_table)
    silver_table = qualify_table_id(SILVER_TRIPS_TABLE)

    ensure_namespace(spark, CATALOG_NAME, SILVER_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, BRONZE_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, GOLD_NAMESPACE)

    log_json(
        msg="silver_transform_start",
        profile=ELT_PROFILE,
        k8s_cluster=K8S_CLUSTER,
        run_id=bronze.run_id,
        bronze_trips_table=bronze_trips_table,
        bronze_taxi_zone_table=bronze_taxi_zone_table,
        silver_table=silver_table,
        bronze_trips_rows=bronze.trips_rows,
    )

    trips_df = spark.table(bronze_trips_table)
    zones_df = spark.table(bronze_taxi_zone_table)

    canonical_df = build_canonical_frame(trips_df, zones_df, bronze.run_id)

    target_partitions = max(1, math.ceil(max(bronze.trips_rows, 1) / SILVER_ROWS_PER_PARTITION))
    canonical_df = canonical_df.repartition(target_partitions, F.col("pickup_date"))

    write_mode = write_partitioned_iceberg_table(
        canonical_df,
        silver_table,
        "pickup_date",
    )

    result = SilverTransformResult(
        run_id=bronze.run_id,
        silver_table=silver_table,
        source_trips_table=bronze_trips_table,
        source_taxi_zone_table=bronze_taxi_zone_table,
        write_mode=write_mode,
        status="ok",
    )
    log_json(msg="silver_transform_success", **result.__dict__)
    return result
