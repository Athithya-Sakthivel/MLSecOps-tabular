from __future__ import annotations

import logging
import math
import os
import sys
from dataclasses import dataclass
from typing import Sequence

from flytekit import Resources, task
from flytekitplugins.spark import Spark
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import broadcast

from workflows.ELT.tasks.bronze_ingest import (
    CATALOG_NAME,
    ICEBERG_TARGET_FILE_SIZE_BYTES,
    SILVER_NAMESPACE,
    SILVER_ROWS_PER_PARTITION,
    SILVER_TRIPS_TABLE,
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

LOG = logging.getLogger("elt_silver_transform")
LOG.setLevel(logging.INFO)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_handler]
LOG.propagate = False


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


def write_partitioned_iceberg_table(df: DataFrame, table_id: str, partition_column: str) -> str:
    table_id = qualify_table_id(table_id)
    if table_exists(df.sparkSession, table_id):
        df.writeTo(table_id).overwritePartitions()
        return "overwrite_partitions"

    (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
        .partitionedBy(F.col(partition_column))
        .create()
    )
    return "create"


def build_feature_frame(trips_df: DataFrame, zones_df: DataFrame, run_id: str) -> DataFrame:
    require_columns(
        trips_df,
        (
            "pickup_ts",
            "dropoff_ts",
            "pickup_location_id",
            "dropoff_location_id",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ),
        "bronze trips table",
    )
    require_columns(
        zones_df,
        ("location_id", "borough", "zone", "service_zone"),
        "bronze taxi zone table",
    )

    zones_df = zones_df.dropDuplicates(["location_id"]).select(
        F.col("location_id").cast("long").alias("location_id"),
        F.col("borough").cast("string").alias("borough"),
        F.col("zone").cast("string").alias("zone"),
        F.col("service_zone").cast("string").alias("service_zone"),
    )

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

    pickup_ts = F.col("pickup_ts")
    dropoff_ts = F.col("dropoff_ts")
    trip_duration_seconds = (F.unix_timestamp(dropoff_ts) - F.unix_timestamp(pickup_ts)).cast("long")
    trip_duration_minutes = F.round(trip_duration_seconds / F.lit(60.0), 3)

    feature_df = (
        joined.withColumn(
            "trip_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(pickup_ts.cast("string"), F.lit("")),
                    F.coalesce(dropoff_ts.cast("string"), F.lit("")),
                    F.coalesce(F.col("pickup_location_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("dropoff_location_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("passenger_count").cast("string"), F.lit("")),
                    F.coalesce(F.col("trip_distance").cast("string"), F.lit("")),
                    F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn("pickup_date", F.to_date(pickup_ts))
        .withColumn("pickup_hour", F.hour(pickup_ts))
        .withColumn("pickup_day_of_week", F.dayofweek(pickup_ts))
        .withColumn("pickup_month", F.month(pickup_ts))
        .withColumn("is_weekend", F.dayofweek(pickup_ts).isin(1, 7))
        .withColumn("trip_duration_seconds", trip_duration_seconds)
        .withColumn("trip_duration_minutes", trip_duration_minutes)
        .withColumn(
            "trip_speed_mph",
            F.when(
                trip_duration_seconds > 0,
                F.round(F.col("trip_distance") / (trip_duration_seconds / F.lit(3600.0)), 3),
            ),
        )
        .withColumn(
            "fare_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.round(F.col("fare_amount") / F.col("trip_distance"), 3),
            ),
        )
        .withColumn(
            "route_pair",
            F.concat_ws(
                "->",
                F.coalesce(F.col("pickup_zone"), F.lit("unknown_pickup_zone")),
                F.coalesce(F.col("dropoff_zone"), F.lit("unknown_dropoff_zone")),
            ),
        )
        .withColumn("run_id", F.lit(run_id))
    )

    selected_columns = [
        "trip_key",
        "run_id",
        "source_uri",
        "source_revision",
        "source_kind",
        "source_file",
        "ingestion_ts",
        "pickup_ts",
        "dropoff_ts",
        "pickup_date",
        "pickup_hour",
        "pickup_day_of_week",
        "pickup_month",
        "is_weekend",
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
        "store_and_fwd_flag",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "ehail_fee",
        "improvement_surcharge",
        "total_amount",
        "payment_type",
        "trip_type",
        "congestion_surcharge",
        "cbd_congestion_fee",
        "trip_duration_seconds",
        "trip_duration_minutes",
        "trip_speed_mph",
        "fare_per_mile",
        "route_pair",
    ]

    existing_selected = [c for c in selected_columns if c in feature_df.columns]
    feature_df = feature_df.select(*existing_selected)

    require_columns(
        feature_df,
        (
            "trip_key",
            "pickup_date",
            "pickup_location_id",
            "dropoff_location_id",
            "trip_duration_seconds",
            "fare_per_mile",
        ),
        "silver feature dataframe",
    )
    return feature_df.filter(
        F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & (F.col("trip_duration_seconds") > 0)
        & (F.col("trip_distance") >= 0)
    )


@task(
    task_config=Spark(
        spark_conf=build_spark_conf(),
        hadoop_conf=build_hadoop_conf(),
        executor_path="/opt/venv/bin/python",
    ),
    container_image=TASK_IMAGE,
    retries=2,
    limits=Resources(mem="2500M"),
)
def silver_transform(bronze: BronzeIngestResult) -> SilverTransformResult:
    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    bronze_trips_table = qualify_table_id(bronze.trips_table)
    bronze_taxi_zone_table = qualify_table_id(bronze.taxi_zone_table)
    silver_table = qualify_table_id(SILVER_TRIPS_TABLE)

    ensure_namespace(spark, CATALOG_NAME, SILVER_NAMESPACE)

    log_json(
        msg="silver_transform_start",
        run_id=bronze.run_id,
        bronze_trips_table=bronze_trips_table,
        bronze_taxi_zone_table=bronze_taxi_zone_table,
        silver_table=silver_table,
    )

    trips_df = spark.table(bronze_trips_table)
    zones_df = spark.table(bronze_taxi_zone_table)

    feature_df = build_feature_frame(trips_df, zones_df, bronze.run_id)

    target_partitions = max(1, math.ceil(bronze.trips_rows / SILVER_ROWS_PER_PARTITION))
    feature_df = feature_df.repartition(target_partitions, F.col("pickup_date"))

    write_mode = write_partitioned_iceberg_table(
        feature_df,
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