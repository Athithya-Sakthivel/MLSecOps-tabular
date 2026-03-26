from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from flytekit import Resources, task
from flytekitplugins.spark import Spark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from workflows.ELT.tasks.bronze_ingest import (
    CATALOG_NAME,
    GOLD_CONTRACT_TABLE,
    GOLD_NAMESPACE,
    GOLD_TRAINING_TABLE,
    ICEBERG_TARGET_FILE_SIZE_BYTES,
    TASK_IMAGE,
    build_hadoop_conf,
    build_spark_conf,
    ensure_namespace,
    get_spark_session,
    log_json,
    qualify_table_id,
    table_exists,
)
from workflows.ELT.tasks.silver_transform import SilverTransformResult

LOG = logging.getLogger("elt_gold_features")
LOG.setLevel(logging.INFO)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_handler]
LOG.propagate = False

FEATURE_VERSION = os.environ.get("GOLD_FEATURE_VERSION", "trip_eta_lgbm_v1")
SCHEMA_VERSION = os.environ.get("GOLD_SCHEMA_VERSION", "trip_eta_frozen_matrix_v1")
ROUTE_PAIR_BUCKETS = int(os.environ.get("ROUTE_PAIR_BUCKETS", "4096"))
ROUTE_PAIR_HASH_SALT = os.environ.get("ROUTE_PAIR_HASH_SALT", "trip_eta_route_pair_v1")
GOLD_REPARTITIONS = int(os.environ.get("GOLD_REPARTITIONS", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")))
MODEL_FAMILY = os.environ.get("MODEL_FAMILY", "lightgbm")
INFERENCE_RUNTIME = os.environ.get("INFERENCE_RUNTIME", "onnxruntime")


@dataclass(frozen=True)
class GoldFeatureResult:
    run_id: str
    gold_table: str
    contract_table: str
    source_silver_table: str
    schema_hash: str
    feature_version: str
    schema_version: str
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


def write_versioned_contract_table(df: DataFrame, table_id: str) -> str:
    table_id = qualify_table_id(table_id)
    if table_exists(df.sparkSession, table_id):
        df.writeTo(table_id).overwritePartitions()
        return "overwrite_partitions"

    (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
        .partitionedBy(F.col("feature_version"))
        .create()
    )
    return "create"


def normalize_text_expr(col_name: str):
    return F.lower(F.trim(F.coalesce(F.col(col_name), F.lit(""))))


def build_borough_map_df(spark):
    rows = [
        {"borough_norm": "unknown", "borough_id": 0, "borough_name": "unknown"},
        {"borough_norm": "manhattan", "borough_id": 1, "borough_name": "Manhattan"},
        {"borough_norm": "queens", "borough_id": 2, "borough_name": "Queens"},
        {"borough_norm": "brooklyn", "borough_id": 3, "borough_name": "Brooklyn"},
        {"borough_norm": "bronx", "borough_id": 4, "borough_name": "Bronx"},
        {"borough_norm": "staten island", "borough_id": 5, "borough_name": "Staten Island"},
        {"borough_norm": "ewr", "borough_id": 6, "borough_name": "EWR"},
    ]
    return spark.createDataFrame(rows)


def distinct_service_zone_values(silver_df: DataFrame) -> list[str]:
    rows = (
        silver_df.select(
            F.coalesce(F.col("pickup_service_zone"), F.lit("")).alias("service_zone")
        )
        .unionByName(
            silver_df.select(
                F.coalesce(F.col("dropoff_service_zone"), F.lit("")).alias("service_zone")
            )
        )
        .select(F.lower(F.trim(F.col("service_zone"))).alias("service_zone_norm"))
        .where(F.col("service_zone_norm") != "")
        .distinct()
        .orderBy("service_zone_norm")
        .collect()
    )
    return [row["service_zone_norm"] for row in rows]


def build_service_zone_map_df(spark, service_zone_values: list[str]):
    rows = [{"service_zone_norm": "unknown", "service_zone_id": 0, "service_zone_name": "unknown"}]
    for idx, value in enumerate(service_zone_values, start=1):
        rows.append(
            {
                "service_zone_norm": value,
                "service_zone_id": idx,
                "service_zone_name": value,
            }
        )
    return spark.createDataFrame(rows)


def route_pair_bucket_expr(pickup_zone_id_col, dropoff_zone_id_col):
    route_hash = F.sha2(
        F.concat_ws(
            "||",
            pickup_zone_id_col.cast("string"),
            dropoff_zone_id_col.cast("string"),
            F.lit(ROUTE_PAIR_HASH_SALT),
        ),
        256,
    )
    bucket = F.pmod(F.conv(F.substring(route_hash, 1, 15), 16, 10).cast("long"), F.lit(ROUTE_PAIR_BUCKETS)).cast("int") + F.lit(1)
    return bucket


def build_window_features(df: DataFrame) -> DataFrame:
    as_of_sec = F.unix_timestamp(F.col("as_of_ts")).cast("long")

    w_zone_hour_7d = (
        Window.partitionBy("pickup_zone_id", "pickup_hour")
        .orderBy(as_of_sec)
        .rangeBetween(-7 * 24 * 60 * 60, -1)
    )
    w_zone_30d = (
        Window.partitionBy("pickup_zone_id")
        .orderBy(as_of_sec)
        .rangeBetween(-30 * 24 * 60 * 60, -1)
    )
    w_zone_hour_90d = (
        Window.partitionBy("pickup_zone_id", "pickup_hour")
        .orderBy(as_of_sec)
        .rangeBetween(-90 * 24 * 60 * 60, -1)
    )

    return (
        df.withColumn("as_of_ts_sec", as_of_sec)
        .withColumn(
            "avg_duration_7d_zone_hour",
            F.coalesce(
                F.avg(F.col("label_trip_duration_seconds")).over(w_zone_hour_7d),
                F.lit(float("nan")),
            ).cast("double"),
        )
        .withColumn(
            "avg_fare_30d_zone",
            F.coalesce(
                F.avg(F.col("fare_amount")).over(w_zone_30d),
                F.lit(float("nan")),
            ).cast("double"),
        )
        .withColumn(
            "trip_count_90d_zone_hour",
            F.coalesce(
                F.count(F.lit(1)).over(w_zone_hour_90d),
                F.lit(0),
            ).cast("double"),
        )
        .drop("as_of_ts_sec")
    )


def build_feature_spec_rows(service_zone_values: list[str]) -> list[dict]:
    return [
        {
            "name": "trip_id",
            "role": "metadata",
            "dtype": "string",
            "nullable": False,
            "unit": "identifier",
            "missing_policy": "required",
        },
        {
            "name": "as_of_ts",
            "role": "metadata",
            "dtype": "timestamp",
            "nullable": False,
            "unit": "timestamp_utc",
            "missing_policy": "required",
        },
        {
            "name": "as_of_date",
            "role": "metadata",
            "dtype": "date",
            "nullable": False,
            "unit": "date_utc",
            "missing_policy": "required",
        },
        {
            "name": "schema_version",
            "role": "metadata",
            "dtype": "string",
            "nullable": False,
            "unit": "version_tag",
            "missing_policy": "required",
        },
        {
            "name": "feature_version",
            "role": "metadata",
            "dtype": "string",
            "nullable": False,
            "unit": "version_tag",
            "missing_policy": "required",
        },
        {
            "name": "pickup_hour",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "hour_0_23",
            "missing_policy": "required",
        },
        {
            "name": "pickup_dow",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "dayofweek_1_sun_7_sat",
            "missing_policy": "required",
        },
        {
            "name": "pickup_month",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "month_1_12",
            "missing_policy": "required",
        },
        {
            "name": "pickup_is_weekend",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "boolean_0_1",
            "missing_policy": "required",
        },
        {
            "name": "pickup_borough_id",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "categorical_id",
            "missing_policy": "0_unknown",
            "categorical_feature": True,
            "domain": [0, 1, 2, 3, 4, 5, 6],
        },
        {
            "name": "pickup_zone_id",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "taxi_zone_location_id",
            "missing_policy": "0_unknown",
            "categorical_feature": True,
            "domain": "positive_location_ids_and_0_unknown",
        },
        {
            "name": "pickup_service_zone_id",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "categorical_id",
            "missing_policy": "0_unknown",
            "categorical_feature": True,
            "domain": [0] + list(range(1, len(service_zone_values) + 1)),
        },
        {
            "name": "dropoff_borough_id",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "categorical_id",
            "missing_policy": "0_unknown",
            "categorical_feature": True,
            "domain": [0, 1, 2, 3, 4, 5, 6],
        },
        {
            "name": "dropoff_zone_id",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "taxi_zone_location_id",
            "missing_policy": "0_unknown",
            "categorical_feature": True,
            "domain": "positive_location_ids_and_0_unknown",
        },
        {
            "name": "dropoff_service_zone_id",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "categorical_id",
            "missing_policy": "0_unknown",
            "categorical_feature": True,
            "domain": [0] + list(range(1, len(service_zone_values) + 1)),
        },
        {
            "name": "route_pair_id",
            "role": "feature",
            "dtype": "int32",
            "nullable": False,
            "unit": "hashed_bucket",
            "missing_policy": "0_unknown",
            "categorical_feature": True,
            "domain": [0] + list(range(1, ROUTE_PAIR_BUCKETS + 1)),
            "hash_algorithm": "sha256",
            "hash_salt": ROUTE_PAIR_HASH_SALT,
            "bucket_count": ROUTE_PAIR_BUCKETS,
        },
        {
            "name": "avg_duration_7d_zone_hour",
            "role": "feature",
            "dtype": "float64",
            "nullable": True,
            "unit": "seconds",
            "missing_policy": "nan_on_cold_start",
        },
        {
            "name": "avg_fare_30d_zone",
            "role": "feature",
            "dtype": "float64",
            "nullable": True,
            "unit": "currency_amount",
            "missing_policy": "nan_on_cold_start",
        },
        {
            "name": "trip_count_90d_zone_hour",
            "role": "feature",
            "dtype": "float64",
            "nullable": False,
            "unit": "count",
            "missing_policy": "0_on_cold_start",
        },
        {
            "name": "label_trip_duration_seconds",
            "role": "label",
            "dtype": "float64",
            "nullable": False,
            "unit": "seconds",
            "missing_policy": "drop_row_if_null",
            "target_metric": "mae",
        },
    ]


def build_encoding_spec(service_zone_values: list[str]) -> dict:
    return {
        "pickup_borough_id": {
            "type": "fixed_enum",
            "unknown": 0,
            "values": {
                1: "Manhattan",
                2: "Queens",
                3: "Brooklyn",
                4: "Bronx",
                5: "Staten Island",
                6: "EWR",
            },
        },
        "dropoff_borough_id": {
            "type": "fixed_enum",
            "unknown": 0,
            "values": {
                1: "Manhattan",
                2: "Queens",
                3: "Brooklyn",
                4: "Bronx",
                5: "Staten Island",
                6: "EWR",
            },
        },
        "pickup_zone_id": {
            "type": "identity_code",
            "unknown": 0,
            "source": "silver.pickup_location_id",
            "note": "stable taxi zone location IDs",
        },
        "dropoff_zone_id": {
            "type": "identity_code",
            "unknown": 0,
            "source": "silver.dropoff_location_id",
            "note": "stable taxi zone location IDs",
        },
        "pickup_service_zone_id": {
            "type": "versioned_lookup",
            "unknown": 0,
            "source": "silver.pickup_service_zone",
            "values": {idx + 1: value for idx, value in enumerate(service_zone_values)},
        },
        "dropoff_service_zone_id": {
            "type": "versioned_lookup",
            "unknown": 0,
            "source": "silver.dropoff_service_zone",
            "values": {idx + 1: value for idx, value in enumerate(service_zone_values)},
        },
        "route_pair_id": {
            "type": "hashed_bucket",
            "unknown": 0,
            "hash_algorithm": "sha256",
            "hash_salt": ROUTE_PAIR_HASH_SALT,
            "bucket_count": ROUTE_PAIR_BUCKETS,
        },
    }


def build_aggregate_spec() -> list[dict]:
    return [
        {
            "name": "avg_duration_7d_zone_hour",
            "source_table": "gold.training_matrix_source_silver",
            "source_column": "label_trip_duration_seconds",
            "filter_predicate": "trip_start_ts in [as_of_ts - 7d, as_of_ts)",
            "window_length": "7d",
            "grouping_keys": ["pickup_zone_id", "pickup_hour"],
            "minimum_history_rule": "no prior rows => NaN",
            "null_fallback": "NaN",
        },
        {
            "name": "avg_fare_30d_zone",
            "source_table": "gold.training_matrix_source_silver",
            "source_column": "fare_amount",
            "filter_predicate": "trip_start_ts in [as_of_ts - 30d, as_of_ts)",
            "window_length": "30d",
            "grouping_keys": ["pickup_zone_id"],
            "minimum_history_rule": "no prior rows => NaN",
            "null_fallback": "NaN",
        },
        {
            "name": "trip_count_90d_zone_hour",
            "source_table": "gold.training_matrix_source_silver",
            "source_column": "count(*)",
            "filter_predicate": "trip_start_ts in [as_of_ts - 90d, as_of_ts)",
            "window_length": "90d",
            "grouping_keys": ["pickup_zone_id", "pickup_hour"],
            "minimum_history_rule": "no prior rows => 0",
            "null_fallback": "0",
        },
    ]


def build_label_spec() -> dict:
    return {
        "name": "label_trip_duration_seconds",
        "dtype": "float64",
        "unit": "seconds",
        "source": "silver.trip_duration_seconds",
        "null_policy": "drop_row_if_null",
        "primary_metric": "mae",
        "secondary_metric": "rmse",
        "target_family": "eta",
    }


def build_schema_hash(feature_spec_rows: list[dict]) -> str:
    canonical = json.dumps(feature_spec_rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def encode_categories(base: DataFrame, service_zone_map_df: DataFrame) -> DataFrame:
    borough_map_df = build_borough_map_df(base.sparkSession)

    pickup_norm = normalize_text_expr("pickup_borough")
    dropoff_norm = normalize_text_expr("dropoff_borough")
    pickup_zone_norm = normalize_text_expr("pickup_service_zone")
    dropoff_zone_norm = normalize_text_expr("dropoff_service_zone")

    encoded = (
        base.withColumn("pickup_borough_norm", pickup_norm)
        .withColumn("dropoff_borough_norm", dropoff_norm)
        .withColumn("pickup_service_zone_norm", pickup_zone_norm)
        .withColumn("dropoff_service_zone_norm", dropoff_zone_norm)
        .join(
            broadcast(
                borough_map_df.select(
                    F.col("borough_norm").alias("pickup_borough_norm"),
                    F.col("borough_id").alias("pickup_borough_id"),
                )
            ),
            on="pickup_borough_norm",
            how="left",
        )
        .join(
            broadcast(
                borough_map_df.select(
                    F.col("borough_norm").alias("dropoff_borough_norm"),
                    F.col("borough_id").alias("dropoff_borough_id"),
                )
            ),
            on="dropoff_borough_norm",
            how="left",
        )
        .join(
            broadcast(
                service_zone_map_df.select(
                    F.col("service_zone_norm").alias("pickup_service_zone_norm"),
                    F.col("service_zone_id").alias("pickup_service_zone_id"),
                )
            ),
            on="pickup_service_zone_norm",
            how="left",
        )
        .join(
            broadcast(
                service_zone_map_df.select(
                    F.col("service_zone_norm").alias("dropoff_service_zone_norm"),
                    F.col("service_zone_id").alias("dropoff_service_zone_id"),
                )
            ),
            on="dropoff_service_zone_norm",
            how="left",
        )
        .drop(
            "pickup_borough_norm",
            "dropoff_borough_norm",
            "pickup_service_zone_norm",
            "dropoff_service_zone_norm",
        )
    )

    return (
        encoded.withColumn("pickup_borough_id", F.coalesce(F.col("pickup_borough_id"), F.lit(0)).cast("int"))
        .withColumn("dropoff_borough_id", F.coalesce(F.col("dropoff_borough_id"), F.lit(0)).cast("int"))
        .withColumn("pickup_service_zone_id", F.coalesce(F.col("pickup_service_zone_id"), F.lit(0)).cast("int"))
        .withColumn("dropoff_service_zone_id", F.coalesce(F.col("dropoff_service_zone_id"), F.lit(0)).cast("int"))
    )


def build_training_matrix(silver_df: DataFrame, run_id: str) -> DataFrame:
    require_columns(
        silver_df,
        (
            "trip_key",
            "pickup_ts",
            "dropoff_ts",
            "pickup_location_id",
            "dropoff_location_id",
            "pickup_borough",
            "pickup_zone",
            "pickup_service_zone",
            "dropoff_borough",
            "dropoff_zone",
            "dropoff_service_zone",
            "trip_duration_seconds",
            "fare_amount",
            "total_amount",
        ),
        "silver canonical table",
    )

    base = (
        silver_df.select(
            "trip_key",
            "pickup_ts",
            "dropoff_ts",
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
            "fare_amount",
            "trip_duration_seconds",
            "trip_duration_minutes",
            "total_amount",
        )
        .withColumn("trip_id", F.col("trip_key"))
        .withColumn("as_of_ts", F.col("pickup_ts"))
        .withColumn("as_of_date", F.to_date(F.col("pickup_ts")))
        .withColumn("schema_version", F.lit(SCHEMA_VERSION))
        .withColumn("feature_version", F.lit(FEATURE_VERSION))
        .withColumn("pickup_hour", F.hour(F.col("as_of_ts")).cast("int"))
        .withColumn("pickup_dow", F.dayofweek(F.col("as_of_ts")).cast("int"))
        .withColumn("pickup_month", F.month(F.col("as_of_ts")).cast("int"))
        .withColumn(
            "pickup_is_weekend",
            F.when(F.dayofweek(F.col("as_of_ts")).isin(1, 7), F.lit(1)).otherwise(F.lit(0)).cast("int"),
        )
        .withColumn("pickup_zone_id", F.coalesce(F.col("pickup_location_id").cast("int"), F.lit(0)).cast("int"))
        .withColumn("dropoff_zone_id", F.coalesce(F.col("dropoff_location_id").cast("int"), F.lit(0)).cast("int"))
    )

    service_zone_values = distinct_service_zone_values(base)
    service_zone_map_df = build_service_zone_map_df(base.sparkSession, service_zone_values)
    base = encode_categories(base, service_zone_map_df)

    base = base.withColumn(
        "route_pair_id",
        F.when(
            (F.col("pickup_zone_id") > 0) & (F.col("dropoff_zone_id") > 0),
            route_pair_bucket_expr(F.col("pickup_zone_id"), F.col("dropoff_zone_id")),
        ).otherwise(F.lit(0)).cast("int"),
    )

    base = base.withColumn(
        "label_trip_duration_seconds",
        F.col("trip_duration_seconds").cast("double"),
    )

    base = (
        base.withColumn("pickup_hour", F.col("pickup_hour").cast("int"))
        .withColumn("pickup_dow", F.col("pickup_dow").cast("int"))
        .withColumn("pickup_month", F.col("pickup_month").cast("int"))
        .withColumn("pickup_is_weekend", F.col("pickup_is_weekend").cast("int"))
        .withColumn("pickup_zone_id", F.col("pickup_zone_id").cast("int"))
        .withColumn("dropoff_zone_id", F.col("dropoff_zone_id").cast("int"))
        .withColumn("pickup_borough_id", F.col("pickup_borough_id").cast("int"))
        .withColumn("dropoff_borough_id", F.col("dropoff_borough_id").cast("int"))
        .withColumn("pickup_service_zone_id", F.col("pickup_service_zone_id").cast("int"))
        .withColumn("dropoff_service_zone_id", F.col("dropoff_service_zone_id").cast("int"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("trip_duration_seconds", F.col("trip_duration_seconds").cast("double"))
        .withColumn("trip_duration_minutes", F.col("trip_duration_minutes").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("route_pair_id", F.col("route_pair_id").cast("int"))
    )

    base = build_window_features(base)

    feature_spec_rows = build_feature_spec_rows(service_zone_values)
    schema_hash = build_schema_hash(feature_spec_rows)

    output_columns = [
        "trip_id",
        "as_of_ts",
        "as_of_date",
        "schema_version",
        "feature_version",
        "pickup_hour",
        "pickup_dow",
        "pickup_month",
        "pickup_is_weekend",
        "pickup_borough_id",
        "pickup_zone_id",
        "pickup_service_zone_id",
        "dropoff_borough_id",
        "dropoff_zone_id",
        "dropoff_service_zone_id",
        "route_pair_id",
        "avg_duration_7d_zone_hour",
        "avg_fare_30d_zone",
        "trip_count_90d_zone_hour",
        "label_trip_duration_seconds",
    ]

    training = base.select(*output_columns)

    required = {
        "trip_id",
        "as_of_ts",
        "as_of_date",
        "pickup_hour",
        "pickup_zone_id",
        "label_trip_duration_seconds",
    }
    missing = required - set(training.columns)
    if missing:
        raise RuntimeError(f"gold training dataframe is missing required columns: {sorted(missing)}")

    training = training.filter(
        F.col("as_of_ts").isNotNull()
        & F.col("trip_id").isNotNull()
        & F.col("label_trip_duration_seconds").isNotNull()
        & (F.col("label_trip_duration_seconds") > 0)
    )

    return training, service_zone_values, feature_spec_rows, schema_hash


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
def gold_features(silver: SilverTransformResult) -> GoldFeatureResult:
    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    source_silver_table = qualify_table_id(silver.silver_table)
    gold_table = qualify_table_id(GOLD_TRAINING_TABLE)
    contract_table = qualify_table_id(GOLD_CONTRACT_TABLE)

    ensure_namespace(spark, CATALOG_NAME, GOLD_NAMESPACE)

    log_json(
        msg="gold_features_start",
        run_id=silver.run_id,
        source_silver_table=source_silver_table,
        gold_table=gold_table,
        contract_table=contract_table,
        feature_version=FEATURE_VERSION,
        schema_version=SCHEMA_VERSION,
        model_family=MODEL_FAMILY,
        inference_runtime=INFERENCE_RUNTIME,
    )

    silver_df = spark.table(source_silver_table)
    training_df, service_zone_values, feature_spec_rows, schema_hash = build_training_matrix(
        silver_df,
        silver.run_id,
    )

    training_df = training_df.repartition(
        GOLD_REPARTITIONS,
        F.col("pickup_zone_id"),
        F.col("pickup_hour"),
    )

    write_mode = write_partitioned_iceberg_table(
        training_df,
        gold_table,
        "as_of_date",
    )

    feature_spec_json = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "feature_version": FEATURE_VERSION,
            "output_columns": feature_spec_rows,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    encoding_spec_json = json.dumps(
        build_encoding_spec(service_zone_values),
        sort_keys=True,
        separators=(",", ":"),
    )
    aggregate_spec_json = json.dumps(
        build_aggregate_spec(),
        sort_keys=True,
        separators=(",", ":"),
    )
    label_spec_json = json.dumps(
        build_label_spec(),
        sort_keys=True,
        separators=(",", ":"),
    )

    contract_row = {
        "run_id": silver.run_id,
        "feature_version": FEATURE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "schema_hash": schema_hash,
        "model_family": MODEL_FAMILY,
        "inference_runtime": INFERENCE_RUNTIME,
        "gold_table": gold_table,
        "source_silver_table": source_silver_table,
        "output_columns_json": json.dumps(
            [
                "trip_id",
                "as_of_ts",
                "as_of_date",
                "schema_version",
                "feature_version",
                "pickup_hour",
                "pickup_dow",
                "pickup_month",
                "pickup_is_weekend",
                "pickup_borough_id",
                "pickup_zone_id",
                "pickup_service_zone_id",
                "dropoff_borough_id",
                "dropoff_zone_id",
                "dropoff_service_zone_id",
                "route_pair_id",
                "avg_duration_7d_zone_hour",
                "avg_fare_30d_zone",
                "trip_count_90d_zone_hour",
                "label_trip_duration_seconds",
            ],
            separators=(",", ":"),
        ),
        "feature_spec_json": feature_spec_json,
        "encoding_spec_json": encoding_spec_json,
        "aggregate_spec_json": aggregate_spec_json,
        "label_spec_json": label_spec_json,
        "created_ts": datetime.now(timezone.utc),
    }

    contract_df = spark.createDataFrame([contract_row])
    contract_write_mode = write_versioned_contract_table(contract_df, contract_table)

    result = GoldFeatureResult(
        run_id=silver.run_id,
        gold_table=gold_table,
        contract_table=contract_table,
        source_silver_table=source_silver_table,
        schema_hash=schema_hash,
        feature_version=FEATURE_VERSION,
        schema_version=SCHEMA_VERSION,
        write_mode=f"{write_mode};contract={contract_write_mode}",
        status="ok",
    )
    log_json(msg="gold_features_success", **result.__dict__)
    return result