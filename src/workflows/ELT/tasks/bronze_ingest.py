


from __future__ import annotations

import json
import logging
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass
from itertools import islice, tee
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, Sequence, Tuple

from flytekit import Resources, current_context, task
from flytekitplugins.spark import Spark
from pyspark.sql import DataFrame, SparkSession, functions as F

LOG = logging.getLogger("elt_bronze_ingest")
LOG.setLevel(logging.INFO)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_handler]
LOG.propagate = False

CATALOG_NAME = os.environ.get("ICEBERG_CATALOG", "iceberg")
ICEBERG_WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE",
    "s3://e2e-mlops-data-681802563986/iceberg/warehouse/",
)
ICEBERG_REST_URI = os.environ.get(
    "ICEBERG_REST_URI",
    "http://iceberg-rest.default.svc.cluster.local:9001/iceberg",
)
ICEBERG_REST_AUTH_TYPE = os.environ.get("ICEBERG_REST_AUTH_TYPE", "")
ICEBERG_REST_USER = os.environ.get("ICEBERG_REST_USER", "")
ICEBERG_REST_PASSWORD = os.environ.get("ICEBERG_REST_PASSWORD", "")

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "")

BRONZE_NAMESPACE = os.environ.get("BRONZE_NAMESPACE", "bronze")
SILVER_NAMESPACE = os.environ.get("SILVER_NAMESPACE", "silver")

BRONZE_TRIPS_TABLE = os.environ.get(
    "BRONZE_TRIPS_TABLE",
    f"{CATALOG_NAME}.bronze.trips_raw",
)
BRONZE_TAXI_ZONE_TABLE = os.environ.get(
    "BRONZE_TAXI_ZONE_TABLE",
    f"{CATALOG_NAME}.bronze.taxi_zone_lookup_raw",
)
SILVER_TRIPS_TABLE = os.environ.get(
    "SILVER_TRIPS_TABLE",
    f"{CATALOG_NAME}.silver.trip_features",
)

TRIPS_DATASET_ID = os.environ.get("TRIPS_DATASET_ID", "koorukuroo/yellow_tripdata")
TRIPS_DATASET_SPLIT = os.environ.get("TRIPS_DATASET_SPLIT", "train")
TRIPS_DATASET_REVISION = os.environ.get(
    "TRIPS_DATASET_REVISION",
    "ef7653853df26ba2cd9ccbae6db2f4094c2d63b0",
)
TAXI_ZONE_LOOKUP_URL = os.environ.get(
    "TAXI_ZONE_LOOKUP_URL",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
)
HF_TOKEN = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN") or None

MAX_ROWS_TO_EXTRACT_FROM_DATASETS = int(os.environ.get("MAX_ROWS_TO_EXTRACT_FROM_DATASETS", "0"))
BRONZE_CHUNK_SIZE = int(os.environ.get("BRONZE_CHUNK_SIZE", "10000"))
BRONZE_ROWS_PER_PARTITION = int(os.environ.get("BRONZE_ROWS_PER_PARTITION", "50000"))
SILVER_ROWS_PER_PARTITION = int(os.environ.get("SILVER_ROWS_PER_PARTITION", "50000"))

ICEBERG_TARGET_FILE_SIZE_BYTES = os.environ.get("ICEBERG_TARGET_FILE_SIZE_BYTES", "268435456")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_EXECUTOR_CORES = os.environ.get("SPARK_EXECUTOR_CORES", "1")
SPARK_EXECUTOR_INSTANCES = os.environ.get("SPARK_EXECUTOR_INSTANCES", "1")
SPARK_DRIVER_CORES = os.environ.get("SPARK_DRIVER_CORES", "1")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")
SPARK_MAX_PARTITION_BYTES = os.environ.get("SPARK_MAX_PARTITION_BYTES", "134217728")

PARQUET_COMPRESSION = os.environ.get("PARQUET_COMPRESSION", "snappy")

ICEBERG_EXPIRE_DAYS = int(os.environ.get("ICEBERG_EXPIRE_DAYS", "7"))
ICEBERG_ORPHAN_DAYS = int(os.environ.get("ICEBERG_ORPHAN_DAYS", "3"))
ICEBERG_RETAIN_LAST = int(os.environ.get("ICEBERG_RETAIN_LAST", "3"))
MAINTENANCE_REWRITE_DAYS = int(os.environ.get("MAINTENANCE_REWRITE_DAYS", "30"))

TASK_IMAGE = os.environ.get(
    "ELT_TASK_IMAGE",
    "ghcr.io/athithya-sakthivel/flyte-elt-task:1.0.9",
).strip()
if not TASK_IMAGE:
    raise RuntimeError("ELT_TASK_IMAGE must be set before importing bronze_ingest.py")


@dataclass(frozen=True)
class BronzeIngestResult:
    run_id: str
    trips_table: str
    taxi_zone_table: str
    trips_rows: int
    taxi_zone_rows: int
    trips_source_ref: str
    taxi_zone_source_ref: str
    trips_write_mode: str
    taxi_zone_write_mode: str


def log_json(**payload) -> None:
    LOG.info(json.dumps(payload, default=str, sort_keys=True))


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_").lower()
    if not normalized:
        raise ValueError(f"invalid column name after normalization: {name!r}")
    return normalized


def normalize_record(row: dict) -> dict:
    out: dict = {}
    for key, value in row.items():
        normalized_key = normalize_column_name(str(key))
        if normalized_key in out:
            raise ValueError(
                f"column collision after normalization: {key!r} -> {normalized_key!r}"
            )
        out[normalized_key] = value
    return out


def first_existing(columns: Sequence[str], candidates: Sequence[str]) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise KeyError(f"none of the candidate columns exist: {list(candidates)}")


def qualify_table_id(table_id: str) -> str:
    parts = table_id.split(".")
    if len(parts) == 3:
        return table_id
    if len(parts) == 2:
        return f"{CATALOG_NAME}.{table_id}"
    raise ValueError(
        f"expected table id in the form catalog.namespace.table or namespace.table, got {table_id!r}"
    )


def parse_table_id(table_id: str) -> Tuple[str, str, str]:
    qualified = qualify_table_id(table_id)
    parts = qualified.split(".")
    return parts[0], parts[1], parts[2]


def ensure_namespace(spark: SparkSession, catalog_name: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace}")


def table_exists(spark: SparkSession, table_id: str) -> bool:
    catalog_name, namespace, table_name = parse_table_id(table_id)
    rows = spark.sql(
        f"SHOW TABLES IN {catalog_name}.{namespace} LIKE '{table_name}'"
    ).limit(1).collect()
    return len(rows) > 0


def get_spark_session() -> SparkSession:
    try:
        spark = current_context().spark_session
    except Exception as exc:
        raise RuntimeError("Flyte did not provide a Spark session for this task") from exc
    if spark is None:
        raise RuntimeError("Flyte did not provide a Spark session for this task")
    return spark


def build_spark_conf() -> dict[str, str]:
    conf = {
        f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG_NAME}.type": "rest",
        f"spark.sql.catalog.{CATALOG_NAME}.uri": ICEBERG_REST_URI,
        f"spark.sql.catalog.{CATALOG_NAME}.warehouse": ICEBERG_WAREHOUSE,
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.shuffle.partitions": SPARK_SHUFFLE_PARTITIONS,
        "spark.sql.files.maxPartitionBytes": SPARK_MAX_PARTITION_BYTES,
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.driver.memory": SPARK_DRIVER_MEMORY,
        "spark.executor.memory": SPARK_EXECUTOR_MEMORY,
        "spark.executor.cores": SPARK_EXECUTOR_CORES,
        "spark.executor.instances": SPARK_EXECUTOR_INSTANCES,
        "spark.driver.cores": SPARK_DRIVER_CORES,
    }
    if ICEBERG_REST_AUTH_TYPE:
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type"] = ICEBERG_REST_AUTH_TYPE
    if ICEBERG_REST_USER:
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.basic.username"] = ICEBERG_REST_USER
    if ICEBERG_REST_PASSWORD:
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.basic.password"] = ICEBERG_REST_PASSWORD
    return conf


def build_hadoop_conf() -> dict[str, str]:
    conf = {
        "fs.s3a.endpoint.region": AWS_REGION,
        "fs.s3a.path.style.access": "false",
    }
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        conf["fs.s3a.access.key"] = AWS_ACCESS_KEY_ID
        conf["fs.s3a.secret.key"] = AWS_SECRET_ACCESS_KEY
        conf["fs.s3a.aws.credentials.provider"] = (
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            if AWS_SESSION_TOKEN
            else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
    if AWS_SESSION_TOKEN:
        conf["fs.s3a.session.token"] = AWS_SESSION_TOKEN
    return conf


def load_streaming_dataset(
    source: str,
    *,
    split: str = "train",
    data_files: str | dict | None = None,
    revision: str | None = None,
    token: str | bool | None = None,
):
    from datasets import load_dataset

    kwargs: dict = {"split": split, "streaming": True}
    if data_files is not None:
        kwargs["data_files"] = data_files
    if revision:
        kwargs["revision"] = revision
    if token:
        kwargs["token"] = token
    return load_dataset(source, **kwargs)


def materialize_iterable_to_parquet(
    iterable: Iterable[dict],
    out_path: Path,
    *,
    label: str,
    chunk_size: int = BRONZE_CHUNK_SIZE,
) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    writer = None
    total_rows = 0
    batch: list[dict] = []

    try:
        for row in iterable:
            batch.append(normalize_record(dict(row)))
            total_rows += 1
            if len(batch) >= chunk_size:
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(out_path),
                        table.schema,
                        compression=PARQUET_COMPRESSION,
                    )
                writer.write_table(table)
                log_json(msg="materialized_chunk", label=label, rows=total_rows, path=str(out_path))
                batch.clear()

        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(
                    str(out_path),
                    table.schema,
                    compression=PARQUET_COMPRESSION,
                )
            writer.write_table(table)
            log_json(msg="materialized_final_chunk", label=label, rows=total_rows, path=str(out_path))
            batch.clear()

        if total_rows == 0:
            raise RuntimeError(f"no rows read from source {label!r}")
        return total_rows
    finally:
        if writer is not None:
            writer.close()


def cast_if_present(df: DataFrame, column: str, spark_type: str) -> DataFrame:
    if column in df.columns:
        return df.withColumn(column, F.col(column).cast(spark_type))
    return df


def add_trip_bronze_columns(df: DataFrame, *, run_id: str, source_ref: str) -> DataFrame:
    pickup_col = first_existing(df.columns, ("lpep_pickup_datetime", "tpep_pickup_datetime", "pickup_ts"))
    dropoff_col = first_existing(df.columns, ("lpep_dropoff_datetime", "tpep_dropoff_datetime", "dropoff_ts"))
    pickup_location_col = first_existing(df.columns, ("pulocation_id", "pickup_location_id"))
    dropoff_location_col = first_existing(df.columns, ("dolocation_id", "dropoff_location_id"))

    pickup_ts = F.to_timestamp(F.col(pickup_col))
    dropoff_ts = F.to_timestamp(F.col(dropoff_col))

    df = (
        df.withColumn("pickup_ts", pickup_ts)
        .withColumn("dropoff_ts", dropoff_ts)
        .withColumn("pickup_location_id", F.col(pickup_location_col).cast("long"))
        .withColumn("dropoff_location_id", F.col(dropoff_location_col).cast("long"))
        .withColumn("event_date", F.to_date(F.coalesce(F.col("pickup_ts"), F.col("dropoff_ts"))))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_uri", F.lit(source_ref))
        .withColumn("source_revision", F.lit(TRIPS_DATASET_REVISION))
        .withColumn("source_kind", F.lit("huggingface_dataset"))
        .withColumn("source_file", F.lit(TRIPS_DATASET_ID))
        .filter(F.col("event_date").isNotNull())
    )

    numeric_double_cols = (
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "ehail_fee",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "cbd_congestion_fee",
    )
    integer_cols = (
        "vendor_id",
        "ratecode_id",
        "passenger_count",
        "payment_type",
        "trip_type",
    )

    for col_name in numeric_double_cols:
        df = cast_if_present(df, col_name, "double")
    for col_name in integer_cols:
        df = cast_if_present(df, col_name, "long")
    if "store_and_fwd_flag" in df.columns:
        df = df.withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast("string"))

    required = {
        "pickup_ts",
        "dropoff_ts",
        "pickup_location_id",
        "dropoff_location_id",
        "event_date",
        "trip_distance",
        "fare_amount",
        "total_amount",
    }
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"trips bronze dataframe is missing required columns: {sorted(missing)}")
    return df


def add_zone_bronze_columns(df: DataFrame, *, run_id: str, source_ref: str) -> DataFrame:
    df = (
        df.select(
            F.col("location_id").cast("long").alias("location_id"),
            F.col("borough").cast("string").alias("borough"),
            F.col("zone").cast("string").alias("zone"),
            F.col("service_zone").cast("string").alias("service_zone"),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_uri", F.lit(source_ref))
        .withColumn("source_revision", F.lit(""))
        .withColumn("source_kind", F.lit("http_csv"))
        .withColumn("source_file", F.lit(TAXI_ZONE_LOOKUP_URL))
        .dropDuplicates(["location_id"])
    )

    required = {"location_id", "borough", "zone", "service_zone"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"taxi zone bronze dataframe is missing required columns: {sorted(missing)}")
    return df


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


def write_replace_iceberg_table(df: DataFrame, table_id: str) -> str:
    table_id = qualify_table_id(table_id)
    if table_exists(df.sparkSession, table_id):
        df.writeTo(table_id).overwrite(F.lit(True))
        return "overwrite"

    (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
        .create()
    )
    return "create"


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
def bronze_ingest() -> BronzeIngestResult:
    run_id = os.environ.get("RUN_ID") or os.environ.get("FLYTE_INTERNAL_EXECUTION_ID") or uuid.uuid4().hex

    trips_source_ref = (
        f"{TRIPS_DATASET_ID}@{TRIPS_DATASET_REVISION}"
        if TRIPS_DATASET_REVISION
        else TRIPS_DATASET_ID
    )
    taxi_zone_source_ref = TAXI_ZONE_LOOKUP_URL

    log_json(
        msg="bronze_ingest_start",
        run_id=run_id,
        trips_source=trips_source_ref,
        taxi_zone_source=taxi_zone_source_ref,
        max_rows=MAX_ROWS_TO_EXTRACT_FROM_DATASETS,
    )

    trips_stream = load_streaming_dataset(
        TRIPS_DATASET_ID,
        split=TRIPS_DATASET_SPLIT,
        revision=TRIPS_DATASET_REVISION,
        token=HF_TOKEN,
    )
    taxi_zone_stream = load_streaming_dataset(
        "csv",
        split="train",
        data_files={"train": TAXI_ZONE_LOOKUP_URL},
        token=HF_TOKEN,
    )

    with TemporaryDirectory(prefix="flyte_elt_bronze_") as tmpdir:
        tmp_root = Path(tmpdir)
        trips_parquet = tmp_root / "trips.parquet"
        zones_parquet = tmp_root / "taxi_zone_lookup.parquet"

        trips_preview, trips_write = tee(trips_stream, 2)
        taxi_preview, taxi_write = tee(taxi_zone_stream, 2)

        for i, row in enumerate(islice(trips_preview, 2), start=1):
            log_json(msg="trip_preview_row", row=i, data=normalize_record(dict(row)))
        for i, row in enumerate(islice(taxi_preview, 2), start=1):
            log_json(msg="taxi_zone_preview_row", row=i, data=normalize_record(dict(row)))

        trips_rows_iter = (
            islice(trips_write, MAX_ROWS_TO_EXTRACT_FROM_DATASETS)
            if MAX_ROWS_TO_EXTRACT_FROM_DATASETS > 0
            else trips_write
        )
        zones_rows_iter = (
            islice(taxi_write, MAX_ROWS_TO_EXTRACT_FROM_DATASETS)
            if MAX_ROWS_TO_EXTRACT_FROM_DATASETS > 0
            else taxi_write
        )

        trips_rows = materialize_iterable_to_parquet(trips_rows_iter, trips_parquet, label="trips")
        taxi_zone_rows = materialize_iterable_to_parquet(
            zones_rows_iter,
            zones_parquet,
            label="taxi_zone_lookup",
        )

        spark = get_spark_session()
        spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

        ensure_namespace(spark, CATALOG_NAME, BRONZE_NAMESPACE)
        ensure_namespace(spark, CATALOG_NAME, SILVER_NAMESPACE)

        trips_df = spark.read.parquet(str(trips_parquet))
        trips_df = add_trip_bronze_columns(trips_df, run_id=run_id, source_ref=trips_source_ref)
        trips_partitions = max(1, math.ceil(trips_rows / BRONZE_ROWS_PER_PARTITION))
        trips_df = trips_df.repartition(trips_partitions, F.col("event_date"))

        taxi_zone_df = spark.read.parquet(str(zones_parquet))
        taxi_zone_df = add_zone_bronze_columns(
            taxi_zone_df,
            run_id=run_id,
            source_ref=taxi_zone_source_ref,
        )
        taxi_zone_df = taxi_zone_df.coalesce(1)

        trips_write_mode = write_partitioned_iceberg_table(
            trips_df,
            BRONZE_TRIPS_TABLE,
            "event_date",
        )
        taxi_zone_write_mode = write_replace_iceberg_table(
            taxi_zone_df,
            BRONZE_TAXI_ZONE_TABLE,
        )

    result = BronzeIngestResult(
        run_id=run_id,
        trips_table=qualify_table_id(BRONZE_TRIPS_TABLE),
        taxi_zone_table=qualify_table_id(BRONZE_TAXI_ZONE_TABLE),
        trips_rows=trips_rows,
        taxi_zone_rows=taxi_zone_rows,
        trips_source_ref=trips_source_ref,
        taxi_zone_source_ref=taxi_zone_source_ref,
        trips_write_mode=trips_write_mode,
        taxi_zone_write_mode=taxi_zone_write_mode,
    )
    log_json(msg="bronze_ingest_success", **result.__dict__)
    return result