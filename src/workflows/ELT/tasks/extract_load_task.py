import json
import logging
import math
import os
import re
import sys
import uuid
from typing import Dict

from flytekit import current_context, task
from flytekitplugins.spark import Spark
from pyspark.sql import DataFrame, SparkSession, functions as F

LOG = logging.getLogger("elt_extract_load_task")
LOG.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [handler]
LOG.propagate = False

CATALOG_NAME = os.environ.get("ICEBERG_CATALOG", "iceberg")
NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "bronze")
TABLE = os.environ.get("ICEBERG_TABLE", "yellow_tripdata_2023_01")
SOURCE_URI = os.environ.get(
    "SOURCE_URI",
    "s3://nyc-tlc/trip data/yellow_tripdata_2023-01.parquet",
)
ICEBERG_REST_URI = os.environ.get(
    "ICEBERG_REST_URI",
    "http://iceberg-rest:9001/iceberg",
)
ICEBERG_WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE",
    "s3://e2e-mlops-data-681802563986/iceberg/warehouse/",
)
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
ICEBERG_REST_AUTH_TYPE = os.environ.get("ICEBERG_REST_AUTH_TYPE", "")
ICEBERG_REST_USER = os.environ.get("ICEBERG_REST_USER", "")
ICEBERG_REST_PASSWORD = os.environ.get("ICEBERG_REST_PASSWORD", "")
TARGET_ROWS = int(os.environ.get("TARGET_ROWS", "5000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
TARGET_FILE_SIZE_BYTES = os.environ.get("ICEBERG_TARGET_FILE_SIZE_BYTES", "268435456")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_EXECUTOR_CORES = os.environ.get("SPARK_EXECUTOR_CORES", "1")
SPARK_EXECUTOR_INSTANCES = os.environ.get("SPARK_EXECUTOR_INSTANCES", "1")
SPARK_DRIVER_CORES = os.environ.get("SPARK_DRIVER_CORES", "1")
SPARK_SHUFFLE_PARTITIONS = os.environ.get(
    "SPARK_SHUFFLE_PARTITIONS",
    str(max(4, math.ceil(TARGET_ROWS / BATCH_SIZE))),
)
SPARK_MAX_PARTITION_BYTES = os.environ.get("SPARK_MAX_PARTITION_BYTES", "134217728")


def jlog(**payload):
    LOG.info(json.dumps(payload, default=str))


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_").lower()
    if not normalized:
        raise ValueError(f"invalid column name after normalization: {name!r}")
    return normalized


def normalize_source_uri(uri: str) -> str:
    if uri.startswith("s3://"):
        return "s3a://" + uri[len("s3://") :]
    return uri


def build_spark_conf() -> Dict[str, str]:
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


def build_hadoop_conf() -> Dict[str, str]:
    return {
        "fs.s3a.endpoint.region": AWS_REGION,
        "fs.s3a.path.style.access": "false",
    }


def get_spark_session() -> SparkSession:
    try:
        ctx = current_context()
        spark = getattr(ctx, "spark_session", None)
        if spark is not None:
            return spark
    except Exception:
        pass

    builder = SparkSession.builder.appName("flyte-elt-extract-load").master("local[*]")
    for key, value in build_spark_conf().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in build_hadoop_conf().items():
        hconf.set(key, value)
    return spark


def ensure_namespace(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")


def table_exists(spark: SparkSession) -> bool:
    rows = spark.sql(
        f"SHOW TABLES IN {CATALOG_NAME}.{NAMESPACE} LIKE '{TABLE}'"
    ).limit(1).collect()
    return len(rows) > 0


def load_source(spark: SparkSession, source_uri: str) -> DataFrame:
    resolved = normalize_source_uri(source_uri)
    jlog(msg="source_read_start", source_uri=source_uri, resolved_source_uri=resolved)
    df = spark.read.parquet(resolved)
    jlog(msg="source_read_complete", schema=df.schema.simpleString())
    return df


def prepare_frame(source_df: DataFrame, source_uri: str, run_id: str) -> DataFrame:
    if len(source_df.columns) == 0:
        raise RuntimeError("source dataframe has no columns")

    normalized_names = [normalize_column_name(c) for c in source_df.columns]
    if len(set(normalized_names)) != len(normalized_names):
        raise RuntimeError(
            f"column collision after normalization: {list(zip(source_df.columns, normalized_names))}"
        )

    df = source_df.select(
        *[F.col(original).alias(normalized) for original, normalized in zip(source_df.columns, normalized_names)]
    )

    if "tpep_pickup_datetime" not in df.columns and "tpep_dropoff_datetime" not in df.columns:
        raise RuntimeError("expected tpep_pickup_datetime or tpep_dropoff_datetime in source schema")

    pickup_ts = F.to_timestamp(F.col("tpep_pickup_datetime")) if "tpep_pickup_datetime" in df.columns else F.lit(None)
    dropoff_ts = F.to_timestamp(F.col("tpep_dropoff_datetime")) if "tpep_dropoff_datetime" in df.columns else F.lit(None)

    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn("event_date", F.to_date(F.coalesce(pickup_ts, dropoff_ts)))
    df = df.withColumn("ingestion_ts", F.current_timestamp())
    df = df.withColumn("run_id", F.lit(run_id))
    df = df.withColumn("source_uri", F.lit(source_uri))
    df = df.filter(F.col("event_date").isNotNull())

    write_partitions = max(1, math.ceil(TARGET_ROWS / BATCH_SIZE))
    df = df.repartition(write_partitions, F.col("event_date"))

    return df


def write_to_iceberg(spark: SparkSession, df: DataFrame) -> str:
    ensure_namespace(spark)
    table_id = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE}"

    if table_exists(spark):
        jlog(msg="iceberg_write_mode", table=table_id, mode="overwrite_partitions")
        df.writeTo(table_id).overwritePartitions()
        return "overwrite_partitions"

    jlog(msg="iceberg_write_mode", table=table_id, mode="create")
    (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", TARGET_FILE_SIZE_BYTES)
        .partitionedBy(F.col("event_date"))
        .create()
    )
    return "create"


def run_extract_load() -> Dict[str, str]:
    run_id = (
        os.environ.get("RUN_ID")
        or os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
        or uuid.uuid4().hex
    )

    jlog(
        msg="job_start",
        run_id=run_id,
        catalog=CATALOG_NAME,
        namespace=NAMESPACE,
        table=TABLE,
        source_uri=SOURCE_URI,
        target_rows=TARGET_ROWS,
        batch_size=BATCH_SIZE,
        iceberg_rest_uri=ICEBERG_REST_URI,
        iceberg_warehouse=ICEBERG_WAREHOUSE,
        aws_region=AWS_REGION,
    )

    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    source_df = load_source(spark, SOURCE_URI).limit(TARGET_ROWS)
    prepared_df = prepare_frame(source_df, SOURCE_URI, run_id)

    stats = prepared_df.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("event_date").alias("partition_count"),
        F.min("event_date").alias("min_event_date"),
        F.max("event_date").alias("max_event_date"),
    ).collect()[0]

    write_mode = write_to_iceberg(spark, prepared_df)

    result = {
        "status": "ok",
        "run_id": str(run_id),
        "table": f"{CATALOG_NAME}.{NAMESPACE}.{TABLE}",
        "source_uri": SOURCE_URI,
        "resolved_source_uri": normalize_source_uri(SOURCE_URI),
        "write_mode": write_mode,
        "row_count": str(stats["row_count"]),
        "partition_count": str(stats["partition_count"]),
        "min_event_date": str(stats["min_event_date"]),
        "max_event_date": str(stats["max_event_date"]),
        "target_rows": str(TARGET_ROWS),
        "batch_size": str(BATCH_SIZE),
    }
    jlog(msg="job_success", **result)
    return result


@task(
    task_config=Spark(
        spark_conf=build_spark_conf(),
        hadoop_conf=build_hadoop_conf(),
    ),
    retries=2,
)
def extract_load_task() -> Dict[str, str]:
    return run_extract_load()