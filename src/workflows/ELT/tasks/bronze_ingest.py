from __future__ import annotations

import json
import logging
import math
import os
import re
import sys
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from itertools import islice
from typing import Any, Iterable, Sequence

from flytekit import Resources, current_context, task
from flytekitplugins.spark import Spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOG = logging.getLogger("elt_bronze_ingest")
LOG.setLevel(logging.INFO)
_HANDLER = logging.StreamHandler(stream=sys.stdout)
_HANDLER.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_HANDLER]
LOG.propagate = False

CATALOG_NAME = os.environ.get("ICEBERG_CATALOG", "iceberg").strip()

ICEBERG_REST_URI = (
    os.environ.get(
        "ICEBERG_REST_URI",
        "http://iceberg-rest.default.svc.cluster.local:9001/iceberg",
    )
    .strip()
    .rstrip("/")
)
ICEBERG_REST_AUTH_TYPE = os.environ.get("ICEBERG_REST_AUTH_TYPE", "none").strip().lower()
ICEBERG_HTTP_TIMEOUT_SECONDS = int(os.environ.get("ICEBERG_HTTP_TIMEOUT_SECONDS", "10"))

ICEBERG_WAREHOUSE = (
    os.environ.get(
        "ICEBERG_WAREHOUSE",
        "s3://e2e-mlops-data-681802563986/iceberg/warehouse/",
    )
    .strip()
    .rstrip("/")
    + "/"
)

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()
ELT_PROFILE = os.environ.get(
    "ELT_PROFILE",
    "dev" if K8S_CLUSTER in {"kind", "minikube", "docker-desktop", "local"} else "prod",
).strip().lower()
IS_PROD = ELT_PROFILE == "prod"

AWS_DEFAULT_REGION = (
    os.environ.get("AWS_DEFAULT_REGION")
    or os.environ.get("AWS_REGION")
    or "ap-south-1"
).strip()
AWS_REGION = (os.environ.get("AWS_REGION") or AWS_DEFAULT_REGION).strip()
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "").strip()
AWS_ROLE_ARN = os.environ.get("AWS_ROLE_ARN", "").strip()
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "").strip()
S3_PATH_STYLE_ACCESS = os.environ.get("S3_PATH_STYLE_ACCESS", "false").strip().lower()

BRONZE_NAMESPACE = os.environ.get("BRONZE_NAMESPACE", "bronze").strip()
SILVER_NAMESPACE = os.environ.get("SILVER_NAMESPACE", "silver").strip()
GOLD_NAMESPACE = os.environ.get("GOLD_NAMESPACE", "gold").strip()

BRONZE_TRIPS_TABLE = os.environ.get(
    "BRONZE_TRIPS_TABLE",
    f"{CATALOG_NAME}.bronze.trips_raw",
).strip()
BRONZE_TAXI_ZONE_TABLE = os.environ.get(
    "BRONZE_TAXI_ZONE_TABLE",
    f"{CATALOG_NAME}.bronze.taxi_zone_lookup_raw",
).strip()
SILVER_TRIPS_TABLE = os.environ.get(
    "SILVER_TRIPS_TABLE",
    f"{CATALOG_NAME}.silver.trip_canonical",
).strip()
GOLD_TRAINING_TABLE = os.environ.get(
    "GOLD_TRAINING_TABLE",
    f"{CATALOG_NAME}.gold.trip_training_matrix",
).strip()
GOLD_CONTRACT_TABLE = os.environ.get(
    "GOLD_CONTRACT_TABLE",
    f"{CATALOG_NAME}.gold.trip_training_contracts",
).strip()

TRIPS_DATASET_ID = os.environ.get("TRIPS_DATASET_ID", "koorukuroo/yellow_tripdata").strip()
TRIPS_DATASET_SPLIT = os.environ.get("TRIPS_DATASET_SPLIT", "train").strip()
TRIPS_DATASET_REVISION = os.environ.get(
    "TRIPS_DATASET_REVISION",
    "ef7653853df26ba2cd9ccbae6db2f4094c2d63b0",
).strip()
TAXI_ZONE_LOOKUP_URL = os.environ.get(
    "TAXI_ZONE_LOOKUP_URL",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
).strip()
HF_TOKEN = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN") or None

TASK_IMAGE = os.environ.get(
    "ELT_TASK_IMAGE",
    "ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-29-08-46--23128af@sha256:ebf5406cfe3aa4507e110229dbcbb47be433bf971eeedc7d5edf38bfc6c897e2",
).strip()
if not TASK_IMAGE:
    raise RuntimeError("ELT_TASK_IMAGE must be set before importing bronze_ingest.py")


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    value = int(os.environ.get(name, str(default)))
    return max(value, minimum)


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _parse_memory_to_mib(value: str) -> int:
    v = value.strip().lower()
    if v.endswith("gib"):
        return int(float(v[:-3]) * 1024)
    if v.endswith("gi"):
        return int(float(v[:-2]) * 1024)
    if v.endswith("gb"):
        return int(float(v[:-2]) * 1024)
    if v.endswith("g"):
        return int(float(v[:-1]) * 1024)
    if v.endswith("mib"):
        return int(float(v[:-3]))
    if v.endswith("mi"):
        return int(float(v[:-2]))
    if v.endswith("mb"):
        return int(float(v[:-2]))
    if v.endswith("m"):
        return int(float(v[:-1]))
    if v.endswith("kib"):
        return max(1, int(float(v[:-3]) / 1024))
    if v.endswith("ki"):
        return max(1, int(float(v[:-2]) / 1024))
    if v.endswith("kb"):
        return max(1, int(float(v[:-2]) / 1024))
    if v.endswith("k"):
        return max(1, int(float(v[:-1]) / 1024))
    if v.isdigit():
        return int(v) // (1024 * 1024)
    raise ValueError(f"invalid Spark memory value: {value!r}")


def _format_memory_from_mib(mib: int) -> str:
    if mib >= 1024 and mib % 1024 == 0:
        return f"{mib // 1024}g"
    return f"{mib}m"


def _spark_memory_env(name: str, default: str, minimum_mib: int) -> str:
    raw = _env_str(name, default)
    mib = max(_parse_memory_to_mib(raw), minimum_mib)
    return _format_memory_from_mib(mib)


if IS_PROD:
    TASK_LIMITS = Resources(cpu="1000m", mem="3Gi")

    SPARK_DRIVER_MEMORY = _spark_memory_env("SPARK_DRIVER_MEMORY", "1g", 768)
    SPARK_EXECUTOR_MEMORY = _spark_memory_env("SPARK_EXECUTOR_MEMORY", "768m", 512)
    SPARK_DRIVER_MEMORY_OVERHEAD = _spark_memory_env("SPARK_DRIVER_MEMORY_OVERHEAD", "256m", 128)
    SPARK_EXECUTOR_MEMORY_OVERHEAD = _spark_memory_env("SPARK_EXECUTOR_MEMORY_OVERHEAD", "256m", 128)

    SPARK_EXECUTOR_CORES = str(_env_int("SPARK_EXECUTOR_CORES", 1, minimum=1))
    SPARK_EXECUTOR_INSTANCES = str(_env_int("SPARK_EXECUTOR_INSTANCES", 1, minimum=1))
    SPARK_DRIVER_CORES = str(_env_int("SPARK_DRIVER_CORES", 1, minimum=1))

    SPARK_SHUFFLE_PARTITIONS = str(_env_int("SPARK_SHUFFLE_PARTITIONS", 8, minimum=1))
    SPARK_MAX_PARTITION_BYTES = _env_str("SPARK_MAX_PARTITION_BYTES", "67108864")
    SPARK_MAX_RESULT_SIZE = _env_str("SPARK_MAX_RESULT_SIZE", "256m")

    TASK_RETRIES = _env_int("BRONZE_TASK_RETRIES", 1, minimum=0)
    MAX_ROWS_TO_EXTRACT_FROM_DATASETS = _env_int("MAX_ROWS_TO_EXTRACT_FROM_DATASETS", 25000, minimum=0)
    BRONZE_CHUNK_SIZE = _env_int("BRONZE_CHUNK_SIZE", 2000, minimum=1)
    BRONZE_ROWS_PER_PARTITION = _env_int("BRONZE_ROWS_PER_PARTITION", 50000, minimum=1)
    ICEBERG_TARGET_FILE_SIZE_BYTES = _env_str("ICEBERG_TARGET_FILE_SIZE_BYTES", "268435456")
else:
    TASK_LIMITS = Resources(cpu="500m", mem="2Gi")

    SPARK_DRIVER_MEMORY = _spark_memory_env("SPARK_DRIVER_MEMORY", "768m", 768)
    SPARK_EXECUTOR_MEMORY = _spark_memory_env("SPARK_EXECUTOR_MEMORY", "512m", 512)
    SPARK_DRIVER_MEMORY_OVERHEAD = _spark_memory_env("SPARK_DRIVER_MEMORY_OVERHEAD", "128m", 128)
    SPARK_EXECUTOR_MEMORY_OVERHEAD = _spark_memory_env("SPARK_EXECUTOR_MEMORY_OVERHEAD", "128m", 128)

    SPARK_EXECUTOR_CORES = str(_env_int("SPARK_EXECUTOR_CORES", 1, minimum=1))
    SPARK_EXECUTOR_INSTANCES = str(_env_int("SPARK_EXECUTOR_INSTANCES", 1, minimum=1))
    SPARK_DRIVER_CORES = str(_env_int("SPARK_DRIVER_CORES", 1, minimum=1))

    SPARK_SHUFFLE_PARTITIONS = str(_env_int("SPARK_SHUFFLE_PARTITIONS", 4, minimum=1))
    SPARK_MAX_PARTITION_BYTES = _env_str("SPARK_MAX_PARTITION_BYTES", "67108864")
    SPARK_MAX_RESULT_SIZE = _env_str("SPARK_MAX_RESULT_SIZE", "128m")

    TASK_RETRIES = _env_int("BRONZE_TASK_RETRIES", 0, minimum=0)
    MAX_ROWS_TO_EXTRACT_FROM_DATASETS = _env_int("MAX_ROWS_TO_EXTRACT_FROM_DATASETS", 10000, minimum=0)
    BRONZE_CHUNK_SIZE = _env_int("BRONZE_CHUNK_SIZE", 1000, minimum=1)
    BRONZE_ROWS_PER_PARTITION = _env_int("BRONZE_ROWS_PER_PARTITION", 25000, minimum=1)
    ICEBERG_TARGET_FILE_SIZE_BYTES = _env_str("ICEBERG_TARGET_FILE_SIZE_BYTES", "268435456")


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


def log_json(**payload: Any) -> None:
    LOG.info(json.dumps(payload, default=str, sort_keys=True))


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_").lower()
    if not normalized:
        raise ValueError(f"invalid column name after normalization: {name!r}")
    return normalized


def normalize_record(row: dict) -> dict:
    out: dict[str, Any] = {}
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


def parse_table_id(table_id: str) -> tuple[str, str, str]:
    qualified = qualify_table_id(table_id)
    parts = qualified.split(".")
    return parts[0], parts[1], parts[2]


def ensure_namespace(spark: SparkSession, catalog_name: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace}")


def table_exists(spark: SparkSession, table_id: str) -> bool:
    catalog_name, namespace, table_name = parse_table_id(table_id)
    table_name = table_name.replace("'", "''")
    rows = spark.sql(
        f"SHOW TABLES IN {catalog_name}.{namespace} LIKE '{table_name}'"
    ).limit(1).collect()
    return bool(rows)


def build_aws_runtime_env() -> dict[str, str]:
    env = {
        "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
        "AWS_REGION": AWS_REGION,
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            env["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
    return env


def _spark_s3a_credential_provider() -> str:
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return (
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            if AWS_SESSION_TOKEN
            else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
    return "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"


def build_task_environment() -> dict[str, str]:
    return build_aws_runtime_env()


def build_hadoop_conf() -> dict[str, str]:
    conf = {
        "fs.s3a.endpoint.region": AWS_REGION,
        "fs.s3a.aws.credentials.provider": _spark_s3a_credential_provider(),
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }

    if S3_ENDPOINT:
        conf["fs.s3a.endpoint"] = S3_ENDPOINT
        conf["fs.s3a.path.style.access"] = S3_PATH_STYLE_ACCESS
    else:
        conf["fs.s3a.endpoint"] = f"s3.{AWS_REGION}.amazonaws.com"
        conf["fs.s3a.path.style.access"] = "false"

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        conf["fs.s3a.access.key"] = AWS_ACCESS_KEY_ID
        conf["fs.s3a.secret.key"] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            conf["fs.s3a.session.token"] = AWS_SESSION_TOKEN

    return conf


def build_spark_conf(
    *,
    spark_driver_memory: str,
    spark_executor_memory: str,
    spark_driver_memory_overhead: str,
    spark_executor_memory_overhead: str,
    spark_executor_cores: str,
    spark_executor_instances: str,
    spark_driver_cores: str,
    spark_shuffle_partitions: str,
    spark_max_partition_bytes: str,
    spark_max_result_size: str,
    spark_task_max_failures: str = "4",
) -> dict[str, str]:
    aws_env = build_aws_runtime_env()
    s3a_provider = _spark_s3a_credential_provider()

    conf = {
        f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG_NAME}.type": "rest",
        f"spark.sql.catalog.{CATALOG_NAME}.uri": ICEBERG_REST_URI,
        f"spark.sql.catalog.{CATALOG_NAME}.warehouse": ICEBERG_WAREHOUSE,
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type": ICEBERG_REST_AUTH_TYPE,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.shuffle.partitions": spark_shuffle_partitions,
        "spark.sql.files.maxPartitionBytes": spark_max_partition_bytes,
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "67108864",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.driver.memory": spark_driver_memory,
        "spark.driver.memoryOverhead": spark_driver_memory_overhead,
        "spark.executor.memory": spark_executor_memory,
        "spark.executor.memoryOverhead": spark_executor_memory_overhead,
        "spark.executor.cores": spark_executor_cores,
        "spark.executor.instances": spark_executor_instances,
        "spark.driver.cores": spark_driver_cores,
        "spark.driver.maxResultSize": spark_max_result_size,
        "spark.kubernetes.authenticate.driver.serviceAccountName": os.environ.get(
            "SPARK_SERVICE_ACCOUNT",
            "spark",
        ),
        "spark.kubernetes.authenticate.executor.serviceAccountName": os.environ.get(
            "SPARK_SERVICE_ACCOUNT",
            "spark",
        ),
        "spark.kubernetes.driver.limit.cores": spark_driver_cores,
        "spark.kubernetes.executor.limit.cores": spark_executor_cores,
        "spark.task.maxFailures": spark_task_max_failures,
        "spark.excludeOnFailure.enabled": "true",
        "spark.excludeOnFailure.timeout": "5m",
        "spark.hadoop.fs.s3a.aws.credentials.provider": s3a_provider,
        "spark.hadoop.fs.s3a.endpoint.region": AWS_REGION,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }

    if S3_ENDPOINT:
        conf["spark.hadoop.fs.s3a.endpoint"] = S3_ENDPOINT
        conf["spark.hadoop.fs.s3a.path.style.access"] = S3_PATH_STYLE_ACCESS
    else:
        conf["spark.hadoop.fs.s3a.endpoint"] = f"s3.{AWS_REGION}.amazonaws.com"
        conf["spark.hadoop.fs.s3a.path.style.access"] = "false"

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        conf["spark.hadoop.fs.s3a.access.key"] = AWS_ACCESS_KEY_ID
        conf["spark.hadoop.fs.s3a.secret.key"] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            conf["spark.hadoop.fs.s3a.session.token"] = AWS_SESSION_TOKEN

    if ICEBERG_REST_AUTH_TYPE and ICEBERG_REST_AUTH_TYPE != "none":
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type"] = ICEBERG_REST_AUTH_TYPE

    if os.environ.get("ICEBERG_REST_USER", "").strip():
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.basic.username"] = os.environ["ICEBERG_REST_USER"].strip()
    if os.environ.get("ICEBERG_REST_PASSWORD", "").strip():
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.basic.password"] = os.environ["ICEBERG_REST_PASSWORD"].strip()

    for key, value in aws_env.items():
        if value:
            conf[f"spark.kubernetes.driverEnv.{key}"] = value
            conf[f"spark.executorEnv.{key}"] = value

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

    kwargs: dict[str, Any] = {"split": split, "streaming": True}
    if data_files is not None:
        kwargs["data_files"] = data_files
    if revision:
        kwargs["revision"] = revision
    if token:
        kwargs["token"] = token
    return load_dataset(source, **kwargs)


def get_spark_session() -> SparkSession:
    try:
        spark = current_context().spark_session
    except Exception:
        spark = None
    if spark is None:
        spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "Spark session not available. This task must run through Flyte's Spark execution path."
        )
    return spark


def probe_iceberg_rest_endpoint() -> None:
    url = f"{ICEBERG_REST_URI}/v1/config"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=ICEBERG_HTTP_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if resp.status != 200:
                raise RuntimeError(f"expected HTTP 200 from {url}, got {resp.status}")
            json.loads(body)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Iceberg REST endpoint returned HTTP {exc.code} for {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"unable to reach Iceberg REST endpoint {url}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Iceberg REST endpoint {url} did not return valid JSON") from exc


def validate_iceberg_catalog(spark: SparkSession) -> None:
    type_key = f"spark.sql.catalog.{CATALOG_NAME}.type"
    uri_key = f"spark.sql.catalog.{CATALOG_NAME}.uri"
    auth_key = f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type"

    catalog_type = spark.conf.get(type_key, "").strip().lower()
    catalog_uri = spark.conf.get(uri_key, "").strip().rstrip("/")
    auth_type = spark.conf.get(auth_key, ICEBERG_REST_AUTH_TYPE).strip().lower()

    if catalog_type != "rest":
        raise RuntimeError(
            f"Iceberg catalog misconfigured: expected {type_key}=rest, got {catalog_type!r}"
        )
    if not catalog_uri:
        raise RuntimeError(f"Iceberg catalog misconfigured: missing {uri_key}")
    if catalog_uri.startswith("s3://"):
        raise RuntimeError(
            f"Iceberg catalog misconfigured: {uri_key} points at a warehouse path, not the REST endpoint: {catalog_uri!r}"
        )
    if not catalog_uri.startswith(("http://", "https://")):
        raise RuntimeError(
            f"Iceberg catalog misconfigured: {uri_key} must be an http(s) REST endpoint, got {catalog_uri!r}"
        )
    if auth_type != "none":
        raise RuntimeError(
            f"Iceberg catalog misconfigured: expected {auth_key}=none, got {auth_type!r}"
        )

    spark_warehouse_key = f"spark.sql.catalog.{CATALOG_NAME}.warehouse"
    spark_warehouse = spark.conf.get(spark_warehouse_key, "").strip()
    if spark_warehouse and spark_warehouse.rstrip("/") != ICEBERG_WAREHOUSE.rstrip("/"):
        raise RuntimeError(
            "Iceberg catalog misconfigured: Spark warehouse does not match the configured warehouse. "
            f"spark={spark_warehouse!r}, env={ICEBERG_WAREHOUSE!r}"
        )

    log_json(
        msg="iceberg_catalog_config",
        catalog=CATALOG_NAME,
        type=catalog_type,
        uri=catalog_uri,
        warehouse_env=ICEBERG_WAREHOUSE,
        warehouse_spark=spark_warehouse or None,
        rest_auth_type=auth_type,
    )

    probe_iceberg_rest_endpoint()

    try:
        spark.sql(f"SHOW NAMESPACES IN {CATALOG_NAME}").limit(1).collect()
    except Exception as exc:
        raise RuntimeError(
            "Iceberg REST catalog probe failed. Confirm that Spark uses "
            "spark.sql.catalog.<name>.type=rest, spark.sql.catalog.<name>.uri points "
            "to the REST endpoint, spark.sql.catalog.<name>.warehouse is set to the S3 warehouse, "
            "and the REST service is configured for auth type none."
        ) from exc


def iter_preview_rows(stream: Iterable[dict], n: int = 2) -> tuple[list[dict], Iterable[dict]]:
    iterator = iter(stream)
    preview = list(islice(iterator, n))
    return preview, iterator


def stream_to_dataframe(
    spark: SparkSession,
    rows: Iterable[dict],
    *,
    label: str,
    chunk_size: int = BRONZE_CHUNK_SIZE,
) -> tuple[DataFrame, int]:
    accumulated_df: DataFrame | None = None
    schema = None
    total_rows = 0
    batch: list[dict] = []

    def flush_batch(current_batch: list[dict], current_schema: Any) -> tuple[DataFrame, Any]:
        if not current_batch:
            raise RuntimeError(f"attempted to flush an empty batch for {label}")
        batch_df = spark.createDataFrame(current_batch, schema=current_schema)
        return batch_df, batch_df.schema

    for row in rows:
        batch.append(normalize_record(dict(row)))
        total_rows += 1
        if len(batch) >= chunk_size:
            batch_df, schema = flush_batch(batch, schema)
            accumulated_df = batch_df if accumulated_df is None else accumulated_df.unionByName(
                batch_df,
                allowMissingColumns=True,
            )
            log_json(
                msg="materialized_batch",
                label=label,
                rows=total_rows,
                batch_rows=len(batch),
            )
            batch.clear()

    if batch:
        batch_df, schema = flush_batch(batch, schema)
        accumulated_df = batch_df if accumulated_df is None else accumulated_df.unionByName(
            batch_df,
            allowMissingColumns=True,
        )
        log_json(
            msg="materialized_final_batch",
            label=label,
            rows=total_rows,
            batch_rows=len(batch),
        )
        batch.clear()

    if accumulated_df is None or total_rows == 0:
        raise RuntimeError(f"no rows read from source {label!r}")

    return accumulated_df, total_rows


def cast_if_present(df: DataFrame, column: str, spark_type: str) -> DataFrame:
    if column in df.columns:
        return df.withColumn(column, F.col(column).cast(spark_type))
    return df


def add_trip_bronze_columns(df: DataFrame, *, run_id: str, source_ref: str) -> DataFrame:
    pickup_col = first_existing(df.columns, ("lpep_pickup_datetime", "tpep_pickup_datetime", "pickup_ts"))
    dropoff_col = first_existing(df.columns, ("lpep_dropoff_datetime", "tpep_dropoff_datetime", "dropoff_ts"))
    pickup_location_col = first_existing(df.columns, ("pulocation_id", "pickup_location_id"))
    dropoff_location_col = first_existing(df.columns, ("dolocation_id", "dropoff_location_id"))

    df = (
        df.withColumn("pickup_ts", F.to_timestamp(F.col(pickup_col)))
        .withColumn("dropoff_ts", F.to_timestamp(F.col(dropoff_col)))
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
    writer = (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
        .partitionedBy(F.col(partition_column))
    )

    if table_exists(df.sparkSession, table_id):
        writer.overwritePartitions()
        return "overwrite_partitions"

    writer.create()
    return "create"


def write_replace_iceberg_table(df: DataFrame, table_id: str) -> str:
    table_id = qualify_table_id(table_id)
    writer = (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
    )

    if table_exists(df.sparkSession, table_id):
        writer.overwrite(F.lit(True))
        return "overwrite"

    writer.create()
    return "create"


def detect_aws_credential_mode() -> str:
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        if AWS_SESSION_TOKEN:
            return "static_env_session"
        return "static_env"
    if os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE"):
        return "web_identity"
    if AWS_ROLE_ARN:
        return "role_arn"
    return "missing"


def _spark_tuning_summary() -> dict[str, Any]:
    return {
        "profile": ELT_PROFILE,
        "cluster": K8S_CLUSTER,
        "driver_memory": SPARK_DRIVER_MEMORY,
        "driver_memory_overhead": SPARK_DRIVER_MEMORY_OVERHEAD,
        "executor_memory": SPARK_EXECUTOR_MEMORY,
        "executor_memory_overhead": SPARK_EXECUTOR_MEMORY_OVERHEAD,
        "driver_cores": SPARK_DRIVER_CORES,
        "executor_cores": SPARK_EXECUTOR_CORES,
        "executor_instances": SPARK_EXECUTOR_INSTANCES,
        "shuffle_partitions": SPARK_SHUFFLE_PARTITIONS,
        "task_limits_cpu": getattr(TASK_LIMITS, "cpu", None),
        "task_limits_mem": getattr(TASK_LIMITS, "mem", None),
    }


@task(
    task_config=Spark(
        spark_conf=build_spark_conf(
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
        ),
        hadoop_conf=build_hadoop_conf(),
        executor_path="/opt/venv/bin/python",
    ),
    container_image=TASK_IMAGE,
    environment=build_task_environment(),
    retries=TASK_RETRIES,
    limits=TASK_LIMITS,
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
        profile=ELT_PROFILE,
        k8s_cluster=K8S_CLUSTER,
        aws_credential_mode=detect_aws_credential_mode(),
        iceberg_catalog=CATALOG_NAME,
        iceberg_rest_uri=ICEBERG_REST_URI,
        iceberg_warehouse=ICEBERG_WAREHOUSE,
        iceberg_rest_auth_type=ICEBERG_REST_AUTH_TYPE,
        trips_source=trips_source_ref,
        taxi_zone_source=taxi_zone_source_ref,
        max_rows=MAX_ROWS_TO_EXTRACT_FROM_DATASETS,
        spark_tuning=_spark_tuning_summary(),
    )

    probe_iceberg_rest_endpoint()

    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    validate_iceberg_catalog(spark)

    ensure_namespace(spark, CATALOG_NAME, BRONZE_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, SILVER_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, GOLD_NAMESPACE)

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

    trips_preview, trips_iter = iter_preview_rows(trips_stream, 2)
    taxi_preview, taxi_iter = iter_preview_rows(taxi_zone_stream, 2)

    for i, row in enumerate(trips_preview, start=1):
        log_json(msg="trip_preview_row", row=i, data=normalize_record(dict(row)))
    for i, row in enumerate(taxi_preview, start=1):
        log_json(msg="taxi_zone_preview_row", row=i, data=normalize_record(dict(row)))

    if MAX_ROWS_TO_EXTRACT_FROM_DATASETS > 0:
        trips_iter = islice(trips_iter, MAX_ROWS_TO_EXTRACT_FROM_DATASETS)
        taxi_iter = islice(taxi_iter, MAX_ROWS_TO_EXTRACT_FROM_DATASETS)

    trips_raw_df, trips_rows = stream_to_dataframe(
        spark,
        trips_iter,
        label="trips",
        chunk_size=BRONZE_CHUNK_SIZE,
    )
    trips_df = add_trip_bronze_columns(trips_raw_df, run_id=run_id, source_ref=trips_source_ref)
    trips_partitions = max(1, math.ceil(trips_rows / BRONZE_ROWS_PER_PARTITION))
    trips_df = trips_df.repartition(trips_partitions, F.col("event_date"))

    taxi_zone_raw_df, taxi_zone_rows = stream_to_dataframe(
        spark,
        taxi_iter,
        label="taxi_zone_lookup",
        chunk_size=min(BRONZE_CHUNK_SIZE, 1000),
    )
    taxi_zone_df = add_zone_bronze_columns(
        taxi_zone_raw_df,
        run_id=run_id,
        source_ref=taxi_zone_source_ref,
    ).coalesce(1)

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