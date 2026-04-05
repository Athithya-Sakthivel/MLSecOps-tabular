from __future__ import annotations

import json
import logging
import os
import re
import sys
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from itertools import chain, islice
from typing import Any

from flytekit import Resources, current_context, task
from flytekitplugins.spark import Spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

LOG = logging.getLogger("elt_bronze_ingest")
LOG.setLevel(logging.INFO)
_HANDLER = logging.StreamHandler(stream=sys.stdout)
_HANDLER.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_HANDLER]
LOG.propagate = False

CATALOG_NAME = os.environ.get("ICEBERG_CATALOG", "iceberg").strip() or "iceberg"

ICEBERG_REST_URI = (
    os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest.default.svc.cluster.local:8181")
    .strip()
    .rstrip("/")
)
ICEBERG_REST_AUTH_TYPE = os.environ.get("ICEBERG_REST_AUTH_TYPE", "none").strip().lower() or "none"
ICEBERG_HTTP_TIMEOUT_SECONDS = int(os.environ.get("ICEBERG_HTTP_TIMEOUT_SECONDS", "10"))

ICEBERG_WAREHOUSE = (
    os.environ.get("ICEBERG_WAREHOUSE", "s3://e2e-mlops-data-681802563986/iceberg/warehouse/")
    .strip()
    .rstrip("/")
    + "/"
)

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()
ELT_PROFILE = (
    os.environ.get("ELT_PROFILE", "dev" if K8S_CLUSTER in {"kind"} else "prod")
    .strip()
    .lower()
)
IS_PROD = ELT_PROFILE == "prod"

AWS_DEFAULT_REGION = (os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "ap-south-1").strip()
AWS_REGION = (os.environ.get("AWS_REGION") or AWS_DEFAULT_REGION).strip()
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "").strip()
AWS_ROLE_ARN = os.environ.get("AWS_ROLE_ARN", "").strip()
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "").strip()
S3_PATH_STYLE_ACCESS = os.environ.get("S3_PATH_STYLE_ACCESS", "false").strip().lower()
SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark").strip() or "spark"

BRONZE_NAMESPACE = os.environ.get("BRONZE_NAMESPACE", "bronze").strip() or "bronze"
SILVER_NAMESPACE = os.environ.get("SILVER_NAMESPACE", "silver").strip() or "silver"
GOLD_NAMESPACE = os.environ.get("GOLD_NAMESPACE", "gold").strip() or "gold"

BRONZE_TRIPS_TABLE = os.environ.get("BRONZE_TRIPS_TABLE", f"{CATALOG_NAME}.bronze.trips_raw").strip()
BRONZE_TAXI_ZONE_TABLE = os.environ.get("BRONZE_TAXI_ZONE_TABLE", f"{CATALOG_NAME}.bronze.taxi_zone_lookup_raw").strip()

SILVER_TRIPS_TABLE = os.environ.get("SILVER_TRIPS_TABLE", f"{CATALOG_NAME}.silver.trip_canonical").strip()
GOLD_TRAINING_TABLE = os.environ.get("GOLD_TRAINING_TABLE", f"{CATALOG_NAME}.gold.trip_training_matrix").strip()
GOLD_CONTRACT_TABLE = os.environ.get("GOLD_CONTRACT_TABLE", f"{CATALOG_NAME}.gold.trip_training_contracts").strip()

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


def _normalize_http_endpoint(value: str, *, default: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        candidate = default.strip()
    if candidate.startswith(("http://", "https://")):
        return candidate.rstrip("/")
    return f"https://{candidate.rstrip('/')}"


def _spark_s3_endpoint() -> str:
    default = f"https://s3.{AWS_REGION}.amazonaws.com"
    return _normalize_http_endpoint(S3_ENDPOINT, default=default)


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
    MAX_ROWS_TO_EXTRACT_FROM_DATASETS = _env_int("MAX_ROWS_TO_EXTRACT_FROM_DATASETS", 500000, minimum=0)
    BRONZE_CHUNK_SIZE = _env_int("BRONZE_CHUNK_SIZE", 2000, minimum=1)
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


TRIPS_SOURCE_FIELDS: tuple[str, ...] = (
    "raw_record_json",
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_ts",
    "dropoff_ts",
    "pulocation_id",
    "dolocation_id",
    "pickup_location_id",
    "dropoff_location_id",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "trip_type",
    "vendorname",
    "record_id",
    "vendor_id_2",
)

TAXI_ZONE_SOURCE_FIELDS: tuple[str, ...] = (
    "raw_record_json",
    "location_id",
    "borough",
    "zone",
    "service_zone",
)

TRIPS_SOURCE_SCHEMA = StructType([StructField(field, StringType(), True) for field in TRIPS_SOURCE_FIELDS])
TAXI_ZONE_SOURCE_SCHEMA = StructType([StructField(field, StringType(), True) for field in TAXI_ZONE_SOURCE_FIELDS])


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
            raise ValueError(f"column collision after normalization: {key!r} -> {normalized_key!r}")
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
    raise ValueError(f"expected table id in the form catalog.namespace.table or namespace.table, got {table_id!r}")


def parse_table_id(table_id: str) -> tuple[str, str, str]:
    qualified = qualify_table_id(table_id)
    parts = qualified.split(".")
    return parts[0], parts[1], parts[2]


def ensure_namespace(spark: SparkSession, catalog_name: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace}")


def table_exists(spark: SparkSession, table_id: str) -> bool:
    catalog_name, namespace, table_name = parse_table_id(table_id)
    table_name = table_name.replace("'", "''")
    rows = spark.sql(f"SHOW TABLES IN {catalog_name}.{namespace} LIKE '{table_name}'").limit(1).collect()
    return bool(rows)


def build_aws_runtime_env() -> dict[str, str]:
    env = {
        "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
        "AWS_REGION": AWS_REGION,
        "AWS_EC2_METADATA_DISABLED": "true",
        "ICEBERG_CATALOG": CATALOG_NAME,
        "ICEBERG_REST_URI": ICEBERG_REST_URI,
        "ICEBERG_REST_AUTH_TYPE": ICEBERG_REST_AUTH_TYPE,
        "ICEBERG_WAREHOUSE": ICEBERG_WAREHOUSE,
        "SPARK_SQL_CATALOG_ICEBERG_TYPE": "rest",
        "SPARK_SQL_CATALOG_ICEBERG_URI": ICEBERG_REST_URI,
        "SPARK_SQL_CATALOG_ICEBERG_WAREHOUSE": ICEBERG_WAREHOUSE,
        "SPARK_SQL_CATALOG_ICEBERG_IO_IMPL": "org.apache.iceberg.aws.s3.S3FileIO",
        "SPARK_SQL_CATALOG_ICEBERG_REST_AUTH_TYPE": ICEBERG_REST_AUTH_TYPE,
        "SPARK_SQL_CATALOG_ICEBERG_CLIENT_REGION": AWS_REGION,
        "SPARK_SQL_CATALOG_ICEBERG_S3_ENDPOINT": _spark_s3_endpoint(),
        "SPARK_SQL_CATALOG_ICEBERG_S3_PATH_STYLE_ACCESS": S3_PATH_STYLE_ACCESS,
        "SPARK_SQL_CATALOG_ICEBERG_HADOOP_FS_S3A_ENDPOINT": _spark_s3_endpoint(),
        "SPARK_SQL_CATALOG_ICEBERG_HADOOP_FS_S3A_PATH_STYLE_ACCESS": S3_PATH_STYLE_ACCESS,
        "SPARK_SQL_CATALOG_ICEBERG_HADOOP_FS_S3A_AWS_CREDENTIALS_PROVIDER": _spark_s3a_credential_provider(),
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
    s3_endpoint = _spark_s3_endpoint()
    conf = {
        "fs.s3a.endpoint.region": AWS_REGION,
        "fs.s3a.aws.credentials.provider": _spark_s3a_credential_provider(),
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.endpoint": s3_endpoint,
        "fs.s3a.path.style.access": S3_PATH_STYLE_ACCESS if S3_ENDPOINT else "false",
    }
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
    s3_endpoint = _spark_s3_endpoint()

    conf = {
        f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG_NAME}.type": "rest",
        f"spark.sql.catalog.{CATALOG_NAME}.uri": ICEBERG_REST_URI,
        f"spark.sql.catalog.{CATALOG_NAME}.warehouse": ICEBERG_WAREHOUSE,
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type": ICEBERG_REST_AUTH_TYPE,
        f"spark.sql.catalog.{CATALOG_NAME}.client.region": AWS_REGION,
        f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint": s3_endpoint,
        f"spark.sql.catalog.{CATALOG_NAME}.s3.path.style.access": S3_PATH_STYLE_ACCESS if S3_ENDPOINT else "false",
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
        "spark.kubernetes.authenticate.driver.serviceAccountName": SPARK_SERVICE_ACCOUNT,
        "spark.kubernetes.authenticate.executor.serviceAccountName": SPARK_SERVICE_ACCOUNT,
        "spark.kubernetes.driver.limit.cores": spark_driver_cores,
        "spark.kubernetes.executor.limit.cores": spark_executor_cores,
        "spark.task.maxFailures": spark_task_max_failures,
        "spark.excludeOnFailure.enabled": "true",
        "spark.excludeOnFailure.timeout": "5m",
        "spark.hadoop.fs.s3a.aws.credentials.provider": s3a_provider,
        "spark.hadoop.fs.s3a.endpoint.region": AWS_REGION,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": s3_endpoint,
        "spark.hadoop.fs.s3a.path.style.access": S3_PATH_STYLE_ACCESS if S3_ENDPOINT else "false",
    }

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        conf["spark.hadoop.fs.s3a.access.key"] = AWS_ACCESS_KEY_ID
        conf["spark.hadoop.fs.s3a.secret.key"] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            conf["spark.hadoop.fs.s3a.session.token"] = AWS_SESSION_TOKEN

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
        raise RuntimeError("Spark session not available. This task must run through Flyte's Spark execution path.")
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


def probe_http_source(url: str) -> None:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=ICEBERG_HTTP_TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                raise RuntimeError(f"expected HTTP 200 from {url}, got {resp.status}")
            resp.read(1)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"source endpoint returned HTTP {exc.code} for {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"unable to reach source endpoint {url}: {exc}") from exc


def validate_iceberg_catalog(spark: SparkSession) -> None:
    impl_key = f"spark.sql.catalog.{CATALOG_NAME}"
    type_key = f"spark.sql.catalog.{CATALOG_NAME}.type"
    uri_key = f"spark.sql.catalog.{CATALOG_NAME}.uri"
    warehouse_key = f"spark.sql.catalog.{CATALOG_NAME}.warehouse"
    auth_key = f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type"

    catalog_impl = spark.conf.get(impl_key, "").strip()
    catalog_type = spark.conf.get(type_key, "").strip().lower()
    catalog_uri = spark.conf.get(uri_key, "").strip().rstrip("/")
    catalog_warehouse = spark.conf.get(warehouse_key, "").strip().rstrip("/") + "/"
    auth_type = spark.conf.get(auth_key, ICEBERG_REST_AUTH_TYPE).strip().lower()

    if catalog_impl != "org.apache.iceberg.spark.SparkCatalog":
        raise RuntimeError(
            f"Iceberg catalog misconfigured: expected {impl_key}=org.apache.iceberg.spark.SparkCatalog, got {catalog_impl!r}"
        )
    if catalog_type != "rest":
        raise RuntimeError(f"Iceberg catalog misconfigured: expected {type_key}=rest, got {catalog_type!r}")
    if catalog_uri != ICEBERG_REST_URI:
        raise RuntimeError(f"Iceberg catalog misconfigured: expected {uri_key}={ICEBERG_REST_URI!r}, got {catalog_uri!r}")
    if not catalog_uri.startswith(("http://", "https://")):
        raise RuntimeError(f"Iceberg catalog misconfigured: {uri_key} must be an http(s) REST endpoint, got {catalog_uri!r}")
    if catalog_warehouse != ICEBERG_WAREHOUSE:
        raise RuntimeError(
            f"Iceberg catalog misconfigured: expected {warehouse_key}={ICEBERG_WAREHOUSE!r}, got {catalog_warehouse!r}"
        )
    if auth_type != "none":
        raise RuntimeError(f"Iceberg catalog misconfigured: expected {auth_key}=none, got {auth_type!r}")

    log_json(
        msg="iceberg_catalog_config",
        catalog=CATALOG_NAME,
        type=catalog_type,
        uri=catalog_uri,
        warehouse_env=ICEBERG_WAREHOUSE,
        warehouse_spark=catalog_warehouse,
        rest_auth_type=auth_type,
    )

    probe_iceberg_rest_endpoint()

    try:
        spark.sql(f"SHOW NAMESPACES IN {CATALOG_NAME}").limit(1).collect()
    except Exception as exc:
        raise RuntimeError(
            "Iceberg REST catalog probe failed. Confirm that Spark uses spark.sql.catalog.<name>.type=rest, "
            "spark.sql.catalog.<name>.uri points to the REST endpoint, spark.sql.catalog.<name>.warehouse is set "
            "to the S3 warehouse, and the REST service is configured for auth type none."
        ) from exc


def iter_preview_rows(stream: Iterable[dict], n: int = 2) -> tuple[list[dict], Iterable[dict]]:
    iterator = iter(stream)
    preview = list(islice(iterator, n))
    return preview, chain(preview, iterator)


def _stringify_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _project_row_to_schema(row: dict[str, Any], *, fields: Sequence[str]) -> dict[str, str | None]:
    return {field: _stringify_value(row.get(field)) for field in fields}


def build_trip_source_row(row: dict[str, Any]) -> dict[str, str | None]:
    normalized = normalize_record(dict(row))
    projected = _project_row_to_schema(normalized, fields=TRIPS_SOURCE_FIELDS)
    projected["raw_record_json"] = json.dumps(normalized, default=str, sort_keys=True)
    return projected


def build_taxi_zone_source_row(row: dict[str, Any]) -> dict[str, str | None]:
    normalized = normalize_record(dict(row))
    projected = _project_row_to_schema(normalized, fields=TAXI_ZONE_SOURCE_FIELDS)
    projected["raw_record_json"] = json.dumps(normalized, default=str, sort_keys=True)
    return projected


def cast_if_present(df: DataFrame, column: str, spark_type: str) -> DataFrame:
    if column in df.columns:
        return df.withColumn(column, F.col(column).cast(spark_type))
    return df


def _safe_to_timestamp(column: F.Column) -> F.Column:
    raw = F.trim(column.cast("string"))
    return F.coalesce(
        F.to_timestamp(raw),
        F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
    )


def add_trip_bronze_columns(df: DataFrame, *, run_id: str, source_ref: str) -> DataFrame:
    pickup_col = first_existing(df.columns, ("lpep_pickup_datetime", "tpep_pickup_datetime", "pickup_ts", "pickup_datetime"))
    dropoff_col = first_existing(df.columns, ("lpep_dropoff_datetime", "tpep_dropoff_datetime", "dropoff_ts", "dropoff_datetime"))
    pickup_location_col = first_existing(df.columns, ("pulocation_id", "pickup_location_id"))
    dropoff_location_col = first_existing(df.columns, ("dolocation_id", "dropoff_location_id"))

    df = (
        df.withColumn("pickup_ts", _safe_to_timestamp(F.col(pickup_col)))
        .withColumn("dropoff_ts", _safe_to_timestamp(F.col(dropoff_col)))
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

    for col_name in (
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
        "airport_fee",
    ):
        df = cast_if_present(df, col_name, "double")
    for col_name in ("vendor_id", "ratecode_id", "passenger_count", "payment_type", "trip_type"):
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
            F.col("raw_record_json").cast("string").alias("raw_record_json"),
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


def write_trip_batch(
    spark: SparkSession,
    batch_rows: list[dict[str, str | None]],
    *,
    table_id: str,
    create_if_missing: bool,
    run_id: str,
    source_ref: str,
) -> tuple[str, bool]:
    if not batch_rows:
        raise RuntimeError("attempted to write an empty trips batch")

    df = spark.createDataFrame(batch_rows, schema=TRIPS_SOURCE_SCHEMA)
    df = add_trip_bronze_columns(df, run_id=run_id, source_ref=source_ref)

    writer = (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
    )

    if create_if_missing:
        writer.partitionedBy(F.col("event_date")).create()
        return "create", True

    writer.append()
    return "append", True


def delete_existing_trip_run_rows(spark: SparkSession, *, table_id: str, run_id: str) -> bool:
    if not table_exists(spark, table_id):
        return False
    escaped_run_id = run_id.replace("'", "''")
    spark.sql(f"DELETE FROM {table_id} WHERE run_id = '{escaped_run_id}'")
    return True


def write_taxi_zone_table(
    spark: SparkSession,
    rows: list[dict[str, str | None]],
    *,
    table_id: str,
    run_id: str,
    source_ref: str,
) -> str:
    if not rows:
        raise RuntimeError("no rows read from source 'taxi_zone_lookup'")

    df = spark.createDataFrame(rows, schema=TAXI_ZONE_SOURCE_SCHEMA)
    df = add_zone_bronze_columns(df, run_id=run_id, source_ref=source_ref).coalesce(1)

    writer = (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
    )

    if table_exists(spark, table_id):
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
    environment=build_task_environment(),
    retries=TASK_RETRIES,
    limits=TASK_LIMITS,
)
def bronze_ingest() -> BronzeIngestResult:
    run_id = os.environ.get("RUN_ID") or os.environ.get("FLYTE_INTERNAL_EXECUTION_ID") or uuid.uuid4().hex

    trips_source_ref = f"{TRIPS_DATASET_ID}@{TRIPS_DATASET_REVISION}" if TRIPS_DATASET_REVISION else TRIPS_DATASET_ID
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
    probe_http_source(TAXI_ZONE_LOOKUP_URL)

    if not TRIPS_DATASET_ID:
        raise RuntimeError("TRIPS_DATASET_ID is empty")
    if TRIPS_DATASET_REVISION is not None and not TRIPS_DATASET_REVISION.strip():
        raise RuntimeError("TRIPS_DATASET_REVISION is empty after stripping")
    if not TAXI_ZONE_LOOKUP_URL.startswith(("http://", "https://")):
        raise RuntimeError(f"TAXI_ZONE_LOOKUP_URL must be an http(s) URL, got {TAXI_ZONE_LOOKUP_URL!r}")

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

    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    validate_iceberg_catalog(spark)

    ensure_namespace(spark, CATALOG_NAME, BRONZE_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, SILVER_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, GOLD_NAMESPACE)

    trips_table_id = qualify_table_id(BRONZE_TRIPS_TABLE)
    taxi_zone_table_id = qualify_table_id(BRONZE_TAXI_ZONE_TABLE)

    trips_table_exists = table_exists(spark, trips_table_id)
    if trips_table_exists:
        deleted = delete_existing_trip_run_rows(spark, table_id=trips_table_id, run_id=run_id)
        log_json(msg="bronze_trip_run_reset", table=trips_table_id, run_id=run_id, deleted=deleted)

    trips_rows = 0
    trips_write_mode = "append" if trips_table_exists else "create"
    trip_batch: list[dict[str, str | None]] = []

    for row in trips_iter:
        trip_batch.append(build_trip_source_row(dict(row)))
        trips_rows += 1
        if len(trip_batch) >= BRONZE_CHUNK_SIZE:
            batch_mode, trips_table_exists = write_trip_batch(
                spark,
                trip_batch,
                table_id=trips_table_id,
                create_if_missing=not trips_table_exists,
                run_id=run_id,
                source_ref=trips_source_ref,
            )
            trips_write_mode = batch_mode
            log_json(msg="materialized_batch", label="trips", rows=trips_rows, batch_rows=len(trip_batch))
            trip_batch = []

    if trip_batch:
        batch_mode, trips_table_exists = write_trip_batch(
            spark,
            trip_batch,
            table_id=trips_table_id,
            create_if_missing=not trips_table_exists,
            run_id=run_id,
            source_ref=trips_source_ref,
        )
        trips_write_mode = batch_mode
        log_json(msg="materialized_final_batch", label="trips", rows=trips_rows, batch_rows=len(trip_batch))
        trip_batch = []

    if trips_rows == 0:
        raise RuntimeError("no rows read from source 'trips'")

    taxi_zone_rows_list = [build_taxi_zone_source_row(dict(row)) for row in taxi_iter]
    taxi_zone_write_mode = write_taxi_zone_table(
        spark,
        taxi_zone_rows_list,
        table_id=taxi_zone_table_id,
        run_id=run_id,
        source_ref=taxi_zone_source_ref,
    )

    result = BronzeIngestResult(
        run_id=run_id,
        trips_table=trips_table_id,
        taxi_zone_table=taxi_zone_table_id,
        trips_rows=trips_rows,
        taxi_zone_rows=len(taxi_zone_rows_list),
        trips_source_ref=trips_source_ref,
        taxi_zone_source_ref=taxi_zone_source_ref,
        trips_write_mode=trips_write_mode,
        taxi_zone_write_mode=taxi_zone_write_mode,
    )
    log_json(msg="bronze_ingest_success", **result.__dict__)
    return result