# src/workflows/train/tasks/common.py
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from flytekit import Resources
from pyarrow import fs as pa_fs
from pyarrow import parquet as pq

DEFAULT_MLFLOW_EXPERIMENT = "trip_duration_eta_lgbm"

LABEL_COLUMN = "label_trip_duration_seconds"
TIMESTAMP_COLUMN = "as_of_ts"
IDENTIFIER_COLUMNS = ["trip_id"]

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()
TRAIN_PROFILE = (
    os.environ.get(
        "TRAIN_PROFILE",
        os.environ.get(
            "ELT_PROFILE",
            "staging" if K8S_CLUSTER in {"kind", "minikube", "docker-desktop", "local"} else "prod",
        ),
    )
    .strip()
    .lower()
)
if TRAIN_PROFILE not in {"staging", "prod"}:
    raise ValueError(f"Invalid TRAIN_PROFILE={TRAIN_PROFILE!r}; expected staging or prod")

GOLD_FEATURE_VERSION = os.environ.get("GOLD_FEATURE_VERSION", "trip_eta_lgbm_v1").strip()
GOLD_SCHEMA_VERSION = os.environ.get("GOLD_SCHEMA_VERSION", "trip_eta_frozen_matrix_v1").strip()

GOLD_TRAINING_TABLE = os.environ.get("GOLD_TRAINING_TABLE", "iceberg.gold.trip_training_matrix").strip()
GOLD_CONTRACT_TABLE = os.environ.get("GOLD_CONTRACT_TABLE", "iceberg.gold.trip_training_contracts").strip()
SOURCE_SILVER_TABLE = os.environ.get("SOURCE_SILVER_TABLE", "iceberg.silver.trip_canonical").strip()
MODEL_FAMILY = os.environ.get("MODEL_FAMILY", "lightgbm").strip()
INFERENCE_RUNTIME = os.environ.get("INFERENCE_RUNTIME", "onnxruntime").strip()

ROUTE_PAIR_BUCKETS = int(os.environ.get("ROUTE_PAIR_BUCKETS", "4096"))
ROUTE_PAIR_HASH_SALT = os.environ.get("ROUTE_PAIR_HASH_SALT", "trip_eta_route_pair_v1").strip()

SERVICE_ZONE_VALUES = tuple(
    item.strip()
    for item in os.environ.get("GOLD_SERVICE_ZONE_VALUES", "airports,boro zone,yellow zone").split(",")
    if item.strip()
)

FEATURE_COLUMNS: list[str] = [
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
]

CATEGORICAL_FEATURES: list[str] = [
    "pickup_borough_id",
    "pickup_zone_id",
    "pickup_service_zone_id",
    "dropoff_borough_id",
    "dropoff_zone_id",
    "dropoff_service_zone_id",
    "route_pair_id",
]

TIME_INTEGER_FEATURES: list[str] = [
    "pickup_hour",
    "pickup_dow",
    "pickup_month",
    "pickup_is_weekend",
]

FLOAT_FEATURES: list[str] = [
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
    "trip_count_90d_zone_hour",
]

NUMERIC_FEATURES: list[str] = [c for c in FEATURE_COLUMNS if c not in CATEGORICAL_FEATURES]

REQUIRED_COLUMNS: list[str] = [
    *IDENTIFIER_COLUMNS,
    TIMESTAMP_COLUMN,
    "as_of_date",
    "schema_version",
    "feature_version",
    *FEATURE_COLUMNS,
    LABEL_COLUMN,
]

CANONICAL_GOLD_DTYPE_MAP: dict[str, str] = {
    "trip_id": "string",
    "as_of_ts": "datetime64[ns, UTC]",
    "as_of_date": "object",
    "schema_version": "string",
    "feature_version": "string",
    "pickup_hour": "int32",
    "pickup_dow": "int32",
    "pickup_month": "int32",
    "pickup_is_weekend": "int32",
    "pickup_borough_id": "int32",
    "pickup_zone_id": "int32",
    "pickup_service_zone_id": "int32",
    "dropoff_borough_id": "int32",
    "dropoff_zone_id": "int32",
    "dropoff_service_zone_id": "int32",
    "route_pair_id": "int32",
    "avg_duration_7d_zone_hour": "float64",
    "avg_fare_30d_zone": "float64",
    "trip_count_90d_zone_hour": "float64",
    "label_trip_duration_seconds": "float64",
}

if TRAIN_PROFILE == "prod":
    LIGHT_TASK_LIMITS = Resources(
        cpu=os.environ.get("TRAIN_TASK_CPU", "1000m"),
        mem=os.environ.get("TRAIN_TASK_MEM", "1024Mi"),
    )
    LIGHT_TASK_RETRIES = int(os.environ.get("TRAIN_TASK_RETRIES", "1"))
else:
    LIGHT_TASK_LIMITS = Resources(
        cpu=os.environ.get("TRAIN_TASK_CPU", "500m"),
        mem=os.environ.get("TRAIN_TASK_MEM", "768Mi"),
    )
    LIGHT_TASK_RETRIES = int(os.environ.get("TRAIN_TASK_RETRIES", "1"))

DEFAULT_VALIDATION_FRACTION = 0.15
DEFAULT_FLAML_TIME_BUDGET_SECONDS = 300
DEFAULT_FLAML_MAX_ITER = 30
DEFAULT_SAMPLE_ROWS = 50_000
DEFAULT_RANDOM_SEED = 42
DEFAULT_NUM_BOOST_ROUND = 1500
DEFAULT_EARLY_STOPPING_ROUNDS = 100
DEFAULT_ONNX_OPSET = 17


@dataclass(frozen=True)
class SplitResult:
    train_df: pd.DataFrame
    valid_df: pd.DataFrame
    cutoff_ts: pd.Timestamp


@dataclass(frozen=True)
class ValidationReport:
    row_count: int
    null_count: int
    min_timestamp: str
    max_timestamp: str


@dataclass(frozen=True)
class DatasetArtifacts:
    model_dir: str
    manifest_path: str
    metrics_path: str
    best_config_path: str
    feature_spec_path: str
    parity_sample_path: str


def log_json(**payload: Any) -> None:
    print(json.dumps(payload, default=str, sort_keys=True))


def _env_string_map() -> dict[str, str]:
    out: dict[str, str] = {
        "TRAIN_PROFILE": TRAIN_PROFILE,
        "K8S_CLUSTER": K8S_CLUSTER,
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "ap-south-1"),
        "AWS_REGION": os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")),
    }
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "S3_ENDPOINT",
        "S3_PATH_STYLE_ACCESS",
    ):
        value = os.environ.get(key, "").strip()
        if value:
            out[key] = value
    return out


def build_task_environment() -> dict[str, str]:
    env = _env_string_map()
    env["PYTHONUNBUFFERED"] = "1"
    return env


def ensure_directory(path: str | Path) -> Path:
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def write_json(path: str | Path, payload: Any) -> str:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=_json_default)
        f.write("\n")
    return str(path)


def read_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def read_json_if_exists(path: str | Path) -> Any | None:
    p = Path(path)
    if not p.is_file():
        return None
    return read_json(p)


def artifact_sidecar_path(base_path: str | Path, suffix: str) -> Path:
    return Path(base_path).with_suffix(suffix)


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.datetime64):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _filesystem_and_path(dataset_uri: str) -> tuple[pa_fs.FileSystem, str]:
    uri = (dataset_uri or "").strip()
    if not uri:
        raise ValueError("dataset_uri must not be empty")

    filesystem, abstract_path = pa_fs.FileSystem.from_uri(uri)
    abstract_path = filesystem.normalize_path(abstract_path)
    return filesystem, abstract_path


def _is_parquet_file(path: str) -> bool:
    name = Path(path).name
    return name.endswith(".parquet") and not name.startswith((".", "_"))


def _discover_parquet_files(filesystem: pa_fs.FileSystem, base_path: str) -> list[str]:
    info = filesystem.get_file_info(base_path)
    if info.type == pa_fs.FileType.NotFound:
        raise FileNotFoundError(base_path)

    if info.type == pa_fs.FileType.File:
        return [base_path] if _is_parquet_file(base_path) else []

    selector = pa_fs.FileSelector(base_path, allow_not_found=False, recursive=True)
    discovered = filesystem.get_file_info(selector)
    files = sorted(
        fi.path
        for fi in discovered
        if fi.type == pa_fs.FileType.File and _is_parquet_file(fi.path)
    )
    return files


def _read_parquet_frame(
    filesystem: pa_fs.FileSystem,
    path: str,
    *,
    columns: list[str] | None,
) -> pd.DataFrame:
    parquet_file = pq.ParquetFile(path, filesystem=filesystem)
    table = parquet_file.read(columns=columns, use_threads=True)
    return table.to_pandas()


def _normalize_as_of_date_column(df: pd.DataFrame) -> pd.DataFrame:
    if "as_of_date" not in df.columns:
        return df
    out = df.copy()
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="raise").dt.date
    return out


def _normalize_utc_timestamp_series(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, utc=True, errors="raise")
    return ts.astype("datetime64[ns, UTC]")


def load_gold_frame(dataset_uri: str, columns: list[str] | None = None) -> pd.DataFrame:
    """
    Robust parquet loader for the gold training matrix.

    Handles both:
    - a single staged parquet file from Flyte
    - a parquet dataset root containing many files
    """
    log_json(msg="load_gold_start", dataset_uri=str(dataset_uri), output_path="/tmp/gold_canonical.parquet")

    filesystem, base_path = _filesystem_and_path(dataset_uri)

    info = filesystem.get_file_info(base_path)
    if info.type == pa_fs.FileType.NotFound:
        raise FileNotFoundError(base_path)

    if info.type == pa_fs.FileType.File:
        if not _is_parquet_file(base_path):
            raise ValueError(f"{dataset_uri} is a file, but not a parquet file")
        log_json(msg="load_gold_single_file_detected", dataset_uri=str(dataset_uri), file=base_path)
        frame = _read_parquet_frame(filesystem, base_path, columns=columns)
        frame = _normalize_as_of_date_column(frame)
        log_json(
            msg="load_gold_complete",
            rows=len(frame),
            cols=len(frame.columns),
            dataset_uri=str(dataset_uri),
        )
        return frame

    files = _discover_parquet_files(filesystem, base_path)
    if not files:
        raise RuntimeError(f"No parquet files found at {dataset_uri}")

    log_json(msg="load_gold_files_discovered", dataset_uri=str(dataset_uri), file_count=len(files))

    frames: list[pd.DataFrame] = []
    for file_path in files:
        try:
            log_json(msg="load_gold_file_start", file=file_path)
            frame = _read_parquet_frame(filesystem, file_path, columns=columns)
            frame = _normalize_as_of_date_column(frame)
            frames.append(frame)
            log_json(msg="load_gold_file_complete", file=file_path, rows=len(frame), cols=len(frame.columns))
        except Exception as exc:
            log_json(msg="load_gold_file_failed", file=file_path, error=str(exc))
            raise

    final_df = pd.concat(frames, ignore_index=True, sort=False)
    final_df = _normalize_as_of_date_column(final_df)

    log_json(
        msg="load_gold_complete",
        rows=len(final_df),
        cols=len(final_df.columns),
        dataset_uri=str(dataset_uri),
    )
    return final_df


def validate_required_columns(df: pd.DataFrame, required_columns: Iterable[str] = REQUIRED_COLUMNS) -> None:
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def dataframe_dtype_map(df: pd.DataFrame) -> dict[str, str]:
    return {column: str(dtype) for column, dtype in df.dtypes.items()}


def validate_gold_contract(
    df: pd.DataFrame,
    *,
    expected_columns: Sequence[str] = REQUIRED_COLUMNS,
    strict_dtypes: bool = True,
    expected_dtypes: dict[str, str] | None = None,
    label: str = "Gold dataset",
) -> None:
    actual_columns = list(df.columns)
    expected_columns_list = list(expected_columns)

    if actual_columns != expected_columns_list:
        missing = [c for c in expected_columns_list if c not in actual_columns]
        extra = [c for c in actual_columns if c not in expected_columns_list]
        raise ValueError(
            f"{label} does not match the frozen Gold contract. "
            f"expected={expected_columns_list}, actual={actual_columns}, missing={missing}, extra={extra}"
        )

    if strict_dtypes:
        expected_dtypes = expected_dtypes or CANONICAL_GOLD_DTYPE_MAP
        actual_dtypes = dataframe_dtype_map(df)
        mismatched = {
            col: {"expected": expected_dtypes.get(col), "actual": actual_dtypes.get(col)}
            for col in expected_columns_list
            if expected_dtypes.get(col) != actual_dtypes.get(col)
        }
        if mismatched:
            raise ValueError(f"{label} has dtype drift: {json.dumps(mismatched, sort_keys=True, default=str)}")


def coerce_contract_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["trip_id"] = out["trip_id"].astype("string")
    out[TIMESTAMP_COLUMN] = _normalize_utc_timestamp_series(out[TIMESTAMP_COLUMN])
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="raise").dt.date
    out["schema_version"] = out["schema_version"].astype("string")
    out["feature_version"] = out["feature_version"].astype("string")

    for col in TIME_INTEGER_FEATURES:
        out[col] = pd.to_numeric(out[col], errors="raise").astype("int32")

    for col in CATEGORICAL_FEATURES:
        out[col] = pd.to_numeric(out[col], errors="raise").astype("int32")

    for col in FLOAT_FEATURES:
        out[col] = pd.to_numeric(out[col], errors="raise").astype("float64")

    out[LABEL_COLUMN] = pd.to_numeric(out[LABEL_COLUMN], errors="raise").astype("float64")
    return out


def assert_no_leakage_columns(df: pd.DataFrame) -> None:
    forbidden_markers = ("post_trip", "actual_", "target_")
    leakage_like = [c for c in df.columns if any(marker in c for marker in forbidden_markers)]
    if leakage_like:
        raise ValueError(f"Potential leakage columns present in Gold: {leakage_like}")


def validate_value_contracts(df: pd.DataFrame) -> None:
    if df["trip_id"].isna().any():
        raise ValueError("trip_id contains nulls")
    if df["trip_id"].duplicated().any():
        raise ValueError("duplicate trip_id rows are not allowed")

    if df[TIMESTAMP_COLUMN].isna().any():
        raise ValueError(f"{TIMESTAMP_COLUMN} contains nulls after parsing")
    if df["as_of_date"].isna().any():
        raise ValueError("as_of_date contains nulls after parsing")
    if not pd.api.types.is_datetime64tz_dtype(df[TIMESTAMP_COLUMN]):
        raise ValueError(f"{TIMESTAMP_COLUMN} must be timezone-aware UTC")

    ts_date = pd.to_datetime(df[TIMESTAMP_COLUMN], utc=True, errors="raise").dt.date
    if not (pd.Series(df["as_of_date"]).reset_index(drop=True) == pd.Series(ts_date).reset_index(drop=True)).all():
        raise ValueError("as_of_date must match the date component of as_of_ts")

    if (df[LABEL_COLUMN] <= 0).any():
        raise ValueError(f"{LABEL_COLUMN} must be strictly positive for duration regression")

    if (df["pickup_hour"] < 0).any() or (df["pickup_hour"] > 23).any():
        raise ValueError("pickup_hour must be in [0, 23]")
    if (df["pickup_dow"] < 1).any() or (df["pickup_dow"] > 7).any():
        raise ValueError("pickup_dow must be in [1, 7]")
    if (df["pickup_month"] < 1).any() or (df["pickup_month"] > 12).any():
        raise ValueError("pickup_month must be in [1, 12]")
    if not set(df["pickup_is_weekend"].dropna().unique()).issubset({0, 1}):
        raise ValueError("pickup_is_weekend must be 0/1")

    for col in CATEGORICAL_FEATURES:
        if (df[col] < 0).any():
            raise ValueError(f"{col} contains negative category ids; reserve 0 for unknown and keep ids non-negative")

    for col in FLOAT_FEATURES:
        non_null = df[col].dropna()
        if (non_null < 0).any():
            raise ValueError(f"{col} contains negative values, which is not allowed")

    if (df["route_pair_id"] > ROUTE_PAIR_BUCKETS).any():
        raise ValueError(f"route_pair_id exceeds the configured bucket count {ROUTE_PAIR_BUCKETS}")


def split_by_time(df: pd.DataFrame, validation_fraction: float) -> SplitResult:
    if not 0.0 < validation_fraction < 0.5:
        raise ValueError("validation_fraction must be > 0 and < 0.5")

    ordered = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)
    split_idx = int(len(ordered) * (1.0 - validation_fraction))
    split_idx = min(max(split_idx, 1), len(ordered) - 1)

    train_df = ordered.iloc[:split_idx].copy()
    valid_df = ordered.iloc[split_idx:].copy()
    cutoff_ts = valid_df[TIMESTAMP_COLUMN].iloc[0]
    return SplitResult(train_df=train_df, valid_df=valid_df, cutoff_ts=cutoff_ts)


def align_model_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.loc[:, FEATURE_COLUMNS].copy()
    for col in FEATURE_COLUMNS:
        if col in CATEGORICAL_FEATURES:
            out[col] = pd.to_numeric(out[col], errors="raise").astype("int32")
        else:
            out[col] = pd.to_numeric(out[col], errors="raise").astype("float64")
    return out


def prepare_model_input_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = align_model_features(df)
    for col in CATEGORICAL_FEATURES:
        out[col] = out[col].astype("category")
    return out


def build_feature_spec() -> dict[str, Any]:
    service_zone_domain = [0, *range(1, len(SERVICE_ZONE_VALUES) + 1)]
    route_pair_domain = [0, *range(1, ROUTE_PAIR_BUCKETS + 1)]
    borough_domain = [0, 1, 2, 3, 4, 5, 6]

    def col(
        name: str,
        role: str,
        dtype: str,
        nullable: bool,
        unit: str,
        missing_policy: str,
        **extra: Any,
    ) -> dict[str, Any]:
        out: dict[str, Any] = {
            "name": name,
            "role": role,
            "dtype": dtype,
            "nullable": nullable,
            "unit": unit,
            "missing_policy": missing_policy,
        }
        out.update(extra)
        return out

    return {
        "feature_version": GOLD_FEATURE_VERSION,
        "schema_version": GOLD_SCHEMA_VERSION,
        "label_column": LABEL_COLUMN,
        "timestamp_column": TIMESTAMP_COLUMN,
        "identifier_columns": IDENTIFIER_COLUMNS,
        "feature_columns": FEATURE_COLUMNS,
        "categorical_features": CATEGORICAL_FEATURES,
        "numeric_features": NUMERIC_FEATURES,
        "feature_order_locked": True,
        "prediction_problem": "trip_duration_regression",
        "prediction_timing": "pre_trip",
        "output_columns": [
            col("trip_id", "metadata", "string", False, "identifier", "required"),
            col("as_of_ts", "metadata", "timestamp", False, "timestamp_utc", "required"),
            col("as_of_date", "metadata", "date", False, "date_utc", "required"),
            col("schema_version", "metadata", "string", False, "version_tag", "required"),
            col("feature_version", "metadata", "string", False, "version_tag", "required"),
            col("pickup_hour", "feature", "int32", False, "hour_0_23", "required"),
            col("pickup_dow", "feature", "int32", False, "dayofweek_1_sun_7_sat", "required"),
            col("pickup_month", "feature", "int32", False, "month_1_12", "required"),
            col("pickup_is_weekend", "feature", "int32", False, "boolean_0_1", "required"),
            col(
                "pickup_borough_id",
                "feature",
                "int32",
                False,
                "categorical_id",
                "0_unknown",
                categorical_feature=True,
                domain=borough_domain,
            ),
            col(
                "pickup_zone_id",
                "feature",
                "int32",
                False,
                "taxi_zone_location_id",
                "0_unknown",
                categorical_feature=True,
                domain="positive_location_ids_and_0_unknown",
            ),
            col(
                "pickup_service_zone_id",
                "feature",
                "int32",
                False,
                "categorical_id",
                "0_unknown",
                categorical_feature=True,
                domain=service_zone_domain,
            ),
            col(
                "dropoff_borough_id",
                "feature",
                "int32",
                False,
                "categorical_id",
                "0_unknown",
                categorical_feature=True,
                domain=borough_domain,
            ),
            col(
                "dropoff_zone_id",
                "feature",
                "int32",
                False,
                "taxi_zone_location_id",
                "0_unknown",
                categorical_feature=True,
                domain="positive_location_ids_and_0_unknown",
            ),
            col(
                "dropoff_service_zone_id",
                "feature",
                "int32",
                False,
                "categorical_id",
                "0_unknown",
                categorical_feature=True,
                domain=service_zone_domain,
            ),
            col(
                "route_pair_id",
                "feature",
                "int32",
                False,
                "hashed_bucket",
                "0_unknown",
                categorical_feature=True,
                domain=route_pair_domain,
                hash_algorithm="sha256",
                hash_salt=ROUTE_PAIR_HASH_SALT,
                bucket_count=ROUTE_PAIR_BUCKETS,
            ),
            col("avg_duration_7d_zone_hour", "feature", "float64", True, "seconds", "nan_on_cold_start"),
            col("avg_fare_30d_zone", "feature", "float64", True, "currency_amount", "nan_on_cold_start"),
            col("trip_count_90d_zone_hour", "feature", "float64", False, "count", "0_on_cold_start"),
            col(
                LABEL_COLUMN,
                "label",
                "float64",
                False,
                "seconds",
                "drop_row_if_null",
                target_metric="mae",
            ),
        ],
    }


def build_encoding_spec() -> dict[str, Any]:
    service_zone_values = list(SERVICE_ZONE_VALUES)
    service_zone_lookup = {idx + 1: value for idx, value in enumerate(service_zone_values)}

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
            "values": service_zone_lookup,
        },
        "dropoff_service_zone_id": {
            "type": "versioned_lookup",
            "unknown": 0,
            "source": "silver.dropoff_service_zone",
            "values": service_zone_lookup,
        },
        "route_pair_id": {
            "type": "hashed_bucket",
            "unknown": 0,
            "hash_algorithm": "sha256",
            "hash_salt": ROUTE_PAIR_HASH_SALT,
            "bucket_count": ROUTE_PAIR_BUCKETS,
        },
    }


def build_aggregate_spec(source_silver_table: str = SOURCE_SILVER_TABLE) -> list[dict[str, Any]]:
    return [
        {
            "name": "avg_duration_7d_zone_hour",
            "source_table": source_silver_table,
            "source_column": "label_trip_duration_seconds",
            "filter_predicate": "pickup_ts in [as_of_ts - 7d, as_of_ts)",
            "window_length": "7d",
            "grouping_keys": ["pickup_zone_id", "pickup_hour"],
            "minimum_history_rule": "no prior rows => NaN",
            "null_fallback": "NaN",
        },
        {
            "name": "avg_fare_30d_zone",
            "source_table": source_silver_table,
            "source_column": "fare_amount",
            "filter_predicate": "pickup_ts in [as_of_ts - 30d, as_of_ts)",
            "window_length": "30d",
            "grouping_keys": ["pickup_zone_id"],
            "minimum_history_rule": "no prior rows => NaN",
            "null_fallback": "NaN",
        },
        {
            "name": "trip_count_90d_zone_hour",
            "source_table": source_silver_table,
            "source_column": "count(*)",
            "filter_predicate": "pickup_ts in [as_of_ts - 90d, as_of_ts)",
            "window_length": "90d",
            "grouping_keys": ["pickup_zone_id", "pickup_hour"],
            "minimum_history_rule": "no prior rows => 0",
            "null_fallback": "0",
        },
    ]


def build_label_spec(source_silver_table: str = SOURCE_SILVER_TABLE) -> dict[str, Any]:
    return {
        "name": LABEL_COLUMN,
        "dtype": "float64",
        "unit": "seconds",
        "source_table": source_silver_table,
        "source_column": "trip_duration_seconds",
        "null_policy": "drop_row_if_null",
        "primary_metric": "mae",
        "secondary_metric": "rmse",
        "target_family": "eta",
    }


def build_schema_hash(feature_spec: dict[str, Any] | None = None) -> str:
    spec = feature_spec or build_feature_spec()
    canonical = json.dumps(spec, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_contract_summary(
    *,
    dataset_uri: str,
    row_count: int,
    dataframe: pd.DataFrame | None = None,
    gold_table: str = GOLD_TRAINING_TABLE,
    source_silver_table: str = SOURCE_SILVER_TABLE,
    run_id: str | None = None,
    cutoff_ts: Any | None = None,
    created_ts: datetime | None = None,
    model_family: str = MODEL_FAMILY,
    inference_runtime: str = INFERENCE_RUNTIME,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    feature_spec = build_feature_spec()
    encoding_spec = build_encoding_spec()
    aggregate_spec = build_aggregate_spec(source_silver_table=source_silver_table)
    label_spec = build_label_spec(source_silver_table=source_silver_table)

    summary: dict[str, Any] = {
        "run_id": run_id,
        "dataset_uri": dataset_uri,
        "row_count": int(row_count),
        "gold_table": gold_table,
        "source_silver_table": source_silver_table,
        "feature_version": GOLD_FEATURE_VERSION,
        "schema_version": GOLD_SCHEMA_VERSION,
        "schema_hash": build_schema_hash(feature_spec),
        "model_family": model_family,
        "inference_runtime": inference_runtime,
        "output_columns_json": json.dumps(
            [row["name"] for row in feature_spec["output_columns"]],
            separators=(",", ":"),
        ),
        "feature_spec_json": json.dumps(feature_spec, sort_keys=True, separators=(",", ":")),
        "encoding_spec_json": json.dumps(encoding_spec, sort_keys=True, separators=(",", ":")),
        "aggregate_spec_json": json.dumps(aggregate_spec, sort_keys=True, separators=(",", ":")),
        "label_spec_json": json.dumps(label_spec, sort_keys=True, separators=(",", ":")),
        "created_ts": created_ts or datetime.now(UTC),
    }
    if cutoff_ts is not None:
        summary["cutoff_ts"] = cutoff_ts
    if dataframe is not None:
        summary["pandas_dtypes"] = dataframe_dtype_map(dataframe)
    if extra:
        summary.update(extra)
    return summary


def build_training_frame(df: pd.DataFrame) -> pd.DataFrame:
    cols = [*FEATURE_COLUMNS, LABEL_COLUMN]
    return df[cols].copy()


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    return align_model_features(df)


def compute_regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    r2 = r2_score(y_true, y_pred)
    return {"mae": float(mae), "rmse": float(rmse), "r2": float(r2)}


def sample_frame(df: pd.DataFrame, max_rows: int, seed: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df.copy()
    return df.sample(n=max_rows, random_state=seed).copy()