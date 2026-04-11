from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import sys
import time
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import boto3
import lightgbm as lgb
import numpy as np
import pandas as pd
import polars as pl
from pyiceberg.catalog import load_catalog
from sklearn.metrics import mean_absolute_error, mean_squared_error, median_absolute_error

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").strip().upper()


def configure_logging() -> None:
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format="%(asctime)sZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        force=True,
    )
    logging.Formatter.converter = time.gmtime


configure_logging()
logger = logging.getLogger(__name__)


@contextmanager
def log_step(step_name: str):
    started = time.perf_counter()
    logger.info("%s started", step_name)
    try:
        yield
    except Exception:
        elapsed = time.perf_counter() - started
        logger.exception("%s failed after %.2fs", step_name, elapsed)
        raise
    else:
        elapsed = time.perf_counter() - started
        logger.info("%s completed in %.2fs", step_name, elapsed)


# ---------------------------------------------------------------------
# Core ELT / training contract
# ---------------------------------------------------------------------

TABLE_IDENTIFIER_TUPLE = ("gold", "trip_training_matrix")
TABLE_IDENTIFIER = "gold.trip_training_matrix"

CONTRACT_TABLE_IDENTIFIER_TUPLE = ("gold", "trip_training_contracts")
CONTRACT_TABLE_IDENTIFIER = "gold.trip_training_contracts"

EXPECTED_SCHEMA_VERSION = "trip_eta_frozen_matrix_v1"
EXPECTED_FEATURE_VERSION = "trip_eta_lgbm_v1"
MODEL_FAMILY = "lightgbm"
INFERENCE_RUNTIME = "onnxruntime"
PREPROCESSING_VERSION = "matrix_identity_v1"
TARGET_TRANSFORM = "log1p"

LABEL_COLUMN = "label_trip_duration_seconds"
PREDICTION_COLUMN = "prediction_seconds"

FIXED_TEST_START_DATE = date(2025, 1, 7)
INNER_VALIDATION_FRACTION = 0.10
LABEL_CAP_QUANTILE = 0.99

MAX_PREDICTION_SECONDS = 24.0 * 3600.0
MAX_BOOST_ROUNDS = 20_000
EARLY_STOPPING_ROUNDS = 150
DEFAULT_TARGET_OPSET = 17

# Frozen matrix contract from ELT.
MATRIX_FEATURE_COLUMNS = [
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

REQUEST_FEATURE_COLUMNS = MATRIX_FEATURE_COLUMNS
ENGINEERED_FEATURE_COLUMNS = MATRIX_FEATURE_COLUMNS
MODEL_INPUT_COLUMNS = MATRIX_FEATURE_COLUMNS
MODEL_FEATURE_COLUMNS = MATRIX_FEATURE_COLUMNS

NUMERIC_FEATURE_COLUMNS = [
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
    "trip_count_90d_zone_hour",
]

CATEGORICAL_COLUMNS = [
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
]

OUTPUT_COLUMNS = [
    "trip_id",
    "pickup_ts",
    "as_of_ts",
    "as_of_date",
    "schema_version",
    "feature_version",
    *MATRIX_FEATURE_COLUMNS,
    LABEL_COLUMN,
]

EXPECTED_COLUMNS = OUTPUT_COLUMNS

CONTRACT_COLUMNS = [
    "run_id",
    "feature_version",
    "schema_version",
    "schema_hash",
    "model_family",
    "inference_runtime",
    "gold_table",
    "source_silver_table",
    "source_silver_snapshot_id",
    "training_row_count",
    "output_columns_json",
    "feature_spec_json",
    "encoding_spec_json",
    "aggregate_spec_json",
    "label_spec_json",
    "created_ts",
]


def _stable_json_dumps(obj: object) -> str:
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def ordered_columns_hash(columns: Sequence[str]) -> str:
    return sha256_text("\n".join(columns))


def write_json(path: Path, payload: JsonValue) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        text = payload
    else:
        text = _stable_json_dumps(payload)
    path.write_text(text, encoding="utf-8")


def _validate_sha256_hex(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field_name} must be a non-empty sha256 hex string")

    normalized = value.strip().lower()
    if len(normalized) != 64:
        raise RuntimeError(f"{field_name} must be a 64-character sha256 hex string")

    if any(ch not in "0123456789abcdef" for ch in normalized):
        raise RuntimeError(f"{field_name} must contain only lowercase hex characters")

    return normalized


def _require_nonempty_str(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"{field_name} must be an integer")
    try:
        return int(value)
    except Exception as exc:
        raise RuntimeError(f"{field_name} must be an integer") from exc


def _require_sequence_of_str(values: Sequence[str], field_name: str) -> tuple[str, ...]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for idx, item in enumerate(values):
        if not isinstance(item, str) or not item.strip():
            raise RuntimeError(f"{field_name}[{idx}] must be a non-empty string")
        value = item.strip()
        if value in seen:
            raise RuntimeError(f"{field_name} contains duplicate value: {value}")
        seen.add(value)
        cleaned.append(value)
    if not cleaned:
        raise RuntimeError(f"{field_name} must not be empty")
    return tuple(cleaned)


def _timestamp_to_iso8601(value: object) -> str:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise RuntimeError("created_ts must be a valid timestamp")
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def _read_iceberg_table_as_dataframe(
    table,
    columns: Sequence[str],
) -> pd.DataFrame:
    lf = pl.scan_iceberg(table).select(list(columns))
    pdf = lf.collect(engine="streaming").to_pandas()
    pdf = pdf.reindex(columns=list(columns)).copy()
    return pdf


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got: {s3_uri!r}")
    bucket = parsed.netloc.strip()
    key = parsed.path.lstrip("/").strip()
    if not bucket or not key:
        raise ValueError(f"Malformed S3 URI: {s3_uri!r}")
    return bucket, key


def require_static_aws_credentials_if_needed(*, use_iam: bool) -> None:
    if use_iam:
        return

    missing: list[str] = []
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        if not os.environ.get(key, "").strip():
            missing.append(key)

    if missing:
        raise RuntimeError(
            "USE_IAM=false requires AWS credentials in the runtime environment: "
            + ", ".join(missing)
        )


def s3_client(*, use_iam: bool):
    require_static_aws_credentials_if_needed(use_iam=use_iam)
    return boto3.client("s3")


def upload_file_to_s3(local_path: Path, s3_uri: str, *, use_iam: bool) -> str:
    bucket, key = parse_s3_uri(s3_uri)
    logger.info("uploading %s -> s3://%s/%s", local_path, bucket, key)
    s3_client(use_iam=use_iam).upload_file(str(local_path), bucket, key)
    return s3_uri


def download_file_from_s3(s3_uri: str, local_path: Path, *, use_iam: bool) -> Path:
    bucket, key = parse_s3_uri(s3_uri)
    logger.info("downloading s3://%s/%s -> %s", bucket, key, local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3_client(use_iam=use_iam).download_file(bucket, key, str(local_path))
    return local_path


@dataclass(frozen=True)
class ELTContract:
    run_id: str
    feature_version: str
    schema_version: str
    schema_hash: str
    model_family: str
    inference_runtime: str
    gold_table: str
    source_silver_table: str
    source_silver_snapshot_id: str
    training_row_count: int
    output_columns_json: str
    feature_spec_json: str
    encoding_spec_json: str
    aggregate_spec_json: str
    label_spec_json: str
    created_ts: str

    def __post_init__(self) -> None:
        _require_nonempty_str(self.run_id, "run_id")
        _require_nonempty_str(self.feature_version, "feature_version")
        _require_nonempty_str(self.schema_version, "schema_version")
        _validate_sha256_hex(self.schema_hash, "schema_hash")
        _require_nonempty_str(self.model_family, "model_family")
        _require_nonempty_str(self.inference_runtime, "inference_runtime")
        _require_nonempty_str(self.gold_table, "gold_table")
        _require_nonempty_str(self.source_silver_table, "source_silver_table")
        _require_nonempty_str(self.source_silver_snapshot_id, "source_silver_snapshot_id")
        if self.training_row_count <= 0:
            raise RuntimeError("training_row_count must be > 0")

        output_columns = json.loads(self.output_columns_json)
        if not isinstance(output_columns, list) or not output_columns:
            raise RuntimeError("output_columns_json must be a non-empty JSON array")

        json.loads(self.feature_spec_json)
        json.loads(self.encoding_spec_json)
        json.loads(self.aggregate_spec_json)
        json.loads(self.label_spec_json)

    def as_dict(self) -> dict[str, JsonValue]:
        return asdict(self)

    def to_json(self) -> str:
        return _stable_json_dumps(asdict(self))


@dataclass(frozen=True)
class SnapshotLineage:
    table_uuid: str
    table_location: str
    current_schema_id: int
    current_snapshot_id: int | None
    metadata_location: str
    format_version: int

    def as_dict(self) -> dict[str, JsonValue]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateConfig:
    name: str
    learning_rate: float
    num_leaves: int
    max_depth: int
    min_child_samples: int
    feature_fraction: float
    subsample: float
    reg_alpha: float
    reg_lambda: float

    def as_dict(self) -> dict[str, JsonValue]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateReport:
    name: str
    params: CandidateConfig
    best_iteration: int
    metrics: dict[str, float]
    score: float

    def as_dict(self) -> dict[str, JsonValue]:
        return asdict(self)


@dataclass(frozen=True)
class BundleArtifactPlan:
    artifact_root_s3_uri: str
    model_s3_uri: str
    schema_s3_uri: str
    metadata_s3_uri: str
    manifest_s3_uri: str

    def as_dict(self) -> dict[str, JsonValue]:
        return asdict(self)


@dataclass(frozen=True)
class SplitFrames:
    train_eval: pd.DataFrame
    test: pd.DataFrame
    train_eval_cutoff: str
    test_start_date: str


@dataclass(frozen=True)
class TrainingResult:
    table_identifier: str
    schema_version: str
    feature_version: str
    preprocessing_version: str
    elt_contract: ELTContract
    lineage: SnapshotLineage
    category_levels: dict[str, list[int]]
    selected_candidate: CandidateConfig
    candidate_reports: list[CandidateReport]
    search_best_metrics: dict[str, float]
    inner_metrics: dict[str, float]
    holdout_metrics: dict[str, float]
    holdout_baseline_metrics: dict[str, float]
    label_cap_seconds: float
    train_label_p50_seconds: float
    best_iteration_inner: int
    final_num_boost_round: int
    train_rows: int
    test_rows: int
    request_feature_columns: list[str]
    engineered_feature_columns: list[str]
    model_input_columns: list[str]
    model_feature_columns: list[str]
    model_name: str
    model_version: str
    bundle_contract: str
    bundle_metadata: str
    artifact_plan: BundleArtifactPlan

    def to_json(self) -> str:
        return _stable_json_dumps(asdict(self))

    def as_dict(self) -> dict[str, JsonValue]:
        return asdict(self)


def read_contract_table_as_dataframe(table) -> pd.DataFrame:
    pdf = _read_iceberg_table_as_dataframe(table, CONTRACT_COLUMNS)
    if "created_ts" in pdf.columns:
        pdf["created_ts"] = pd.to_datetime(pdf["created_ts"], utc=True, errors="raise")
    return pdf


def validate_contract_dataframe(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("Contract table is empty.")

    if list(df.columns) != CONTRACT_COLUMNS:
        raise ValueError(
            "Contract schema mismatch.\n"
            f"Expected: {CONTRACT_COLUMNS}\n"
            f"Actual:   {list(df.columns)}"
        )

    for col_name in CONTRACT_COLUMNS:
        if df[col_name].isna().any():
            raise ValueError(f"{col_name} contains nulls but is required to be non-null.")

    _validate_sha256_hex(df["schema_hash"].iloc[0], "schema_hash")

    output_columns = json.loads(df["output_columns_json"].iloc[0])
    if not isinstance(output_columns, list) or output_columns != EXPECTED_COLUMNS:
        raise ValueError("output_columns_json does not match the expected frozen matrix columns.")

    json.loads(df["feature_spec_json"].iloc[0])
    json.loads(df["encoding_spec_json"].iloc[0])
    json.loads(df["aggregate_spec_json"].iloc[0])
    json.loads(df["label_spec_json"].iloc[0])

    if int(df["training_row_count"].iloc[0]) <= 0:
        raise ValueError("training_row_count must be > 0")


def _contract_row_to_dataclass(row: pd.Series) -> ELTContract:
    return ELTContract(
        run_id=_require_nonempty_str(row["run_id"], "run_id"),
        feature_version=_require_nonempty_str(row["feature_version"], "feature_version"),
        schema_version=_require_nonempty_str(row["schema_version"], "schema_version"),
        schema_hash=_validate_sha256_hex(row["schema_hash"], "schema_hash"),
        model_family=_require_nonempty_str(row["model_family"], "model_family"),
        inference_runtime=_require_nonempty_str(row["inference_runtime"], "inference_runtime"),
        gold_table=_require_nonempty_str(row["gold_table"], "gold_table"),
        source_silver_table=_require_nonempty_str(row["source_silver_table"], "source_silver_table"),
        source_silver_snapshot_id=_require_nonempty_str(
            row["source_silver_snapshot_id"], "source_silver_snapshot_id"
        ),
        training_row_count=_require_int(row["training_row_count"], "training_row_count"),
        output_columns_json=_require_nonempty_str(row["output_columns_json"], "output_columns_json"),
        feature_spec_json=_require_nonempty_str(row["feature_spec_json"], "feature_spec_json"),
        encoding_spec_json=_require_nonempty_str(row["encoding_spec_json"], "encoding_spec_json"),
        aggregate_spec_json=_require_nonempty_str(row["aggregate_spec_json"], "aggregate_spec_json"),
        label_spec_json=_require_nonempty_str(row["label_spec_json"], "label_spec_json"),
        created_ts=_timestamp_to_iso8601(row["created_ts"]),
    )


def load_elt_contract(
    catalog_name: str,
    iceberg_rest_uri: str,
    iceberg_warehouse: str,
) -> ELTContract:
    table = load_iceberg_table(
        catalog_name,
        iceberg_rest_uri,
        iceberg_warehouse,
        CONTRACT_TABLE_IDENTIFIER_TUPLE,
    )
    df = read_contract_table_as_dataframe(table)
    validate_contract_dataframe(df)

    if "created_ts" in df.columns:
        df = df.sort_values("created_ts")

    row = df.iloc[-1]
    return _contract_row_to_dataclass(row)


def build_bundle_contract(
    *,
    output_names: Sequence[str],
    input_name: str = "input",
    feature_order: Sequence[str] = REQUEST_FEATURE_COLUMNS,
    request_feature_order: Sequence[str] = REQUEST_FEATURE_COLUMNS,
    engineered_feature_order: Sequence[str] = ENGINEERED_FEATURE_COLUMNS,
    schema_version: str = EXPECTED_SCHEMA_VERSION,
    feature_version: str = EXPECTED_FEATURE_VERSION,
    preprocessing_version: str = PREPROCESSING_VERSION,
    target_transform: str = TARGET_TRANSFORM,
    allow_extra_features: bool = False,
) -> str:
    feature_order_tuple = _require_sequence_of_str(feature_order, "feature_order")
    request_feature_order_tuple = _require_sequence_of_str(
        request_feature_order, "request_feature_order"
    )
    engineered_feature_order_tuple = _require_sequence_of_str(
        engineered_feature_order, "engineered_feature_order"
    )
    output_names_tuple = _require_sequence_of_str(output_names, "output_names")

    if feature_order_tuple != request_feature_order_tuple:
        raise ValueError("feature_order and request_feature_order must match exactly")
    if feature_order_tuple != engineered_feature_order_tuple:
        raise ValueError("feature_order and engineered_feature_order must match exactly")

    payload = {
        "schema_version": schema_version,
        "feature_version": feature_version,
        "preprocessing_version": preprocessing_version,
        "target_transform": target_transform,
        "input_name": input_name,
        "feature_order": list(feature_order_tuple),
        "request_feature_order": list(request_feature_order_tuple),
        "engineered_feature_order": list(engineered_feature_order_tuple),
        "output_names": list(output_names_tuple),
        "allow_extra_features": bool(allow_extra_features),
        "feature_order_hash": ordered_columns_hash(feature_order_tuple),
        "request_feature_order_hash": ordered_columns_hash(request_feature_order_tuple),
        "engineered_feature_order_hash": ordered_columns_hash(engineered_feature_order_tuple),
    }
    return _stable_json_dumps(payload)


def build_bundle_metadata(
    *,
    elt_contract: ELTContract,
    lineage: SnapshotLineage,
    artifact_plan: BundleArtifactPlan,
    model_name: str,
    model_version: str,
    bundle_contract_json: str,
    category_levels: dict[str, list[int]],
    selected_candidate: CandidateConfig,
    candidate_reports: Sequence[CandidateReport],
    search_best_metrics: dict[str, float],
    inner_metrics: dict[str, float],
    holdout_metrics: dict[str, float],
    holdout_baseline_metrics: dict[str, float],
    label_cap_seconds: float,
    train_label_p50_seconds: float,
    best_iteration_inner: int,
    final_num_boost_round: int,
    train_rows: int,
    test_rows: int,
) -> str:
    bundle_contract = json.loads(bundle_contract_json)

    payload = {
        "run_id": elt_contract.run_id,
        "feature_version": elt_contract.feature_version,
        "schema_version": elt_contract.schema_version,
        "schema_hash": elt_contract.schema_hash,
        "model_family": elt_contract.model_family,
        "inference_runtime": elt_contract.inference_runtime,
        "gold_table": elt_contract.gold_table,
        "source_silver_table": elt_contract.source_silver_table,
        "source_silver_snapshot_id": elt_contract.source_silver_snapshot_id,
        "training_row_count": elt_contract.training_row_count,
        "output_columns_json": elt_contract.output_columns_json,
        "feature_spec_json": elt_contract.feature_spec_json,
        "encoding_spec_json": elt_contract.encoding_spec_json,
        "aggregate_spec_json": elt_contract.aggregate_spec_json,
        "label_spec_json": elt_contract.label_spec_json,
        "created_ts": elt_contract.created_ts,
        "lineage": lineage.as_dict(),
        "artifact_plan": artifact_plan.as_dict(),
        "model_name": model_name,
        "model_version": model_version,
        "preprocessing_version": bundle_contract["preprocessing_version"],
        "request_feature_order": list(bundle_contract["request_feature_order"]),
        "engineered_feature_order": list(bundle_contract["engineered_feature_order"]),
        "request_feature_order_hash": bundle_contract["request_feature_order_hash"],
        "engineered_feature_order_hash": bundle_contract["engineered_feature_order_hash"],
        "category_levels": {key: [int(v) for v in values] for key, values in category_levels.items()},
        "selected_candidate": selected_candidate.as_dict(),
        "candidate_reports": [report.as_dict() for report in candidate_reports],
        "search_best_metrics": dict(search_best_metrics),
        "inner_metrics": dict(inner_metrics),
        "holdout_metrics": dict(holdout_metrics),
        "holdout_baseline_metrics": dict(holdout_baseline_metrics),
        "label_cap_seconds": float(label_cap_seconds),
        "train_label_p50_seconds": float(train_label_p50_seconds),
        "best_iteration_inner": int(best_iteration_inner),
        "final_num_boost_round": int(final_num_boost_round),
        "train_rows": int(train_rows),
        "test_rows": int(test_rows),
    }
    return _stable_json_dumps(payload)


def load_iceberg_table(
    catalog_name: str,
    iceberg_rest_uri: str,
    iceberg_warehouse: str,
    table_identifier: tuple[str, str] = TABLE_IDENTIFIER_TUPLE,
):
    catalog = load_catalog(
        catalog_name,
        type="rest",
        uri=iceberg_rest_uri,
        warehouse=iceberg_warehouse,
    )
    return catalog.load_table(table_identifier)


def table_snapshot_lineage(table) -> SnapshotLineage:
    snapshot = table.current_snapshot()
    metadata = table.metadata
    return SnapshotLineage(
        table_uuid=str(getattr(metadata, "table_uuid", "")),
        table_location=str(getattr(metadata, "location", "")),
        current_schema_id=int(getattr(metadata, "current_schema_id", -1)),
        current_snapshot_id=int(snapshot.snapshot_id) if snapshot is not None else None,
        metadata_location=str(getattr(metadata, "metadata_location", "")),
        format_version=int(getattr(metadata, "format_version", -1)),
    )


def read_table_as_dataframe(table) -> pd.DataFrame:
    pdf = _read_iceberg_table_as_dataframe(table, EXPECTED_COLUMNS)
    pdf["as_of_ts"] = pd.to_datetime(pdf["as_of_ts"], utc=True, errors="raise")
    pdf["as_of_date"] = pd.to_datetime(pdf["as_of_date"], errors="raise").dt.date
    return pdf


def validate_raw_dataframe(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("Gold dataset is empty.")

    if list(df.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            "Gold schema mismatch.\n"
            f"Expected: {EXPECTED_COLUMNS}\n"
            f"Actual:   {list(df.columns)}"
        )

    if df["schema_version"].nunique(dropna=False) != 1:
        raise ValueError("schema_version is not single-valued.")
    if df["feature_version"].nunique(dropna=False) != 1:
        raise ValueError("feature_version is not single-valued.")
    if str(df["schema_version"].iloc[0]) != EXPECTED_SCHEMA_VERSION:
        raise ValueError(
            f"schema_version must be {EXPECTED_SCHEMA_VERSION!r}, got {df['schema_version'].iloc[0]!r}"
        )
    if str(df["feature_version"].iloc[0]) != EXPECTED_FEATURE_VERSION:
        raise ValueError(
            f"feature_version must be {EXPECTED_FEATURE_VERSION!r}, got {df['feature_version'].iloc[0]!r}"
        )

    as_of_ts_dates = pd.to_datetime(df["as_of_ts"], utc=True, errors="raise").dt.date
    as_of_date_dates = pd.to_datetime(df["as_of_date"], errors="raise").dt.date
    if not (as_of_ts_dates == as_of_date_dates).all():
        raise ValueError("as_of_date does not match as_of_ts.date for all rows.")

    for col_name in [
        "trip_id",
        "pickup_ts",
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
        LABEL_COLUMN,
    ]:
        if df[col_name].isna().any():
            raise ValueError(f"{col_name} contains nulls but is required to be non-null.")

    for col_name in [
        "avg_duration_7d_zone_hour",
        "avg_fare_30d_zone",
    ]:
        if col_name not in df.columns:
            raise ValueError(f"{col_name} is missing from the dataset.")

    if not pd.to_numeric(df["pickup_hour"], errors="coerce").between(0, 23).all():
        raise ValueError("pickup_hour must be in [0, 23].")
    if not pd.to_numeric(df["pickup_dow"], errors="coerce").between(1, 7).all():
        raise ValueError("pickup_dow must be in [1, 7].")
    if not pd.to_numeric(df["pickup_month"], errors="coerce").between(1, 12).all():
        raise ValueError("pickup_month must be in [1, 12].")
    if not pd.to_numeric(df["pickup_is_weekend"], errors="coerce").isin([0, 1]).all():
        raise ValueError("pickup_is_weekend must be 0 or 1.")
    if not (pd.to_numeric(df[LABEL_COLUMN], errors="coerce") > 0).all():
        raise ValueError("label_trip_duration_seconds must be strictly positive.")

    for col_name in [
        "pickup_borough_id",
        "pickup_zone_id",
        "pickup_service_zone_id",
        "dropoff_borough_id",
        "dropoff_zone_id",
        "dropoff_service_zone_id",
        "route_pair_id",
        "trip_count_90d_zone_hour",
    ]:
        if (pd.to_numeric(df[col_name], errors="coerce") < 0).any():
            raise ValueError(f"{col_name} contains negative values.")

    if (pd.to_numeric(df["avg_duration_7d_zone_hour"], errors="coerce") < 0).dropna().any():
        raise ValueError("avg_duration_7d_zone_hour contains negative values.")
    if (pd.to_numeric(df["avg_fare_30d_zone"], errors="coerce") < 0).dropna().any():
        raise ValueError("avg_fare_30d_zone contains negative values.")


def load_elt_contract_table(
    catalog_name: str,
    iceberg_rest_uri: str,
    iceberg_warehouse: str,
):
    return load_iceberg_table(
        catalog_name,
        iceberg_rest_uri,
        iceberg_warehouse,
        CONTRACT_TABLE_IDENTIFIER_TUPLE,
    )


def split_train_test_by_date(
    df: pd.DataFrame,
    test_start_date: date = FIXED_TEST_START_DATE,
) -> SplitFrames:
    working = df.copy()
    working["as_of_date"] = pd.to_datetime(working["as_of_date"], errors="raise").dt.date

    train_eval_df = working.loc[working["as_of_date"] < test_start_date].copy()
    test_df = working.loc[working["as_of_date"] >= test_start_date].copy()

    if train_eval_df.empty:
        raise ValueError("Training frame is empty after fixed date split.")
    if test_df.empty:
        raise ValueError("Test frame is empty after fixed date split.")

    return SplitFrames(
        train_eval=train_eval_df,
        test=test_df,
        train_eval_cutoff=str(train_eval_df["as_of_date"].max()),
        test_start_date=str(test_start_date),
    )


def split_by_date_fraction(
    df: pd.DataFrame,
    keep_fraction: float,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    if not (0.0 < keep_fraction < 1.0):
        raise ValueError("keep_fraction must be between 0 and 1.")

    working = df.copy()
    working["as_of_date"] = pd.to_datetime(working["as_of_date"], errors="raise").dt.date

    date_counts = (
        working.groupby("as_of_date", sort=True)
        .size()
        .reset_index(name="row_count")
        .sort_values("as_of_date")
        .reset_index(drop=True)
    )
    if len(date_counts) < 2:
        raise ValueError("Not enough distinct dates to split the dataset.")

    total_rows = int(date_counts["row_count"].sum())
    cutoff_rows = max(1, min(total_rows - 1, round(total_rows * keep_fraction)))
    cumulative = date_counts["row_count"].cumsum()
    cutoff_idx = int(cumulative.searchsorted(cutoff_rows, side="left"))
    cutoff_idx = max(0, min(cutoff_idx, len(date_counts) - 2))
    cutoff_date = date_counts.loc[cutoff_idx, "as_of_date"]

    left = working.loc[working["as_of_date"] <= cutoff_date].copy()
    right = working.loc[working["as_of_date"] > cutoff_date].copy()

    if left.empty or right.empty:
        raise ValueError("Date split produced an empty side.")
    return left, right, str(cutoff_date)


def evenly_spaced_sample(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df.copy()
    indices = np.linspace(0, len(df) - 1, num=max_rows, dtype=int)
    return df.iloc[indices].copy()


def build_category_levels(
    df: pd.DataFrame,
    columns: list[str] = CATEGORICAL_COLUMNS,
) -> dict[str, list[int]]:
    levels: dict[str, list[int]] = {}
    for col_name in columns:
        values = pd.to_numeric(df[col_name], errors="coerce").dropna().astype("int64").unique()
        values = np.asarray(np.sort(values), dtype="int64")
        levels[col_name] = [int(v) for v in values.tolist()]
    return levels


def encode_categorical_series(series: pd.Series, levels: list[int]) -> pd.Series:
    mapping = {int(v): int(i + 1) for i, v in enumerate(levels)}
    values = pd.to_numeric(series, errors="coerce").fillna(0).astype("int64")
    encoded = values.map(mapping).fillna(0).astype("int32")
    return encoded


def prepare_model_features(
    raw_df: pd.DataFrame,
    category_levels: dict[str, list[int]] | None = None,
) -> pd.DataFrame:
    _ = category_levels
    missing = [col for col in MATRIX_FEATURE_COLUMNS if col not in raw_df.columns]
    if missing:
        raise ValueError(f"Missing model input columns: {missing}")

    out = raw_df[MATRIX_FEATURE_COLUMNS].copy()

    for col_name in CATEGORICAL_COLUMNS:
        values = pd.to_numeric(out[col_name], errors="raise")
        if values.isna().any():
            raise ValueError(f"{col_name} contains nulls")
        out[col_name] = values.astype("int32")

    for col_name in NUMERIC_FEATURE_COLUMNS:
        values = pd.to_numeric(out[col_name], errors="raise").astype("float32")
        if np.isinf(values.to_numpy(dtype="float32", copy=False)).any():
            raise ValueError(f"{col_name} contains infinite values")
        out[col_name] = values

    return out[MATRIX_FEATURE_COLUMNS]


def prepare_training_frame(
    raw_df: pd.DataFrame,
    category_levels: dict[str, list[int]] | None = None,
) -> pd.DataFrame:
    features = prepare_model_features(raw_df, category_levels=category_levels)
    label = pd.to_numeric(raw_df[LABEL_COLUMN], errors="raise").astype("float32")
    frame = features.copy()
    frame[LABEL_COLUMN] = label.to_numpy(dtype="float32")
    return frame[[*MATRIX_FEATURE_COLUMNS, LABEL_COLUMN]]


def to_log_target(y_seconds: np.ndarray, cap_seconds: float) -> np.ndarray:
    clipped = np.clip(np.asarray(y_seconds, dtype="float32"), 0.0, cap_seconds)
    return np.log1p(clipped).astype("float32")


def from_log_target(y_log: np.ndarray) -> np.ndarray:
    return np.expm1(np.asarray(y_log, dtype="float32")).astype("float32")


def clip_seconds(values: np.ndarray | pd.Series | list[float], cap_seconds: float) -> np.ndarray:
    return np.clip(np.asarray(values, dtype="float32"), 0.0, cap_seconds).astype("float32")


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    y_true = np.asarray(y_true, dtype="float64")
    y_pred = np.asarray(y_pred, dtype="float64")
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(math.sqrt(mean_squared_error(y_true, y_pred))),
        "medae": float(median_absolute_error(y_true, y_pred)),
    }


def prediction_digest(df: pd.DataFrame) -> str:
    frame = df.copy()
    if "trip_id" in frame.columns and "as_of_ts" in frame.columns:
        frame = frame[["trip_id", "as_of_ts"]].copy()
    payload = frame.astype(str).to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def feature_digest(df: pd.DataFrame, columns: list[str]) -> str:
    payload = df[columns].astype(str).to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def make_base_params(num_threads: int) -> dict[str, object]:
    params = dict(BASE_LGBM_PARAMS)
    params["n_jobs"] = int(num_threads)
    return params


def predict_seconds_from_model(
    model: lgb.LGBMRegressor,
    raw_df: pd.DataFrame,
    category_levels: dict[str, list[int]] | None,
    num_iteration: int | None,
) -> np.ndarray:
    features = prepare_model_features(raw_df, category_levels=category_levels)
    if num_iteration is None:
        preds_log = model.predict(features)
    else:
        preds_log = model.predict(features, num_iteration=num_iteration)
    preds_seconds = from_log_target(preds_log)
    return clip_seconds(preds_seconds, MAX_PREDICTION_SECONDS)


def fit_lgbm_candidate(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    candidate: CandidateConfig,
    label_cap_seconds: float,
    num_threads: int,
    category_levels: dict[str, list[int]] | None,
    max_boost_rounds: int,
) -> tuple[lgb.LGBMRegressor, int, dict[str, float]]:
    params = make_base_params(num_threads)
    params.update(
        {
            "learning_rate": float(candidate.learning_rate),
            "num_leaves": int(candidate.num_leaves),
            "max_depth": int(candidate.max_depth),
            "min_child_samples": int(candidate.min_child_samples),
            "subsample": float(candidate.subsample),
            "feature_fraction": float(candidate.feature_fraction),
            "reg_alpha": float(candidate.reg_alpha),
            "reg_lambda": float(candidate.reg_lambda),
        }
    )

    train_prepared = prepare_training_frame(train_df, category_levels)
    val_prepared = prepare_training_frame(val_df, category_levels)

    train_X = train_prepared[MATRIX_FEATURE_COLUMNS]
    val_X = val_prepared[MATRIX_FEATURE_COLUMNS]
    y_train_raw = train_prepared[LABEL_COLUMN].to_numpy(dtype="float32")
    y_val_raw = val_prepared[LABEL_COLUMN].to_numpy(dtype="float32")

    y_train = to_log_target(y_train_raw, label_cap_seconds)
    y_val = to_log_target(y_val_raw, label_cap_seconds)

    model = lgb.LGBMRegressor(**params, n_estimators=int(max_boost_rounds))
    model.fit(
        train_X,
        y_train,
        eval_set=[(val_X, y_val)],
        categorical_feature=CATEGORICAL_COLUMNS,
        callbacks=[
            lgb.early_stopping(
                stopping_rounds=EARLY_STOPPING_ROUNDS,
                first_metric_only=True,
                verbose=False,
            ),
        ],
    )

    best_iteration = int(model.best_iteration_ or max_boost_rounds)
    val_pred_seconds = predict_seconds_from_model(model, val_df, category_levels, best_iteration)
    val_true_seconds = y_val_raw
    val_true_capped = clip_seconds(y_val_raw, label_cap_seconds)

    raw_metrics = compute_metrics(val_true_seconds, val_pred_seconds)
    capped_metrics = compute_metrics(val_true_capped, val_pred_seconds)

    metrics = {
        "mae_seconds_raw": raw_metrics["mae"],
        "rmse_seconds_raw": raw_metrics["rmse"],
        "medae_seconds_raw": raw_metrics["medae"],
        "mae_seconds_capped": capped_metrics["mae"],
        "rmse_seconds_capped": capped_metrics["rmse"],
        "medae_seconds_capped": capped_metrics["medae"],
    }
    return model, best_iteration, metrics


def evaluate_model(
    model: lgb.LGBMRegressor,
    df: pd.DataFrame,
    best_iteration: int,
    label_cap_seconds: float,
    category_levels: dict[str, list[int]] | None,
) -> dict[str, float]:
    if df.empty:
        raise ValueError("Evaluation frame is empty.")

    y_true_raw = pd.to_numeric(df[LABEL_COLUMN], errors="raise").astype("float32").to_numpy()
    y_true_capped = clip_seconds(y_true_raw, label_cap_seconds)
    y_pred = predict_seconds_from_model(model, df, category_levels, best_iteration)

    raw_metrics = compute_metrics(y_true_raw, y_pred)
    capped_metrics = compute_metrics(y_true_capped, y_pred)

    return {
        "rows": float(len(df)),
        "mae_seconds_raw": raw_metrics["mae"],
        "rmse_seconds_raw": raw_metrics["rmse"],
        "medae_seconds_raw": raw_metrics["medae"],
        "mae_seconds_capped": capped_metrics["mae"],
        "rmse_seconds_capped": capped_metrics["rmse"],
        "medae_seconds_capped": capped_metrics["medae"],
    }


def compute_baseline_metrics(
    holdout_df: pd.DataFrame,
    train_eval_df: pd.DataFrame,
    label_cap_seconds: float,
) -> dict[str, float]:
    train_eval_labels = pd.to_numeric(train_eval_df[LABEL_COLUMN], errors="raise").astype("float32").to_numpy()
    train_eval_median = float(np.median(np.clip(train_eval_labels, 0.0, label_cap_seconds)))

    holdout_labels = pd.to_numeric(holdout_df[LABEL_COLUMN], errors="raise").astype("float32").to_numpy()
    holdout_capped = clip_seconds(holdout_labels, label_cap_seconds)
    baseline_pred = np.full(shape=len(holdout_df), fill_value=train_eval_median, dtype="float32")

    return compute_metrics(holdout_capped, baseline_pred)


def export_onnx_model(final_model: lgb.LGBMRegressor, feature_count: int) -> object:
    from onnxmltools import convert_lightgbm
    from onnxmltools.convert.common.data_types import FloatTensorType

    initial_types = [("input", FloatTensorType([None, feature_count]))]
    onnx_model = convert_lightgbm(
        final_model.booster_,
        initial_types=initial_types,
    )
    return onnx_model


def build_artifact_plan(
    model_artifacts_s3_bucket: str,
    feature_version: str,
    lineage: SnapshotLineage,
    train_eval_cutoff: str,
) -> BundleArtifactPlan:
    bucket = model_artifacts_s3_bucket.rstrip("/")
    artifact_root = f"{bucket}/{feature_version}/{lineage.table_uuid}/{train_eval_cutoff}"
    return BundleArtifactPlan(
        artifact_root_s3_uri=artifact_root,
        model_s3_uri=f"{artifact_root}/model.onnx",
        schema_s3_uri=f"{artifact_root}/schema.json",
        metadata_s3_uri=f"{artifact_root}/metadata.json",
        manifest_s3_uri=f"{artifact_root}/manifest.json",
    )


def build_training_result(
    *,
    elt_contract: ELTContract,
    lineage: SnapshotLineage,
    category_levels: dict[str, list[int]],
    selected_candidate: CandidateConfig,
    candidate_reports: list[CandidateReport],
    search_best_metrics: dict[str, float],
    inner_metrics: dict[str, float],
    holdout_metrics: dict[str, float],
    holdout_baseline_metrics: dict[str, float],
    label_cap_seconds: float,
    train_label_p50_seconds: float,
    best_iteration_inner: int,
    final_num_boost_round: int,
    train_rows: int,
    test_rows: int,
    artifact_plan: BundleArtifactPlan,
    model_name: str,
    model_version: str,
    output_names: Sequence[str],
) -> TrainingResult:
    request_feature_columns = list(REQUEST_FEATURE_COLUMNS)
    engineered_feature_columns = list(ENGINEERED_FEATURE_COLUMNS)
    bundle_contract = build_bundle_contract(
        output_names=output_names,
        input_name="input",
        feature_order=request_feature_columns,
        request_feature_order=request_feature_columns,
        engineered_feature_order=engineered_feature_columns,
        schema_version=elt_contract.schema_version,
        feature_version=elt_contract.feature_version,
        preprocessing_version=PREPROCESSING_VERSION,
        target_transform=TARGET_TRANSFORM,
        allow_extra_features=False,
    )
    bundle_metadata = build_bundle_metadata(
        elt_contract=elt_contract,
        lineage=lineage,
        artifact_plan=artifact_plan,
        model_name=model_name,
        model_version=model_version,
        bundle_contract_json=bundle_contract,
        category_levels=category_levels,
        selected_candidate=selected_candidate,
        candidate_reports=candidate_reports,
        search_best_metrics=search_best_metrics,
        inner_metrics=inner_metrics,
        holdout_metrics=holdout_metrics,
        holdout_baseline_metrics=holdout_baseline_metrics,
        label_cap_seconds=label_cap_seconds,
        train_label_p50_seconds=train_label_p50_seconds,
        best_iteration_inner=best_iteration_inner,
        final_num_boost_round=final_num_boost_round,
        train_rows=train_rows,
        test_rows=test_rows,
    )

    return TrainingResult(
        table_identifier=TABLE_IDENTIFIER,
        schema_version=elt_contract.schema_version,
        feature_version=elt_contract.feature_version,
        preprocessing_version=PREPROCESSING_VERSION,
        elt_contract=elt_contract,
        lineage=lineage,
        category_levels=category_levels,
        selected_candidate=selected_candidate,
        candidate_reports=candidate_reports,
        search_best_metrics=search_best_metrics,
        inner_metrics=inner_metrics,
        holdout_metrics=holdout_metrics,
        holdout_baseline_metrics=holdout_baseline_metrics,
        label_cap_seconds=float(label_cap_seconds),
        train_label_p50_seconds=float(train_label_p50_seconds),
        best_iteration_inner=int(best_iteration_inner),
        final_num_boost_round=int(final_num_boost_round),
        train_rows=int(train_rows),
        test_rows=int(test_rows),
        request_feature_columns=request_feature_columns,
        engineered_feature_columns=engineered_feature_columns,
        model_input_columns=request_feature_columns,
        model_feature_columns=engineered_feature_columns,
        model_name=model_name,
        model_version=model_version,
        bundle_contract=bundle_contract,
        bundle_metadata=bundle_metadata,
        artifact_plan=artifact_plan,
    )


BASE_LGBM_PARAMS: dict[str, object] = {
    "objective": "regression_l1",
    "metric": ["l1", "rmse"],
    "boosting_type": "gbdt",
    "force_row_wise": True,
    "random_state": 42,
    "bagging_seed": 42,
    "feature_fraction_seed": 42,
    "data_random_seed": 42,
    "drop_seed": 42,
    "extra_seed": 42,
    "verbosity": -1,
    "max_cat_threshold": 64,
    "min_data_per_group": 10,
    "cat_smooth": 20.0,
    "cat_l2": 20.0,
    "subsample_freq": 1,
}

CANDIDATE_CONFIGS: list[CandidateConfig] = [
    CandidateConfig(
        name="best_trial_replay",
        learning_rate=0.036521134985273,
        num_leaves=20,
        max_depth=-1,
        min_child_samples=36,
        feature_fraction=0.745279704149923,
        subsample=0.9041688589514847,
        reg_alpha=0.0012819238704314968,
        reg_lambda=0.0022138840237013553,
    ),
    CandidateConfig(
        name="compact_regularized",
        learning_rate=0.03,
        num_leaves=31,
        max_depth=-1,
        min_child_samples=30,
        feature_fraction=0.85,
        subsample=0.85,
        reg_alpha=0.05,
        reg_lambda=1.0,
    ),
    CandidateConfig(
        name="wider_tree",
        learning_rate=0.025,
        num_leaves=63,
        max_depth=-1,
        min_child_samples=20,
        feature_fraction=0.80,
        subsample=0.80,
        reg_alpha=0.01,
        reg_lambda=1.5,
    ),
    CandidateConfig(
        name="deeper_tree",
        learning_rate=0.035,
        num_leaves=96,
        max_depth=7,
        min_child_samples=40,
        feature_fraction=0.75,
        subsample=0.90,
        reg_alpha=0.10,
        reg_lambda=2.0,
    ),
    CandidateConfig(
        name="high_bias_low_variance",
        learning_rate=0.05,
        num_leaves=31,
        max_depth=-1,
        min_child_samples=80,
        feature_fraction=0.90,
        subsample=0.90,
        reg_alpha=0.20,
        reg_lambda=3.0,
    ),
    CandidateConfig(
        name="small_tree_fast",
        learning_rate=0.04,
        num_leaves=15,
        max_depth=-1,
        min_child_samples=60,
        feature_fraction=0.70,
        subsample=0.90,
        reg_alpha=0.0,
        reg_lambda=0.5,
    ),
    CandidateConfig(
        name="extra_wide_low_lr",
        learning_rate=0.02,
        num_leaves=128,
        max_depth=-1,
        min_child_samples=15,
        feature_fraction=0.75,
        subsample=0.85,
        reg_alpha=0.05,
        reg_lambda=2.0,
    ),
]