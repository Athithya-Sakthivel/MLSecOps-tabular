#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import sys
import time
from collections.abc import Iterator
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
def log_step(step_name: str) -> Iterator[None]:
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


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

def train_final_model(
    train_eval_df: pd.DataFrame,
    best_candidate: CandidateConfig,
    final_num_boost_round: int,
    label_cap_seconds: float,
    num_threads: int,
    category_levels: dict[str, list[int]],
) -> lgb.LGBMRegressor:
    params = make_base_params(num_threads)
    params.update(
        {
            "learning_rate": float(best_candidate.learning_rate),
            "num_leaves": int(best_candidate.num_leaves),
            "max_depth": int(best_candidate.max_depth),
            "min_child_samples": int(best_candidate.min_child_samples),
            "subsample": float(best_candidate.subsample),
            "feature_fraction": float(best_candidate.feature_fraction),
            "reg_alpha": float(best_candidate.reg_alpha),
            "reg_lambda": float(best_candidate.reg_lambda),
        }
    )

    train_prepared = prepare_training_frame(train_eval_df, category_levels)
    train_X = train_prepared[MODEL_FEATURE_COLUMNS]
    y_train_raw = train_prepared[LABEL_COLUMN].to_numpy(dtype="float32")
    y_train = to_log_target(y_train_raw, label_cap_seconds)

    model = lgb.LGBMRegressor(**params, n_estimators=int(final_num_boost_round))
    model.fit(train_X, y_train, categorical_feature=CATEGORICAL_COLUMNS)
    return model

    
def numeric_metrics(metrics: dict[str, object]) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, value in metrics.items():
        if isinstance(value, (int, float, np.floating)):
            out[key] = float(value)
    return out


TABLE_IDENTIFIER_TUPLE = ("gold", "trip_training_matrix")
TABLE_IDENTIFIER = "gold.trip_training_matrix"

EXPECTED_SCHEMA_VERSION = "trip_eta_frozen_matrix_v1"
EXPECTED_FEATURE_VERSION = "trip_eta_lgbm_v1"
MODEL_FAMILY = "lightgbm"
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

EXPECTED_COLUMNS = [
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
    LABEL_COLUMN,
]

REQUIRED_NON_NULL_COLUMNS = [
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
    "trip_count_90d_zone_hour",
    LABEL_COLUMN,
]

ALLOWED_NULL_COLUMNS = {
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
}

RAW_CATEGORICAL_COLUMNS = [
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

RAW_NUMERIC_COLUMNS = [
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
    "trip_count_90d_zone_hour",
]

MODEL_INPUT_COLUMNS = RAW_CATEGORICAL_COLUMNS + RAW_NUMERIC_COLUMNS

DERIVED_NUMERIC_COLUMNS = [
    "pickup_hour_sin",
    "pickup_hour_cos",
    "pickup_dow_sin",
    "pickup_dow_cos",
    "avg_duration_7d_zone_hour_is_missing",
    "avg_fare_30d_zone_is_missing",
    "avg_duration_7d_zone_hour_log1p",
    "avg_fare_30d_zone_log1p",
    "trip_count_90d_zone_hour_log1p",
    "pickup_zone_hour_id",
    "dropoff_zone_hour_id",
    "pickup_zone_dow_id",
    "dropoff_zone_dow_id",
    "route_hour_id",
    "route_dow_id",
    "zone_pair_id",
    "borough_pair_id",
    "service_zone_pair_id",
]

MODEL_FEATURE_COLUMNS = [*MODEL_INPUT_COLUMNS, *DERIVED_NUMERIC_COLUMNS]
CATEGORICAL_COLUMNS = list(RAW_CATEGORICAL_COLUMNS)


@dataclass(frozen=True)
class SnapshotLineage:
    table_uuid: str
    table_location: str
    current_schema_id: int
    current_snapshot_id: int | None
    metadata_location: str
    format_version: int

    def __getitem__(self, key: str):
        return getattr(self, key)

    def as_dict(self) -> dict[str, object]:
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

    def __getitem__(self, key: str):
        return getattr(self, key)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateReport:
    name: str
    params: CandidateConfig
    best_iteration: int
    metrics: dict[str, float]
    score: float

    def __getitem__(self, key: str):
        return getattr(self, key)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ModelArtifactPlan:
    artifact_root_s3_uri: str
    onnx_model_s3_uri: str
    mlflow_model_s3_uri: str
    summary_s3_uri: str
    manifest_s3_uri: str

    def __getitem__(self, key: str):
        return getattr(self, key)

    def as_dict(self) -> dict[str, object]:
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
    artifact_plan: ModelArtifactPlan
    model_input_columns: list[str]
    model_feature_columns: list[str]

    def to_json(self) -> str:
        payload = asdict(self)
        return json.dumps(payload, indent=2, default=str)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


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


def load_rest_catalog(catalog_name: str, iceberg_rest_uri: str, iceberg_warehouse: str):
    return load_catalog(
        catalog_name,
        type="rest",
        uri=iceberg_rest_uri,
        warehouse=iceberg_warehouse,
    )


def load_iceberg_table(catalog_name: str, iceberg_rest_uri: str, iceberg_warehouse: str):
    catalog = load_rest_catalog(catalog_name, iceberg_rest_uri, iceberg_warehouse)
    return catalog.load_table(TABLE_IDENTIFIER_TUPLE)


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
    lf = pl.scan_iceberg(table).select(EXPECTED_COLUMNS)
    pdf = lf.collect(engine="streaming").to_pandas()
    pdf = pdf.reindex(columns=EXPECTED_COLUMNS).copy()
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
    if not df["trip_id"].is_unique:
        raise ValueError("trip_id contains duplicates.")

    as_of_ts_dates = pd.to_datetime(df["as_of_ts"], utc=True, errors="raise").dt.date
    as_of_date_dates = pd.to_datetime(df["as_of_date"], errors="raise").dt.date
    if not (as_of_ts_dates == as_of_date_dates).all():
        raise ValueError("as_of_date does not match as_of_ts.date for all rows.")

    for col_name in REQUIRED_NON_NULL_COLUMNS:
        if df[col_name].isna().any():
            raise ValueError(f"{col_name} contains nulls but is required to be non-null.")

    for col_name in EXPECTED_COLUMNS:
        if col_name not in REQUIRED_NON_NULL_COLUMNS and col_name not in ALLOWED_NULL_COLUMNS:
            if df[col_name].isna().any():
                raise ValueError(f"{col_name} contains unexpected nulls.")

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


def split_by_date_fraction(df: pd.DataFrame, keep_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame, str]:
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


def build_category_levels(df: pd.DataFrame, columns: list[str] = CATEGORICAL_COLUMNS) -> dict[str, list[int]]:
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


def _log1p_with_nan(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").astype("float32")
    arr = values.to_numpy(dtype="float32", copy=True)
    out = np.log1p(np.clip(arr, 0.0, None))
    out[np.isnan(arr)] = np.nan
    return pd.Series(out, index=series.index, dtype="float32")


def prepare_model_features(raw_df: pd.DataFrame, category_levels: dict[str, list[int]]) -> pd.DataFrame:
    missing = [col for col in RAW_CATEGORICAL_COLUMNS + RAW_NUMERIC_COLUMNS if col not in raw_df.columns]
    if missing:
        raise ValueError(f"Missing model input columns: {missing}")

    out = raw_df[RAW_CATEGORICAL_COLUMNS + RAW_NUMERIC_COLUMNS].copy()

    for col_name in RAW_CATEGORICAL_COLUMNS:
        out[col_name] = encode_categorical_series(out[col_name], category_levels[col_name])

    for col_name in RAW_NUMERIC_COLUMNS:
        out[col_name] = pd.to_numeric(out[col_name], errors="coerce").astype("float32")

    hour = out["pickup_hour"].astype("float32")
    dow0 = out["pickup_dow"].astype("float32") - 1.0

    out["pickup_hour_sin"] = np.sin(2.0 * np.pi * hour / 24.0).astype("float32")
    out["pickup_hour_cos"] = np.cos(2.0 * np.pi * hour / 24.0).astype("float32")
    out["pickup_dow_sin"] = np.sin(2.0 * np.pi * dow0 / 7.0).astype("float32")
    out["pickup_dow_cos"] = np.cos(2.0 * np.pi * dow0 / 7.0).astype("float32")

    out["avg_duration_7d_zone_hour_is_missing"] = out["avg_duration_7d_zone_hour"].isna().astype("int8")
    out["avg_fare_30d_zone_is_missing"] = out["avg_fare_30d_zone"].isna().astype("int8")

    out["avg_duration_7d_zone_hour_log1p"] = _log1p_with_nan(out["avg_duration_7d_zone_hour"])
    out["avg_fare_30d_zone_log1p"] = _log1p_with_nan(out["avg_fare_30d_zone"])
    out["trip_count_90d_zone_hour_log1p"] = _log1p_with_nan(out["trip_count_90d_zone_hour"])

    pickup_zone = out["pickup_zone_id"].astype("int32")
    dropoff_zone = out["dropoff_zone_id"].astype("int32")
    pickup_borough = out["pickup_borough_id"].astype("int32")
    dropoff_borough = out["dropoff_borough_id"].astype("int32")
    pickup_service_zone = out["pickup_service_zone_id"].astype("int32")
    dropoff_service_zone = out["dropoff_service_zone_id"].astype("int32")
    route_pair = out["route_pair_id"].astype("int32")

    out["pickup_zone_hour_id"] = (pickup_zone * 24 + out["pickup_hour"].astype("int32")).astype("int32")
    out["dropoff_zone_hour_id"] = (dropoff_zone * 24 + out["pickup_hour"].astype("int32")).astype("int32")
    out["pickup_zone_dow_id"] = (pickup_zone * 7 + (out["pickup_dow"].astype("int32") - 1)).astype("int32")
    out["dropoff_zone_dow_id"] = (dropoff_zone * 7 + (out["pickup_dow"].astype("int32") - 1)).astype("int32")
    out["route_hour_id"] = (route_pair * 24 + out["pickup_hour"].astype("int32")).astype("int32")
    out["route_dow_id"] = (route_pair * 7 + (out["pickup_dow"].astype("int32") - 1)).astype("int32")
    out["zone_pair_id"] = (pickup_zone * 10000 + dropoff_zone).astype("int32")
    out["borough_pair_id"] = (pickup_borough * 16 + dropoff_borough).astype("int32")
    out["service_zone_pair_id"] = (pickup_service_zone * 16 + dropoff_service_zone).astype("int32")

    return out[MODEL_FEATURE_COLUMNS]


def prepare_training_frame(raw_df: pd.DataFrame, category_levels: dict[str, list[int]]) -> pd.DataFrame:
    features = prepare_model_features(raw_df, category_levels=category_levels)
    label = pd.to_numeric(raw_df[LABEL_COLUMN], errors="raise").astype("float32")
    frame = features.copy()
    frame[LABEL_COLUMN] = label.to_numpy(dtype="float32")
    return frame[[*MODEL_FEATURE_COLUMNS, LABEL_COLUMN]]


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
    category_levels: dict[str, list[int]],
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
    category_levels: dict[str, list[int]],
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

    train_X = train_prepared[MODEL_FEATURE_COLUMNS]
    val_X = val_prepared[MODEL_FEATURE_COLUMNS]
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
    category_levels: dict[str, list[int]],
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
) -> ModelArtifactPlan:
    bucket = model_artifacts_s3_bucket.rstrip("/")
    artifact_root = f"{bucket}/{feature_version}/{lineage.table_uuid}/{train_eval_cutoff}"
    return ModelArtifactPlan(
        artifact_root_s3_uri=artifact_root,
        onnx_model_s3_uri=f"{artifact_root}/onnx_model",
        mlflow_model_s3_uri=f"{artifact_root}/mlflow_model",
        summary_s3_uri=f"{artifact_root}/training_summary.json",
        manifest_s3_uri=f"{artifact_root}/manifest.json",
    )


def build_training_result(
    *,
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
    artifact_plan: ModelArtifactPlan,
) -> TrainingResult:
    return TrainingResult(
        table_identifier=TABLE_IDENTIFIER,
        schema_version=EXPECTED_SCHEMA_VERSION,
        feature_version=EXPECTED_FEATURE_VERSION,
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
        artifact_plan=artifact_plan,
        model_input_columns=MODEL_INPUT_COLUMNS,
        model_feature_columns=MODEL_FEATURE_COLUMNS,
    )