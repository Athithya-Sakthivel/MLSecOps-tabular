from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

LABEL_COLUMN = "label_trip_duration_seconds"
TIMESTAMP_COLUMN = "trip_start_ts"
IDENTIFIER_COLUMNS = ["trip_id"]

# Frozen feature contract for the ETA / duration regression use case.
FEATURE_COLUMNS: list[str] = [
    "pickup_hour",
    "pickup_dow",
    "pickup_month",
    "pickup_is_weekend",
    "pickup_borough_id",
    "pickup_zone_id",
    "pickup_service_zone_id",
    "route_pair_id",
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
    "trip_count_90d_zone_hour",
]

# Columns that are encoded as integer categories in Gold.
CATEGORICAL_FEATURES: list[str] = [
    "pickup_borough_id",
    "pickup_zone_id",
    "pickup_service_zone_id",
    "route_pair_id",
]

NUMERIC_FEATURES: list[str] = [c for c in FEATURE_COLUMNS if c not in CATEGORICAL_FEATURES]

# Required for training. Extra columns in Gold are ignored.
REQUIRED_COLUMNS: list[str] = [
    *IDENTIFIER_COLUMNS,
    TIMESTAMP_COLUMN,
    *FEATURE_COLUMNS,
    LABEL_COLUMN,
]

# Precision and validation defaults.
DEFAULT_VALIDATION_FRACTION = 0.15
DEFAULT_FLAML_TIME_BUDGET_SECONDS = 300
DEFAULT_FLAML_MAX_ITER = 30
DEFAULT_SAMPLE_ROWS = 50_000
DEFAULT_RANDOM_SEED = 42
DEFAULT_NUM_BOOST_ROUND = 1500
DEFAULT_EARLY_STOPPING_ROUNDS = 100
DEFAULT_ONNX_OPSET = 17
DEFAULT_MLFLOW_EXPERIMENT = "trip_duration_eta_lgbm"


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


def _json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.ndarray,)):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def load_gold_frame(dataset_uri: str, columns: list[str] | None = None) -> pd.DataFrame:
    # pandas delegates to pyarrow/fsspec/s3fs for local and remote parquet URIs.
    return pd.read_parquet(dataset_uri, columns=columns, engine="pyarrow")


def validate_required_columns(df: pd.DataFrame, required_columns: Iterable[str] = REQUIRED_COLUMNS) -> None:
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def coerce_contract_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out[TIMESTAMP_COLUMN] = pd.to_datetime(out[TIMESTAMP_COLUMN], utc=True, errors="raise")

    for col in IDENTIFIER_COLUMNS + CATEGORICAL_FEATURES:
        out[col] = pd.to_numeric(out[col], errors="raise").astype("int64")

    for col in [*NUMERIC_FEATURES, LABEL_COLUMN]:
        out[col] = pd.to_numeric(out[col], errors="raise").astype("float64")

    # Time encodings should remain integer-like for model stability.
    for col in ["pickup_hour", "pickup_dow", "pickup_month", "pickup_is_weekend"]:
        out[col] = pd.to_numeric(out[col], errors="raise").astype("int64")

    return out


def assert_no_leakage_columns(df: pd.DataFrame) -> None:
    forbidden_markers = ("post_trip", "actual_", "target_")
    leakage_like = [c for c in df.columns if any(marker in c for marker in forbidden_markers)]
    if leakage_like:
        raise ValueError(f"Potential leakage columns present in Gold: {leakage_like}")


def validate_value_contracts(df: pd.DataFrame) -> None:
    if (df[LABEL_COLUMN] <= 0).any():
        raise ValueError(f"{LABEL_COLUMN} must be strictly positive for duration regression")

    for col in CATEGORICAL_FEATURES:
        if (df[col] < 0).any():
            raise ValueError(f"{col} contains negative category ids; reserve 0 for unknown and keep ids non-negative")

    if df[TIMESTAMP_COLUMN].isna().any():
        raise ValueError(f"{TIMESTAMP_COLUMN} contains nulls after parsing")


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


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    return df[FEATURE_COLUMNS].copy()


def build_training_frame(df: pd.DataFrame) -> pd.DataFrame:
    cols = [*FEATURE_COLUMNS, LABEL_COLUMN]
    return df[cols].copy()


def make_feature_spec() -> dict[str, Any]:
    return {
        "label_column": LABEL_COLUMN,
        "timestamp_column": TIMESTAMP_COLUMN,
        "identifier_columns": IDENTIFIER_COLUMNS,
        "feature_columns": FEATURE_COLUMNS,
        "categorical_features": CATEGORICAL_FEATURES,
        "numeric_features": NUMERIC_FEATURES,
        "feature_order_locked": True,
        "prediction_problem": "trip_duration_regression",
        "prediction_timing": "pre_trip",
    }


# Backward-compatible alias used by task modules.
build_feature_spec = make_feature_spec


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
