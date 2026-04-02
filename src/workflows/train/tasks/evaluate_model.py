from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flytekit import task
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile

from workflows.train.tasks.common import (
    DEFAULT_VALIDATION_FRACTION,
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    LIGHT_TASK_LIMITS,
    LIGHT_TASK_RETRIES,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
    artifact_sidecar_path,
    build_feature_spec,
    build_schema_hash,
    build_task_environment,
    coerce_contract_dtypes,
    compute_regression_metrics,
    load_gold_frame,
    log_json,
    prepare_model_input_frame,
    read_json,
    read_json_if_exists,
    split_by_time,
    validate_gold_contract,
    validate_value_contracts,
)

EvaluationMetrics = dict[str, float]


def _normalize_json_value(value: Any) -> Any:
    """
    Normalize JSON-ish inputs so comparisons are stable whether the source is:
    - already parsed dict/list
    - a JSON string
    - None / empty string
    """
    if value is None:
        return None

    if isinstance(value, (dict, list, int, float, bool)):
        return value

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return value

    return value


def _json_text(value: Any) -> str:
    normalized = _normalize_json_value(value)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@task(
    cache=False,
    environment=build_task_environment(),
    retries=LIGHT_TASK_RETRIES,
    limits=LIGHT_TASK_LIMITS,
)
def evaluate_model(
    train_artifacts_dir: FlyteDirectory,
    gold_dataset: FlyteFile,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
) -> EvaluationMetrics:
    """
    Evaluate the fitted LightGBM booster on the exact chronological validation split.

    This task is strict about contract drift:
    - the training artifact feature spec must match the current Gold contract,
    - the saved contract hash must match the current Gold contract hash,
    - the feature column order and categorical contract must match,
    - the validation split cutoff must match the training run cutoff when present.
    """
    from lightgbm import Booster

    artifact_dir = Path(str(train_artifacts_dir))
    manifest_path = artifact_dir / "manifest.json"
    feature_spec_path = artifact_dir / "feature_spec.json"
    booster_path = artifact_dir / "model.txt"

    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing training manifest: {manifest_path}")
    if not feature_spec_path.is_file():
        raise FileNotFoundError(f"Missing training feature spec: {feature_spec_path}")
    if not booster_path.is_file():
        raise FileNotFoundError(f"Missing LightGBM model artifact: {booster_path}")

    manifest = read_json(manifest_path)
    artifact_feature_spec = read_json(feature_spec_path)
    artifact_contract = read_json_if_exists(artifact_dir / "contract.json") or manifest

    current_feature_spec = build_feature_spec()
    current_schema_hash = build_schema_hash(current_feature_spec)
    expected_feature_spec_json = _json_text(current_feature_spec)

    if artifact_feature_spec != current_feature_spec:
        raise ValueError("Training feature_spec does not match the current Gold contract")

    artifact_schema_hash = artifact_contract.get("schema_hash")
    if artifact_schema_hash != current_schema_hash:
        raise ValueError("Training contract hash does not match the current Gold contract")

    if list(manifest.get("feature_columns", [])) != FEATURE_COLUMNS:
        raise ValueError("Training feature column order does not match the current Gold contract")

    expected_categorical_features = [
        "pickup_borough_id",
        "pickup_zone_id",
        "pickup_service_zone_id",
        "dropoff_borough_id",
        "dropoff_zone_id",
        "dropoff_service_zone_id",
        "route_pair_id",
    ]
    if list(manifest.get("categorical_features", [])) != expected_categorical_features:
        raise ValueError("Training categorical feature contract does not match the current Gold contract")

    artifact_feature_spec_json = artifact_contract.get("feature_spec_json")
    if artifact_feature_spec_json is not None and str(artifact_feature_spec_json).strip():
        if _json_text(artifact_feature_spec_json) != expected_feature_spec_json:
            raise ValueError("Training artifact feature_spec_json does not match the current Gold contract")

    gold_uri = str(gold_dataset)
    log_json(
        msg="evaluate_model_start",
        train_artifacts_dir=str(artifact_dir),
        gold_dataset=gold_uri,
        validation_fraction=validation_fraction,
        schema_hash=current_schema_hash,
        feature_version=current_feature_spec["feature_version"],
        schema_version=current_feature_spec["schema_version"],
    )

    booster = Booster(model_file=str(booster_path))

    df = load_gold_frame(gold_uri)
    validate_gold_contract(df, strict_dtypes=False, label="Gold input frame")
    df = coerce_contract_dtypes(df)
    validate_gold_contract(df, strict_dtypes=True, label="Gold canonical frame")
    validate_value_contracts(df)
    df = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

    effective_validation_fraction = float(manifest.get("validation_fraction", validation_fraction))
    split = split_by_time(df, validation_fraction=effective_validation_fraction)
    valid_df = split.valid_df

    manifest_cutoff = manifest.get("cutoff_ts")
    if manifest_cutoff is not None and str(manifest_cutoff) != str(split.cutoff_ts):
        raise ValueError(
            "Validation split cutoff drifted from training: "
            f"training_cutoff={manifest_cutoff}, current_cutoff={split.cutoff_ts}"
        )

    current_dataset_contract = read_json_if_exists(artifact_sidecar_path(gold_uri, ".contract.json"))
    if current_dataset_contract is not None:
        dataset_schema_hash = current_dataset_contract.get("schema_hash")
        if dataset_schema_hash != current_schema_hash:
            raise ValueError("Current Gold dataset contract hash does not match the training contract")

        dataset_feature_spec_json = current_dataset_contract.get("feature_spec_json")
        if dataset_feature_spec_json is not None and str(dataset_feature_spec_json).strip():
            if _json_text(dataset_feature_spec_json) != expected_feature_spec_json:
                raise ValueError("Current Gold dataset feature spec does not match the training contract")

    features = prepare_model_input_frame(valid_df)
    y_true = valid_df[LABEL_COLUMN].to_numpy(dtype="float64")
    y_pred = booster.predict(features, num_iteration=booster.best_iteration or None)

    metrics: EvaluationMetrics = compute_regression_metrics(y_true, y_pred)
    metrics.update(
        {
            "validation_rows": float(len(valid_df)),
            "train_rows": float(len(split.train_df)),
            "manifest_best_iteration": float(int(manifest.get("boost_rounds", 0))),
        }
    )

    log_json(
        msg="evaluate_model_success",
        train_artifacts_dir=str(artifact_dir),
        gold_dataset=gold_uri,
        schema_hash=current_schema_hash,
        feature_version=current_feature_spec["feature_version"],
        schema_version=current_feature_spec["schema_version"],
        validation_rows=len(valid_df),
        train_rows=len(split.train_df),
        cutoff_ts=str(split.cutoff_ts),
        gold_table=str(manifest.get("gold_table", "")),
        source_silver_table=str(manifest.get("source_silver_table", SOURCE_SILVER_TABLE)),
        **metrics,
    )
    return metrics