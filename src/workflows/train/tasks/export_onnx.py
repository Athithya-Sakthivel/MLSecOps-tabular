from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
from flytekit import task
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile

from workflows.train.tasks.common import (
    CATEGORICAL_FEATURES,
    DEFAULT_ONNX_OPSET,
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    LIGHT_TASK_LIMITS,
    LIGHT_TASK_RETRIES,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
    align_model_features,
    build_feature_spec,
    build_schema_hash,
    build_task_environment,
    coerce_contract_dtypes,
    compute_regression_metrics,
    log_json,
    prepare_model_input_frame,
    read_json,
    read_json_if_exists,
    validate_gold_contract,
    validate_value_contracts,
    write_json,
)


@task(
    cache=False,
    environment=build_task_environment(),
    retries=LIGHT_TASK_RETRIES,
    limits=LIGHT_TASK_LIMITS,
)
def export_onnx(
    train_artifacts_dir: FlyteDirectory,
    gold_dataset: FlyteFile,
    onnx_opset: int = DEFAULT_ONNX_OPSET,
) -> FlyteDirectory:
    """
    Convert the trained LightGBM booster to ONNX and verify prediction parity.

    This task is strict about contract drift:
    - the saved training feature spec must match the current Gold contract,
    - the saved contract hash must match the current Gold contract hash,
    - the feature column order and categorical feature contract must match,
    - the parity sample must be validated before ONNX comparison.
    """
    import onnxruntime as ort
    from lightgbm import Booster
    from onnxmltools.convert.common.data_types import FloatTensorType
    from onnxmltools.convert.lightgbm import convert as convert_lightgbm

    artifact_dir = Path(train_artifacts_dir)
    manifest_path = artifact_dir / "manifest.json"
    feature_spec_path = artifact_dir / "feature_spec.json"
    contract_path = artifact_dir / "contract.json"
    sample_path = artifact_dir / "validation_sample.parquet"
    booster_path = artifact_dir / "model.txt"

    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing training manifest: {manifest_path}")
    if not feature_spec_path.is_file():
        raise FileNotFoundError(f"Missing feature spec artifact: {feature_spec_path}")
    if not sample_path.is_file():
        raise FileNotFoundError(f"Validation sample missing: {sample_path}")
    if not booster_path.is_file():
        raise FileNotFoundError(f"Missing LightGBM model artifact: {booster_path}")

    manifest = read_json(manifest_path)
    artifact_feature_spec = read_json(feature_spec_path)
    artifact_contract = read_json_if_exists(contract_path) or manifest

    current_feature_spec = build_feature_spec()
    current_schema_hash = build_schema_hash(current_feature_spec)

    if artifact_feature_spec != current_feature_spec:
        raise ValueError("Training feature_spec does not match the current Gold contract")
    if artifact_contract.get("schema_hash") != current_schema_hash:
        raise ValueError("Training contract hash does not match the current Gold contract")
    if list(manifest.get("feature_columns", [])) != FEATURE_COLUMNS:
        raise ValueError("Training feature column order does not match the current Gold contract")
    if list(manifest.get("categorical_features", [])) != CATEGORICAL_FEATURES:
        raise ValueError("Training categorical feature contract does not match the current Gold contract")

    booster = Booster(model_file=str(booster_path))
    gold_uri = str(gold_dataset)

    log_json(
        msg="export_onnx_start",
        train_artifacts_dir=str(artifact_dir),
        gold_dataset=gold_uri,
        onnx_opset=onnx_opset,
        schema_hash=current_schema_hash,
        feature_version=current_feature_spec["feature_version"],
        schema_version=current_feature_spec["schema_version"],
    )

    onnx_dir = Path(tempfile.mkdtemp(prefix="flyte_lgbm_onnx_"))
    onnx_path = onnx_dir / "model.onnx"

    initial_types = [("input", FloatTensorType([None, len(FEATURE_COLUMNS)]))]
    onnx_model = convert_lightgbm(
        booster,
        initial_types=initial_types,
        target_opset=onnx_opset,
        zipmap=False,
    )

    with onnx_path.open("wb") as f:
        f.write(onnx_model.SerializeToString())

    sample_df = pd.read_parquet(sample_path)
    validate_gold_contract(sample_df, strict_dtypes=False, label="Validation parity sample")
    sample_df = coerce_contract_dtypes(sample_df)
    validate_gold_contract(sample_df, strict_dtypes=True, label="Validation parity sample")
    validate_value_contracts(sample_df)

    booster_features = prepare_model_input_frame(sample_df)
    onnx_features = align_model_features(sample_df).to_numpy(dtype=np.float32, copy=False)

    best_iteration = getattr(booster, "best_iteration", None)
    if isinstance(best_iteration, int) and best_iteration > 0:
        booster_pred = booster.predict(booster_features, num_iteration=best_iteration)
    else:
        booster_pred = booster.predict(booster_features)

    session = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    input_name = session.get_inputs()[0].name
    onnx_pred = session.run(None, {input_name: onnx_features})[0]

    booster_pred = np.asarray(booster_pred, dtype=np.float64).reshape(-1)
    onnx_pred = np.asarray(onnx_pred, dtype=np.float64).reshape(-1)

    if booster_pred.shape != onnx_pred.shape:
        raise ValueError(
            f"Prediction shape mismatch: booster={booster_pred.shape}, onnx={onnx_pred.shape}"
        )

    parity_metrics = compute_regression_metrics(booster_pred, onnx_pred)
    parity_metrics.update(
        {
            "max_abs_error": float(np.max(np.abs(booster_pred - onnx_pred))),
            "sample_rows": len(sample_df),
            "onnx_opset": int(onnx_opset),
            "schema_hash": current_schema_hash,
            "feature_version": current_feature_spec["feature_version"],
            "schema_version": current_feature_spec["schema_version"],
            "gold_table": manifest.get("gold_table", ""),
            "source_silver_table": manifest.get("source_silver_table", SOURCE_SILVER_TABLE),
            "validation_sample_path": str(sample_path),
            "onnx_path": str(onnx_path),
        }
    )

    write_json(onnx_dir / "onnx_parity.json", parity_metrics)
    write_json(
        onnx_dir / "onnx_manifest.json",
        {
            "source_manifest": manifest,
            "source_contract": artifact_contract,
            "feature_spec": current_feature_spec,
            "gold_dataset": gold_uri,
            "onnx_path": str(onnx_path),
            "schema_hash": current_schema_hash,
            "feature_columns": FEATURE_COLUMNS,
            "categorical_features": CATEGORICAL_FEATURES,
            "label_column": LABEL_COLUMN,
            "timestamp_column": TIMESTAMP_COLUMN,
        },
    )

    log_json(msg="export_onnx_success", onnx_path=str(onnx_path), **parity_metrics)
    return FlyteDirectory(path=str(onnx_dir))