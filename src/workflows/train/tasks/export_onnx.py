from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
from flytekit import task
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile

from tasks.common import (
    CATEGORICAL_FEATURES,
    DEFAULT_ONNX_OPSET,
    FEATURE_COLUMNS,
    ensure_directory,
    compute_regression_metrics,
    load_gold_frame,
    read_json,
    write_json,
)


@task(cache=False)
def export_onnx(
    train_artifacts_dir: FlyteDirectory,
    gold_dataset: FlyteFile,
    onnx_opset: int = DEFAULT_ONNX_OPSET,
) -> FlyteDirectory:
    """Convert the trained LightGBM booster to ONNX and verify prediction parity."""

    from lightgbm import Booster
    import onnxruntime as ort
    from onnxmltools.convert.common.data_types import FloatTensorType
    from onnxmltools.convert.lightgbm import convert as convert_lightgbm

    artifact_dir = Path(str(train_artifacts_dir))
    manifest = read_json(artifact_dir / "manifest.json")
    feature_spec = read_json(artifact_dir / "feature_spec.json")
    model_path = artifact_dir / "model.txt"
    sample_path = artifact_dir / "validation_sample.parquet"

    if not sample_path.exists():
        raise FileNotFoundError(f"Validation sample missing: {sample_path}")

    booster = Booster(model_file=str(model_path))
    onnx_dir = Path(tempfile.mkdtemp(prefix="flyte_lgbm_onnx_"))
    ensure_directory(onnx_dir)

    initial_types = [("input", FloatTensorType([None, len(FEATURE_COLUMNS)]))]
    onnx_model = convert_lightgbm(
        booster,
        initial_types=initial_types,
        target_opset=onnx_opset,
        zipmap=False,
    )

    onnx_path = onnx_dir / "model.onnx"
    with onnx_path.open("wb") as f:
        f.write(onnx_model.SerializeToString())

    sample_df = pd.read_parquet(sample_path)
    sample_features = sample_df[FEATURE_COLUMNS].copy()
    for col in CATEGORICAL_FEATURES:
        sample_features[col] = pd.to_numeric(sample_features[col], errors="raise").astype("int64")
    sample_matrix = sample_features.to_numpy(dtype=np.float32)

    booster_pred = booster.predict(sample_features, num_iteration=booster.best_iteration or None)
    session = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    input_name = session.get_inputs()[0].name
    onnx_pred = session.run(None, {input_name: sample_matrix})[0]
    onnx_pred = np.asarray(onnx_pred).reshape(-1)

    parity_metrics = compute_regression_metrics(booster_pred, onnx_pred)
    parity_metrics.update(
        {
            "max_abs_error": float(np.max(np.abs(booster_pred - onnx_pred))),
            "sample_rows": int(len(sample_df)),
            "onnx_opset": int(onnx_opset),
        }
    )

    write_json(onnx_dir / "onnx_parity.json", parity_metrics)
    write_json(
        onnx_dir / "onnx_manifest.json",
        {
            "source_manifest": manifest,
            "feature_spec": feature_spec,
            "gold_dataset": str(gold_dataset),
            "onnx_path": str(onnx_path),
        },
    )

    return FlyteDirectory(path=str(onnx_dir))