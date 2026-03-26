from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
from flytekit import task
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile

from tasks.common import (
    CATEGORICAL_FEATURES,
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    TIMESTAMP_COLUMN,
    compute_regression_metrics,
    load_gold_frame,
    read_json,
    split_by_time,
)


@task(cache=False)
def evaluate_model(
    train_artifacts_dir: FlyteDirectory,
    gold_dataset: FlyteFile,
    validation_fraction: float = 0.15,
) -> Dict[str, Any]:
    """Evaluate the fitted LightGBM booster on the exact chronological validation split."""

    from lightgbm import Booster

    artifact_dir = Path(str(train_artifacts_dir))
    manifest = read_json(artifact_dir / "manifest.json")
    model_path = artifact_dir / "model.txt"

    booster = Booster(model_file=str(model_path))

    df = load_gold_frame(str(gold_dataset))
    df = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)
    split = split_by_time(df, validation_fraction=validation_fraction)
    valid_df = split.valid_df

    features = valid_df[FEATURE_COLUMNS].copy()
    for col in CATEGORICAL_FEATURES:
        features[col] = pd.to_numeric(features[col], errors="raise").astype("int64")

    y_true = valid_df[LABEL_COLUMN].to_numpy(dtype="float64")
    y_pred = booster.predict(features, num_iteration=booster.best_iteration or None)

    metrics: Dict[str, Any] = compute_regression_metrics(y_true, y_pred)
    metrics.update(
        {
            "validation_rows": int(len(valid_df)),
            "train_rows": int(len(split.train_df)),
            "manifest_best_iteration": int(manifest.get("boost_rounds", 0)),
        }
    )
    return metrics