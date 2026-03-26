from __future__ import annotations

from typing import Any, Dict

from flytekit import workflow

from tasks.common import (
    DEFAULT_EARLY_STOPPING_ROUNDS,
    DEFAULT_FLAML_MAX_ITER,
    DEFAULT_FLAML_TIME_BUDGET_SECONDS,
    DEFAULT_NUM_BOOST_ROUND,
    DEFAULT_RANDOM_SEED,
    DEFAULT_SAMPLE_ROWS,
    DEFAULT_VALIDATION_FRACTION,
)
from tasks.evaluate_model import evaluate_model
from tasks.export_onnx import export_onnx
from tasks.load_gold import load_gold
from tasks.register_model import register_model
from tasks.train_model import train_model
from tasks.validate_dataset import validate_dataset


@workflow
def train(
    gold_dataset_uri: str,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    flaml_time_budget_seconds: int = DEFAULT_FLAML_TIME_BUDGET_SECONDS,
    flaml_max_iter: int = DEFAULT_FLAML_MAX_ITER,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    random_seed: int = DEFAULT_RANDOM_SEED,
    num_boost_round: int = DEFAULT_NUM_BOOST_ROUND,
    early_stopping_rounds: int = DEFAULT_EARLY_STOPPING_ROUNDS,
) -> Dict[str, Any]:
    """Production training workflow for the ETA regression model."""

    gold_canonical = load_gold(dataset_uri=gold_dataset_uri)
    gold_validated = validate_dataset(gold_dataset=gold_canonical, validation_fraction=validation_fraction)

    train_artifacts_dir = train_model(
        gold_dataset=gold_validated,
        validation_fraction=validation_fraction,
        flaml_time_budget_seconds=flaml_time_budget_seconds,
        flaml_max_iter=flaml_max_iter,
        sample_rows=sample_rows,
        random_seed=random_seed,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
    )

    evaluation_metrics = evaluate_model(
        train_artifacts_dir=train_artifacts_dir,
        gold_dataset=gold_validated,
        validation_fraction=validation_fraction,
    )

    onnx_artifacts_dir = export_onnx(
        train_artifacts_dir=train_artifacts_dir,
        gold_dataset=gold_validated,
    )

    register_model(
        train_artifacts_dir=train_artifacts_dir,
        onnx_artifacts_dir=onnx_artifacts_dir,
        evaluation_metrics=evaluation_metrics,
    )

    return evaluation_metrics