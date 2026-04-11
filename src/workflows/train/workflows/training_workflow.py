from __future__ import annotations

from flytekit import workflow

from workflows.train.tasks.evaluate_and_register_task import evaluate_and_register_task
from workflows.train.tasks.train_model_task import train_model_task


@workflow
def train(
    train_num_threads: int = 2,
    tuning_sample_rows: int = 100_000,
    max_boost_rounds: int = 5_000,
    mlflow_experiment_name: str = "trip_eta_lgbm",
    max_eval_rows: int = 100_000,
) -> str:
    training_result_json = train_model_task(
        train_num_threads=train_num_threads,
        tuning_sample_rows=tuning_sample_rows,
        max_boost_rounds=max_boost_rounds,
    )

    return evaluate_and_register_task(
        training_result_json=training_result_json,
        mlflow_experiment_name=mlflow_experiment_name,
        max_eval_rows=max_eval_rows,
    )