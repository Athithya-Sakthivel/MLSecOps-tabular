from __future__ import annotations

from flytekit import LaunchPlan

from workflows.train.workflows.training_workflow import train

DEFAULT_TRAIN_NUM_THREADS = 2
DEFAULT_TUNING_SAMPLE_ROWS = 100_000
DEFAULT_MAX_BOOST_ROUNDS = 5_000
DEFAULT_MLFLOW_EXPERIMENT_NAME = "trip_eta_lgbm"
DEFAULT_MAX_EVAL_ROWS = 100_000

TRAIN_WORKFLOW_LP_NAME = "train_default"

TRAIN_WORKFLOW_LP = LaunchPlan.get_or_create(
    workflow=train,
    name=TRAIN_WORKFLOW_LP_NAME,
    default_inputs={
        "train_num_threads": DEFAULT_TRAIN_NUM_THREADS,
        "tuning_sample_rows": DEFAULT_TUNING_SAMPLE_ROWS,
        "max_boost_rounds": DEFAULT_MAX_BOOST_ROUNDS,
        "mlflow_experiment_name": DEFAULT_MLFLOW_EXPERIMENT_NAME,
        "max_eval_rows": DEFAULT_MAX_EVAL_ROWS,
    },
)

__all__ = [
    "DEFAULT_MAX_BOOST_ROUNDS",
    "DEFAULT_MAX_EVAL_ROWS",
    "DEFAULT_MLFLOW_EXPERIMENT_NAME",
    "DEFAULT_TRAIN_NUM_THREADS",
    "DEFAULT_TUNING_SAMPLE_ROWS",
    "TRAIN_WORKFLOW_LP",
    "TRAIN_WORKFLOW_LP_NAME",
]