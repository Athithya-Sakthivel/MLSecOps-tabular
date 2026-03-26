from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from flytekit import task
from flytekit.types.directory import FlyteDirectory

from tasks.common import DEFAULT_MLFLOW_EXPERIMENT, read_json


@task(cache=False)
def register_model(
    train_artifacts_dir: FlyteDirectory,
    onnx_artifacts_dir: FlyteDirectory,
    evaluation_metrics: Dict[str, Any],
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
) -> None:
    """Log the model and all artifacts to MLflow as the final registry step."""

    import mlflow

    train_dir = Path(str(train_artifacts_dir))
    onnx_dir = Path(str(onnx_artifacts_dir))

    manifest = read_json(train_dir / "manifest.json")
    feature_spec = read_json(train_dir / "feature_spec.json")
    best_config = read_json(train_dir / "best_config.json")
    train_metrics = read_json(train_dir / "metrics.json")
    onnx_parity = read_json(onnx_dir / "onnx_parity.json")

    mlflow.set_experiment(mlflow_experiment_name)
    with mlflow.start_run():
        mlflow.set_tags(
            {
                "problem_type": feature_spec["prediction_problem"],
                "prediction_timing": feature_spec["prediction_timing"],
                "feature_contract_version": "v1",
                "model_family": "lightgbm",
                "orchestration": "flyte",
            }
        )

        mlflow.log_params({f"flaml__{k}": v for k, v in best_config.items()})
        mlflow.log_params({f"manifest__{k}": v for k, v in manifest.items() if isinstance(v, (str, int, float, bool))})
        mlflow.log_metrics({f"train__{k}": float(v) for k, v in train_metrics.items() if isinstance(v, (int, float))})
        mlflow.log_metrics({f"eval__{k}": float(v) for k, v in evaluation_metrics.items() if isinstance(v, (int, float))})
        mlflow.log_metrics({f"onnx_parity__{k}": float(v) for k, v in onnx_parity.items() if isinstance(v, (int, float))})

        mlflow.log_artifact(str(train_dir / "model.txt"), artifact_path="model")
        mlflow.log_artifact(str(train_dir / "manifest.json"), artifact_path="model")
        mlflow.log_artifact(str(train_dir / "feature_spec.json"), artifact_path="model")
        mlflow.log_artifact(str(train_dir / "best_config.json"), artifact_path="model")
        mlflow.log_artifact(str(train_dir / "metrics.json"), artifact_path="model")
        mlflow.log_artifact(str(onnx_dir / "model.onnx"), artifact_path="onnx")
        mlflow.log_artifact(str(onnx_dir / "onnx_manifest.json"), artifact_path="onnx")
        mlflow.log_artifact(str(onnx_dir / "onnx_parity.json"), artifact_path="onnx")

        sample_path = train_dir / "validation_sample.parquet"
        if sample_path.exists():
            mlflow.log_artifact(str(sample_path), artifact_path="debug")