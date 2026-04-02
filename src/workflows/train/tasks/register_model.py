from __future__ import annotations

from pathlib import Path
from typing import Any

from flytekit import task
from flytekit.types.directory import FlyteDirectory

from workflows.train.tasks.common import (
    DEFAULT_MLFLOW_EXPERIMENT,
    LIGHT_TASK_LIMITS,
    LIGHT_TASK_RETRIES,
    TRAIN_PROFILE,
    build_feature_spec,
    build_schema_hash,
    build_task_environment,
    log_json,
    read_json,
)


def _materialize_directory(directory: FlyteDirectory, *, label: str) -> Path:
    """
    Download a FlyteDirectory input into a local filesystem path.
    """
    local_path = Path(directory.download())
    if not local_path.exists():
        raise FileNotFoundError(f"{label} directory does not exist after download: {local_path}")
    if not local_path.is_dir():
        raise NotADirectoryError(f"{label} is not a directory: {local_path}")
    return local_path


def _require_file(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required {label}: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"Expected a file for {label}, found something else: {path}")


def _scalar_params(values: dict[str, Any]) -> dict[str, str]:
    """
    MLflow params are logged as strings; keep only scalar values.
    """
    out: dict[str, str] = {}
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            out[key] = str(value)
    return out


def _scalar_metrics(values: dict[str, Any]) -> dict[str, float]:
    """
    MLflow metrics must be numeric.
    """
    out: dict[str, float] = {}
    for key, value in values.items():
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            out[key] = float(value)
    return out


@task(
    cache=False,
    environment=build_task_environment(),
    retries=LIGHT_TASK_RETRIES,
    limits=LIGHT_TASK_LIMITS,
)
def register_model(
    train_artifacts_dir: FlyteDirectory,
    onnx_artifacts_dir: FlyteDirectory,
    evaluation_metrics: dict[str, float],
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
) -> None:
    """
    Register the training run artifacts, contract files, and ONNX parity outputs in MLflow.
    """
    import mlflow

    train_dir = _materialize_directory(train_artifacts_dir, label="train_artifacts_dir")
    onnx_dir = _materialize_directory(onnx_artifacts_dir, label="onnx_artifacts_dir")

    manifest_path = train_dir / "manifest.json"
    feature_spec_path = train_dir / "feature_spec.json"
    contract_path = train_dir / "contract.json"
    best_config_path = train_dir / "best_config.json"
    lightgbm_params_path = train_dir / "lightgbm_params.json"
    metrics_path = train_dir / "metrics.json"
    runtime_config_path = train_dir / "runtime_config.json"
    validation_sample_path = train_dir / "validation_sample.parquet"

    onnx_path = onnx_dir / "model.onnx"
    onnx_manifest_path = onnx_dir / "onnx_manifest.json"
    onnx_parity_path = onnx_dir / "onnx_parity.json"

    for file_path, label in [
        (manifest_path, "manifest.json"),
        (feature_spec_path, "feature_spec.json"),
        (contract_path, "contract.json"),
        (best_config_path, "best_config.json"),
        (lightgbm_params_path, "lightgbm_params.json"),
        (metrics_path, "metrics.json"),
        (onnx_path, "model.onnx"),
        (onnx_manifest_path, "onnx_manifest.json"),
        (onnx_parity_path, "onnx_parity.json"),
    ]:
        _require_file(file_path, label=label)

    manifest = read_json(manifest_path)
    feature_spec = read_json(feature_spec_path)
    contract = read_json(contract_path)
    best_config = read_json(best_config_path)
    train_metrics = read_json(metrics_path)
    onnx_parity = read_json(onnx_parity_path)

    current_feature_spec = build_feature_spec()
    current_schema_hash = build_schema_hash(current_feature_spec)

    if feature_spec != current_feature_spec:
        raise ValueError("Training feature_spec does not match the current Gold contract")
    if contract.get("schema_hash") != current_schema_hash:
        raise ValueError("Training contract hash does not match the current Gold contract")
    if list(manifest.get("feature_columns", [])) != list(current_feature_spec.get("feature_columns", [])):
        raise ValueError("Training feature column order does not match the current Gold contract")
    if list(manifest.get("categorical_features", [])) != list(current_feature_spec.get("categorical_features", [])):
        raise ValueError("Training categorical feature contract does not match the current Gold contract")
    if manifest.get("schema_version") != current_feature_spec["schema_version"]:
        raise ValueError("Training schema version does not match the current Gold contract")
    if manifest.get("feature_version") != current_feature_spec["feature_version"]:
        raise ValueError("Training feature version does not match the current Gold contract")

    mlflow.set_experiment(mlflow_experiment_name)

    with mlflow.start_run():
        mlflow.set_tags(
            {
                "problem_type": "trip_duration_regression",
                "prediction_timing": "pre_trip",
                "model_family": str(manifest.get("model_family", "lightgbm")),
                "inference_runtime": str(manifest.get("inference_runtime", "onnxruntime")),
                "feature_version": str(manifest.get("feature_version", current_feature_spec["feature_version"])),
                "schema_version": str(manifest.get("schema_version", current_feature_spec["schema_version"])),
                "schema_hash": str(manifest.get("schema_hash", current_schema_hash)),
                "gold_table": str(manifest.get("gold_table", "")),
                "source_silver_table": str(manifest.get("source_silver_table", "")),
                "train_cutoff_ts": str(manifest.get("cutoff_ts", "")),
                "validation_fraction": str(manifest.get("validation_fraction", "")),
                "train_profile": str(manifest.get("train_profile", TRAIN_PROFILE)),
                "ray_num_workers": str(manifest.get("ray_num_workers", "")),
                "ray_worker_cpu": str(manifest.get("ray_worker_cpu", "")),
                "ray_worker_mem": str(manifest.get("ray_worker_mem", "")),
                "feature_contract_version": "frozen_gold_contract_v1",
                "orchestration": "flyte",
            }
        )

        mlflow.log_params(_scalar_params(best_config))
        mlflow.log_params(
            _scalar_params({f"manifest__{k}": v for k, v in manifest.items()})
        )

        mlflow.log_metrics(_scalar_metrics(train_metrics))
        mlflow.log_metrics(_scalar_metrics(evaluation_metrics))
        mlflow.log_metrics(_scalar_metrics(onnx_parity))

        mlflow.log_artifact(str(manifest_path), artifact_path="model")
        mlflow.log_artifact(str(feature_spec_path), artifact_path="model")
        mlflow.log_artifact(str(contract_path), artifact_path="model")
        mlflow.log_artifact(str(best_config_path), artifact_path="model")
        mlflow.log_artifact(str(lightgbm_params_path), artifact_path="model")
        mlflow.log_artifact(str(metrics_path), artifact_path="model")

        if runtime_config_path.exists():
            mlflow.log_artifact(str(runtime_config_path), artifact_path="model")
        if validation_sample_path.exists():
            mlflow.log_artifact(str(validation_sample_path), artifact_path="debug")

        mlflow.log_artifact(str(onnx_path), artifact_path="onnx")
        mlflow.log_artifact(str(onnx_manifest_path), artifact_path="onnx")
        mlflow.log_artifact(str(onnx_parity_path), artifact_path="onnx")

    log_json(
        msg="register_model_success",
        mlflow_experiment_name=mlflow_experiment_name,
        feature_version=manifest.get("feature_version"),
        schema_version=manifest.get("schema_version"),
        schema_hash=manifest.get("schema_hash"),
        gold_table=manifest.get("gold_table"),
        source_silver_table=manifest.get("source_silver_table"),
        train_cutoff_ts=manifest.get("cutoff_ts"),
        train_profile=manifest.get("train_profile", TRAIN_PROFILE),
    )