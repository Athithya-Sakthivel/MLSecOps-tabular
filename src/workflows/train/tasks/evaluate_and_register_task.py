#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import mlflow
import mlflow.pyfunc
import numpy as np
import onnxruntime as ort
import pandas as pd
from flytekit import Resources, task
from mlflow.models import infer_signature

from workflows.train.shared_utils import (
    EXPECTED_COLUMNS,
    LABEL_COLUMN,
    MODEL_FAMILY,
    MODEL_FEATURE_COLUMNS,
    PREDICTION_COLUMN,
    TARGET_TRANSFORM,
    TrainingResult,
    clip_seconds,
    download_file_from_s3,
    evenly_spaced_sample,
    feature_digest,
    from_log_target,
    load_iceberg_table,
    log_step,
    logger,
    numeric_metrics,
    prepare_model_features,
    read_table_as_dataframe,
    split_train_test_by_date,
    validate_raw_dataframe,
)

ICEBERG_REST_URI = os.environ.get(
    "ICEBERG_REST_URI",
    "http://iceberg-rest.default.svc.cluster.local:8181",
)
ICEBERG_WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE",
    "s3://e2e-mlops-data-681802563986/iceberg/warehouse/",
)
ICEBERG_CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "default")
MLFLOW_TRACKING_URI = os.environ.get(
    "MLFLOW_TRACKING_URI",
    "http://mlflow.mlflow.svc.cluster.local:5000",
)
USE_IAM = os.environ.get("USE_IAM", "0").strip().lower() in {"1", "true", "yes", "y", "on"}

PYFUNC_MODEL_NAME = "trip_eta_lgbm_pyfunc"
RAW_ONNX_FILENAME = "model.onnx"
SUMMARY_FILENAME = "training_summary.json"


class Log1pLightGBMPyFuncModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context: Any) -> None:
        summary_path = Path(context.artifacts["summary"])
        onnx_path = Path(context.artifacts["onnx_model"])

        logger.info("loading pyfunc artifacts summary=%s onnx=%s", summary_path, onnx_path)

        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self._category_levels = {
            key: [int(v) for v in values]
            for key, values in summary["category_levels"].items()
        }
        self._prediction_cap_seconds = float(summary["label_cap_seconds"])

        self._onnx_session = ort.InferenceSession(
            path_or_bytes=str(onnx_path),
            providers=ort.get_available_providers(),
        )
        self._input_name = self._onnx_session.get_inputs()[0].name

    def predict(self, context: Any, model_input: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(model_input, pd.DataFrame):
            if hasattr(model_input, "to_pandas"):
                model_input = model_input.to_pandas()
            else:
                model_input = pd.DataFrame(model_input)

        features = prepare_model_features(
            model_input,
            category_levels=self._category_levels,
        )
        x = features[MODEL_FEATURE_COLUMNS].to_numpy(dtype=np.float32, copy=False)
        raw_pred = self._onnx_session.run(None, {self._input_name: x})[0]
        pred_seconds = from_log_target(raw_pred)
        pred_seconds = clip_seconds(pred_seconds, self._prediction_cap_seconds)
        return pd.DataFrame({PREDICTION_COLUMN: pred_seconds})


@task(
    cache=False,
    retries=1,
    requests=Resources(cpu="1", mem="2Gi"),
    limits=Resources(cpu="2", mem="4Gi"),
)
def evaluate_and_register_task(
    training_result: TrainingResult,
    mlflow_experiment_name: str,
    max_eval_rows: int,
) -> str:
    if max_eval_rows < 1:
        raise ValueError("max_eval_rows must be >= 1")

    artifact_plan = training_result.artifact_plan

    with log_step("reload_iceberg_table_for_evaluation"):
        table = load_iceberg_table(ICEBERG_CATALOG_NAME, ICEBERG_REST_URI, ICEBERG_WAREHOUSE)
        raw_df = read_table_as_dataframe(table)
        logger.info("evaluation dataset rows=%d cols=%d", len(raw_df), len(raw_df.columns))

    with log_step("validate_raw_dataframe_for_evaluation"):
        validate_raw_dataframe(raw_df)

    with log_step("split_holdout_for_evaluation"):
        splits = split_train_test_by_date(raw_df)
        test_df = splits.test if len(splits.test) <= max_eval_rows else evenly_spaced_sample(
            splits.test,
            max_eval_rows,
        )
        if len(test_df) != len(splits.test):
            logger.info(
                "downsampled evaluation set from %d to %d rows",
                len(splits.test),
                len(test_df),
            )

    input_example = test_df[EXPECTED_COLUMNS[:-1]].head(5).copy()
    prediction_example = pd.DataFrame({PREDICTION_COLUMN: np.zeros(len(input_example), dtype=np.float32)})
    signature = infer_signature(input_example, prediction_example)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(mlflow_experiment_name)

    logger.info(
        "mlflow configured tracking_uri=%s experiment=%s",
        MLFLOW_TRACKING_URI,
        mlflow_experiment_name,
    )
    logger.info(
        "artifact_plan onnx=%s summary=%s manifest=%s mlflow_model=%s",
        artifact_plan.onnx_model_s3_uri,
        artifact_plan.summary_s3_uri,
        artifact_plan.manifest_s3_uri,
        artifact_plan.mlflow_model_s3_uri,
    )

    with tempfile.TemporaryDirectory(prefix="trip_eta_eval_") as tmp:
        tmpdir = Path(tmp)
        onnx_local = tmpdir / RAW_ONNX_FILENAME
        summary_local = tmpdir / SUMMARY_FILENAME

        with log_step("download_artifacts_from_s3"):
            download_file_from_s3(artifact_plan.onnx_model_s3_uri, onnx_local, use_iam=USE_IAM)
            download_file_from_s3(artifact_plan.summary_s3_uri, summary_local, use_iam=USE_IAM)

        with log_step("mlflow_start_run_and_log"):
            with mlflow.start_run(run_name=training_result.feature_version) as run:
                mlflow.log_params(
                    {
                        "table_identifier": training_result.table_identifier,
                        "schema_version": training_result.schema_version,
                        "feature_version": training_result.feature_version,
                        "model_family": MODEL_FAMILY,
                        "target_transform": TARGET_TRANSFORM,
                        "train_rows": training_result.train_rows,
                        "test_rows": training_result.test_rows,
                        "best_iteration_inner": training_result.best_iteration_inner,
                        "final_num_boost_round": training_result.final_num_boost_round,
                        "label_cap_seconds": training_result.label_cap_seconds,
                        "train_label_p50_seconds": training_result.train_label_p50_seconds,
                        "artifact_root_s3_uri": artifact_plan.artifact_root_s3_uri,
                        "onnx_model_s3_uri": artifact_plan.onnx_model_s3_uri,
                        "summary_s3_uri": artifact_plan.summary_s3_uri,
                        "manifest_s3_uri": artifact_plan.manifest_s3_uri,
                        "mlflow_model_s3_uri": artifact_plan.mlflow_model_s3_uri,
                        "test_digest": feature_digest(test_df, ["trip_id", "as_of_ts"]),
                        "use_iam": str(USE_IAM).lower(),
                    }
                )

                mlflow.log_metrics(numeric_metrics(training_result.holdout_metrics))
                mlflow.log_metrics(
                    {
                        "holdout_baseline_mae_seconds_capped": float(
                            training_result.holdout_baseline_metrics["mae"]
                        ),
                        "holdout_baseline_rmse_seconds_capped": float(
                            training_result.holdout_baseline_metrics["rmse"]
                        ),
                        "holdout_baseline_medae_seconds_capped": float(
                            training_result.holdout_baseline_metrics["medae"]
                        ),
                    }
                )

                mlflow.log_dict(training_result.as_dict(), "training_summary.json")
                mlflow.log_dict(artifact_plan.as_dict(), "artifact_plan.json")

                logger.info("logging pyfunc model %s", PYFUNC_MODEL_NAME)
                model_info = mlflow.pyfunc.log_model(
                    name=PYFUNC_MODEL_NAME,
                    python_model=Log1pLightGBMPyFuncModel(),
                    artifacts={
                        "onnx_model": str(onnx_local),
                        "summary": str(summary_local),
                    },
                    signature=signature,
                    input_example=input_example,
                )

                logger.info("running mlflow.models.evaluate on %d rows", len(test_df))
                eval_result = mlflow.models.evaluate(
                    model=model_info.model_uri,
                    data=test_df,
                    targets=LABEL_COLUMN,
                    model_type="regressor",
                )

                mlflow.log_metrics(numeric_metrics(eval_result.metrics))

                for artifact_name, artifact in eval_result.artifacts.items():
                    artifact_uri = getattr(artifact, "uri", None)
                    artifact_content = getattr(artifact, "content", None)
                    if artifact_uri is not None:
                        mlflow.log_text(str(artifact_uri), f"evaluation/{artifact_name}.txt")
                    elif artifact_content is not None:
                        mlflow.log_text(str(artifact_content), f"evaluation/{artifact_name}.txt")

                run_id = run.info.run_id
                result = {
                    "run_id": run_id,
                    "model_uri": model_info.model_uri,
                    "artifact_plan": artifact_plan.as_dict(),
                    "evaluation_metrics": numeric_metrics(eval_result.metrics),
                }

                logger.info(
                    "mlflow run complete run_id=%s model_uri=%s",
                    run_id,
                    model_info.model_uri,
                )
                return json.dumps(result, indent=2, default=str)