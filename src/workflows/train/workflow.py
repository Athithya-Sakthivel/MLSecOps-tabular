#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse

import boto3
import mlflow
import mlflow.pyfunc
import numpy as np
import onnx
import onnxruntime as ort
import pandas as pd
from flytekit import task, workflow
from mlflow.models import infer_signature

from workflows.train.core import (
    EXPECTED_COLUMNS,
    EXPECTED_FEATURE_VERSION,
    INNER_VALIDATION_FRACTION,
    LABEL_COLUMN,
    MAX_PREDICTION_SECONDS,
    MODEL_FAMILY,
    MODEL_FEATURE_COLUMNS,
    PREDICTION_COLUMN,
    TARGET_TRANSFORM,
    TrainingResult,
    build_artifact_plan,
    build_training_result,
    clip_seconds,
    compute_baseline_metrics,
    compute_metrics,
    evenly_spaced_sample,
    export_onnx_model,
    feature_digest,
    from_log_target,
    load_iceberg_table,
    prepare_model_features,
    read_table_as_dataframe,
    search_best_model,
    split_by_date_fraction,
    split_train_test_by_date,
    table_snapshot_lineage,
    train_final_model,
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
MODEL_ARTIFACTS_S3_BUCKET = os.environ.get(
    "MODEL_ARTIFACTS_S3_BUCKET",
    "s3://e2e-mlops-data-681802563986/model-artifacts",
)
USE_IAM = os.environ.get("USE_IAM", "0").strip().lower() in {"1", "true", "yes", "y", "on"}

PYFUNC_MODEL_NAME = "trip_eta_lgbm_pyfunc"
RAW_ONNX_FILENAME = "model.onnx"
SUMMARY_FILENAME = "training_summary.json"
MANIFEST_FILENAME = "manifest.json"
FALLBACK_EVAL_SAMPLE_CAP = 250000

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").strip().upper()


def _configure_logging() -> None:
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format="%(asctime)sZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        force=True,
    )
    logging.Formatter.converter = time.gmtime


_configure_logging()
logger = logging.getLogger(__name__)


@contextmanager
def log_step(message: str):
    started = time.perf_counter()
    logger.info("%s started", message)
    try:
        yield
    except Exception:
        elapsed = time.perf_counter() - started
        logger.exception("%s failed after %.2fs", message, elapsed)
        raise
    else:
        elapsed = time.perf_counter() - started
        logger.info("%s completed in %.2fs", message, elapsed)


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got: {s3_uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Malformed S3 URI: {s3_uri}")
    return bucket, key


def _require_static_aws_credentials_if_needed() -> None:
    if USE_IAM:
        return

    missing: list[str] = []
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        if not os.environ.get(key, "").strip():
            missing.append(key)

    if missing:
        raise RuntimeError(
            "USE_IAM=false requires AWS credentials in the runtime environment: "
            + ", ".join(missing)
        )


def _s3_client():
    _require_static_aws_credentials_if_needed()
    return boto3.client("s3")


def upload_file_to_s3(local_path: Path, s3_uri: str) -> str:
    bucket, key = _parse_s3_uri(s3_uri)
    logger.info("uploading %s -> %s/%s", local_path, bucket, key)
    _s3_client().upload_file(str(local_path), bucket, key)
    return s3_uri


def download_file_from_s3(s3_uri: str, local_path: Path) -> Path:
    bucket, key = _parse_s3_uri(s3_uri)
    logger.info("downloading %s -> %s", s3_uri, local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    _s3_client().download_file(bucket, key, str(local_path))
    return local_path


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _numeric_metrics(metrics: dict[str, object]) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, value in metrics.items():
        if isinstance(value, (int, float, np.floating)):
            out[key] = float(value)
    return out


def _predict_seconds_from_model(
    model,
    raw_df: pd.DataFrame,
    category_levels: dict[str, list[int]],
    num_iteration: int | None,
) -> np.ndarray:
    features = prepare_model_features(raw_df, category_levels=category_levels)
    if num_iteration is None:
        preds_log = model.predict(features)
    else:
        preds_log = model.predict(features, num_iteration=num_iteration)
    preds_seconds = from_log_target(preds_log)
    return clip_seconds(preds_seconds, MAX_PREDICTION_SECONDS)


def _evaluate_model(
    model,
    df: pd.DataFrame,
    best_iteration: int,
    label_cap_seconds: float,
    category_levels: dict[str, list[int]],
) -> dict[str, float]:
    if df.empty:
        raise ValueError("Evaluation frame is empty.")

    y_true_raw = pd.to_numeric(df[LABEL_COLUMN], errors="raise").astype("float32").to_numpy()
    y_true_capped = clip_seconds(y_true_raw, label_cap_seconds)
    y_pred = _predict_seconds_from_model(model, df, category_levels, best_iteration)

    raw_metrics = compute_metrics(y_true_raw, y_pred)
    capped_metrics = compute_metrics(y_true_capped, y_pred)

    return {
        "rows": float(len(df)),
        "mae_seconds_raw": raw_metrics["mae"],
        "rmse_seconds_raw": raw_metrics["rmse"],
        "medae_seconds_raw": raw_metrics["medae"],
        "mae_seconds_capped": capped_metrics["mae"],
        "rmse_seconds_capped": capped_metrics["rmse"],
        "medae_seconds_capped": capped_metrics["medae"],
    }


class Log1pLightGBMPyFuncModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context: object) -> None:
        summary_path = Path(context.artifacts["summary"])
        onnx_path = Path(context.artifacts["onnx_model"])
        logger.info("loading pyfunc context summary=%s onnx=%s", summary_path, onnx_path)

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

    def predict(self, context: object, model_input: pd.DataFrame) -> pd.DataFrame:
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


def _materialize_training_bundle(
    training_result: TrainingResult,
    onnx_model,
) -> tuple[Path, Path, Path]:
    local_bundle = Path(tempfile.mkdtemp(prefix="trip_eta_lgbm_"))
    raw_onnx_path = local_bundle / RAW_ONNX_FILENAME
    summary_path = local_bundle / SUMMARY_FILENAME
    manifest_path = local_bundle / MANIFEST_FILENAME

    logger.info("materializing training bundle in %s", local_bundle)
    onnx.save_model(onnx_model, str(raw_onnx_path))
    write_json(summary_path, training_result.as_dict())
    write_json(
        manifest_path,
        {
            "table_identifier": training_result.table_identifier,
            "schema_version": training_result.schema_version,
            "feature_version": training_result.feature_version,
            "artifact_plan": training_result.artifact_plan.as_dict(),
            "train_rows": training_result.train_rows,
            "test_rows": training_result.test_rows,
            "best_iteration_inner": training_result.best_iteration_inner,
            "final_num_boost_round": training_result.final_num_boost_round,
            "holdout_mae_seconds_capped": training_result.holdout_metrics["mae_seconds_capped"],
            "holdout_baseline_mae_seconds_capped": training_result.holdout_baseline_metrics["mae"],
        },
    )
    return raw_onnx_path, summary_path, manifest_path


@task(cache=False)
def train_model_task(
    train_num_threads: int,
    tuning_sample_rows: int,
    max_boost_rounds: int,
) -> TrainingResult:
    if train_num_threads < 1:
        raise ValueError("train_num_threads must be >= 1")
    if tuning_sample_rows < 1:
        raise ValueError("tuning_sample_rows must be >= 1")
    if max_boost_rounds < 1:
        raise ValueError("max_boost_rounds must be >= 1")

    _require_static_aws_credentials_if_needed()

    with log_step("load_iceberg_table"):
        table = load_iceberg_table(ICEBERG_CATALOG_NAME, ICEBERG_REST_URI, ICEBERG_WAREHOUSE)

    with log_step("read_table_as_dataframe"):
        raw_df = read_table_as_dataframe(table)
        logger.info("raw dataset rows=%d cols=%d", len(raw_df), len(raw_df.columns))

    with log_step("validate_raw_dataframe"):
        validate_raw_dataframe(raw_df)

    with log_step("split_train_test_by_date"):
        splits = split_train_test_by_date(raw_df)
        train_eval_df = splits.train_eval
        test_df = splits.test
        logger.info(
            "split summary train_eval_rows=%d test_rows=%d cutoff=%s test_start=%s",
            len(train_eval_df),
            len(test_df),
            splits.train_eval_cutoff,
            splits.test_start_date,
        )

    with log_step("split_by_date_fraction"):
        inner_train_df, inner_validation_df, inner_cutoff = split_by_date_fraction(
            train_eval_df,
            1.0 - INNER_VALIDATION_FRACTION,
        )
        logger.info(
            "inner split summary train_rows=%d validation_rows=%d cutoff=%s",
            len(inner_train_df),
            len(inner_validation_df),
            inner_cutoff,
        )

    train_eval_label_values = pd.to_numeric(train_eval_df[LABEL_COLUMN], errors="raise").astype("float32").to_numpy()
    label_cap_seconds = float(np.quantile(train_eval_label_values, 0.99))
    train_label_p50_seconds = float(np.median(np.clip(train_eval_label_values, 0.0, label_cap_seconds)))
    logger.info(
        "label stats cap_seconds=%.3f p50_seconds=%.3f",
        label_cap_seconds,
        train_label_p50_seconds,
    )

    with log_step("search_best_model"):
        (
            best_candidate,
            candidate_reports,
            search_best_metrics,
            best_iteration_inner,
            category_levels,
        ) = search_best_model(
            train_eval_df=inner_train_df,
            label_cap_seconds=label_cap_seconds,
            num_threads=train_num_threads,
            tuning_sample_rows=tuning_sample_rows,
            max_boost_rounds=max_boost_rounds,
        )
        logger.info(
            "selected candidate=%s best_iteration=%d search_mae_capped=%.6f",
            best_candidate.name,
            best_iteration_inner,
            search_best_metrics["mae_seconds_capped"],
        )

    final_num_boost_round = int(max(50, best_iteration_inner))
    logger.info("final_num_boost_round=%d", final_num_boost_round)

    with log_step("train_final_model"):
        final_model = train_final_model(
            train_eval_df=train_eval_df,
            best_candidate=best_candidate,
            final_num_boost_round=final_num_boost_round,
            label_cap_seconds=label_cap_seconds,
            num_threads=train_num_threads,
            category_levels=category_levels,
        )

    with log_step("evaluate_inner_validation"):
        inner_metrics = _evaluate_model(
            model=final_model,
            df=inner_validation_df,
            best_iteration=final_num_boost_round,
            label_cap_seconds=label_cap_seconds,
            category_levels=category_levels,
        )
        logger.info("inner validation mae_capped=%.6f", inner_metrics["mae_seconds_capped"])

    test_eval_df = test_df if len(test_df) <= FALLBACK_EVAL_SAMPLE_CAP else evenly_spaced_sample(
        test_df,
        FALLBACK_EVAL_SAMPLE_CAP,
    )
    if len(test_eval_df) != len(test_df):
        logger.info(
            "downsampled holdout evaluation from %d to %d rows",
            len(test_df),
            len(test_eval_df),
        )

    with log_step("evaluate_holdout"):
        holdout_metrics = _evaluate_model(
            model=final_model,
            df=test_eval_df,
            best_iteration=final_num_boost_round,
            label_cap_seconds=label_cap_seconds,
            category_levels=category_levels,
        )
        holdout_baseline_metrics = compute_baseline_metrics(
            holdout_df=test_eval_df,
            train_eval_df=train_eval_df,
            label_cap_seconds=label_cap_seconds,
        )
        logger.info(
            "holdout mae_capped=%.6f baseline_mae_capped=%.6f",
            holdout_metrics["mae_seconds_capped"],
            holdout_baseline_metrics["mae"],
        )

    with log_step("build_artifact_plan"):
        lineage = table_snapshot_lineage(table)
        artifact_plan = build_artifact_plan(
            model_artifacts_s3_bucket=MODEL_ARTIFACTS_S3_BUCKET,
            feature_version=EXPECTED_FEATURE_VERSION,
            lineage=lineage,
            train_eval_cutoff=splits.train_eval_cutoff,
        )
        logger.info("artifact_root=%s", artifact_plan.artifact_root_s3_uri)

    training_result = build_training_result(
        lineage=lineage,
        category_levels=category_levels,
        selected_candidate=best_candidate,
        candidate_reports=candidate_reports,
        search_best_metrics=search_best_metrics,
        inner_metrics=inner_metrics,
        holdout_metrics=holdout_metrics,
        holdout_baseline_metrics=holdout_baseline_metrics,
        label_cap_seconds=label_cap_seconds,
        train_label_p50_seconds=train_label_p50_seconds,
        best_iteration_inner=best_iteration_inner,
        final_num_boost_round=final_num_boost_round,
        train_rows=len(train_eval_df),
        test_rows=len(test_df),
        artifact_plan=artifact_plan,
    )

    with log_step("export_and_upload_artifacts"):
        raw_onnx_path, summary_path, manifest_path = _materialize_training_bundle(
            training_result=training_result,
            onnx_model=export_onnx_model(final_model, feature_count=len(MODEL_FEATURE_COLUMNS)),
        )
        upload_file_to_s3(raw_onnx_path, artifact_plan.onnx_model_s3_uri)
        upload_file_to_s3(summary_path, artifact_plan.summary_s3_uri)
        upload_file_to_s3(manifest_path, artifact_plan.manifest_s3_uri)

    logger.info("train_model_task finished successfully")
    return training_result


@task(cache=False)
def evaluate_and_register_task(
    training_result: TrainingResult,
    mlflow_experiment_name: str,
    max_eval_rows: int,
) -> str:
    if max_eval_rows < 1:
        raise ValueError("max_eval_rows must be >= 1")

    _require_static_aws_credentials_if_needed()

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
            logger.info("downsampled mlflow evaluation set from %d to %d rows", len(splits.test), len(test_df))

    input_example = test_df[EXPECTED_COLUMNS[:-1]].head(5).copy()
    prediction_example = pd.DataFrame({PREDICTION_COLUMN: np.zeros(len(input_example), dtype=np.float32)})
    signature = infer_signature(input_example, prediction_example)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(mlflow_experiment_name)

    with tempfile.TemporaryDirectory(prefix="trip_eta_eval_") as tmp, log_step("mlflow_registration_and_evaluation"):
        tmpdir = Path(tmp)
        onnx_local = tmpdir / RAW_ONNX_FILENAME
        summary_local = tmpdir / SUMMARY_FILENAME

        download_file_from_s3(artifact_plan.onnx_model_s3_uri, onnx_local)
        download_file_from_s3(artifact_plan.summary_s3_uri, summary_local)

        with mlflow.start_run(run_name=training_result.feature_version):
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
                    "test_digest": feature_digest(test_df, ["trip_id", "as_of_ts"]),
                    "use_iam": str(USE_IAM).lower(),
                }
            )

            mlflow.log_metrics(_numeric_metrics(training_result.holdout_metrics))
            mlflow.log_metrics(
                {
                    "holdout_baseline_mae_seconds_capped": float(training_result.holdout_baseline_metrics["mae"]),
                    "holdout_baseline_rmse_seconds_capped": float(training_result.holdout_baseline_metrics["rmse"]),
                    "holdout_baseline_medae_seconds_capped": float(training_result.holdout_baseline_metrics["medae"]),
                }
            )

            mlflow.log_dict(training_result.as_dict(), "training_summary.json")
            mlflow.log_dict(artifact_plan.as_dict(), "artifact_plan.json")

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

            eval_result = mlflow.models.evaluate(
                model=model_info.model_uri,
                data=test_df,
                targets=LABEL_COLUMN,
                model_type="regressor",
            )

            mlflow.log_metrics(_numeric_metrics(eval_result.metrics))

            for artifact_name, artifact in eval_result.artifacts.items():
                artifact_uri = getattr(artifact, "uri", None)
                artifact_content = getattr(artifact, "content", None)
                if artifact_uri is not None:
                    mlflow.log_text(str(artifact_uri), f"evaluation/{artifact_name}.txt")
                elif artifact_content is not None:
                    mlflow.log_text(str(artifact_content), f"evaluation/{artifact_name}.txt")

            result = {
                "run_id": mlflow.active_run().info.run_id if mlflow.active_run() else None,
                "model_uri": model_info.model_uri,
                "artifact_plan": artifact_plan.as_dict(),
                "evaluation_metrics": _numeric_metrics(eval_result.metrics),
            }
            logger.info("mlflow run_id=%s", result["run_id"])
            return json.dumps(result, indent=2, default=str)


@workflow
def train(
    mlflow_experiment_name: str,
    train_num_threads: int,
    tuning_sample_rows: int,
    max_eval_rows: int,
    max_boost_rounds: int,
) -> str:
    training_result = train_model_task(
        train_num_threads=train_num_threads,
        tuning_sample_rows=tuning_sample_rows,
        max_boost_rounds=max_boost_rounds,
    )
    return evaluate_and_register_task(
        training_result=training_result,
        mlflow_experiment_name=mlflow_experiment_name,
        max_eval_rows=max_eval_rows,
    )


train_and_register_workflow = train


def main(argv: list[str] | None = None) -> int:
    _ = argv
    logger.info("workflow module loaded; execute the Flyte launch plan, not this file directly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
