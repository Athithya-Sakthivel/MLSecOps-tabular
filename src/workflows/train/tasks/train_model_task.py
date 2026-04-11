from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import mlflow
import mlflow.pyfunc
import numpy as np
import onnxruntime as ort
import pandas as pd
from flytekit import Resources, task
from mlflow.models import infer_signature

from workflows.train.shared_utils import (
    LABEL_COLUMN,
    MATRIX_FEATURE_COLUMNS,
    MODEL_FAMILY,
    PREDICTION_COLUMN,
    PREPROCESSING_VERSION,
    TARGET_TRANSFORM,
    clip_seconds,
    download_file_from_s3,
    evenly_spaced_sample,
    from_log_target,
    load_iceberg_table,
    log_step,
    logger,
    ordered_columns_hash,
    read_table_as_dataframe,
    sha256_file,
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
SCHEMA_FILENAME = "schema.json"
METADATA_FILENAME = "metadata.json"
MANIFEST_FILENAME = "manifest.json"

FALLBACK_EVAL_SAMPLE_CAP = 250_000


def _json_safe(value: object) -> object:
    return json.loads(json.dumps(value, default=str))


def _require_dict(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be an object")
    return value


def _require_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{name} must be a non-empty string")
    return value.strip()


def _coerce_json_object(value: object, name: str) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        raw = json.loads(value)
        if not isinstance(raw, dict):
            raise RuntimeError(f"{name} must decode to a JSON object")
        return raw
    raise RuntimeError(f"{name} must be a JSON object or JSON string")


def _load_json_file(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"{path} must contain a JSON object")
    return raw


def _numeric_metrics(metrics: dict[str, object]) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, value in metrics.items():
        if isinstance(value, (int, float, np.floating)) and not isinstance(value, bool):
            out[key] = float(value)
    return out


def _validate_bundle_contract(
    *,
    schema_payload: dict[str, object],
    metadata_payload: dict[str, object],
    manifest_payload: dict[str, object],
    artifact_plan_root: str,
    model_version: str,
) -> None:
    if schema_payload.get("preprocessing_version") != PREPROCESSING_VERSION:
        raise RuntimeError(
            f"schema.json preprocessing_version must be {PREPROCESSING_VERSION!r}, got {schema_payload.get('preprocessing_version')!r}"
        )

    if schema_payload.get("target_transform") != TARGET_TRANSFORM:
        raise RuntimeError(
            f"schema.json target_transform must be {TARGET_TRANSFORM!r}, got {schema_payload.get('target_transform')!r}"
        )

    feature_order = schema_payload.get("feature_order")
    if not isinstance(feature_order, list) or feature_order != MATRIX_FEATURE_COLUMNS:
        raise RuntimeError("schema.json feature_order does not match the frozen matrix contract")

    if schema_payload.get("input_name") != "input":
        raise RuntimeError(
            f"schema.json input_name must be 'input', got {schema_payload.get('input_name')!r}"
        )

    output_names = schema_payload.get("output_names")
    if not isinstance(output_names, list) or not output_names:
        raise RuntimeError("schema.json output_names must be a non-empty list")

    if schema_payload.get("allow_extra_features") is not False:
        raise RuntimeError("schema.json allow_extra_features must be false")

    if metadata_payload.get("request_feature_order") != MATRIX_FEATURE_COLUMNS:
        raise RuntimeError("metadata.json request_feature_order does not match the frozen matrix contract")
    if metadata_payload.get("engineered_feature_order") != MATRIX_FEATURE_COLUMNS:
        raise RuntimeError("metadata.json engineered_feature_order does not match the frozen matrix contract")
    if metadata_payload.get("preprocessing_version") != PREPROCESSING_VERSION:
        raise RuntimeError(
            f"metadata.json preprocessing_version must be {PREPROCESSING_VERSION!r}, got {metadata_payload.get('preprocessing_version')!r}"
        )
    if metadata_payload.get("model_version") != model_version:
        raise RuntimeError(
            f"metadata.json model_version must be {model_version!r}, got {metadata_payload.get('model_version')!r}"
        )

    artifact_plan = _require_dict(metadata_payload.get("artifact_plan"), "metadata.json artifact_plan")
    if artifact_plan.get("artifact_root_s3_uri") != artifact_plan_root:
        raise RuntimeError(
            f"metadata.json artifact root mismatch: expected {artifact_plan_root!r}, got {artifact_plan.get('artifact_root_s3_uri')!r}"
        )

    if manifest_payload.get("source_uri") != artifact_plan_root:
        raise RuntimeError(
            f"manifest.json source_uri mismatch: expected {artifact_plan_root!r}, got {manifest_payload.get('source_uri')!r}"
        )
    if manifest_payload.get("format_version") != 1:
        raise RuntimeError("manifest.json format_version must be 1")


def _coerce_eval_features(model_input, feature_order: list[str]) -> pd.DataFrame:
    if isinstance(model_input, pd.DataFrame):
        df = model_input.copy()
    elif hasattr(model_input, "to_pandas"):
        df = model_input.to_pandas()
    else:
        df = pd.DataFrame(model_input)

    missing = [name for name in feature_order if name not in df.columns]
    if missing:
        raise ValueError(f"Missing model input columns: {missing}")

    out = df[feature_order].copy()

    for col_name in [
        "pickup_hour",
        "pickup_dow",
        "pickup_month",
        "pickup_is_weekend",
        "pickup_borough_id",
        "pickup_zone_id",
        "pickup_service_zone_id",
        "dropoff_borough_id",
        "dropoff_zone_id",
        "dropoff_service_zone_id",
        "route_pair_id",
    ]:
        values = pd.to_numeric(out[col_name], errors="raise")
        if values.isna().any():
            raise ValueError(f"{col_name} contains nulls")
        out[col_name] = values.astype("int32")

    for col_name in [
        "avg_duration_7d_zone_hour",
        "avg_fare_30d_zone",
        "trip_count_90d_zone_hour",
    ]:
        values = pd.to_numeric(out[col_name], errors="raise").astype("float32")
        if np.isinf(values.to_numpy(dtype="float32", copy=False)).any():
            raise ValueError(f"{col_name} contains infinite values")
        out[col_name] = values

    return out[feature_order]


class Log1pFrozenMatrixPyFuncModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context) -> None:
        schema_path = Path(context.artifacts["schema"])
        metadata_path = Path(context.artifacts["metadata"])
        manifest_path = Path(context.artifacts["manifest"])
        onnx_path = Path(context.artifacts["onnx_model"])

        logger.info(
            "loading pyfunc artifacts schema=%s metadata=%s manifest=%s onnx=%s",
            schema_path,
            metadata_path,
            manifest_path,
            onnx_path,
        )

        schema_payload = _load_json_file(schema_path)
        metadata_payload = _load_json_file(metadata_path)
        manifest_payload = _load_json_file(manifest_path)

        artifact_plan = _require_dict(metadata_payload.get("artifact_plan"), "metadata.json artifact_plan")
        _validate_bundle_contract(
            schema_payload=schema_payload,
            metadata_payload=metadata_payload,
            manifest_payload=manifest_payload,
            artifact_plan_root=str(artifact_plan["artifact_root_s3_uri"]),
            model_version=_require_text(metadata_payload.get("model_version"), "metadata.json model_version"),
        )

        if sha256_file(onnx_path) != manifest_payload.get("model_sha256"):
            raise RuntimeError("Downloaded ONNX checksum does not match manifest.json")
        if sha256_file(schema_path) != manifest_payload.get("schema_sha256"):
            raise RuntimeError("Downloaded schema checksum does not match manifest.json")
        if sha256_file(metadata_path) != manifest_payload.get("metadata_sha256"):
            raise RuntimeError("Downloaded metadata checksum does not match manifest.json")

        self._prediction_cap_seconds = float(metadata_payload["label_cap_seconds"])
        self._feature_order = list(schema_payload["feature_order"])

        self._session = ort.InferenceSession(
            path_or_bytes=str(onnx_path),
            providers=["CPUExecutionProvider"],
        )
        inputs = self._session.get_inputs()
        if not inputs:
            raise RuntimeError("ONNX session declares no inputs")
        self._input_name = inputs[0].name

        expected_input_name = str(schema_payload.get("input_name") or "input")
        if self._input_name != expected_input_name:
            raise RuntimeError(
                f"ONNX input name mismatch: expected {expected_input_name!r}, got {self._input_name!r}"
            )

        logger.info(
            "pyfunc artifacts loaded feature_order_hash=%s",
            ordered_columns_hash(self._feature_order),
        )

    def predict(self, context, model_input):
        features = _coerce_eval_features(model_input, self._feature_order)
        x = features.to_numpy(dtype=np.float32, copy=False)

        outputs = self._session.run(None, {self._input_name: x})
        if not outputs:
            raise RuntimeError("ONNX session returned no outputs")

        raw_pred = np.asarray(outputs[0], dtype=np.float32).reshape(-1)
        pred_seconds = from_log_target(raw_pred)
        pred_seconds = clip_seconds(pred_seconds, self._prediction_cap_seconds)

        return pd.DataFrame({PREDICTION_COLUMN: pred_seconds})


def _downsample_for_evaluation(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df.copy()
    return evenly_spaced_sample(df, max_rows)


@task(
    cache=False,
    retries=1,
    requests=Resources(cpu="1", mem="2Gi"),
    limits=Resources(cpu="2", mem="3Gi"),
)
def evaluate_and_register_task(
    training_result_json: str,
    mlflow_experiment_name: str,
    max_eval_rows: int,
) -> str:
    if max_eval_rows < 1:
        raise ValueError("max_eval_rows must be >= 1")

    training_result = _coerce_json_object(json.loads(training_result_json), "training_result_json")

    artifact_plan = _require_dict(training_result.get("artifact_plan"), "training_result_json.artifact_plan")
    bundle_contract = _coerce_json_object(training_result.get("bundle_contract"), "training_result_json.bundle_contract")
    bundle_metadata = _coerce_json_object(training_result.get("bundle_metadata"), "training_result_json.bundle_metadata")
    elt_contract = _require_dict(training_result.get("elt_contract"), "training_result_json.elt_contract")

    with log_step("reload_iceberg_table_for_evaluation"):
        table = load_iceberg_table(ICEBERG_CATALOG_NAME, ICEBERG_REST_URI, ICEBERG_WAREHOUSE)
        raw_df = read_table_as_dataframe(table)
        logger.info("evaluation dataset rows=%d cols=%d", len(raw_df), len(raw_df.columns))

    with log_step("validate_raw_dataframe_for_evaluation"):
        validate_raw_dataframe(raw_df)

    with log_step("split_holdout_for_evaluation"):
        splits = split_train_test_by_date(raw_df)
        test_df = _downsample_for_evaluation(splits.test, max_eval_rows)
        if len(test_df) != len(splits.test):
            logger.info(
                "downsampled evaluation set from %d to %d rows",
                len(splits.test),
                len(test_df),
            )

    eval_df = test_df[[*MATRIX_FEATURE_COLUMNS, LABEL_COLUMN]].copy()
    input_example = eval_df[MATRIX_FEATURE_COLUMNS].head(5).copy()
    prediction_example = pd.DataFrame(
        {PREDICTION_COLUMN: np.zeros(len(input_example), dtype=np.float32)}
    )
    signature = infer_signature(input_example, prediction_example)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(mlflow_experiment_name)

    logger.info(
        "mlflow configured tracking_uri=%s experiment=%s",
        MLFLOW_TRACKING_URI,
        mlflow_experiment_name,
    )
    logger.info(
        "artifact_plan root=%s model=%s schema=%s metadata=%s manifest=%s",
        artifact_plan["artifact_root_s3_uri"],
        artifact_plan["model_s3_uri"],
        artifact_plan["schema_s3_uri"],
        artifact_plan["metadata_s3_uri"],
        artifact_plan["manifest_s3_uri"],
    )

    with tempfile.TemporaryDirectory(prefix="trip_eta_eval_") as tmp:
        tmpdir = Path(tmp)
        onnx_local = tmpdir / RAW_ONNX_FILENAME
        schema_local = tmpdir / SCHEMA_FILENAME
        metadata_local = tmpdir / METADATA_FILENAME
        manifest_local = tmpdir / MANIFEST_FILENAME

        with log_step("download_bundle_from_s3"):
            download_file_from_s3(artifact_plan["model_s3_uri"], onnx_local, use_iam=USE_IAM)
            download_file_from_s3(artifact_plan["schema_s3_uri"], schema_local, use_iam=USE_IAM)
            download_file_from_s3(artifact_plan["metadata_s3_uri"], metadata_local, use_iam=USE_IAM)
            download_file_from_s3(artifact_plan["manifest_s3_uri"], manifest_local, use_iam=USE_IAM)

        schema_payload = _load_json_file(schema_local)
        metadata_payload = _load_json_file(metadata_local)
        manifest_payload = _load_json_file(manifest_local)

        _validate_bundle_contract(
            schema_payload=schema_payload,
            metadata_payload=metadata_payload,
            manifest_payload=manifest_payload,
            artifact_plan_root=str(artifact_plan["artifact_root_s3_uri"]),
            model_version=_require_text(training_result.get("model_version"), "training_result_json.model_version"),
        )

        if sha256_file(onnx_local) != manifest_payload.get("model_sha256"):
            raise RuntimeError("Downloaded ONNX checksum does not match manifest.json")
        if sha256_file(schema_local) != manifest_payload.get("schema_sha256"):
            raise RuntimeError("Downloaded schema checksum does not match manifest.json")
        if sha256_file(metadata_local) != manifest_payload.get("metadata_sha256"):
            raise RuntimeError("Downloaded metadata checksum does not match manifest.json")

        with log_step("mlflow_start_run_and_log"):
            with mlflow.start_run(run_name=_require_text(training_result.get("feature_version"), "training_result_json.feature_version")) as run:
                mlflow.set_tags(
                    {
                        "table_identifier": _require_text(training_result.get("table_identifier"), "training_result_json.table_identifier"),
                        "schema_version": _require_text(training_result.get("schema_version"), "training_result_json.schema_version"),
                        "feature_version": _require_text(training_result.get("feature_version"), "training_result_json.feature_version"),
                        "model_family": MODEL_FAMILY,
                        "target_transform": TARGET_TRANSFORM,
                    }
                )

                mlflow.log_params(
                    {
                        "table_identifier": _require_text(training_result.get("table_identifier"), "training_result_json.table_identifier"),
                        "schema_version": _require_text(training_result.get("schema_version"), "training_result_json.schema_version"),
                        "feature_version": _require_text(training_result.get("feature_version"), "training_result_json.feature_version"),
                        "preprocessing_version": _require_text(training_result.get("preprocessing_version"), "training_result_json.preprocessing_version"),
                        "model_family": MODEL_FAMILY,
                        "target_transform": TARGET_TRANSFORM,
                        "model_name": _require_text(training_result.get("model_name"), "training_result_json.model_name"),
                        "model_version": _require_text(training_result.get("model_version"), "training_result_json.model_version"),
                        "train_rows": int(training_result["train_rows"]),
                        "test_rows": int(training_result["test_rows"]),
                        "best_iteration_inner": int(training_result["best_iteration_inner"]),
                        "final_num_boost_round": int(training_result["final_num_boost_round"]),
                        "label_cap_seconds": float(training_result["label_cap_seconds"]),
                        "train_label_p50_seconds": float(training_result["train_label_p50_seconds"]),
                        "artifact_root_s3_uri": str(artifact_plan["artifact_root_s3_uri"]),
                        "onnx_model_s3_uri": str(artifact_plan["model_s3_uri"]),
                        "schema_s3_uri": str(artifact_plan["schema_s3_uri"]),
                        "metadata_s3_uri": str(artifact_plan["metadata_s3_uri"]),
                        "manifest_s3_uri": str(artifact_plan["manifest_s3_uri"]),
                        "request_feature_order_hash": ordered_columns_hash(
                            list(training_result["request_feature_columns"])
                        ),
                        "engineered_feature_order_hash": ordered_columns_hash(
                            list(training_result["engineered_feature_columns"])
                        ),
                        "schema_hash": str(elt_contract["schema_hash"]),
                        "source_silver_snapshot_id": str(elt_contract["source_silver_snapshot_id"]),
                        "use_iam": str(USE_IAM).lower(),
                    }
                )

                holdout_metrics = _require_dict(
                    training_result["holdout_metrics"],
                    "training_result_json.holdout_metrics",
                )
                mlflow.log_metrics(_numeric_metrics(holdout_metrics))

                holdout_baseline = _require_dict(
                    training_result["holdout_baseline_metrics"],
                    "training_result_json.holdout_baseline_metrics",
                )
                mlflow.log_metrics(
                    {
                        "holdout_baseline_mae_seconds_capped": float(holdout_baseline["mae"]),
                        "holdout_baseline_rmse_seconds_capped": float(holdout_baseline["rmse"]),
                        "holdout_baseline_medae_seconds_capped": float(holdout_baseline["medae"]),
                    }
                )

                mlflow.log_text(training_result_json, "training_result.json")
                mlflow.log_text(json.dumps(_json_safe(bundle_contract), indent=2, ensure_ascii=False), "bundle_contract.json")
                mlflow.log_text(json.dumps(_json_safe(bundle_metadata), indent=2, ensure_ascii=False), "bundle_metadata.json")
                mlflow.log_text(json.dumps(_json_safe(artifact_plan), indent=2, ensure_ascii=False), "artifact_plan.json")

                mlflow.log_artifact(str(onnx_local), artifact_path="bundle")
                mlflow.log_artifact(str(schema_local), artifact_path="bundle")
                mlflow.log_artifact(str(metadata_local), artifact_path="bundle")
                mlflow.log_artifact(str(manifest_local), artifact_path="bundle")

                logger.info("logging pyfunc model %s", PYFUNC_MODEL_NAME)
                model_info = mlflow.pyfunc.log_model(
                    name=PYFUNC_MODEL_NAME,
                    python_model=Log1pFrozenMatrixPyFuncModel(),
                    artifacts={
                        "onnx_model": str(onnx_local),
                        "schema": str(schema_local),
                        "metadata": str(metadata_local),
                        "manifest": str(manifest_local),
                    },
                    signature=signature,
                    input_example=input_example,
                )

                logger.info("running mlflow.models.evaluate on %d rows", len(eval_df))
                eval_result = mlflow.models.evaluate(
                    model=model_info.model_uri,
                    data=eval_df,
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
                    "run_id": run.info.run_id,
                    "model_uri": model_info.model_uri,
                    "artifact_plan": artifact_plan,
                    "evaluation_metrics": _numeric_metrics(eval_result.metrics),
                }

                logger.info(
                    "mlflow run complete run_id=%s model_uri=%s",
                    run.info.run_id,
                    model_info.model_uri,
                )
                return json.dumps(_json_safe(result), indent=2, ensure_ascii=False)