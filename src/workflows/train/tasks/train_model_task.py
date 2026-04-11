from __future__ import annotations

import os
import tempfile
from pathlib import Path

import lightgbm as lgb
import numpy as np
import onnx
import onnxruntime as ort
import pandas as pd
from flytekit import Resources, task

from workflows.train.shared_utils import (
    CANDIDATE_CONFIGS,
    EXPECTED_FEATURE_VERSION,
    EXPECTED_SCHEMA_VERSION,
    INNER_VALIDATION_FRACTION,
    LABEL_COLUMN,
    MATRIX_FEATURE_COLUMNS,
    MODEL_FAMILY,
    CandidateConfig,
    CandidateReport,
    ELTContract,
    TrainingResult,
    build_artifact_plan,
    build_category_levels,
    build_training_result,
    clip_seconds,
    compute_baseline_metrics,
    compute_metrics,
    evenly_spaced_sample,
    export_onnx_model,
    from_log_target,
    load_elt_contract,
    load_iceberg_table,
    log_step,
    logger,
    make_base_params,
    read_table_as_dataframe,
    sha256_file,
    split_by_date_fraction,
    split_train_test_by_date,
    table_snapshot_lineage,
    to_log_target,
    upload_file_to_s3,
    validate_raw_dataframe,
    write_json,
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
MODEL_ARTIFACTS_S3_BUCKET = os.environ.get(
    "MODEL_ARTIFACTS_S3_BUCKET",
    "s3://e2e-mlops-data-681802563986/model-artifacts",
)
USE_IAM = os.environ.get("USE_IAM", "0").strip().lower() in {"1", "true", "yes", "y", "on"}

MODEL_NAME = os.environ.get("MODEL_NAME", "trip_eta_lgbm")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")

RAW_ONNX_FILENAME = "model.onnx"
SCHEMA_FILENAME = "schema.json"
METADATA_FILENAME = "metadata.json"
MANIFEST_FILENAME = "manifest.json"

FALLBACK_EVAL_SAMPLE_CAP = 250_000
PARITY_SAMPLE_ROWS = 128
MAX_TRAIN_NUM_THREADS = 3


def _categorical_feature_names() -> list[str]:
    return [
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
    ]


def _validate_training_dataframe(df: pd.DataFrame, elt_contract: ELTContract) -> None:
    expected_columns = [
        "trip_id",
        "pickup_ts",
        "as_of_ts",
        "as_of_date",
        "schema_version",
        "feature_version",
        *MATRIX_FEATURE_COLUMNS,
        LABEL_COLUMN,
    ]

    if df.empty:
        raise ValueError("Gold dataset is empty.")

    if list(df.columns) != expected_columns:
        raise ValueError(
            "Gold schema mismatch.\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {list(df.columns)}"
        )

    if df["schema_version"].nunique(dropna=False) != 1:
        raise ValueError("schema_version is not single-valued.")
    if df["feature_version"].nunique(dropna=False) != 1:
        raise ValueError("feature_version is not single-valued.")
    if str(df["schema_version"].iloc[0]) != elt_contract.schema_version:
        raise ValueError(
            f"schema_version must be {elt_contract.schema_version!r}, got {df['schema_version'].iloc[0]!r}"
        )
    if str(df["feature_version"].iloc[0]) != elt_contract.feature_version:
        raise ValueError(
            f"feature_version must be {elt_contract.feature_version!r}, got {df['feature_version'].iloc[0]!r}"
        )

    as_of_ts_dates = pd.to_datetime(df["as_of_ts"], utc=True, errors="raise").dt.date
    as_of_date_dates = pd.to_datetime(df["as_of_date"], errors="raise").dt.date
    if not (as_of_ts_dates == as_of_date_dates).all():
        raise ValueError("as_of_date does not match as_of_ts.date for all rows.")

    required_non_null = [
        "trip_id",
        "pickup_ts",
        "as_of_ts",
        "as_of_date",
        "schema_version",
        "feature_version",
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
        "trip_count_90d_zone_hour",
        LABEL_COLUMN,
    ]
    for col_name in required_non_null:
        if df[col_name].isna().any():
            raise ValueError(f"{col_name} contains nulls but is required to be non-null.")

    for col_name in ["avg_duration_7d_zone_hour", "avg_fare_30d_zone"]:
        if col_name not in df.columns:
            raise ValueError(f"{col_name} is missing from the dataset.")

    if not pd.to_numeric(df["pickup_hour"], errors="coerce").between(0, 23).all():
        raise ValueError("pickup_hour must be in [0, 23].")
    if not pd.to_numeric(df["pickup_dow"], errors="coerce").between(1, 7).all():
        raise ValueError("pickup_dow must be in [1, 7].")
    if not pd.to_numeric(df["pickup_month"], errors="coerce").between(1, 12).all():
        raise ValueError("pickup_month must be in [1, 12].")
    if not pd.to_numeric(df["pickup_is_weekend"], errors="coerce").isin([0, 1]).all():
        raise ValueError("pickup_is_weekend must be 0 or 1.")
    if not (pd.to_numeric(df[LABEL_COLUMN], errors="coerce") > 0).all():
        raise ValueError("label_trip_duration_seconds must be strictly positive.")

    for col_name in [
        "pickup_borough_id",
        "pickup_zone_id",
        "pickup_service_zone_id",
        "dropoff_borough_id",
        "dropoff_zone_id",
        "dropoff_service_zone_id",
        "route_pair_id",
        "trip_count_90d_zone_hour",
    ]:
        if (pd.to_numeric(df[col_name], errors="coerce") < 0).any():
            raise ValueError(f"{col_name} contains negative values.")

    for col_name in ["avg_duration_7d_zone_hour", "avg_fare_30d_zone"]:
        series = pd.to_numeric(df[col_name], errors="coerce")
        finite = series.dropna().to_numpy(dtype="float64", copy=False)
        if np.isinf(finite).any():
            raise ValueError(f"{col_name} contains infinite values.")


def _prepare_matrix_frame(raw_df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in MATRIX_FEATURE_COLUMNS if col not in raw_df.columns]
    if missing:
        raise ValueError(f"Missing model input columns: {missing}")

    out = raw_df[MATRIX_FEATURE_COLUMNS].copy()

    for col_name in _categorical_feature_names():
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

    return out[MATRIX_FEATURE_COLUMNS]


def _fit_lgbm_candidate(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    candidate: CandidateConfig,
    label_cap_seconds: float,
    num_threads: int,
    max_boost_rounds: int,
) -> tuple[lgb.LGBMRegressor, int, dict[str, float]]:
    params = make_base_params(num_threads)
    params.update(
        {
            "learning_rate": float(candidate.learning_rate),
            "num_leaves": int(candidate.num_leaves),
            "max_depth": int(candidate.max_depth),
            "min_child_samples": int(candidate.min_child_samples),
            "subsample": float(candidate.subsample),
            "feature_fraction": float(candidate.feature_fraction),
            "reg_alpha": float(candidate.reg_alpha),
            "reg_lambda": float(candidate.reg_lambda),
        }
    )

    train_prepared = _prepare_matrix_frame(train_df)
    val_prepared = _prepare_matrix_frame(val_df)

    train_X = train_prepared[MATRIX_FEATURE_COLUMNS]
    val_X = val_prepared[MATRIX_FEATURE_COLUMNS]
    y_train_raw = pd.to_numeric(train_df[LABEL_COLUMN], errors="raise").astype("float32").to_numpy()
    y_val_raw = pd.to_numeric(val_df[LABEL_COLUMN], errors="raise").astype("float32").to_numpy()

    y_train = to_log_target(y_train_raw, label_cap_seconds)
    y_val = to_log_target(y_val_raw, label_cap_seconds)

    model = lgb.LGBMRegressor(**params, n_estimators=int(max_boost_rounds))
    model.fit(
        train_X,
        y_train,
        eval_set=[(val_X, y_val)],
        categorical_feature=_categorical_feature_names(),
        callbacks=[
            lgb.early_stopping(
                stopping_rounds=150,
                first_metric_only=True,
                verbose=False,
            ),
        ],
    )

    best_iteration = int(model.best_iteration_ or max_boost_rounds)
    val_pred_seconds = _predict_seconds_from_lgbm(
        model=model,
        df=val_df,
        best_iteration=best_iteration,
        label_cap_seconds=label_cap_seconds,
    )
    val_true_seconds = y_val_raw
    val_true_capped = clip_seconds(y_val_raw, label_cap_seconds)

    raw_metrics = compute_metrics(val_true_seconds, val_pred_seconds)
    capped_metrics = compute_metrics(val_true_capped, val_pred_seconds)

    metrics = {
        "mae_seconds_raw": raw_metrics["mae"],
        "rmse_seconds_raw": raw_metrics["rmse"],
        "medae_seconds_raw": raw_metrics["medae"],
        "mae_seconds_capped": capped_metrics["mae"],
        "rmse_seconds_capped": capped_metrics["rmse"],
        "medae_seconds_capped": capped_metrics["medae"],
    }
    return model, best_iteration, metrics


def _predict_seconds_from_lgbm(
    model: lgb.LGBMRegressor,
    df: pd.DataFrame,
    best_iteration: int,
    label_cap_seconds: float,
) -> np.ndarray:
    features = _prepare_matrix_frame(df)
    preds_log = model.predict(features, num_iteration=best_iteration)
    preds_seconds = from_log_target(preds_log)
    return clip_seconds(preds_seconds, label_cap_seconds)


def _evaluate_model_local(
    model: lgb.LGBMRegressor,
    df: pd.DataFrame,
    best_iteration: int,
    label_cap_seconds: float,
) -> dict[str, float]:
    if df.empty:
        raise ValueError("Evaluation frame is empty.")

    y_true_raw = pd.to_numeric(df[LABEL_COLUMN], errors="raise").astype("float32").to_numpy()
    y_true_capped = clip_seconds(y_true_raw, label_cap_seconds)
    y_pred = _predict_seconds_from_lgbm(
        model=model,
        df=df,
        best_iteration=best_iteration,
        label_cap_seconds=label_cap_seconds,
    )

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


def _search_best_model_logged(
    train_eval_df: pd.DataFrame,
    label_cap_seconds: float,
    num_threads: int,
    tuning_sample_rows: int,
    max_boost_rounds: int,
) -> tuple[CandidateConfig, list[CandidateReport], dict[str, float], int]:
    search_df = train_eval_df.sort_values("as_of_ts").reset_index(drop=True)
    search_df = evenly_spaced_sample(
        search_df,
        max_rows=min(len(search_df), int(tuning_sample_rows)),
    )

    with log_step("split_search_train_val"):
        search_train_df, search_val_df, search_cutoff = split_by_date_fraction(search_df, 0.80)
        logger.info(
            "search frame rows=%d train_rows=%d val_rows=%d cutoff=%s",
            len(search_df),
            len(search_train_df),
            len(search_val_df),
            search_cutoff,
        )

    candidate_reports: list[CandidateReport] = []
    best_candidate: CandidateConfig | None = None
    best_metrics: dict[str, float] | None = None
    best_iteration = 0
    best_score = float("inf")

    for candidate in CANDIDATE_CONFIGS:
        logger.info("training candidate=%s", candidate.name)
        _model, candidate_best_iteration, metrics = _fit_lgbm_candidate(
            train_df=search_train_df,
            val_df=search_val_df,
            candidate=candidate,
            label_cap_seconds=label_cap_seconds,
            num_threads=num_threads,
            max_boost_rounds=max_boost_rounds,
        )

        score = float(metrics["mae_seconds_capped"])
        logger.info(
            "candidate=%s best_iteration=%d mae_capped=%.6f rmse_capped=%.6f medae_capped=%.6f",
            candidate.name,
            candidate_best_iteration,
            metrics["mae_seconds_capped"],
            metrics["rmse_seconds_capped"],
            metrics["medae_seconds_capped"],
        )

        candidate_reports.append(
            CandidateReport(
                name=candidate.name,
                params=candidate,
                best_iteration=int(candidate_best_iteration),
                metrics=metrics,
                score=score,
            )
        )

        if score < best_score:
            best_score = score
            best_candidate = candidate
            best_iteration = int(candidate_best_iteration)
            best_metrics = metrics

    if best_candidate is None or best_metrics is None:
        raise RuntimeError("Hyperparameter search failed to produce a valid candidate.")

    logger.info(
        "selected candidate=%s best_iteration=%d mae_capped=%.6f",
        best_candidate.name,
        best_iteration,
        best_metrics["mae_seconds_capped"],
    )

    return best_candidate, candidate_reports, best_metrics, best_iteration


def _onnx_output_names(onnx_model: object) -> list[str]:
    graph = getattr(onnx_model, "graph", None)
    if graph is None:
        raise RuntimeError("Exported ONNX model does not expose a graph")
    output_names = [
        str(output.name).strip()
        for output in getattr(graph, "output", [])
        if str(output.name).strip()
    ]
    if not output_names:
        raise RuntimeError("Exported ONNX model does not declare any outputs")
    if len(set(output_names)) != len(output_names):
        raise RuntimeError("Exported ONNX model contains duplicate output names")
    return output_names


def _materialize_training_bundle(
    *,
    bundle_root: Path,
    training_result: TrainingResult,
    onnx_model: object,
) -> tuple[Path, Path, Path, Path]:
    bundle_root.mkdir(parents=True, exist_ok=True)

    model_path = bundle_root / RAW_ONNX_FILENAME
    schema_path = bundle_root / SCHEMA_FILENAME
    metadata_path = bundle_root / METADATA_FILENAME
    manifest_path = bundle_root / MANIFEST_FILENAME

    logger.info("materializing training bundle in %s", bundle_root)

    onnx.save_model(onnx_model, str(model_path))

    write_json(schema_path, training_result.bundle_contract)
    write_json(metadata_path, training_result.bundle_metadata)

    model_sha256 = sha256_file(model_path)
    schema_sha256 = sha256_file(schema_path)
    metadata_sha256 = sha256_file(metadata_path)

    manifest_payload = {
        "format_version": 1,
        "source_uri": training_result.artifact_plan.artifact_root_s3_uri,
        "model_version": training_result.model_version,
        "model_sha256": model_sha256,
        "schema_sha256": schema_sha256,
        "metadata_sha256": metadata_sha256,
    }
    write_json(manifest_path, manifest_payload)

    if sha256_file(model_path) != model_sha256:
        raise RuntimeError("model.onnx checksum validation failed after write")
    if sha256_file(schema_path) != schema_sha256:
        raise RuntimeError("schema.json checksum validation failed after write")
    if sha256_file(metadata_path) != metadata_sha256:
        raise RuntimeError("metadata.json checksum validation failed after write")

    return model_path, schema_path, metadata_path, manifest_path


def _verify_onnx_parity(
    *,
    final_model: lgb.LGBMRegressor,
    onnx_model_path: Path,
    sample_df: pd.DataFrame,
    best_iteration: int,
    tolerance: float = 1e-4,
) -> None:
    if sample_df.empty:
        return

    sample = sample_df.head(min(PARITY_SAMPLE_ROWS, len(sample_df))).copy()
    features = _prepare_matrix_frame(sample)
    x = features.to_numpy(dtype=np.float32, copy=False)

    native_log = np.asarray(
        final_model.predict(features, num_iteration=best_iteration),
        dtype=np.float32,
    ).reshape(-1)

    session = ort.InferenceSession(str(onnx_model_path), providers=["CPUExecutionProvider"])
    inputs = session.get_inputs()
    if not inputs:
        raise RuntimeError("ONNX session declares no inputs")
    onnx_output = session.run(None, {inputs[0].name: x})
    if not onnx_output:
        raise RuntimeError("ONNX session returned no outputs")

    onnx_log = np.asarray(onnx_output[0], dtype=np.float32).reshape(-1)
    if native_log.shape != onnx_log.shape:
        raise RuntimeError(
            f"ONNX parity shape mismatch: native={native_log.shape}, onnx={onnx_log.shape}"
        )

    max_abs_diff = float(np.max(np.abs(native_log - onnx_log))) if native_log.size else 0.0
    if max_abs_diff > tolerance:
        raise RuntimeError(
            f"ONNX parity check failed: max_abs_diff={max_abs_diff:.8f} tolerance={tolerance:.8f}"
        )

    logger.info(
        "onnx parity check passed max_abs_diff=%.8f sample_rows=%d",
        max_abs_diff,
        len(sample),
    )


@task(
    cache=False,
    retries=1,
    requests=Resources(cpu="2", mem="3Gi"),
    limits=Resources(cpu="3", mem="3Gi"),
)
def train_model_task(
    train_num_threads: int,
    tuning_sample_rows: int,
    max_boost_rounds: int,
) -> str:
    if train_num_threads < 1:
        raise ValueError("train_num_threads must be >= 1")
    if train_num_threads > MAX_TRAIN_NUM_THREADS:
        raise ValueError(
            f"train_num_threads must be <= {MAX_TRAIN_NUM_THREADS} to stay within quota and avoid oversubscription"
        )
    if tuning_sample_rows < 1:
        raise ValueError("tuning_sample_rows must be >= 1")
    if max_boost_rounds < 1:
        raise ValueError("max_boost_rounds must be >= 1")

    with log_step("load_elt_contract"):
        elt_contract = load_elt_contract(
            ICEBERG_CATALOG_NAME,
            ICEBERG_REST_URI,
            ICEBERG_WAREHOUSE,
        )
        if elt_contract.schema_version != EXPECTED_SCHEMA_VERSION:
            raise RuntimeError(
                f"Unexpected schema_version {elt_contract.schema_version!r}; expected {EXPECTED_SCHEMA_VERSION!r}"
            )
        if elt_contract.feature_version != EXPECTED_FEATURE_VERSION:
            raise RuntimeError(
                f"Unexpected feature_version {elt_contract.feature_version!r}; expected {EXPECTED_FEATURE_VERSION!r}"
            )
        if elt_contract.model_family != MODEL_FAMILY:
            raise RuntimeError(
                f"Unexpected model_family {elt_contract.model_family!r}; expected {MODEL_FAMILY!r}"
            )

    with log_step("load_iceberg_table"):
        table = load_iceberg_table(
            ICEBERG_CATALOG_NAME,
            ICEBERG_REST_URI,
            ICEBERG_WAREHOUSE,
        )

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

    train_eval_label_values = pd.to_numeric(
        train_eval_df[LABEL_COLUMN], errors="raise"
    ).astype("float32").to_numpy()
    label_cap_seconds = float(np.quantile(train_eval_label_values, 0.99))
    train_label_p50_seconds = float(
        np.median(np.clip(train_eval_label_values, 0.0, label_cap_seconds))
    )
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
        ) = _search_best_model_logged(
            train_eval_df=inner_train_df,
            label_cap_seconds=label_cap_seconds,
            num_threads=train_num_threads,
            tuning_sample_rows=tuning_sample_rows,
            max_boost_rounds=max_boost_rounds,
        )

    final_num_boost_round = int(max(50, best_iteration_inner))
    logger.info("final_num_boost_round=%d", final_num_boost_round)

    with log_step("train_final_model"):
        final_params = make_base_params(train_num_threads)
        final_params.update(
            {
                "learning_rate": float(best_candidate.learning_rate),
                "num_leaves": int(best_candidate.num_leaves),
                "max_depth": int(best_candidate.max_depth),
                "min_child_samples": int(best_candidate.min_child_samples),
                "subsample": float(best_candidate.subsample),
                "feature_fraction": float(best_candidate.feature_fraction),
                "reg_alpha": float(best_candidate.reg_alpha),
                "reg_lambda": float(best_candidate.reg_lambda),
            }
        )

        train_prepared = _prepare_matrix_frame(train_eval_df)
        train_X = train_prepared[MATRIX_FEATURE_COLUMNS]
        y_train_raw = pd.to_numeric(
            train_eval_df[LABEL_COLUMN], errors="raise"
        ).astype("float32").to_numpy()
        y_train = to_log_target(y_train_raw, label_cap_seconds)

        final_model = lgb.LGBMRegressor(**final_params, n_estimators=int(final_num_boost_round))
        final_model.fit(
            train_X,
            y_train,
            categorical_feature=_categorical_feature_names(),
        )

    with log_step("evaluate_inner_validation"):
        inner_metrics = _evaluate_model_local(
            model=final_model,
            df=inner_validation_df,
            best_iteration=final_num_boost_round,
            label_cap_seconds=label_cap_seconds,
        )
        logger.info(
            "inner validation rows=%d mae_capped=%.6f rmse_capped=%.6f medae_capped=%.6f",
            int(inner_metrics["rows"]),
            inner_metrics["mae_seconds_capped"],
            inner_metrics["rmse_seconds_capped"],
            inner_metrics["medae_seconds_capped"],
        )

    test_eval_df = (
        test_df
        if len(test_df) <= FALLBACK_EVAL_SAMPLE_CAP
        else evenly_spaced_sample(test_df, FALLBACK_EVAL_SAMPLE_CAP)
    )
    if len(test_eval_df) != len(test_df):
        logger.info(
            "downsampled holdout evaluation from %d to %d rows",
            len(test_df),
            len(test_eval_df),
        )

    with log_step("evaluate_holdout"):
        holdout_metrics = _evaluate_model_local(
            model=final_model,
            df=test_eval_df,
            best_iteration=final_num_boost_round,
            label_cap_seconds=label_cap_seconds,
        )
        holdout_baseline_metrics = compute_baseline_metrics(
            holdout_df=test_eval_df,
            train_eval_df=train_eval_df,
            label_cap_seconds=label_cap_seconds,
        )
        logger.info(
            "holdout rows=%d mae_capped=%.6f baseline_mae_capped=%.6f",
            int(holdout_metrics["rows"]),
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

    category_levels = build_category_levels(train_eval_df)

    with tempfile.TemporaryDirectory(prefix="trip_eta_bundle_") as tmp:
        bundle_root = Path(tmp)

        with log_step("export_onnx_model"):
            onnx_model = export_onnx_model(final_model, feature_count=len(MATRIX_FEATURE_COLUMNS))
            output_names = _onnx_output_names(onnx_model)

        training_result = build_training_result(
            elt_contract=elt_contract,
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
            model_name=MODEL_NAME,
            model_version=MODEL_VERSION,
            output_names=output_names,
        )

        with log_step("materialize_local_bundle"):
            model_path, schema_path, metadata_path, manifest_path = _materialize_training_bundle(
                bundle_root=bundle_root,
                training_result=training_result,
                onnx_model=onnx_model,
            )

        with log_step("onnx_parity_check"):
            _verify_onnx_parity(
                final_model=final_model,
                onnx_model_path=model_path,
                sample_df=test_eval_df,
                best_iteration=final_num_boost_round,
            )

        with log_step("upload_artifacts_to_s3"):
            upload_file_to_s3(model_path, artifact_plan.model_s3_uri, use_iam=USE_IAM)
            upload_file_to_s3(schema_path, artifact_plan.schema_s3_uri, use_iam=USE_IAM)
            upload_file_to_s3(metadata_path, artifact_plan.metadata_s3_uri, use_iam=USE_IAM)
            upload_file_to_s3(manifest_path, artifact_plan.manifest_s3_uri, use_iam=USE_IAM)

    logger.info("train_model_task finished successfully")
    return training_result.to_json()


__all__ = ["train_model_task"]