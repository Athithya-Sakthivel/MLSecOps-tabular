#!/usr/bin/env python3
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
from flytekit import Resources, task

from workflows.train.shared_utils import (
    CANDIDATE_CONFIGS,
    EXPECTED_FEATURE_VERSION,
    INNER_VALIDATION_FRACTION,
    LABEL_COLUMN,
    MODEL_FEATURE_COLUMNS,
    TrainingResult,
    build_artifact_plan,
    build_category_levels,
    build_training_result,
    compute_baseline_metrics,
    evaluate_model,
    evenly_spaced_sample,
    export_onnx_model,
    fit_lgbm_candidate,
    load_iceberg_table,
    log_step,
    logger,
    read_table_as_dataframe,
    split_by_date_fraction,
    split_train_test_by_date,
    table_snapshot_lineage,
    train_final_model,
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

RAW_ONNX_FILENAME = "model.onnx"
SUMMARY_FILENAME = "training_summary.json"
MANIFEST_FILENAME = "manifest.json"
FALLBACK_EVAL_SAMPLE_CAP = 250_000


def _search_best_model_logged(
    train_eval_df: pd.DataFrame,
    label_cap_seconds: float,
    num_threads: int,
    tuning_sample_rows: int,
    max_boost_rounds: int,
) -> tuple[object, list[object], dict[str, float], int, dict[str, list[int]]]:
    search_df = train_eval_df.sort_values("as_of_ts").reset_index(drop=True)
    search_df = evenly_spaced_sample(search_df, max_rows=min(len(search_df), int(tuning_sample_rows)))

    with log_step("split_search_train_val"):
        search_train_df, search_val_df, search_cutoff = split_by_date_fraction(search_df, 0.80)
        logger.info(
            "search frame rows=%d train_rows=%d val_rows=%d cutoff=%s",
            len(search_df),
            len(search_train_df),
            len(search_val_df),
            search_cutoff,
        )

    category_levels = build_category_levels(search_train_df)
    logger.info("category levels built for %d categorical columns", len(category_levels))

    candidate_reports = []
    best_candidate = None
    best_metrics = None
    best_iteration = 0
    best_score = float("inf")

    for candidate in CANDIDATE_CONFIGS:
        logger.info("training candidate=%s", candidate.name)
        _model, candidate_best_iteration, metrics = fit_lgbm_candidate(
            train_df=search_train_df,
            val_df=search_val_df,
            candidate=candidate,
            label_cap_seconds=label_cap_seconds,
            num_threads=num_threads,
            category_levels=category_levels,
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
            {
                "name": candidate.name,
                "params": candidate,
                "best_iteration": int(candidate_best_iteration),
                "metrics": metrics,
                "score": score,
            }
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

    return best_candidate, candidate_reports, best_metrics, best_iteration, category_levels


def _materialize_training_bundle(
    training_result: TrainingResult,
    onnx_model: object,
) -> tuple[Path, Path, Path]:
    local_bundle = Path(tempfile.mkdtemp(prefix="trip_eta_lgbm_"))
    raw_onnx_path = local_bundle / RAW_ONNX_FILENAME
    summary_path = local_bundle / SUMMARY_FILENAME
    manifest_path = local_bundle / MANIFEST_FILENAME

    logger.info("materializing training bundle in %s", local_bundle)

    import onnx

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


@task(
    cache=False,
    retries=1,
    requests=Resources(cpu="2", mem="2Gi"),
    limits=Resources(cpu="4", mem="4Gi"),
)
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
        final_model = train_final_model(
            train_eval_df=train_eval_df,
            best_candidate=best_candidate,
            final_num_boost_round=final_num_boost_round,
            label_cap_seconds=label_cap_seconds,
            num_threads=train_num_threads,
            category_levels=category_levels,
        )

    with log_step("evaluate_inner_validation"):
        inner_metrics = evaluate_model(
            model=final_model,
            df=inner_validation_df,
            best_iteration=final_num_boost_round,
            label_cap_seconds=label_cap_seconds,
            category_levels=category_levels,
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
        holdout_metrics = evaluate_model(
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
        onnx_model = export_onnx_model(final_model, feature_count=len(MODEL_FEATURE_COLUMNS))
        raw_onnx_path, summary_path, manifest_path = _materialize_training_bundle(
            training_result=training_result,
            onnx_model=onnx_model,
        )
        upload_file_to_s3(raw_onnx_path, artifact_plan.onnx_model_s3_uri, use_iam=USE_IAM)
        upload_file_to_s3(summary_path, artifact_plan.summary_s3_uri, use_iam=USE_IAM)
        upload_file_to_s3(manifest_path, artifact_plan.manifest_s3_uri, use_iam=USE_IAM)

    logger.info("train_model_task finished successfully")
    return training_result