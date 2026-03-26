from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from flytekit import Resources, task
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekitplugins.ray import HeadNodeConfig, RayJobConfig, WorkerNodeConfig

from tasks.common import (
    CATEGORICAL_FEATURES,
    DEFAULT_EARLY_STOPPING_ROUNDS,
    DEFAULT_FLAML_MAX_ITER,
    DEFAULT_FLAML_TIME_BUDGET_SECONDS,
    DEFAULT_NUM_BOOST_ROUND,
    DEFAULT_RANDOM_SEED,
    DEFAULT_SAMPLE_ROWS,
    DEFAULT_VALIDATION_FRACTION,
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    TIMESTAMP_COLUMN,
    assert_no_leakage_columns,
    build_feature_spec,
    coerce_contract_dtypes,
    compute_regression_metrics,
    ensure_directory,
    load_gold_frame,
    split_by_time,
    sample_frame,
    validate_required_columns,
    validate_value_contracts,
    write_json,
)


@task(
    cache=False,
    task_config=RayJobConfig(
        head_node_config=HeadNodeConfig(ray_start_params={"log-color": "True"}),
        worker_node_config=[
            WorkerNodeConfig(
                group_name="ml-workers",
                replicas=2,
                requests=Resources(cpu="2", mem="2Gi"),
                limits=Resources(cpu="2", mem="2Gi"),
            )
        ],
        enable_autoscaling=False,
        shutdown_after_job_finishes=True,
        ttl_seconds_after_finished=300,
    ),
)
def train_model(
    gold_dataset: FlyteFile,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    flaml_time_budget_seconds: int = DEFAULT_FLAML_TIME_BUDGET_SECONDS,
    flaml_max_iter: int = DEFAULT_FLAML_MAX_ITER,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    random_seed: int = DEFAULT_RANDOM_SEED,
    num_boost_round: int = DEFAULT_NUM_BOOST_ROUND,
    early_stopping_rounds: int = DEFAULT_EARLY_STOPPING_ROUNDS,
) -> FlyteDirectory:
    """Train a LightGBM regression model using FLAML and Ray Train."""

    from flaml import AutoML
    from flaml.automl.model import LGBMEstimator
    import ray
    from ray.train import ScalingConfig
    from ray.train.lightgbm import LightGBMTrainer, RayTrainReportCallback

    df = load_gold_frame(
        str(gold_dataset),
        columns=[*FEATURE_COLUMNS, LABEL_COLUMN, TIMESTAMP_COLUMN, "trip_id"],
    )
    validate_required_columns(df)
    assert_no_leakage_columns(df)
    df = coerce_contract_dtypes(df)
    validate_value_contracts(df)
    df = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

    split = split_by_time(df, validation_fraction=validation_fraction)
    train_df = split.train_df
    valid_df = split.valid_df
    if len(train_df) < 100 or len(valid_df) < 10:
        raise ValueError("Insufficient rows after chronological split for reliable training")

    search_df = sample_frame(train_df, max_rows=sample_rows, seed=random_seed)
    X_search = search_df[FEATURE_COLUMNS]
    y_search = search_df[LABEL_COLUMN]

    automl = AutoML()
    automl.fit(
        X_train=X_search,
        y_train=y_search,
        task="regression",
        metric="mae",
        estimator_list=["lgbm"],
        time_budget=flaml_time_budget_seconds,
        max_iter=flaml_max_iter,
        n_jobs=1,
        log_type="silent",
        seed=random_seed,
        model_history=False,
    )

    best_config = dict(automl.best_config)
    safe_best_config = {k: v for k, v in best_config.items() if not str(k).startswith("FLAML_")}
    flaml_estimator = LGBMEstimator(task="regression", **safe_best_config)
    original_params = dict(flaml_estimator.params)
    boost_rounds = int(original_params.pop("n_estimators", num_boost_round))
    if boost_rounds <= 0:
        boost_rounds = num_boost_round

    train_ray = ray.data.from_pandas(train_df[FEATURE_COLUMNS + [LABEL_COLUMN]])
    valid_ray = ray.data.from_pandas(valid_df[FEATURE_COLUMNS + [LABEL_COLUMN]])

    trainer = LightGBMTrainer(
        lambda config: None,
        train_loop_config={
            "params": original_params,
            "num_boost_round": boost_rounds,
            "early_stopping_rounds": early_stopping_rounds,
            "random_seed": random_seed,
        },
        datasets={"train": train_ray, "validation": valid_ray},
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    )
    result = trainer.fit()

    booster = RayTrainReportCallback.get_model(result.checkpoint)

    valid_features = valid_df[FEATURE_COLUMNS].copy()
    for col in CATEGORICAL_FEATURES:
        valid_features[col] = pd.to_numeric(valid_features[col], errors="raise").astype("int64")
    y_valid = valid_df[LABEL_COLUMN].to_numpy(dtype="float64")
    preds = booster.predict(valid_features, num_iteration=booster.best_iteration or None)
    metrics = compute_regression_metrics(y_valid, preds)
    metrics.update(
        {
            "best_iteration": int(getattr(booster, "best_iteration", 0) or 0),
            "train_rows": int(len(train_df)),
            "valid_rows": int(len(valid_df)),
            "flaml_time_budget_seconds": int(flaml_time_budget_seconds),
            "flaml_max_iter": int(flaml_max_iter),
            "cutoff_ts": split.cutoff_ts,
        }
    )

    out_dir = Path(tempfile.mkdtemp(prefix="flyte_lgbm_artifacts_"))
    ensure_directory(out_dir)

    booster.save_model(str(out_dir / "model.txt"))

    parity_sample = valid_df.sample(n=min(2048, len(valid_df)), random_state=random_seed).copy()
    parity_sample.to_parquet(out_dir / "validation_sample.parquet", index=False)

    feature_spec = build_feature_spec()
    write_json(out_dir / "feature_spec.json", feature_spec)
    write_json(out_dir / "best_config.json", best_config)
    write_json(out_dir / "lightgbm_params.json", original_params)
    write_json(out_dir / "metrics.json", metrics)
    write_json(
        out_dir / "manifest.json",
        {
            "dataset_source": str(gold_dataset),
            "timestamp_column": TIMESTAMP_COLUMN,
            "cutoff_ts": split.cutoff_ts,
            "feature_spec": feature_spec,
            "best_config": best_config,
            "lightgbm_params": original_params,
            "boost_rounds": boost_rounds,
            "random_seed": random_seed,
            "validation_fraction": validation_fraction,
            "ray_train_result_metrics": getattr(result, "metrics", {}),
        },
    )

    return FlyteDirectory(path=str(out_dir))