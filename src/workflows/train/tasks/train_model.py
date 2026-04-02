from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd
from flytekit import Resources, task
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flyteplugins.ray import HeadNodeConfig, RayJobConfig, WorkerNodeConfig

from workflows.train.tasks.common import (
    CATEGORICAL_FEATURES,
    DEFAULT_EARLY_STOPPING_ROUNDS,
    DEFAULT_FLAML_MAX_ITER,
    DEFAULT_FLAML_TIME_BUDGET_SECONDS,
    DEFAULT_MLFLOW_EXPERIMENT,
    DEFAULT_NUM_BOOST_ROUND,
    DEFAULT_RANDOM_SEED,
    DEFAULT_SAMPLE_ROWS,
    DEFAULT_VALIDATION_FRACTION,
    FEATURE_COLUMNS,
    GOLD_FEATURE_VERSION,
    GOLD_SCHEMA_VERSION,
    GOLD_TRAINING_TABLE,
    LABEL_COLUMN,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
    TRAIN_PROFILE,
    artifact_sidecar_path,
    build_contract_summary,
    build_feature_spec,
    build_schema_hash,
    build_task_environment,
    coerce_contract_dtypes,
    compute_regression_metrics,
    ensure_directory,
    load_gold_frame,
    log_json,
    prepare_model_input_frame,
    read_json_if_exists,
    sample_frame,
    split_by_time,
    validate_gold_contract,
    validate_value_contracts,
    write_json,
)

LightGBMParam = str | int | float | bool | list[str]


class TrainLoopConfig(TypedDict):
    params: dict[str, LightGBMParam]
    num_boost_round: int
    early_stopping_rounds: int
    feature_columns: list[str]
    categorical_features: list[str]
    label_column: str
    num_threads: int


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    value = int(os.environ.get(name, str(default)))
    return max(value, minimum)


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def _normalize_lgb_params(params: dict[str, Any]) -> dict[str, LightGBMParam]:
    normalized: dict[str, LightGBMParam] = {}
    for key, value in params.items():
        if isinstance(value, (str, int, float, bool)):
            normalized[str(key)] = value
        elif isinstance(value, list) and all(isinstance(item, str) for item in value):
            normalized[str(key)] = [str(item) for item in value]
        elif value is None:
            continue
        else:
            normalized[str(key)] = str(value)
    return normalized


def _resolve_boost_rounds(params: dict[str, Any], default_rounds: int) -> int:
    for key in ("n_estimators", "num_iterations", "num_boost_round", "num_rounds"):
        value = params.get(key)
        if value is not None:
            try:
                rounds = int(value)
                if rounds > 0:
                    return rounds
            except (TypeError, ValueError):
                continue
    return max(1, int(default_rounds))


def _require_columns(frame: pd.DataFrame, required_columns: list[str], *, label: str) -> None:
    missing = [col for col in required_columns if col not in frame.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def _coerce_worker_frame(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str],
    categorical_features: list[str],
    label_column: str,
) -> pd.DataFrame:
    coerced = frame.copy()

    for col in categorical_features:
        coerced[col] = pd.to_numeric(coerced[col], errors="raise").astype("int32").astype("category")

    for col in [c for c in feature_columns if c not in categorical_features]:
        coerced[col] = pd.to_numeric(coerced[col], errors="raise").astype("float64")

    coerced[label_column] = pd.to_numeric(coerced[label_column], errors="raise").astype("float64")
    return coerced


def _worker_train_loop(config: TrainLoopConfig) -> None:
    import lightgbm as lgb
    import ray
    from ray.train.lightgbm import RayTrainReportCallback, get_network_params

    train_shard = ray.train.get_dataset_shard("train")
    valid_shard = ray.train.get_dataset_shard("validation")
    if train_shard is None or valid_shard is None:
        raise RuntimeError("Ray dataset shards are unavailable inside the LightGBM worker")

    train_df = train_shard.materialize().to_pandas()
    valid_df = valid_shard.materialize().to_pandas()

    feature_columns = list(config["feature_columns"])
    categorical_features = list(config["categorical_features"])
    label_column = config["label_column"]
    num_boost_round = int(config["num_boost_round"])
    early_stopping_rounds = int(config["early_stopping_rounds"])
    params = dict(config["params"])

    _require_columns(train_df, [*feature_columns, label_column], label="train shard")
    _require_columns(valid_df, [*feature_columns, label_column], label="validation shard")

    train_df = _coerce_worker_frame(
        train_df,
        feature_columns=feature_columns,
        categorical_features=categorical_features,
        label_column=label_column,
    )
    valid_df = _coerce_worker_frame(
        valid_df,
        feature_columns=feature_columns,
        categorical_features=categorical_features,
        label_column=label_column,
    )

    train_set = lgb.Dataset(
        train_df[feature_columns],
        label=train_df[label_column],
        categorical_feature=categorical_features,
        free_raw_data=False,
    )
    valid_set = lgb.Dataset(
        valid_df[feature_columns],
        label=valid_df[label_column],
        categorical_feature=categorical_features,
        free_raw_data=False,
    )

    params.update(
        {
            "objective": "regression",
            "metric": ["l1", "rmse"],
            "verbosity": -1,
            "tree_learner": "data_parallel",
            "pre_partition": True,
            **get_network_params(),
        }
    )

    if "num_threads" not in params:
        params["num_threads"] = max(1, int(config["num_threads"]))

    lgb.train(
        params,
        train_set,
        valid_sets=[valid_set],
        valid_names=["validation"],
        num_boost_round=num_boost_round,
        callbacks=[
            RayTrainReportCallback(),
            lgb.early_stopping(early_stopping_rounds, verbose=False),
        ],
    )


if TRAIN_PROFILE == "prod":
    TASK_LIMITS = Resources(
        cpu=_env_str("TRAIN_TASK_CPU", "1000m"),
        mem=_env_str("TRAIN_TASK_MEM", "1024Mi"),
    )
    RAY_NUM_WORKERS = _env_int("RAY_NUM_WORKERS", 4, minimum=1)
    RAY_WORKER_CPU = _env_int("RAY_WORKER_CPU", 2, minimum=1)
    RAY_WORKER_MEM = _env_str("RAY_WORKER_MEM", "4Gi")
    FLAML_TIME_BUDGET_SECONDS = _env_int("FLAML_TIME_BUDGET_SECONDS", 900, minimum=60)
    FLAML_MAX_ITER = _env_int("FLAML_MAX_ITER", 100, minimum=10)
    SAMPLE_ROWS = _env_int("SAMPLE_ROWS", 100_000, minimum=100)
    NUM_BOOST_ROUND = _env_int("NUM_BOOST_ROUND", 3000, minimum=100)
    EARLY_STOPPING_ROUNDS = _env_int("EARLY_STOPPING_ROUNDS", 200, minimum=10)
    TASK_RETRIES = _env_int("TRAIN_TASK_RETRIES", 1, minimum=0)
else:
    TASK_LIMITS = Resources(
        cpu=_env_str("TRAIN_TASK_CPU", "500m"),
        mem=_env_str("TRAIN_TASK_MEM", "768Mi"),
    )
    RAY_NUM_WORKERS = _env_int("RAY_NUM_WORKERS", 2, minimum=1)
    RAY_WORKER_CPU = _env_int("RAY_WORKER_CPU", 1, minimum=1)
    RAY_WORKER_MEM = _env_str("RAY_WORKER_MEM", "2Gi")
    FLAML_TIME_BUDGET_SECONDS = _env_int("FLAML_TIME_BUDGET_SECONDS", 120, minimum=30)
    FLAML_MAX_ITER = _env_int("FLAML_MAX_ITER", 20, minimum=5)
    SAMPLE_ROWS = _env_int("SAMPLE_ROWS", 25_000, minimum=100)
    NUM_BOOST_ROUND = _env_int("NUM_BOOST_ROUND", 1500, minimum=100)
    EARLY_STOPPING_ROUNDS = _env_int("EARLY_STOPPING_ROUNDS", 100, minimum=10)
    TASK_RETRIES = _env_int("TRAIN_TASK_RETRIES", 1, minimum=0)


@task(
    cache=False,
    task_config=RayJobConfig(
        head_node_config=HeadNodeConfig(ray_start_params={"log-color": "True"}),
        worker_node_config=[
            WorkerNodeConfig(
                group_name="ml-workers",
                replicas=RAY_NUM_WORKERS,
                requests=Resources(cpu=str(RAY_WORKER_CPU), mem=RAY_WORKER_MEM),
                limits=Resources(cpu=str(RAY_WORKER_CPU), mem=RAY_WORKER_MEM),
            )
        ],
        enable_autoscaling=False,
        shutdown_after_job_finishes=True,
        ttl_seconds_after_finished=300,
    ),
    environment=build_task_environment(),
    retries=TASK_RETRIES,
    limits=TASK_LIMITS,
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
    """
    Train a LightGBM regression model against the frozen Gold contract and emit
    model + contract artifacts.
    """
    import ray
    from flaml import AutoML
    from flaml.automl.model import LGBMEstimator
    from ray.train import ScalingConfig
    from ray.train.lightgbm import LightGBMTrainer, RayTrainReportCallback

    run_id = os.environ.get("RUN_ID") or os.environ.get("FLYTE_INTERNAL_EXECUTION_ID") or uuid.uuid4().hex
    dataset_uri = str(gold_dataset)

    log_json(
        msg="train_model_start",
        run_id=run_id,
        train_profile=TRAIN_PROFILE,
        dataset_uri=dataset_uri,
        validation_fraction=validation_fraction,
        flaml_time_budget_seconds=flaml_time_budget_seconds,
        flaml_max_iter=flaml_max_iter,
        sample_rows=sample_rows,
        random_seed=random_seed,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        ray_num_workers=RAY_NUM_WORKERS,
        ray_worker_cpu=RAY_WORKER_CPU,
        ray_worker_mem=RAY_WORKER_MEM,
        gold_feature_version=GOLD_FEATURE_VERSION,
        gold_schema_version=GOLD_SCHEMA_VERSION,
    )

    current_feature_spec = build_feature_spec()
    current_schema_hash = build_schema_hash(current_feature_spec)

    expected_feature_spec_path = artifact_sidecar_path(dataset_uri, ".feature_spec.json")
    expected_contract_path = artifact_sidecar_path(dataset_uri, ".contract.json")

    current_dataset_feature_spec = read_json_if_exists(expected_feature_spec_path)
    if current_dataset_feature_spec is not None and current_dataset_feature_spec != current_feature_spec:
        raise ValueError("Gold feature_spec sidecar does not match the current training contract")

    current_dataset_contract = read_json_if_exists(expected_contract_path)
    if isinstance(current_dataset_contract, dict):
        contract_schema_hash = current_dataset_contract.get("schema_hash")
        if contract_schema_hash not in {None, current_schema_hash}:
            raise ValueError("Gold contract sidecar schema hash does not match the current training contract")

    raw_df = load_gold_frame(dataset_uri)
    _require_columns(raw_df, [*FEATURE_COLUMNS, LABEL_COLUMN, TIMESTAMP_COLUMN], label="Gold input frame")
    validate_gold_contract(raw_df, strict_dtypes=False, label="Gold input frame")

    df = coerce_contract_dtypes(raw_df)
    _require_columns(df, [*FEATURE_COLUMNS, LABEL_COLUMN, TIMESTAMP_COLUMN], label="Gold canonical frame")
    validate_gold_contract(df, strict_dtypes=True, label="Gold canonical frame")
    validate_value_contracts(df)

    df = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

    split = split_by_time(df, validation_fraction=validation_fraction)
    if len(split.train_df) < 100 or len(split.valid_df) < 10:
        raise ValueError("Insufficient rows after chronological split for reliable training")

    search_df = sample_frame(split.train_df, max_rows=sample_rows, seed=random_seed)
    _require_columns(search_df, [*FEATURE_COLUMNS, LABEL_COLUMN], label="sampled train frame")
    X_search = prepare_model_input_frame(search_df)
    y_search = search_df[LABEL_COLUMN].astype("float64")

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

    best_config = dict(automl.best_config or {})
    safe_best_config = {
        k: v for k, v in best_config.items() if not str(k).startswith("FLAML_")
    }
    flaml_estimator = LGBMEstimator(task="regression", **safe_best_config)
    worker_params = _normalize_lgb_params(flaml_estimator.params)

    boost_rounds = _resolve_boost_rounds(worker_params, num_boost_round)
    for key in ("n_estimators", "num_iterations", "num_boost_round", "num_rounds"):
        worker_params.pop(key, None)

    worker_params.setdefault("objective", "regression")
    worker_params.setdefault("verbosity", -1)
    worker_params.setdefault("metric", ["l1", "rmse"])
    worker_params["num_threads"] = max(1, RAY_WORKER_CPU)

    train_df = split.train_df[[*FEATURE_COLUMNS, LABEL_COLUMN]].copy()
    valid_df = split.valid_df[[*FEATURE_COLUMNS, LABEL_COLUMN]].copy()

    train_ray = ray.data.from_pandas(train_df)
    valid_ray = ray.data.from_pandas(valid_df)

    trainer = LightGBMTrainer(
        _worker_train_loop,
        train_loop_config={
            "params": worker_params,
            "num_boost_round": boost_rounds,
            "early_stopping_rounds": early_stopping_rounds,
            "feature_columns": list(FEATURE_COLUMNS),
            "categorical_features": list(CATEGORICAL_FEATURES),
            "label_column": LABEL_COLUMN,
            "num_threads": RAY_WORKER_CPU,
        },
        datasets={"train": train_ray, "validation": valid_ray},
        scaling_config=ScalingConfig(
            num_workers=RAY_NUM_WORKERS,
            resources_per_worker={"CPU": RAY_WORKER_CPU},
            use_gpu=False,
        ),
    )
    result = trainer.fit()
    if result.checkpoint is None:
        raise RuntimeError("Ray training did not produce a checkpoint")

    booster = RayTrainReportCallback.get_model(result.checkpoint)

    valid_features = prepare_model_input_frame(split.valid_df)
    y_valid = split.valid_df[LABEL_COLUMN].to_numpy(dtype="float64")
    preds = booster.predict(valid_features, num_iteration=booster.best_iteration or None)

    metrics = compute_regression_metrics(y_valid, preds)
    metrics.update(
        {
            "best_iteration": float(int(getattr(booster, "best_iteration", 0) or 0)),
            "train_rows": float(len(split.train_df)),
            "valid_rows": float(len(split.valid_df)),
            "flaml_time_budget_seconds": float(flaml_time_budget_seconds),
            "flaml_max_iter": float(flaml_max_iter),
            "cutoff_ts": str(split.cutoff_ts),
            "train_profile": TRAIN_PROFILE,
            "ray_num_workers": float(RAY_NUM_WORKERS),
            "ray_worker_cpu": float(RAY_WORKER_CPU),
            "feature_version": current_feature_spec["feature_version"],
            "schema_version": current_feature_spec["schema_version"],
            "schema_hash": current_schema_hash,
        }
    )

    out_dir = Path(tempfile.mkdtemp(prefix="flyte_lgbm_artifacts_"))
    ensure_directory(out_dir)

    booster.save_model(str(out_dir / "model.txt"))

    parity_sample = split.valid_df.sample(
        n=min(2048, len(split.valid_df)),
        random_state=random_seed,
    ).copy()
    parity_sample = parity_sample.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)
    parity_sample.to_parquet(out_dir / "validation_sample.parquet", index=False)

    contract_summary = build_contract_summary(
        dataset_uri=dataset_uri,
        row_count=len(df),
        dataframe=df,
        gold_table=GOLD_TRAINING_TABLE,
        source_silver_table=SOURCE_SILVER_TABLE,
        run_id=run_id,
        cutoff_ts=split.cutoff_ts,
        extra={
            "train_profile": TRAIN_PROFILE,
            "validation_fraction": validation_fraction,
            "train_rows": len(split.train_df),
            "valid_rows": len(split.valid_df),
            "sample_rows": sample_rows,
            "random_seed": random_seed,
            "flaml_time_budget_seconds": flaml_time_budget_seconds,
            "flaml_max_iter": flaml_max_iter,
            "num_boost_round": num_boost_round,
            "early_stopping_rounds": early_stopping_rounds,
            "ray_num_workers": RAY_NUM_WORKERS,
            "ray_worker_cpu": RAY_WORKER_CPU,
            "ray_worker_mem": RAY_WORKER_MEM,
            "feature_version": current_feature_spec["feature_version"],
            "schema_version": current_feature_spec["schema_version"],
            "schema_hash": current_schema_hash,
        },
    )

    manifest = {
        **contract_summary,
        "best_config": best_config,
        "lightgbm_params": worker_params,
        "boost_rounds": boost_rounds,
        "metrics": metrics,
        "ray_train_result_metrics": getattr(result, "metrics", {}),
        "dataset_source": dataset_uri,
        "feature_columns": list(FEATURE_COLUMNS),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "label_column": LABEL_COLUMN,
        "validation_sample_rows": len(parity_sample),
        "validation_sample_path": str(out_dir / "validation_sample.parquet"),
        "mlflow_experiment_name": DEFAULT_MLFLOW_EXPERIMENT,
    }

    write_json(out_dir / "feature_spec.json", current_feature_spec)
    write_json(out_dir / "contract.json", contract_summary)
    write_json(out_dir / "best_config.json", best_config)
    write_json(out_dir / "lightgbm_params.json", worker_params)
    write_json(out_dir / "metrics.json", metrics)
    write_json(
        out_dir / "runtime_config.json",
        {
            "run_id": run_id,
            "train_profile": TRAIN_PROFILE,
            "ray_num_workers": RAY_NUM_WORKERS,
            "ray_worker_cpu": RAY_WORKER_CPU,
            "ray_worker_mem": RAY_WORKER_MEM,
            "gold_feature_version": GOLD_FEATURE_VERSION,
            "gold_schema_version": GOLD_SCHEMA_VERSION,
        },
    )
    write_json(out_dir / "manifest.json", manifest)

    log_json(
        msg="train_model_success",
        run_id=run_id,
        schema_hash=current_schema_hash,
        best_iteration=int(metrics["best_iteration"]),
        train_rows=int(metrics["train_rows"]),
        valid_rows=int(metrics["valid_rows"]),
        output_dir=str(out_dir),
    )

    return FlyteDirectory(path=str(out_dir))