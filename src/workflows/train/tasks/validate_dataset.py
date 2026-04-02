from __future__ import annotations

from pathlib import Path

from flytekit import task
from flytekit.types.file import FlyteFile

from workflows.train.tasks.common import (
    DEFAULT_VALIDATION_FRACTION,
    GOLD_TRAINING_TABLE,
    LIGHT_TASK_LIMITS,
    LIGHT_TASK_RETRIES,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
    artifact_sidecar_path,
    build_contract_summary,
    build_feature_spec,
    build_schema_hash,
    build_task_environment,
    coerce_contract_dtypes,
    ensure_directory,
    load_gold_frame,
    log_json,
    split_by_time,
    validate_gold_contract,
    validate_value_contracts,
    write_json,
)


@task(
    cache=False,
    environment=build_task_environment(),
    retries=LIGHT_TASK_RETRIES,
    limits=LIGHT_TASK_LIMITS,
)
def validate_dataset(
    gold_dataset: FlyteFile,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    output_path: str = "/tmp/gold_validated.parquet",
) -> FlyteFile:
    """
    Validate the Gold dataset against the frozen ML contract, then emit a canonical,
    timestamp-ordered parquet snapshot and a validation sidecar.
    """
    if not 0.0 < validation_fraction < 1.0:
        raise ValueError(
            f"validation_fraction must be strictly between 0 and 1, got {validation_fraction}"
        )

    dataset_uri = str(gold_dataset)
    log_json(
        msg="validate_dataset_start",
        gold_dataset=dataset_uri,
        validation_fraction=validation_fraction,
        output_path=output_path,
    )

    raw_df = load_gold_frame(dataset_uri)
    validate_gold_contract(raw_df, strict_dtypes=False, label="Gold input frame")

    df = coerce_contract_dtypes(raw_df)
    validate_gold_contract(df, strict_dtypes=True, label="Gold canonical frame")
    validate_value_contracts(df)

    if TIMESTAMP_COLUMN not in df.columns:
        raise ValueError(f"Missing required timestamp column: {TIMESTAMP_COLUMN}")

    split = split_by_time(df, validation_fraction=validation_fraction)
    if split.train_df.empty or split.valid_df.empty:
        raise ValueError("Time split produced an empty train or validation partition")

    validated_df = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

    out_path = Path(output_path)
    ensure_directory(out_path.parent)
    validated_df.to_parquet(out_path, index=False)

    feature_spec = build_feature_spec()
    schema_hash = build_schema_hash(feature_spec)
    contract = build_contract_summary(
        dataset_uri=dataset_uri,
        row_count=len(validated_df),
        dataframe=validated_df,
        gold_table=GOLD_TRAINING_TABLE,
        source_silver_table=SOURCE_SILVER_TABLE,
        cutoff_ts=split.cutoff_ts,
        extra={
            "task": "validate_dataset",
            "validation_fraction": validation_fraction,
            "train_rows": len(split.train_df),
            "valid_rows": len(split.valid_df),
            "validated_columns": list(validated_df.columns),
            "output_path": str(out_path),
        },
    )

    feature_spec_path = artifact_sidecar_path(out_path, ".feature_spec.json")
    contract_path = artifact_sidecar_path(out_path, ".contract.json")
    validation_report_path = artifact_sidecar_path(out_path, ".validation_report.json")

    write_json(feature_spec_path, feature_spec)
    write_json(contract_path, contract)
    write_json(
        validation_report_path,
        {
            "dataset_uri": dataset_uri,
            "schema_hash": schema_hash,
            "feature_version": feature_spec["feature_version"],
            "schema_version": feature_spec["schema_version"],
            "rows": len(validated_df),
            "train_rows": len(split.train_df),
            "valid_rows": len(split.valid_df),
            "cutoff_ts": split.cutoff_ts,
            "timestamp_column": TIMESTAMP_COLUMN,
            "gold_table": GOLD_TRAINING_TABLE,
            "source_silver_table": SOURCE_SILVER_TABLE,
            "contract_sidecar": str(contract_path),
            "feature_spec_sidecar": str(feature_spec_path),
        },
    )

    log_json(
        msg="validate_dataset_success",
        gold_dataset=dataset_uri,
        output_path=str(out_path),
        row_count=len(validated_df),
        schema_hash=schema_hash,
        feature_version=feature_spec["feature_version"],
        schema_version=feature_spec["schema_version"],
        cutoff_ts=split.cutoff_ts,
        train_rows=len(split.train_df),
        valid_rows=len(split.valid_df),
    )

    return FlyteFile(path=str(out_path))