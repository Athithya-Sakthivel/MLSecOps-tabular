from __future__ import annotations

from pathlib import Path

from flytekit import task
from flytekit.types.file import FlyteFile

from workflows.train.tasks.common import (
    GOLD_FEATURE_VERSION,
    GOLD_SCHEMA_VERSION,
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
    validate_gold_contract,
    validate_value_contracts,
    write_json,
)


def _stable_sort_dataframe(df):
    """
    Deterministically sort the dataframe before writing parquet.

    We sort by the configured timestamp column first, then by a small set of
    common contract columns when they exist. This reduces nondeterminism across
    repeated Flyte runs.
    """
    sort_columns = []

    for candidate in (
        TIMESTAMP_COLUMN,
        "trip_id",
        "as_of_date",
        "feature_version",
        "schema_version",
    ):
        if candidate in df.columns and candidate not in sort_columns:
            sort_columns.append(candidate)

    if not sort_columns:
        return df.reset_index(drop=True)

    return df.sort_values(sort_columns, kind="mergesort").reset_index(drop=True)


@task(
    cache=False,
    environment=build_task_environment(),
    retries=LIGHT_TASK_RETRIES,
    limits=LIGHT_TASK_LIMITS,
)
def load_gold(dataset_uri: str, output_path: str = "/tmp/gold_canonical.parquet") -> FlyteFile:
    """
    Read the Gold dataset, validate the frozen contract, canonicalize dtypes,
    and materialize a deterministic parquet snapshot with sidecar contract files.
    """
    log_json(msg="load_gold_start", dataset_uri=dataset_uri, output_path=output_path)

    raw_df = load_gold_frame(dataset_uri)
    validate_gold_contract(raw_df, strict_dtypes=False, label="Gold input frame")

    df = coerce_contract_dtypes(raw_df)
    validate_gold_contract(df, strict_dtypes=True, label="Gold canonical frame")
    validate_value_contracts(df)

    df = _stable_sort_dataframe(df)

    out_path = Path(output_path).expanduser()
    ensure_directory(out_path.parent)
    df.to_parquet(out_path, index=False)

    feature_spec = build_feature_spec()
    schema_hash = build_schema_hash(feature_spec)

    contract = build_contract_summary(
        dataset_uri=dataset_uri,
        row_count=len(df),
        dataframe=df,
        gold_table=GOLD_TRAINING_TABLE,
        source_silver_table=SOURCE_SILVER_TABLE,
        extra={
            "task": "load_gold",
            "output_path": str(out_path),
            "validated_columns": list(df.columns),
            "gold_feature_version": GOLD_FEATURE_VERSION,
            "gold_schema_version": GOLD_SCHEMA_VERSION,
            "schema_hash": schema_hash,
        },
    )

    feature_spec_path = artifact_sidecar_path(out_path, ".feature_spec.json")
    contract_path = artifact_sidecar_path(out_path, ".contract.json")

    write_json(feature_spec_path, feature_spec)
    write_json(contract_path, contract)

    log_json(
        msg="load_gold_success",
        dataset_uri=dataset_uri,
        output_path=str(out_path),
        row_count=len(df),
        schema_hash=schema_hash,
        feature_version=feature_spec["feature_version"],
        schema_version=feature_spec["schema_version"],
        contract_sidecar=str(contract_path),
        feature_spec_sidecar=str(feature_spec_path),
        gold_table=GOLD_TRAINING_TABLE,
        source_silver_table=SOURCE_SILVER_TABLE,
    )

    return FlyteFile(path=str(out_path))