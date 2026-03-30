from __future__ import annotations

from pathlib import Path

from flytekit import task
from flytekit.types.file import FlyteFile
from tasks.common import (
    REQUIRED_COLUMNS,
    TIMESTAMP_COLUMN,
    assert_no_leakage_columns,
    build_feature_spec,
    coerce_contract_dtypes,
    ensure_directory,
    load_gold_frame,
    validate_required_columns,
    validate_value_contracts,
    write_json,
)


@task(cache=False)
def load_gold(dataset_uri: str, output_path: str = "/tmp/gold_canonical.parquet") -> FlyteFile:
    """Read the Gold dataset and materialize a canonical parquet snapshot.

    This task is intentionally narrow: it validates the contract and normalizes the
    exact training columns used downstream. It does not create features.
    """

    df = load_gold_frame(dataset_uri, columns=REQUIRED_COLUMNS)
    validate_required_columns(df)
    assert_no_leakage_columns(df)
    df = coerce_contract_dtypes(df)
    validate_value_contracts(df)

    out_path = Path(output_path)
    ensure_directory(out_path.parent)
    df.to_parquet(out_path, index=False)

    # Sidecar contract file for debugging and lineage inspection.
    write_json(out_path.with_suffix(".feature_spec.json"), build_feature_spec())
    write_json(
        out_path.with_suffix(".contract.json"),
        {
            "dataset_uri": dataset_uri,
            "columns": REQUIRED_COLUMNS,
            "timestamp_column": TIMESTAMP_COLUMN,
            "rows": len(df),
        },
    )

    return FlyteFile(path=str(out_path))
