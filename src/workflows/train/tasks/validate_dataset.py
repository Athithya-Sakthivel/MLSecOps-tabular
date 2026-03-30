from __future__ import annotations

from pathlib import Path

from flytekit import task
from flytekit.types.file import FlyteFile
from tasks.common import (
    DEFAULT_VALIDATION_FRACTION,
    REQUIRED_COLUMNS,
    TIMESTAMP_COLUMN,
    assert_no_leakage_columns,
    build_feature_spec,
    coerce_contract_dtypes,
    ensure_directory,
    load_gold_frame,
    split_by_time,
    validate_required_columns,
    validate_value_contracts,
    write_json,
)


@task(cache=False)
def validate_dataset(
    gold_dataset: FlyteFile,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    output_path: str = "/tmp/gold_validated.parquet",
) -> FlyteFile:
    """Validate the Gold dataset against the frozen ML contract.

    This task intentionally does not perform feature engineering. It only validates,
    canonicalizes dtypes, and ensures a deterministic time ordering can be produced.
    """

    df = load_gold_frame(str(gold_dataset), columns=REQUIRED_COLUMNS)
    validate_required_columns(df)
    assert_no_leakage_columns(df)
    df = coerce_contract_dtypes(df)
    validate_value_contracts(df)

    # Enforce that the chronological split is possible; this catches degenerate datasets early.
    split = split_by_time(df, validation_fraction=validation_fraction)
    if split.train_df.empty or split.valid_df.empty:
        raise ValueError("Time split produced an empty train or validation partition")

    # The validated output is a canonical, ordered parquet snapshot.
    validated_df = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)
    out_path = Path(output_path)
    ensure_directory(out_path.parent)
    validated_df.to_parquet(out_path, index=False)

    write_json(out_path.with_suffix(".feature_spec.json"), build_feature_spec())
    write_json(
        out_path.with_suffix(".validation_report.json"),
        {
            "rows": len(validated_df),
            "train_rows": len(split.train_df),
            "valid_rows": len(split.valid_df),
            "cutoff_ts": split.cutoff_ts,
        },
    )

    return FlyteFile(path=str(out_path))
