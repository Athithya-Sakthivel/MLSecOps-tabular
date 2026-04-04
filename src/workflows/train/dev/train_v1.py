#!/usr/bin/env python3
from __future__ import annotations

import pandas as pd
import ray
from pyiceberg.catalog import load_catalog

RAY_ADDRESS = "172.31.24.31:6379"

ICEBERG_REST_URI = "http://127.0.0.1:8181"
ICEBERG_WAREHOUSE = "s3://e2e-mlops-data-681802563986/iceberg/warehouse/"

CATALOG_NAME = "default"
CATALOG_KWARGS = {
    "type": "rest",
    "uri": ICEBERG_REST_URI,
    "warehouse": ICEBERG_WAREHOUSE,
}

TABLE_NAMESPACE = ("gold",)
TABLE_IDENTIFIER = "gold.trip_training_matrix"
TABLE_IDENTIFIER_TUPLE = ("gold", "trip_training_matrix")
TABLE_METADATA_LOCATION = (
    "s3://e2e-mlops-data-681802563986/iceberg/warehouse/gold/"
    "trip_training_matrix/metadata/"
    "00000-80989b1b-ddfd-4a25-9cc4-48195708491c.metadata.json"
)

EXPECTED_SCHEMA_VERSION = "trip_eta_frozen_matrix_v1"
EXPECTED_FEATURE_VERSION = "trip_eta_lgbm_v1"

EXPECTED_COLUMNS = [
    "trip_id",
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
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
    "trip_count_90d_zone_hour",
    "label_trip_duration_seconds",
]

REQUIRED_NON_NULL_COLUMNS = [
    "trip_id",
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
    "label_trip_duration_seconds",
]

ALLOWED_NULL_COLUMNS = {
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
}


def load_rest_catalog():
    return load_catalog(CATALOG_NAME, **CATALOG_KWARGS)


def bootstrap_catalog() -> None:
    catalog = load_rest_catalog()

    namespaces = catalog.list_namespaces()
    if TABLE_NAMESPACE not in namespaces:
        catalog.create_namespace(TABLE_NAMESPACE)

    tables = catalog.list_tables(TABLE_NAMESPACE)
    if TABLE_IDENTIFIER_TUPLE not in tables:
        catalog.register_table(TABLE_IDENTIFIER_TUPLE, TABLE_METADATA_LOCATION)


def read_gold_dataset() -> ray.data.Dataset:
    return ray.data.read_iceberg(
        table_identifier=TABLE_IDENTIFIER,
        catalog_kwargs=CATALOG_KWARGS,
        override_num_blocks=12,
    )


def validate_gold_dataset(ds: ray.data.Dataset) -> pd.DataFrame:
    row_count = ds.count()
    if row_count <= 0:
        raise ValueError("Gold dataset is empty.")

    schema = ds.schema()
    if schema is None:
        raise ValueError("Ray could not infer a schema for the Iceberg dataset.")

    actual_columns = list(schema.names)
    if actual_columns != EXPECTED_COLUMNS:
        raise ValueError(
            "Gold schema mismatch.\n"
            f"Expected: {EXPECTED_COLUMNS}\n"
            f"Actual:   {actual_columns}"
        )

    rows = ds.take(row_count)
    df = pd.DataFrame.from_records(rows, columns=EXPECTED_COLUMNS)

    if list(df.columns) != EXPECTED_COLUMNS:
        raise ValueError("Column order changed after materialization.")

    df["as_of_ts"] = pd.to_datetime(df["as_of_ts"], utc=True, errors="raise")
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="raise").dt.date

    if df["schema_version"].nunique(dropna=False) != 1:
        raise ValueError("schema_version is not single-valued.")
    if df["schema_version"].iloc[0] != EXPECTED_SCHEMA_VERSION:
        raise ValueError(f"Unexpected schema_version: {df['schema_version'].iloc[0]!r}")

    if df["feature_version"].nunique(dropna=False) != 1:
        raise ValueError("feature_version is not single-valued.")
    if df["feature_version"].iloc[0] != EXPECTED_FEATURE_VERSION:
        raise ValueError(f"Unexpected feature_version: {df['feature_version'].iloc[0]!r}")

    if df["trip_id"].isna().any():
        raise ValueError("trip_id contains nulls.")
    if df["trip_id"].duplicated().any():
        raise ValueError("trip_id contains duplicates.")

    if df["label_trip_duration_seconds"].isna().any():
        raise ValueError("label_trip_duration_seconds contains nulls.")
    if (df["label_trip_duration_seconds"] <= 0).any():
        raise ValueError("label_trip_duration_seconds must be strictly positive.")

    for col in REQUIRED_NON_NULL_COLUMNS:
        if df[col].isna().any():
            raise ValueError(f"{col} contains nulls but is required to be non-null.")

    for col in df.columns:
        if col not in REQUIRED_NON_NULL_COLUMNS and col not in ALLOWED_NULL_COLUMNS:
            if df[col].isna().any():
                raise ValueError(f"{col} contains unexpected nulls.")

    if not df["pickup_hour"].between(0, 23).all():
        raise ValueError("pickup_hour must be in [0, 23].")
    if not df["pickup_dow"].between(1, 7).all():
        raise ValueError("pickup_dow must be in [1, 7].")
    if not df["pickup_month"].between(1, 12).all():
        raise ValueError("pickup_month must be in [1, 12].")
    if not df["pickup_is_weekend"].isin([0, 1]).all():
        raise ValueError("pickup_is_weekend must be 0 or 1.")

    encoded_id_cols = [
        "pickup_borough_id",
        "pickup_zone_id",
        "pickup_service_zone_id",
        "dropoff_borough_id",
        "dropoff_zone_id",
        "dropoff_service_zone_id",
        "route_pair_id",
    ]
    for col in encoded_id_cols:
        if (df[col] < 0).any():
            raise ValueError(f"{col} contains negative values.")

    df = df.sort_values(["as_of_ts", "trip_id"], kind="mergesort").reset_index(drop=True)

    if not df["as_of_ts"].is_monotonic_increasing:
        raise ValueError("Chronological ordering failed after sort.")

    return df


def main() -> None:
    ray.init(address=RAY_ADDRESS)

    bootstrap_catalog()
    ds = read_gold_dataset()
    validated_df = validate_gold_dataset(ds)

    print("GOLD TABLE VALIDATION PASSED")
    print(f"RAY_ADDRESS: {RAY_ADDRESS}")
    print(f"ICEBERG_REST_URI: {ICEBERG_REST_URI}")
    print(f"ICEBERG_WAREHOUSE: {ICEBERG_WAREHOUSE}")
    print(f"TABLE_IDENTIFIER: {TABLE_IDENTIFIER}")
    print(f"ROWS: {len(validated_df)}")
    print(f"SCHEMA_VERSION: {validated_df['schema_version'].iat[0]}")
    print(f"FEATURE_VERSION: {validated_df['feature_version'].iat[0]}")
    print(
        f"DATE_RANGE: {validated_df['as_of_date'].min()} -> {validated_df['as_of_date'].max()}"
    )
    print(
        f"TS_RANGE: {validated_df['as_of_ts'].min()} -> {validated_df['as_of_ts'].max()}"
    )
    print("SAMPLE_ROWS:")
    print(validated_df.head(3).to_string(index=False))


if __name__ == "__main__":
    main()