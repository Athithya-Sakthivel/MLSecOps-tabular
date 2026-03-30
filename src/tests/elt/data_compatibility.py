#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import islice, tee
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_dataset

LOGGER = logging.getLogger("elt")

TRIPS_DATASET_ID = "koorukuroo/yellow_tripdata"
TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


@dataclass(frozen=True)
class ExtractedTable:
    name: str
    source: str
    path: Path
    row_count: int


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value == "" else int(value)


def get_hf_token() -> str | None:
    return os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_HUB_TOKEN") or None


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_streaming_parquet(rows: Iterable[dict], out_path: Path, chunk_size: int = 10_000) -> int:
    ensure_parent_dir(out_path)

    writer: pq.ParquetWriter | None = None
    batch: list[dict] = []
    total_rows = 0

    try:
        for row in rows:
            batch.append(dict(row))
            total_rows += 1

            if len(batch) >= chunk_size:
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    writer = pq.ParquetWriter(str(out_path), table.schema)
                writer.write_table(table)
                LOGGER.info("Wrote %d rows to %s", total_rows, out_path)
                batch.clear()

        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(str(out_path), table.schema)
            writer.write_table(table)
            LOGGER.info("Wrote %d rows to %s", total_rows, out_path)

        if total_rows == 0:
            LOGGER.warning("No rows read from source; nothing written: %s", out_path)

        return total_rows
    finally:
        if writer is not None:
            writer.close()


def preview_rows(rows: Iterable[dict], label: str, n: int = 2) -> None:
    LOGGER.info("Previewing first %d rows from %s", n, label)
    for i, row in enumerate(islice(rows, n), start=1):
        LOGGER.info("%s row %d: %s", label, i, dict(row))


def load_trips_stream(token: str | None):
    kwargs = {
        "split": "train",
        "streaming": True,
    }
    if token:
        kwargs["token"] = token
    return load_dataset(TRIPS_DATASET_ID, **kwargs)


def load_taxi_zone_lookup_stream(token: str | None):
    kwargs = {
        "split": "train",
        "streaming": True,
        "data_files": TAXI_ZONE_LOOKUP_URL,
    }
    if token:
        kwargs["token"] = token
    return load_dataset("csv", **kwargs)


def extract_trips(raw_dir: Path, token: str | None, max_rows: int) -> ExtractedTable:
    source_label = TRIPS_DATASET_ID
    out_path = raw_dir / "trips.parquet"

    LOGGER.info("Loading trips source: %s", source_label)
    ds = load_trips_stream(token)

    preview_iter, write_iter = tee(ds, 2)
    preview_rows(preview_iter, source_label, n=2)

    rows = islice(write_iter, max_rows) if max_rows > 0 else write_iter
    row_count = write_streaming_parquet(rows, out_path)

    return ExtractedTable(
        name="trips",
        source=source_label,
        path=out_path,
        row_count=row_count,
    )


def extract_taxi_zones(raw_dir: Path, token: str | None, max_rows: int) -> ExtractedTable:
    source_label = "nyc_taxi_zone_lookup"
    out_path = raw_dir / "taxi_zone_lookup.parquet"

    LOGGER.info("Loading taxi zone lookup source: %s", TAXI_ZONE_LOOKUP_URL)
    ds = load_taxi_zone_lookup_stream(token)

    preview_iter, write_iter = tee(ds, 2)
    preview_rows(preview_iter, source_label, n=2)

    rows = islice(write_iter, max_rows) if max_rows > 0 else write_iter
    row_count = write_streaming_parquet(rows, out_path)

    return ExtractedTable(
        name="taxi_zone_lookup",
        source=TAXI_ZONE_LOOKUP_URL,
        path=out_path,
        row_count=row_count,
    )


def load_parquet_copy(src: Path, dst: Path) -> int:
    ensure_parent_dir(dst)
    table = pq.read_table(src)
    pq.write_table(table, dst)
    LOGGER.info("Loaded %d rows: %s -> %s", table.num_rows, src, dst)
    return table.num_rows


def load_warehouse(raw_dir: Path, warehouse_dir: Path) -> tuple[Path, Path]:
    trips_src = raw_dir / "trips.parquet"
    zones_src = raw_dir / "taxi_zone_lookup.parquet"

    if not trips_src.exists():
        raise FileNotFoundError(f"Missing extracted trips file: {trips_src}")
    if not zones_src.exists():
        raise FileNotFoundError(f"Missing extracted taxi zone file: {zones_src}")

    trips_dst = warehouse_dir / "fact_trips.parquet"
    zones_dst = warehouse_dir / "dim_taxi_zones.parquet"

    load_parquet_copy(trips_src, trips_dst)
    load_parquet_copy(zones_src, zones_dst)

    return trips_dst, zones_dst


def validate_joinability(trips_path: Path, zones_path: Path) -> None:
    trips = pq.read_table(trips_path, columns=["PULocationID", "DOLocationID"])
    zones = pq.read_table(zones_path, columns=["LocationID"])

    trips_schema = set(trips.column_names)
    zones_schema = set(zones.column_names)

    required_trip_cols = {"PULocationID", "DOLocationID"}
    required_zone_cols = {"LocationID"}

    missing_trip_cols = required_trip_cols - trips_schema
    missing_zone_cols = required_zone_cols - zones_schema

    if missing_trip_cols:
        raise ValueError(f"Trips table is missing columns required for join: {sorted(missing_trip_cols)}")
    if missing_zone_cols:
        raise ValueError(f"Zone lookup table is missing columns required for join: {sorted(missing_zone_cols)}")

    LOGGER.info("Join keys verified: trips(PULocationID, DOLocationID) -> zones(LocationID)")


def run_etl(raw_dir: Path, warehouse_dir: Path, max_rows: int, token: str | None) -> None:
    LOGGER.info("Raw directory: %s", raw_dir)
    LOGGER.info("Warehouse directory: %s", warehouse_dir)
    LOGGER.info("MAX_ROWS_TO_EXTRACT_FROM_DATASETS=%d", max_rows)
    LOGGER.info("HF token present: %s", bool(token))

    trips = extract_trips(raw_dir, token, max_rows)
    LOGGER.info("Extracted trips: %s (%d rows)", trips.path, trips.row_count)

    zones = extract_taxi_zones(raw_dir, token, max_rows)
    LOGGER.info("Extracted taxi zones: %s (%d rows)", zones.path, zones.row_count)

    trips_dst, zones_dst = load_warehouse(raw_dir, warehouse_dir)
    validate_joinability(trips_dst, zones_dst)

    LOGGER.info("Load complete: %s and %s", trips_dst, zones_dst)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract and load NYC TLC trip data and taxi zone lookup.")
    parser.add_argument("--raw-dir", default="data/raw", help="Directory for extracted parquet files.")
    parser.add_argument("--warehouse-dir", default="data/warehouse", help="Directory for loaded parquet tables.")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=env_int("MAX_ROWS_TO_EXTRACT_FROM_DATASETS", 0),
        help="Cap rows extracted from each source. 0 means no cap.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level.",
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    token = get_hf_token()
    raw_dir = Path(args.raw_dir)
    warehouse_dir = Path(args.warehouse_dir)

    run_etl(raw_dir=raw_dir, warehouse_dir=warehouse_dir, max_rows=args.max_rows, token=token)


if __name__ == "__main__":
    main()
