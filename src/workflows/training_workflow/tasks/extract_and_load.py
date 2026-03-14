
#!/usr/bin/env python3
import os
import sys
import time
import json
import logging
from typing import Optional, Dict, Any

import pyarrow as pa
import pandas as pd

LOG = logging.getLogger("extract_load_data")
LOG.setLevel(logging.INFO)
h = logging.StreamHandler(stream=sys.stdout)
h.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [h]


def jlog(**k):
    LOG.info(json.dumps(k, default=str))


TARGET_ROWS = int(os.environ.get("TARGET_ROWS", "5000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "auto")
RAY_OBJSTORE_PROPORTION = float(os.environ.get("RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION", "0.6"))
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:9001/iceberg")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", os.environ.get("GRAVITINO_ICEBERG_REST_WAREHOUSE", ""))
ICEBERG_REST_AUTH_TYPE = os.environ.get("ICEBERG_REST_AUTH_TYPE", "")
ICEBERG_REST_USER = os.environ.get("ICEBERG_REST_USER", "")
ICEBERG_REST_PASSWORD = os.environ.get("ICEBERG_REST_PASSWORD", "")
FALLBACK_S3_PREFIX = os.environ.get("FALLBACK_S3_PREFIX", "s3://e2e-mlops-data-681802563986/iceberg/warehouse/raw_fallback/")


def init_ray():
    jlog(msg="ray_init_start", address=RAY_ADDRESS)
    os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", os.environ.get("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0"))
    addr = (RAY_ADDRESS or "auto").strip()
    try:
        import ray
    except Exception as e:
        jlog(msg="ray_import_failed", error=str(e))
        raise
    if addr.lower() in ("", "local", "none"):
        os.environ.setdefault("RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION", str(RAY_OBJSTORE_PROPORTION))
        ray.init()
        jlog(msg="ray_init", mode="local", RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=os.environ.get("RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION"))
        return
    if addr.lower() == "auto":
        ray.init(address="auto")
        jlog(msg="ray_init", mode="connect_auto")
    else:
        ray.init(address=addr)
        jlog(msg="ray_init", mode="connect", address=addr)


def robust_sample_dataset(ds):
    jlog(msg="sample_start")
    try:
        try:
            sample_batch = ds.limit(1).take_batch()
        except Exception:
            sample_batch = None
        if sample_batch is not None:
            if isinstance(sample_batch, pd.DataFrame):
                jlog(msg="sample_obtained", method="take_batch (pandas)", rows=len(sample_batch))
                return sample_batch
            if isinstance(sample_batch, (list, tuple)):
                try:
                    df = pd.DataFrame(sample_batch)
                    jlog(msg="sample_obtained", method="take_batch->DataFrame", rows=len(df))
                    return df
                except Exception:
                    pass
            if isinstance(sample_batch, dict):
                df = pd.DataFrame([sample_batch])
                jlog(msg="sample_obtained", method="take_batch dict->df", rows=len(df))
                return df
            try:
                df = pd.DataFrame(sample_batch)
                jlog(msg="sample_obtained", method="take_batch fallback->df", rows=len(df))
                return df
            except Exception:
                pass
        try:
            sample = ds.take(1)
        except Exception as e:
            raise RuntimeError(f"take failed: {e}")
        if isinstance(sample, list):
            if not sample:
                raise RuntimeError("empty sample list")
            first = sample[0]
            if isinstance(first, pd.DataFrame):
                jlog(msg="sample_obtained", method="take[0] pandas", rows=len(first))
                return first
            if isinstance(first, dict):
                df = pd.DataFrame([first])
                jlog(msg="sample_obtained", method="take[0] dict->df", rows=len(df))
                return df
            try:
                df = pd.DataFrame(sample)
                jlog(msg="sample_obtained", method="take list->df", rows=len(df))
                return df
            except Exception as e:
                raise RuntimeError(f"could not convert sample list to DataFrame: {e}")
        if isinstance(sample, dict):
            df = pd.DataFrame([sample])
            jlog(msg="sample_obtained", method="take dict->df", rows=len(df))
            return df
        raise RuntimeError(f"unsupported sample type: {type(sample).__name__}")
    except Exception as e:
        jlog(msg="sample_failed", error=str(e))
        raise


def pa_schema_from_df(df: pd.DataFrame) -> pa.Schema:
    t = pa.Table.from_pandas(df, preserve_index=False)
    return t.schema


def load_iceberg_catalog(uri: str, warehouse: str):
    from pyiceberg.catalog import load_catalog

    props: Dict[str, Any] = {
        "uri": uri
    }

    if warehouse:
        props["warehouse"] = warehouse

    if ICEBERG_REST_AUTH_TYPE:
        props["auth"] = ICEBERG_REST_AUTH_TYPE

    if ICEBERG_REST_USER:
        props["username"] = ICEBERG_REST_USER

    if ICEBERG_REST_PASSWORD:
        props["password"] = ICEBERG_REST_PASSWORD

    redacted = dict(props)
    if "password" in redacted:
        redacted["password"] = "<redacted>"

    jlog(msg="load_catalog_start", props=redacted)

    catalog = load_catalog("rest", **props)

    jlog(msg="load_catalog_success", type=type(catalog).__name__)

    return catalog
    
def write_fallback_parquet(ds, prefix: str, run_id: str) -> str:
    ts = int(time.time())
    target = os.path.join(prefix.rstrip("/"), f"{run_id}/{ts}/")
    jlog(msg="fallback_write_start", path=target)
    ds.limit(TARGET_ROWS).write_parquet(target)
    jlog(msg="fallback_write_complete", path=target)
    return target


def main():
    run_id = os.environ.get("RUN_ID", f"run_{int(time.time())}")
    jlog(ts=time.time(), msg="job_start", run_id=run_id, pipeline_env=os.environ.get("PIPELINE_ENV", "prod"))
    try:
        init_ray()
    except Exception as e:
        jlog(msg="ray_init_failed", error=str(e))
        sys.exit(2)
    try:
        import ray.data as rd
    except Exception as e:
        jlog(msg="import_ray_data_failed", error=str(e))
        raise
    try:
        source_uri = os.environ.get("SOURCE_URI", "s3://e2e-mlops-data-681802563986/raw/transactions/")
        jlog(msg="read_source_start", source_type="parquet", uri=source_uri)
        ds = rd.read_parquet(source_uri)
        jlog(msg="read_source_registered")
        sample_df = robust_sample_dataset(ds)
        if sample_df is None or sample_df.shape[0] == 0:
            raise RuntimeError("no sample rows")
        pa_schema = pa_schema_from_df(sample_df)
        jlog(msg="schema_inferred", schema=str(pa_schema))
    except Exception as e:
        jlog(msg="etl_failed", phase="read_and_sample", error=str(e))
        raise

    auth_cfg = None
    if ICEBERG_REST_AUTH_TYPE:
        auth_cfg = {"type": ICEBERG_REST_AUTH_TYPE}
        if ICEBERG_REST_USER:
            auth_cfg["username"] = ICEBERG_REST_USER
        if ICEBERG_REST_PASSWORD:
            auth_cfg["password"] = ICEBERG_REST_PASSWORD

    try:
        jlog(msg="attempt_load_catalog", uri=ICEBERG_REST_URI, warehouse=ICEBERG_WAREHOUSE, auth_present=bool(auth_cfg))
        catalog = load_iceberg_catalog(ICEBERG_REST_URI, ICEBERG_WAREHOUSE, auth=auth_cfg)
        table_name = os.environ.get("ICEBERG_TABLE", "default.transactions")
        if "." not in table_name:
            raise RuntimeError(f"ICEBERG_TABLE must be namespace.table, got: {table_name}")
        namespace, table = table_name.split(".", 1)
        try:
            tbl = catalog.load_table(f"{namespace}.{table}")
            jlog(msg="table_loaded", table=f"{namespace}.{table}")
        except Exception as e_load:
            jlog(msg="table_load_failed", error=str(e_load))
            raise
        batch_iter = ds.limit(TARGET_ROWS).repartition(1).iter_batches(batch_format="pandas", batch_size=BATCH_SIZE)
        for batch in batch_iter:
            if isinstance(batch, pd.DataFrame):
                pa_table = pa.Table.from_pandas(batch, preserve_index=False)
            else:
                pa_table = pa.Table.from_pandas(pd.DataFrame(batch), preserve_index=False)
            try:
                try:
                    append_method = getattr(tbl, "append", None)
                    if callable(append_method):
                        append_method(pa_table)
                    else:
                        new_append = getattr(tbl, "new_append", None)
                        if callable(new_append):
                            new_append().append_table(pa_table).commit()
                        else:
                            raise RuntimeError("no supported append API on table object")
                    jlog(msg="append_success", rows=pa_table.num_rows)
                except Exception as e_app:
                    jlog(msg="append_failed", error=str(e_app))
                    raise
            except Exception:
                raise
        jlog(msg="etl_success", run_id=run_id)
    except Exception as e:
        jlog(msg="catalog_unavailable_falling_back", error=str(e))
        try:
            fallback_path = write_fallback_parquet(ds, FALLBACK_S3_PREFIX, run_id)
            jlog(msg="etl_success_fallback", fallback_path=fallback_path, run_id=run_id)
        except Exception as e2:
            jlog(msg="fallback_failed", error=str(e2))
            raise


if __name__ == "__main__":
    try:
        main()
    except Exception as fatal:
        jlog(msg="fatal", error=str(fatal))
        sys.exit(1)
        