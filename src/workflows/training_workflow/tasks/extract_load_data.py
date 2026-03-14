import os
import tempfile
import logging
from typing import Optional
import boto3
import botocore
import pandas as pd
from flytekit import task, Resources
from flytekit.types.file import FlyteFile

logger = logging.getLogger("extract_load_data")
logger.setLevel(logging.INFO)

@task(cache=True, cache_version="v1", retries=1, requests=Resources(cpu="1", mem="2Gi"), limits=Resources(cpu="1", mem="3Gi"))
def extract_load_data(start_date: str, end_date: str, raw_uri: str, feature_schema_version: Optional[str] = None) -> FlyteFile:
    s3 = boto3.client("s3")
    tmp = tempfile.NamedTemporaryFile(prefix="raw_snapshot_", suffix=".parquet", delete=False)
    outfile = tmp.name
    tmp.close()
    if raw_uri.startswith("s3://") and raw_uri.endswith("/"):
        bucket_key = raw_uri[len("s3://"):]
        bucket, prefix = bucket_key.split("/", 1)
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        objects = []
        for p in pages:
            for o in p.get("Contents", []):
                objects.append(o["Key"])
        if not objects:
            logger.error("No objects found at %s", raw_uri)
            raise RuntimeError("no raw objects found")
        paths = []
        for key in objects:
            local = tempfile.NamedTemporaryFile(prefix="raw_part_", suffix=".parquet", delete=False).name
            s3.download_file(bucket, key, local)
            paths.append(local)
        df_list = [pd.read_parquet(p) for p in paths]
        df = pd.concat(df_list, ignore_index=True)
        df.to_parquet(outfile, index=False)
    elif raw_uri.startswith("s3://"):
        uri_no = raw_uri[len("s3://"):]
        bucket, key = uri_no.split("/", 1)
        s3.download_file(bucket, key, outfile)
    else:
        if os.path.exists(raw_uri):
            df = pd.read_parquet(raw_uri)
            df.to_parquet(outfile, index=False)
        else:
            logger.error("raw_uri not found: %s", raw_uri)
            raise RuntimeError("raw_uri not found")
    logger.info("Wrote raw snapshot %s rows=%d", outfile, pd.read_parquet(outfile).shape[0])
    return FlyteFile(outfile)