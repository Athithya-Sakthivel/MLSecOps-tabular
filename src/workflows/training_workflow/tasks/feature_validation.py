import os
import logging
import pandas as pd
import great_expectations as ge
from flytekit import task, Resources
from flytekit.types.directory import FlyteDirectory

logger = logging.getLogger("feature_validation")
logger.setLevel(logging.INFO)

@task(cache=True, cache_version="v1", retries=0, requests=Resources(cpu="1", mem="2Gi"), limits=Resources(cpu="2", mem="4Gi"))
def feature_validation(features: FlyteDirectory, feature_schema_version: str = None) -> FlyteDirectory:
    local_dir = features.download()
    parquet_path = os.path.join(local_dir, "features.parquet")
    df = pd.read_parquet(parquet_path)
    ge_df = ge.from_pandas(df)
    r = ge_df.expect_column_values_to_be_between("x2", min_value=0, max_value=1e9)
    if not r.get("success", False):
        logger.error("Feature validation expectation failed")
        raise RuntimeError("feature validation failed")
    logger.info("Feature validation passed rows=%d", len(df))
    return FlyteDirectory(local_dir)