import logging
import pandas as pd
import great_expectations as ge
from flytekit import task, Resources
from flytekit.types.file import FlyteFile

logger = logging.getLogger("raw_data_validation")
logger.setLevel(logging.INFO)

@task(cache=True, cache_version="v1", retries=0, requests=Resources(cpu="0.5", mem="1Gi"), limits=Resources(cpu="1", mem="2Gi"))
def raw_data_validation(raw: FlyteFile) -> FlyteFile:
    local = raw.download()
    df = pd.read_parquet(local)
    ge_df = ge.from_pandas(df)
    result = ge_df.validate()
    if not result["success"]:
        logger.error("Great Expectations raw data validation failed")
        raise RuntimeError("raw data validation failed")
    if df.duplicated().any():
        logger.error("Duplicate rows detected in raw data")
        raise RuntimeError("raw data validation failed: duplicates")
    logger.info("Raw data validation passed rows=%d", len(df))
    return FlyteFile(local)