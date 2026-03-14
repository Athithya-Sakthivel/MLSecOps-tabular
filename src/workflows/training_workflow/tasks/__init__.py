=== FILE: src/workflows/training_workflow/flyte_training_workflow.py
from flytekit import workflow
from flytekit.types.file import FlyteFile

from tasks.extract_load_data import extract_load_data
from tasks.raw_data_validation import raw_data_validation
from tasks.ray_data_preprocessing import ray_data_preprocessing
from tasks.feature_validation import feature_validation
from tasks.train_and_evaluate_model import train_and_evaluate_model
from tasks.export_and_register_model import export_and_register_model

DEFAULT_RAW_URI = "s3://your-bucket/path/to/raw/"
DEFAULT_FEATURE_SCHEMA = "v1"

@workflow
def training_workflow(start_date: str, end_date: str, raw_uri: str = DEFAULT_RAW_URI, feature_schema_version: str = DEFAULT_FEATURE_SCHEMA) -> FlyteFile:
    raw_snapshot = extract_load_data(start_date=start_date, end_date=end_date, raw_uri=raw_uri, feature_schema_version=feature_schema_version)
    validated_raw = raw_data_validation(raw=raw_snapshot)
    features_dir = ray_data_preprocessing(raw=validated_raw, feature_schema_version=feature_schema_version)
    validated_features = feature_validation(features=features_dir, feature_schema_version=feature_schema_version)
    model_file, onnx_file, eval_metric = train_and_evaluate_model(features=validated_features)
    metadata = export_and_register_model(model=model_file, onnx=onnx_file, evaluation_metric=eval_metric)
    return metadata

=== FILE: src/workflows/training_workflow/tasks/__init__.py
# intentionally empty to make tasks a package

=== FILE: src/workflows/training_workflow/tasks/extract_load_data.py
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

=== FILE: src/workflows/training_workflow/tasks/raw_data_validation.py
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

=== FILE: src/workflows/training_workflow/tasks/ray_data_preprocessing.py
import os
import tempfile
import logging
from flytekit import task, Resources
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekitplugins.ray import HeadNodeConfig, WorkerNodeConfig, RayJobConfig

logger = logging.getLogger("ray_data_preprocessing")
logger.setLevel(logging.INFO)

DEFAULT_RUNTIME_PIP = ["pandas==2.2.2", "pyarrow==23.0.1", "ray==2.54.0", "ray[data]==2.54.0"]

ray_runtime = RayJobConfig(
    head_node_config=HeadNodeConfig(ray_start_params={"log_to_driver": False}),
    worker_node_config=[WorkerNodeConfig(group_name="workers", replicas=2)],
    runtime_env={"pip": DEFAULT_RUNTIME_PIP},
    enable_autoscaling=False,
    shutdown_after_job_finishes=True,
    ttl_seconds_after_finished=300
)

@task(task_config=ray_runtime, interruptible=True, cache=True, cache_version="v1", requests=Resources(cpu="4", mem="16Gi"), limits=Resources(cpu="8", mem="32Gi"))
def ray_data_preprocessing(raw: FlyteFile, feature_schema_version: str = None) -> FlyteDirectory:
    local_raw = raw.download()
    import ray
    import ray.data as rd
    try:
        ray.init(address="auto")
    except Exception:
        ray.init(ignore_reinit_error=True)
    ds = rd.read_parquet(local_raw)
    def fe(df_batch):
        df_batch["x2"] = df_batch["x"] * 2.0
        return df_batch
    ds2 = ds.map_batches(fe)
    out_dir = tempfile.mkdtemp(prefix="features_")
    out_path = os.path.join(out_dir, "features.parquet")
    ds2.write_parquet(out_path)
    logger.info("Wrote features to %s", out_path)
    return FlyteDirectory(out_dir)

=== FILE: src/workflows/training_workflow/tasks/feature_validation.py


=== FILE: src/workflows/training_workflow/tasks/train_and_evaluate_model.py
import os
import tempfile
import logging
from typing import Tuple
from flytekit import task, Resources
from flytekit.types.file import FlyteFile
from flytekit.types.directory import FlyteDirectory
from flytekitplugins.ray import HeadNodeConfig, WorkerNodeConfig, RayJobConfig

logger = logging.getLogger("train_and_evaluate_model")
logger.setLevel(logging.INFO)

RUNTIME_PIP = [
    "flaml==2.5.0",
    "scikit-learn==1.7.2",
    "lightgbm==4.6.0",
    "joblib==1.4.2",
    "numpy==1.26.4",
    "pandas==2.2.2",
    "skl2onnx==1.20.1"
]

train_ray_runtime = RayJobConfig(
    head_node_config=HeadNodeConfig(ray_start_params={"log_to_driver": False}),
    worker_node_config=[WorkerNodeConfig(group_name="train-workers", replicas=3)],
    runtime_env={"pip": RUNTIME_PIP},
    enable_autoscaling=False,
    shutdown_after_job_finishes=True,
    ttl_seconds_after_finished=300
)

@task(task_config=train_ray_runtime, interruptible=True, cache=False, retries=2, timeout=3600*2, requests=Resources(cpu="8", mem="32Gi"), limits=Resources(cpu="16", mem="64Gi"))
def train_and_evaluate_model(features: FlyteDirectory) -> Tuple[FlyteFile, FlyteFile, float]:
    local_dir = features.download()
    parquet_path = os.path.join(local_dir, "features.parquet")
    import pandas as pd
    import joblib
    from flaml import AutoML
    from sklearn.metrics import r2_score
    from skl2onnx import convert_sklearn
    from skl2onnx.common.data_types import FloatTensorType
    df = pd.read_parquet(parquet_path)
    X = df[["x", "x2"]].to_numpy()
    y = df["y"].to_numpy()
    automl = AutoML()
    automl.fit(X_train=X, y_train=y, task="regression", time_budget=60)
    model = automl.model
    preds = model.predict(X)
    score = float(r2_score(y, preds))
    model_path = tempfile.NamedTemporaryFile(prefix="model_", suffix=".pkl", delete=False).name
    joblib.dump(model, model_path)
    initial_type = [("input", FloatTensorType([None, X.shape[1]]))]
    onnx_proto = convert_sklearn(model, initial_types=initial_type)
    onnx_path = tempfile.NamedTemporaryFile(prefix="model_", suffix=".onnx", delete=False).name
    with open(onnx_path, "wb") as f:
        f.write(onnx_proto.SerializeToString())
    logger.info("Training completed model=%s onnx=%s r2=%s", model_path, onnx_path, score)
    return FlyteFile(model_path), FlyteFile(onnx_path), score

=== FILE: src/workflows/training_workflow/tasks/export_and_register_model.py
import os
import json
import tempfile
import logging
from flytekit import task, Resources
from flytekit.types.file import FlyteFile
import mlflow

logger = logging.getLogger("export_and_register_model")
logger.setLevel(logging.INFO)

MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MLFLOW_MODEL_NAME", "training_model")
ARTIFACT_PATH = "model"

@task(cache=False, interruptible=False, retries=0, requests=Resources(cpu="0.5", mem="1Gi"), limits=Resources(cpu="1", mem="2Gi"))
def export_and_register_model(model: FlyteFile, onnx: FlyteFile, evaluation_metric: float) -> FlyteFile:
    local_model = model.download()
    local_onnx = onnx.download()
    mlflow.set_tracking_uri(MLFLOW_URI)
    run_name = f"flyte_train_{os.environ.get('FLYTE_EXECUTION_ID','local')}"
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_metric("r2", float(evaluation_metric))
        mlflow.log_artifact(local_model, artifact_path=ARTIFACT_PATH)
        mlflow.log_artifact(local_onnx, artifact_path="onnx")
        model_uri = f"runs:/{run.info.run_id}/{ARTIFACT_PATH}"
    try:
        mv = mlflow.register_model(model_uri, MODEL_NAME)
        registered_version = mv.version
    except Exception:
        registered_version = None
    meta = {
        "mlflow_run_id": run.info.run_id,
        "mlflow_artifact_uri": mlflow.get_artifact_uri(ARTIFACT_PATH),
        "mlflow_registered_version": registered_version,
        "r2": float(evaluation_metric)
    }
    meta_path = tempfile.NamedTemporaryFile(prefix="meta_", suffix=".json", delete=False).name
    with open(meta_path, "w") as f:
        json.dump(meta, f)
    logger.info("Registered model metadata written %s", meta_path)
    return FlyteFile(meta_path)