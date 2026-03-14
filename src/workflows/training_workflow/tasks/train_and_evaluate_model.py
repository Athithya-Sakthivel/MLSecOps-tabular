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
