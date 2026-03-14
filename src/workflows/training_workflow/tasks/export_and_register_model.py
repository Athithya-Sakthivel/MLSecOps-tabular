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