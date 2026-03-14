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