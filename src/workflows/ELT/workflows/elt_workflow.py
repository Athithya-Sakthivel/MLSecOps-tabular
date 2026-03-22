# export PYTHONPATH=/workspace/src
# kubectl -n default port-forward svc/iceberg-rest 19001:9001
from typing import Dict

from flytekit import workflow

from workflows.ELT.tasks.extract_load_task import extract_load_task


@workflow
def elt_workflow() -> Dict[str, str]:
    return extract_load_task()