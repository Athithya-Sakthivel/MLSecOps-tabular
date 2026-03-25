from __future__ import annotations

from flytekit import workflow

from workflows.ELT.tasks.bronze_ingest import BronzeIngestResult, bronze_ingest
from workflows.ELT.tasks.maintenance_optimize import MaintenanceResult, maintenance_optimize
from workflows.ELT.tasks.silver_transform import SilverTransformResult, silver_transform


@workflow
def elt_workflow() -> MaintenanceResult:
    bronze: BronzeIngestResult = bronze_ingest()
    silver: SilverTransformResult = silver_transform(bronze=bronze)
    return maintenance_optimize(bronze=bronze, silver=silver)