# src/workflows/ELT/workflows/iceberg_maintenance_workflow.py
from __future__ import annotations

from flytekit import workflow

from workflows.ELT.tasks.maintenance_optimize import MaintenanceResult, maintenance_optimize

__all__ = ["iceberg_maintenance_workflow"]


@workflow
def iceberg_maintenance_workflow() -> MaintenanceResult:
    return maintenance_optimize()
