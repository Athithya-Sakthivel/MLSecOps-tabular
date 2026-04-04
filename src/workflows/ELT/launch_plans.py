from __future__ import annotations

from flytekit import CronSchedule, LaunchPlan, workflow

from workflows.ELT.tasks.maintenance_optimize import (
    MaintenanceResult,
    maintenance_optimize,
)
from workflows.ELT.workflows.elt_workflow import elt_workflow

__all__ = [
    "ELT_WORKFLOW_LP",
    "ELT_WORKFLOW_LP_NAME",
    "ICEBERG_MAINTENANCE_DAILY_LP",
    "ICEBERG_MAINTENANCE_DAILY_LP_NAME",
    "ICEBERG_MAINTENANCE_WEEKLY_LP",
    "ICEBERG_MAINTENANCE_WEEKLY_LP_NAME",
    "iceberg_maintenance_workflow",
]

"""
Maintenance behavior (from maintenance_optimize):

Tables:
  Bronze: BRONZE_TRIPS_TABLE, BRONZE_TAXI_ZONE_TABLE
  Silver: SILVER_TRIPS_TABLE
  Gold:   GOLD_TRAINING_TABLE, GOLD_CONTRACT_TABLE

Ops:
  Expire + Orphan cleanup: all tables
  Rewrite: GOLD_TRAINING_TABLE only (recent partitions, ~30d)

| Layer   | Expire | Orphan | Rewrite |
|---------|--------|--------|---------|
| Bronze  |   ✓    |   ✓    |   ✗     |
| Silver  |   ✓    |   ✓    |   ✗     |
| Gold    |   ✓    |   ✓    |   ✓     |
"""


# thin wrapper (no logic)
@workflow
def iceberg_maintenance_workflow() -> MaintenanceResult:
    return maintenance_optimize()


# manual ELT (no schedule)
ELT_WORKFLOW_LP = LaunchPlan.get_or_create(
    workflow=elt_workflow,
    name="elt_workflow_lp",
)
ELT_WORKFLOW_LP_NAME = ELT_WORKFLOW_LP.name


# daily: expire + orphan only (default behavior)
ICEBERG_MAINTENANCE_DAILY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_daily_lp",
    schedule=CronSchedule(schedule="30 2 * * *"),
    default_inputs={},
)
ICEBERG_MAINTENANCE_DAILY_LP_NAME = ICEBERG_MAINTENANCE_DAILY_LP.name


# weekly: includes rewrite (gold only)
ICEBERG_MAINTENANCE_WEEKLY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_weekly_lp",
    schedule=CronSchedule(schedule="30 3 * * 0"),
    default_inputs={},
)
ICEBERG_MAINTENANCE_WEEKLY_LP_NAME = ICEBERG_MAINTENANCE_WEEKLY_LP.name