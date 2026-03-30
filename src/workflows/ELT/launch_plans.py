# src/workflows/ELT/launch_plans.py
from __future__ import annotations

from flytekit import CronSchedule, LaunchPlan

from workflows.ELT.workflows.elt_workflow import elt_workflow
from workflows.ELT.workflows.iceberg_maintenance_workflow import iceberg_maintenance_workflow

__all__ = [
    "ELT_WORKFLOW_LP",
    "ELT_WORKFLOW_LP_NAME",
    "ICEBERG_MAINTENANCE_DAILY_LP",
    "ICEBERG_MAINTENANCE_DAILY_LP_NAME",
    "ICEBERG_MAINTENANCE_WEEKLY_LP",
    "ICEBERG_MAINTENANCE_WEEKLY_LP_NAME",
]

# Default manual entrypoint for ELT.
ELT_WORKFLOW_LP = LaunchPlan.get_or_create(workflow=elt_workflow)
ELT_WORKFLOW_LP_NAME = ELT_WORKFLOW_LP.name

# Daily maintenance runs at 02:30 UTC.
ICEBERG_MAINTENANCE_DAILY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_daily_lp",
    schedule=CronSchedule(schedule="30 2 * * *"),
)
ICEBERG_MAINTENANCE_DAILY_LP_NAME = ICEBERG_MAINTENANCE_DAILY_LP.name

# Weekly maintenance runs at 03:30 UTC on Sunday.
ICEBERG_MAINTENANCE_WEEKLY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_weekly_lp",
    schedule=CronSchedule(schedule="30 3 * * 0"),
)
ICEBERG_MAINTENANCE_WEEKLY_LP_NAME = ICEBERG_MAINTENANCE_WEEKLY_LP.name
