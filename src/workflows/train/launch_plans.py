from __future__ import annotations

from flytekit import LaunchPlan

from workflows.train.workflows.train import train

__all__ = [
    "TRAIN_WORKFLOW_LP",
    "TRAIN_WORKFLOW_LP_NAME",
]

TRAIN_WORKFLOW_LP = LaunchPlan.get_or_create(workflow=train)
TRAIN_WORKFLOW_LP_NAME = TRAIN_WORKFLOW_LP.name