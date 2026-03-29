# src/workflows/ELT/workflows/elt_workflow.py
from __future__ import annotations

from flytekit import workflow

from workflows.ELT.tasks.bronze_ingest import BronzeIngestResult, bronze_ingest
from workflows.ELT.tasks.gold_features import GoldFeatureResult, gold_features
from workflows.ELT.tasks.silver_transform import SilverTransformResult, silver_transform

__all__ = ["elt_workflow"]


@workflow
def elt_workflow() -> GoldFeatureResult:
    bronze: BronzeIngestResult = bronze_ingest()
    silver: SilverTransformResult = silver_transform(bronze=bronze)
    return gold_features(silver=silver)