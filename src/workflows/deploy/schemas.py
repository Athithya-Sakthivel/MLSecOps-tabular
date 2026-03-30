from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import numpy as np


def coerce_instances(payload: Any) -> list[dict[str, Any]]:
    """
    Accept:
      - {"instances": [...]}  preferred
      - {"inputs": [...]}      backward-compatible alias
      - a raw list of objects
      - a single object dict
    """
    if isinstance(payload, dict):
        if "instances" in payload:
            instances = payload["instances"]
        elif "inputs" in payload:
            instances = payload["inputs"]
        else:
            instances = [payload]
    else:
        instances = payload

    if not isinstance(instances, list) or not instances:
        raise ValueError("Body must contain one or more instances")

    normalized: list[dict[str, Any]] = []
    for idx, row in enumerate(instances):
        if not isinstance(row, dict):
            raise ValueError(f"Instance at index {idx} must be an object")
        normalized.append(row)

    return normalized


def build_feature_matrix(
    instances: Sequence[dict[str, Any]],
    feature_order: Sequence[str],
    *,
    allow_extra_features: bool = False,
    dtype: Any = np.float32,
) -> np.ndarray:
    feature_set = set(feature_order)
    rows: list[list[float]] = []

    for row_idx, row in enumerate(instances):
        missing = [name for name in feature_order if name not in row]
        if missing:
            raise ValueError(f"Missing required features at row {row_idx}: {', '.join(missing)}")

        if not allow_extra_features:
            extra = [name for name in row.keys() if name not in feature_set]
            if extra:
                raise ValueError(f"Unexpected features at row {row_idx}: {', '.join(sorted(extra))}")

        values: list[float] = []
        for name in feature_order:
            value = row[name]
            if value is None:
                raise ValueError(f"Feature '{name}' is null at row {row_idx}")
            try:
                numeric = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Feature '{name}' must be numeric at row {row_idx}") from exc
            if not math.isfinite(numeric):
                raise ValueError(f"Feature '{name}' must be finite at row {row_idx}")
            values.append(numeric)

        rows.append(values)

    matrix = np.asarray(rows, dtype=dtype)
    if matrix.ndim != 2:
        matrix = np.asarray(rows, dtype=dtype).reshape(len(instances), -1)
    return matrix


def split_model_outputs(
    outputs: Sequence[Any],
    output_names: Sequence[str],
    row_count: int,
) -> list[dict[str, Any]]:
    if len(outputs) != len(output_names):
        raise ValueError("Output name count does not match model outputs")

    arrays = [np.asarray(output) for output in outputs]
    if row_count < 1:
        return []

    normalized: list[list[Any]] = [[] for _ in range(row_count)]

    for name, arr in zip(output_names, arrays, strict=True):
        if arr.ndim == 0:
            scalar = arr.item()
            for row in normalized:
                row.append((name, scalar))
            continue

        if arr.shape[0] != row_count:
            if arr.size == 1:
                scalar = arr.reshape(()).item()
                for row in normalized:
                    row.append((name, scalar))
                continue
            raise ValueError(f"Output '{name}' has batch dimension {arr.shape[0]}, expected {row_count}")

        for idx in range(row_count):
            value = arr[idx]
            normalized[idx].append((name, value.tolist() if hasattr(value, "tolist") else value))

    return [{key: value for key, value in row} for row in normalized]
