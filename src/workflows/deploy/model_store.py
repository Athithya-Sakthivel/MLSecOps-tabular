from __future__ import annotations

import hashlib
import json
import math
import os
import posixpath
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fsspec
import onnxruntime as ort
from config import Settings


@dataclass(frozen=True)
class ModelSchema:
    feature_order: tuple[str, ...]
    input_name: str | None = None
    output_names: tuple[str, ...] = ()
    allow_extra_features: bool = False


@dataclass(frozen=True)
class ModelMetadata:
    model_name: str | None = None
    model_version: str | None = None
    sha256: str | None = None
    raw: dict[str, Any] | None = None


@dataclass(frozen=True)
class LoadedModel:
    model_path: Path
    cache_dir: Path
    schema: ModelSchema
    metadata: ModelMetadata
    session: ort.InferenceSession
    input_name: str
    output_names: tuple[str, ...]


def _cache_key(settings: Settings) -> str:
    h = hashlib.sha256()
    h.update(settings.model_uri.encode("utf-8"))
    h.update(b"::")
    h.update(settings.model_version.encode("utf-8"))
    return h.hexdigest()[:16]


def _cache_dir(settings: Settings) -> Path:
    cache_dir = Path(settings.model_cache_dir) / _cache_key(settings)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _is_model_file_path(uri_path: str) -> bool:
    return uri_path.endswith(".onnx")


def _bundle_source_paths(fs: fsspec.AbstractFileSystem, path: str) -> tuple[str, str, str | None]:
    if _is_model_file_path(path) or fs.isfile(path):
        model_src = path
        root = posixpath.dirname(path)
    else:
        root = path.rstrip("/")
        model_src = posixpath.join(root, "model.onnx")

    schema_src = posixpath.join(root, "schema.json")
    metadata_src = posixpath.join(root, "metadata.json")
    return model_src, schema_src, metadata_src


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _copy_with_hash(src_fs: fsspec.AbstractFileSystem, src_path: str, dst_path: Path) -> str:
    hasher = hashlib.sha256()
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=dst_path.name, suffix=".tmp", dir=str(dst_path.parent))
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)

    try:
        with src_fs.open(src_path, "rb") as src, tmp_path.open("wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
                dst.write(chunk)
        tmp_path.replace(dst_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise

    return hasher.hexdigest()


def _read_text(src_fs: fsspec.AbstractFileSystem, src_path: str) -> str:
    with src_fs.open(src_path, "r") as f:
        return f.read()


def _maybe_read_json(src_fs: fsspec.AbstractFileSystem, src_path: str) -> dict[str, Any] | None:
    if not src_fs.exists(src_path):
        return None
    with src_fs.open(src_path, "r") as f:
        return json.load(f)


def load_model_bundle(settings: Settings) -> tuple[Path, ModelSchema, ModelMetadata]:
    """
    Materialize an immutable local bundle:
      bundle/
        model.onnx
        schema.json
        metadata.json   (optional)
    """
    cache_dir = _cache_dir(settings)
    model_path = cache_dir / "model.onnx"
    schema_path = cache_dir / "schema.json"
    metadata_path = cache_dir / "metadata.json"

    if model_path.exists() and schema_path.exists():
        if settings.model_sha256:
            existing_sha = _hash_file(model_path)
            if existing_sha.lower() != settings.model_sha256.lower():
                model_path.unlink(missing_ok=True)
                schema_path.unlink(missing_ok=True)
                metadata_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Cached model checksum mismatch for {settings.model_uri}: "
                    f"expected {settings.model_sha256}, got {existing_sha}"
                )
        schema_obj = json.loads(schema_path.read_text())
        metadata_obj = json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
        return model_path, _parse_schema(schema_obj, settings), _parse_metadata(metadata_obj)

    src_fs, src_path = fsspec.core.url_to_fs(settings.model_uri)
    model_src, schema_src, metadata_src = _bundle_source_paths(src_fs, src_path)

    if not src_fs.exists(model_src):
        raise FileNotFoundError(f"Model artifact not found: {settings.model_uri}")
    if not src_fs.exists(schema_src):
        raise FileNotFoundError(f"schema.json is required next to the model artifact: {schema_src}")

    model_sha256 = _copy_with_hash(src_fs, model_src, model_path)
    if settings.model_sha256 and model_sha256.lower() != settings.model_sha256.lower():
        model_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"MODEL_SHA256 mismatch for {settings.model_uri}: expected {settings.model_sha256}, got {model_sha256}"
        )

    schema_text = _read_text(src_fs, schema_src)
    schema_path.write_text(schema_text)
    metadata_obj = _maybe_read_json(src_fs, metadata_src)
    if metadata_obj is not None:
        metadata_path.write_text(json.dumps(metadata_obj, indent=2, sort_keys=True))

    schema = _parse_schema(json.loads(schema_text), settings)
    metadata = _parse_metadata(metadata_obj or {})
    return model_path, schema, metadata


def _parse_schema(raw: dict[str, Any], settings: Settings) -> ModelSchema:
    feature_order = raw.get("feature_order") or list(settings.feature_order)
    if not isinstance(feature_order, list) or not feature_order:
        raise RuntimeError("schema.json must define a non-empty feature_order array")

    input_name = raw.get("input_name") or settings.model_input_name
    output_names = raw.get("output_names") or list(settings.model_output_names)
    if output_names and not isinstance(output_names, list):
        raise RuntimeError("schema.json output_names must be a list when provided")

    allow_extra_features = bool(raw.get("allow_extra_features", settings.allow_extra_features))
    return ModelSchema(
        feature_order=tuple(str(name) for name in feature_order),
        input_name=str(input_name) if input_name else None,
        output_names=tuple(str(name) for name in output_names) if output_names else (),
        allow_extra_features=allow_extra_features,
    )


def _parse_metadata(raw: dict[str, Any]) -> ModelMetadata:
    return ModelMetadata(
        model_name=raw.get("model_name"),
        model_version=raw.get("model_version"),
        sha256=raw.get("sha256"),
        raw=raw or {},
    )


def build_onnx_session(model_path: Path, settings: Settings) -> ort.InferenceSession:
    sess_options = ort.SessionOptions()
    sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    sess_options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL

    intra = settings.ort_intra_op_num_threads
    if intra <= 0:
        intra = max(1, math.ceil(settings.replica_num_cpus))

    inter = settings.ort_inter_op_num_threads
    if inter <= 0:
        inter = 1

    sess_options.intra_op_num_threads = intra
    sess_options.inter_op_num_threads = inter
    sess_options.log_severity_level = settings.ort_log_severity_level

    providers = list(settings.ort_providers) or ["CPUExecutionProvider"]
    return ort.InferenceSession(str(model_path), sess_options=sess_options, providers=providers)


def load_loaded_model(settings: Settings) -> LoadedModel:
    model_path, schema, metadata = load_model_bundle(settings)
    session = build_onnx_session(model_path, settings)

    input_name = schema.input_name or session.get_inputs()[0].name
    output_names = schema.output_names or tuple(output.name for output in session.get_outputs())

    return LoadedModel(
        model_path=model_path,
        cache_dir=model_path.parent,
        schema=schema,
        metadata=metadata,
        session=session,
        input_name=input_name,
        output_names=output_names,
    )
