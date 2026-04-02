from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import math
import os
import posixpath
import shutil
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from string import hexdigits
from typing import Any

import fsspec
import onnxruntime as ort
from config import Settings

_BUNDLE_FORMAT_VERSION = 1
_BUNDLE_MODEL_NAME = "model.onnx"
_BUNDLE_SCHEMA_NAME = "schema.json"
_BUNDLE_METADATA_NAME = "metadata.json"
_BUNDLE_MANIFEST_NAME = "manifest.json"
_CHUNK_SIZE = 1024 * 1024
_MAX_SCHEMA_BYTES = 1_048_576
_MAX_METADATA_BYTES = 1_048_576
_LOCK_TIMEOUT_SECONDS = 300

_LOG_LEVEL_ALIASES = {
    "WARN": "WARNING",
    "EXCEPTION": "ERROR",
}


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in {
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            }:
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _configure_logging() -> None:
    raw = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    level_name = _LOG_LEVEL_ALIASES.get(raw, raw)
    if level_name not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        level_name = "INFO"

    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())

    logging.basicConfig(
        level=getattr(logging, level_name),
        handlers=[handler],
        force=True,
    )


_configure_logging()
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ModelSchema:
    feature_order: tuple[str, ...]
    input_name: str | None = None
    output_names: tuple[str, ...] = ()
    allow_extra_features: bool = False


@dataclass(frozen=True, slots=True)
class ModelMetadata:
    model_name: str | None = None
    model_version: str | None = None
    sha256: str | None = None
    raw: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ModelBundleManifest:
    format_version: int
    source_uri: str
    model_version: str | None
    model_sha256: str
    schema_sha256: str
    metadata_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class LoadedModel:
    model_path: Path
    cache_dir: Path
    schema: ModelSchema
    metadata: ModelMetadata
    session: ort.InferenceSession
    input_name: str
    output_names: tuple[str, ...]


def _normalize_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field_name} is required and must be a sha256 hex string")

    normalized = value.strip().lower()
    if len(normalized) != 64 or any(ch not in hexdigits.lower() for ch in normalized):
        raise RuntimeError(f"{field_name} must be a 64-character lowercase hex sha256 digest")
    return normalized


def _optional_str(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field_name} must be a non-empty string when provided")
    return value.strip()


def _canonical_json_text(obj: dict[str, Any]) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _cache_key(model_sha256: str, schema_sha256: str, model_version: str | None) -> str:
    h = hashlib.sha256()
    h.update(model_sha256.encode("utf-8"))
    h.update(b"\0")
    h.update(schema_sha256.encode("utf-8"))
    h.update(b"\0")
    h.update((model_version or "").encode("utf-8"))
    return h.hexdigest()[:16]


def _cache_dir(settings: Settings, model_sha256: str, schema_sha256: str, model_version: str | None) -> Path:
    cache_dir = Path(settings.model_cache_dir) / _cache_key(model_sha256, schema_sha256, model_version)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _bundle_source_paths(fs: fsspec.AbstractFileSystem, path: str) -> tuple[str, str, str]:
    if path.endswith(".onnx") or fs.isfile(path):
        model_src = path
        root = posixpath.dirname(path)
    else:
        root = path.rstrip("/")
        model_src = posixpath.join(root, _BUNDLE_MODEL_NAME)

    schema_src = posixpath.join(root, _BUNDLE_SCHEMA_NAME)
    metadata_src = posixpath.join(root, _BUNDLE_METADATA_NAME)
    return model_src, schema_src, metadata_src


def _read_limited_text(src_fs: fsspec.AbstractFileSystem, src_path: str, max_bytes: int) -> str:
    with src_fs.open(src_path, "rb") as f:
        data = f.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise RuntimeError(f"{src_path} exceeds the configured size limit of {max_bytes} bytes")
    return data.decode("utf-8")


def _read_json_object_from_fs(
    src_fs: fsspec.AbstractFileSystem,
    src_path: str,
    *,
    max_bytes: int,
) -> dict[str, Any]:
    raw = json.loads(_read_limited_text(src_fs, src_path, max_bytes))
    if not isinstance(raw, dict):
        raise RuntimeError(f"{src_path} must contain a JSON object")
    return raw


def _read_json_object_from_path(src_path: Path) -> dict[str, Any]:
    raw = json.loads(src_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"{src_path} must contain a JSON object")
    return raw


def _atomic_write_text(dst_path: Path, text: str) -> None:
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{dst_path.name}.",
        suffix=".tmp",
        dir=str(dst_path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_text(text, encoding="utf-8")
        tmp_path.replace(dst_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _atomic_write_json(dst_path: Path, obj: dict[str, Any]) -> None:
    _atomic_write_text(dst_path, _canonical_json_text(obj))


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(_CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _copy_with_hash(src_fs: fsspec.AbstractFileSystem, src_path: str, dst_path: Path) -> str:
    hasher = hashlib.sha256()
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{dst_path.name}.",
        suffix=".tmp",
        dir=str(dst_path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)

    try:
        with src_fs.open(src_path, "rb") as src, tmp_path.open("wb") as dst:
            while True:
                chunk = src.read(_CHUNK_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)
                dst.write(chunk)
        tmp_path.replace(dst_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return hasher.hexdigest()


@contextlib.contextmanager
def _acquire_lock(lock_dir: Path, timeout_seconds: int = _LOCK_TIMEOUT_SECONDS) -> Iterator[None]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            lock_dir.mkdir(parents=False, exist_ok=False)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for bundle lock: {lock_dir}") from None
            time.sleep(0.2)

    try:
        yield
    finally:
        shutil.rmtree(lock_dir, ignore_errors=True)


def _parse_schema(raw: dict[str, Any]) -> ModelSchema:
    feature_order_raw = raw.get("feature_order")
    if not isinstance(feature_order_raw, list) or not feature_order_raw:
        raise RuntimeError("schema.json must define a non-empty feature_order array")

    feature_order: list[str] = []
    seen: set[str] = set()
    for idx, item in enumerate(feature_order_raw):
        if not isinstance(item, str) or not item.strip():
            raise RuntimeError(f"feature_order[{idx}] must be a non-empty string")
        name = item.strip()
        if name in seen:
            raise RuntimeError(f"feature_order contains duplicate feature name: {name}")
        seen.add(name)
        feature_order.append(name)

    input_name = _optional_str(raw.get("input_name"), "input_name")

    output_names_raw = raw.get("output_names")
    if output_names_raw is None:
        output_names: tuple[str, ...] = ()
    elif not isinstance(output_names_raw, list):
        raise RuntimeError("output_names must be a list when provided")
    else:
        outs: list[str] = []
        out_seen: set[str] = set()
        for idx, item in enumerate(output_names_raw):
            if not isinstance(item, str) or not item.strip():
                raise RuntimeError(f"output_names[{idx}] must be a non-empty string")
            name = item.strip()
            if name in out_seen:
                raise RuntimeError(f"output_names contains duplicate output name: {name}")
            out_seen.add(name)
            outs.append(name)
        output_names = tuple(outs)

    allow_extra_features = raw.get("allow_extra_features", False)
    if not isinstance(allow_extra_features, bool):
        raise RuntimeError("allow_extra_features must be a boolean")

    return ModelSchema(
        feature_order=tuple(feature_order),
        input_name=input_name,
        output_names=output_names,
        allow_extra_features=allow_extra_features,
    )


def _parse_metadata(raw: dict[str, Any] | None) -> ModelMetadata:
    if not raw:
        return ModelMetadata(raw={})

    model_name = raw.get("model_name")
    model_version = raw.get("model_version")
    sha256 = raw.get("sha256")

    return ModelMetadata(
        model_name=model_name if isinstance(model_name, str) else None,
        model_version=model_version if isinstance(model_version, str) else None,
        sha256=sha256 if isinstance(sha256, str) else None,
        raw=raw,
    )


def _manifest_dict(
    *,
    settings: Settings,
    model_sha256: str,
    schema_sha256: str,
    metadata_sha256: str | None,
) -> dict[str, Any]:
    return {
        "format_version": _BUNDLE_FORMAT_VERSION,
        "source_uri": str(settings.model_uri),
        "model_version": str(settings.model_version) if settings.model_version is not None else None,
        "model_sha256": model_sha256,
        "schema_sha256": schema_sha256,
        "metadata_sha256": metadata_sha256,
    }


def _load_manifest(path: Path) -> ModelBundleManifest:
    raw = _read_json_object_from_path(path)
    if raw.get("format_version") != _BUNDLE_FORMAT_VERSION:
        raise RuntimeError(f"Unsupported bundle format version: {raw.get('format_version')}")

    return ModelBundleManifest(
        format_version=int(raw["format_version"]),
        source_uri=str(raw["source_uri"]),
        model_version=raw.get("model_version") if raw.get("model_version") is not None else None,
        model_sha256=_normalize_sha256(raw["model_sha256"], "manifest.model_sha256"),
        schema_sha256=_normalize_sha256(raw["schema_sha256"], "manifest.schema_sha256"),
        metadata_sha256=(
            _normalize_sha256(raw["metadata_sha256"], "manifest.metadata_sha256")
            if raw.get("metadata_sha256")
            else None
        ),
    )


def _validate_bundle_files(cache_dir: Path) -> None:
    required = (
        cache_dir / _BUNDLE_MODEL_NAME,
        cache_dir / _BUNDLE_SCHEMA_NAME,
        cache_dir / _BUNDLE_MANIFEST_NAME,
    )
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Incomplete cached bundle: {missing}")


def _validate_manifest(
    manifest: ModelBundleManifest,
    *,
    settings: Settings,
    model_sha256: str,
    schema_sha256: str,
) -> None:
    expected_source_uri = str(settings.model_uri)
    expected_model_version = str(settings.model_version) if settings.model_version is not None else None

    if manifest.source_uri != expected_source_uri:
        raise RuntimeError(
            f"Cached manifest source_uri mismatch: expected {expected_source_uri}, got {manifest.source_uri}"
        )
    if manifest.model_version != expected_model_version:
        raise RuntimeError(
            f"Cached manifest model_version mismatch: expected {expected_model_version}, got {manifest.model_version}"
        )
    if manifest.model_sha256 != model_sha256:
        raise RuntimeError(
            f"Cached manifest model_sha256 mismatch: expected {model_sha256}, got {manifest.model_sha256}"
        )
    if manifest.schema_sha256 != schema_sha256:
        raise RuntimeError(
            f"Cached manifest schema_sha256 mismatch: expected {schema_sha256}, got {manifest.schema_sha256}"
        )


def _cleanup_bundle(cache_dir: Path) -> None:
    shutil.rmtree(cache_dir, ignore_errors=True)


def load_model_bundle(settings: Settings) -> tuple[Path, ModelSchema, ModelMetadata]:
    """
    Materialize an immutable local bundle:
      bundle/
        model.onnx
        schema.json
        metadata.json   (optional)
        manifest.json
    """
    model_sha256 = _normalize_sha256(settings.model_sha256, "model_sha256")

    src_fs, src_path = fsspec.core.url_to_fs(settings.model_uri)
    model_src, schema_src, metadata_src = _bundle_source_paths(src_fs, src_path)

    logger.info(
        "bundle.load.start",
        extra={
            "event": "bundle.load.start",
            "model_uri": str(settings.model_uri),
            "model_version": str(settings.model_version) if settings.model_version is not None else None,
            "source_model_path": model_src,
            "source_schema_path": schema_src,
        },
    )

    if not src_fs.exists(model_src):
        logger.error(
            "bundle.source_missing_model",
            extra={"event": "bundle.source_missing_model", "model_uri": str(settings.model_uri), "source_model_path": model_src},
        )
        raise FileNotFoundError(f"Model artifact not found: {settings.model_uri}")

    if not src_fs.exists(schema_src):
        logger.error(
            "bundle.source_missing_schema",
            extra={"event": "bundle.source_missing_schema", "model_uri": str(settings.model_uri), "source_schema_path": schema_src},
        )
        raise FileNotFoundError(f"schema.json is required next to the model artifact: {schema_src}")

    schema_raw = _read_json_object_from_fs(src_fs, schema_src, max_bytes=_MAX_SCHEMA_BYTES)
    schema = _parse_schema(schema_raw)
    schema_canonical = _canonical_json_text(schema_raw)
    schema_sha256 = _sha256_text(schema_canonical)

    metadata_raw: dict[str, Any] | None = None
    metadata_canonical_sha256: str | None = None
    if src_fs.exists(metadata_src):
        metadata_raw = _read_json_object_from_fs(src_fs, metadata_src, max_bytes=_MAX_METADATA_BYTES)
        metadata_canonical_sha256 = _sha256_text(_canonical_json_text(metadata_raw))

    cache_dir = _cache_dir(settings, model_sha256, schema_sha256, settings.model_version)
    lock_dir = cache_dir.parent / f".{cache_dir.name}.lock"
    model_path = cache_dir / _BUNDLE_MODEL_NAME
    schema_path = cache_dir / _BUNDLE_SCHEMA_NAME
    metadata_path = cache_dir / _BUNDLE_METADATA_NAME
    manifest_path = cache_dir / _BUNDLE_MANIFEST_NAME

    with _acquire_lock(lock_dir):
        if cache_dir.exists():
            try:
                _validate_bundle_files(cache_dir)
                manifest = _load_manifest(manifest_path)
                _validate_manifest(
                    manifest,
                    settings=settings,
                    model_sha256=model_sha256,
                    schema_sha256=schema_sha256,
                )

                cached_schema = _parse_schema(_read_json_object_from_path(schema_path))
                cached_metadata = _parse_metadata(
                    _read_json_object_from_path(metadata_path) if metadata_path.is_file() else None
                )

                cached_model_sha256 = _hash_file(model_path)
                if cached_model_sha256 != model_sha256:
                    raise RuntimeError(
                        f"Cached model checksum mismatch for {settings.model_uri}: "
                        f"expected {model_sha256}, got {cached_model_sha256}"
                    )

                logger.info(
                    "bundle.load.cache_hit",
                    extra={
                        "event": "bundle.load.cache_hit",
                        "cache_dir": str(cache_dir),
                        "model_sha256": model_sha256,
                        "schema_sha256": schema_sha256,
                    },
                )
                return model_path, cached_schema, cached_metadata
            except Exception:
                logger.exception(
                    "bundle.load.cache_invalid",
                    extra={
                        "event": "bundle.load.cache_invalid",
                        "cache_dir": str(cache_dir),
                        "model_uri": str(settings.model_uri),
                    },
                )
                _cleanup_bundle(cache_dir)

        cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "bundle.load.materialize_start",
            extra={
                "event": "bundle.load.materialize_start",
                "cache_dir": str(cache_dir),
                "model_uri": str(settings.model_uri),
                "model_sha256": model_sha256,
                "schema_sha256": schema_sha256,
            },
        )

        try:
            actual_model_sha256 = _copy_with_hash(src_fs, model_src, model_path)
            if actual_model_sha256 != model_sha256:
                logger.error(
                    "bundle.load.checksum_mismatch",
                    extra={
                        "event": "bundle.load.checksum_mismatch",
                        "cache_dir": str(cache_dir),
                        "model_uri": str(settings.model_uri),
                        "expected_sha256": model_sha256,
                        "actual_sha256": actual_model_sha256,
                    },
                )
                _cleanup_bundle(cache_dir)
                raise RuntimeError(
                    f"MODEL_SHA256 mismatch for {settings.model_uri}: "
                    f"expected {model_sha256}, got {actual_model_sha256}"
                )

            _atomic_write_text(schema_path, schema_canonical)
            if metadata_raw is not None:
                _atomic_write_json(metadata_path, metadata_raw)

            manifest = _manifest_dict(
                settings=settings,
                model_sha256=model_sha256,
                schema_sha256=schema_sha256,
                metadata_sha256=metadata_canonical_sha256,
            )
            _atomic_write_json(manifest_path, manifest)

            logger.info(
                "bundle.load.materialize_complete",
                extra={
                    "event": "bundle.load.materialize_complete",
                    "cache_dir": str(cache_dir),
                    "model_sha256": model_sha256,
                    "schema_sha256": schema_sha256,
                    "metadata_present": metadata_raw is not None,
                },
            )
            return model_path, schema, _parse_metadata(metadata_raw)
        except Exception:
            logger.exception(
                "bundle.load.materialize_failed",
                extra={
                    "event": "bundle.load.materialize_failed",
                    "cache_dir": str(cache_dir),
                    "model_uri": str(settings.model_uri),
                },
            )
            _cleanup_bundle(cache_dir)
            raise


def _normalize_providers(settings: Settings) -> list[Any]:
    raw_providers = list(getattr(settings, "ort_providers", None) or ["CPUExecutionProvider"])
    available = set(ort.get_available_providers())

    normalized: list[Any] = []
    for provider in raw_providers:
        if isinstance(provider, tuple):
            if len(provider) != 2:
                raise RuntimeError("Provider tuples must be of the form (name, options_dict)")
            name, options = provider
            if not isinstance(name, str) or not name.strip():
                raise RuntimeError("Provider name must be a non-empty string")
            if not isinstance(options, dict):
                raise RuntimeError(f"Provider options for {name} must be a dict")
            if name not in available:
                raise RuntimeError(
                    f"Requested provider '{name}' is not available in this onnxruntime build. "
                    f"Available providers: {tuple(ort.get_available_providers())}"
                )
            normalized.append((name, options))
        else:
            name = str(provider).strip()
            if not name:
                raise RuntimeError("Provider name must be a non-empty string")
            if name not in available:
                raise RuntimeError(
                    f"Requested provider '{name}' is not available in this onnxruntime build. "
                    f"Available providers: {tuple(ort.get_available_providers())}"
                )
            normalized.append(name)

    return normalized


def build_onnx_session(model_path: Path, settings: Settings) -> ort.InferenceSession:
    sess_options = ort.SessionOptions()
    sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    sess_options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL

    intra = int(getattr(settings, "ort_intra_op_num_threads", 0) or 0)
    if intra <= 0:
        replica_cpus = float(getattr(settings, "replica_num_cpus", 1))
        intra = max(1, math.ceil(replica_cpus))

    inter = int(getattr(settings, "ort_inter_op_num_threads", 0) or 0)
    if inter <= 0:
        inter = 1

    sess_options.intra_op_num_threads = intra
    sess_options.inter_op_num_threads = inter
    sess_options.log_severity_level = int(getattr(settings, "ort_log_severity_level", 2))

    providers = _normalize_providers(settings)

    logger.info(
        "onnx.session.create_start",
        extra={
            "event": "onnx.session.create_start",
            "model_path": str(model_path),
            "providers": providers,
            "intra_op_num_threads": intra,
            "inter_op_num_threads": inter,
            "log_severity_level": sess_options.log_severity_level,
        },
    )

    try:
        session = ort.InferenceSession(
            str(model_path),
            sess_options=sess_options,
            providers=providers,
        )
    except Exception:
        logger.exception(
            "onnx.session.create_failed",
            extra={
                "event": "onnx.session.create_failed",
                "model_path": str(model_path),
                "providers": providers,
            },
        )
        raise

    logger.info(
        "onnx.session.create_complete",
        extra={
            "event": "onnx.session.create_complete",
            "model_path": str(model_path),
            "providers": providers,
            "input_count": len(session.get_inputs()),
            "output_count": len(session.get_outputs()),
        },
    )
    return session


def _resolve_session_io(session: ort.InferenceSession, schema: ModelSchema) -> tuple[str, tuple[str, ...]]:
    session_inputs = tuple(inp.name for inp in session.get_inputs())
    session_outputs = tuple(out.name for out in session.get_outputs())

    if not session_inputs:
        logger.error("onnx.session.no_inputs", extra={"event": "onnx.session.no_inputs"})
        raise RuntimeError("The ONNX model declares no inputs")

    if not session_outputs:
        logger.error("onnx.session.no_outputs", extra={"event": "onnx.session.no_outputs"})
        raise RuntimeError("The ONNX model declares no outputs")

    if schema.input_name is not None:
        if schema.input_name not in session_inputs:
            logger.error(
                "onnx.schema.input_missing",
                extra={
                    "event": "onnx.schema.input_missing",
                    "schema_input_name": schema.input_name,
                    "session_inputs": session_inputs,
                },
            )
            raise RuntimeError(
                f"Schema input_name '{schema.input_name}' not found in model inputs: {session_inputs}"
            )
        input_name = schema.input_name
    elif len(session_inputs) == 1:
        input_name = session_inputs[0]
    else:
        logger.error(
            "onnx.schema.input_ambiguous",
            extra={
                "event": "onnx.schema.input_ambiguous",
                "session_inputs": session_inputs,
            },
        )
        raise RuntimeError(
            f"The model has multiple inputs {session_inputs}, but schema.json does not define input_name"
        )

    if schema.output_names:
        missing = [name for name in schema.output_names if name not in session_outputs]
        if missing:
            logger.error(
                "onnx.schema.output_missing",
                extra={
                    "event": "onnx.schema.output_missing",
                    "schema_output_names": schema.output_names,
                    "missing_outputs": missing,
                    "session_outputs": session_outputs,
                },
            )
            raise RuntimeError(
                f"Schema output_names not found in model outputs: {missing}. Available outputs: {session_outputs}"
            )
        output_names = schema.output_names
    else:
        output_names = session_outputs

    logger.info(
        "onnx.session.io_resolved",
        extra={
            "event": "onnx.session.io_resolved",
            "input_name": input_name,
            "output_names": output_names,
        },
    )
    return input_name, output_names


def load_loaded_model(settings: Settings) -> LoadedModel:
    start = time.perf_counter()
    model_path, schema, metadata = load_model_bundle(settings)
    session = build_onnx_session(model_path, settings)
    input_name, output_names = _resolve_session_io(session, schema)

    elapsed_ms = int((time.perf_counter() - start) * 1000)
    logger.info(
        "model.ready",
        extra={
            "event": "model.ready",
            "model_path": str(model_path),
            "cache_dir": str(model_path.parent),
            "model_name": metadata.model_name,
            "model_version": metadata.model_version,
            "elapsed_ms": elapsed_ms,
            "input_name": input_name,
            "output_names": output_names,
        },
    )

    return LoadedModel(
        model_path=model_path,
        cache_dir=model_path.parent,
        schema=schema,
        metadata=metadata,
        session=session,
        input_name=input_name,
        output_names=output_names,
    )