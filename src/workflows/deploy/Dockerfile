# syntax=docker/dockerfile:1.7

FROM python:3.13.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    MODEL_CACHE_DIR=/mlsecops/model-cache

WORKDIR /app

# System packages kept minimal. ca-certificates helps outbound TLS (pip, HTTPS, object storage clients).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Deterministic non-root user/group and writable cache path for per-replica ONNX/model caching.
RUN groupadd --gid 1000 appgroup \
    && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash appuser \
    && mkdir -p /app /mlsecops/model-cache \
    && chown -R 1000:1000 /app /mlsecops \
    && chmod -R 750 /app /mlsecops

# Install Python dependencies first for better layer caching.
COPY --chown=1000:1000 requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the deploy/ build context.
COPY --chown=1000:1000 . /app

USER 1000:1000

# Keep the default entrypoint neutral; Ray/KubeRay will launch the module.
CMD ["python", "-m", "service"]