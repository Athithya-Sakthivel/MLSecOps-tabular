# syntax=docker/dockerfile:1.7

FROM python:3.12.13-slim-bookworm@sha256:31c0807da611e2e377a2e9b566ad4eb038ac5a5838cbbbe6f2262259b5dc77a0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    MODEL_CACHE_DIR=/mlsecops/model-cache \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

WORKDIR /app

# Minimal runtime packages:
# - ca-certificates: TLS for pip, S3, HTTPS endpoints
# - tini: proper PID 1 signal handling and child reaping
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini curl \
    && rm -rf /var/lib/apt/lists/*

# Deterministic non-root user/group and writable cache path for per-replica model caching.
RUN groupadd --gid 1000 appgroup \
    && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash appuser \
    && mkdir -p /app /mlsecops/model-cache \
    && chown -R 1000:1000 /app /mlsecops \
    && chmod -R 750 /app /mlsecops

# Install dependencies first for better layer caching.
COPY --chown=1000:1000 requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install -r /app/requirements.txt

# Copy application code.
COPY --chown=1000:1000 . /app

USER 1000:1000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "service"]