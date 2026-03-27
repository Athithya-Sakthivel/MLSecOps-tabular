# syntax=docker/dockerfile:1.7

FROM ghcr.io/gitleaks/gitleaks:v8.30.1 AS gitleaks

FROM ghcr.io/athithya-sakthivel/trivy-safe-0.69.3@sha256:bcc376de8d77cfe086a917230e818dc9f8528e3c852f7b1aff648949b6258d1c

ARG TARGETARCH
ARG OPENGREP_VERSION=v1.16.5

# Fully pinned SHA (amd64 musl binary)
ARG OPENGREP_SHA256=28457bfff6edcb6d9e8013c32d519dfaab7ef29325e79a6cb3ce9cb24933689a

USER root

ENV PATH="/usr/local/bin:${PATH}" \
    HOME="/home/appuser" \
    XDG_CACHE_HOME="/home/appuser/.cache" \
    TRIVY_CACHE_DIR="/home/appuser/.cache/trivy"

RUN apk add --no-cache bash ca-certificates curl git openssh-client \
    && update-ca-certificates

# Copy pinned gitleaks binary
COPY --from=gitleaks /usr/bin/gitleaks /usr/local/bin/gitleaks

# Install OpenGrep with strict verification
RUN set -eu; \
    case "${TARGETARCH:-amd64}" in \
      amd64) DIST="opengrep_musllinux_x86" ;; \
      arm64) DIST="opengrep_musllinux_aarch64" ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH:-unset}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL --retry 5 --retry-all-errors \
      -o /usr/local/bin/opengrep \
      "https://github.com/opengrep/opengrep/releases/download/${OPENGREP_VERSION}/${DIST}"; \
    echo "${OPENGREP_SHA256}  /usr/local/bin/opengrep" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/opengrep

# Verify toolchain (fail build if mismatch)
RUN set -eu; \
    command -v gitleaks; \
    command -v opengrep; \
    command -v trivy; \
    gitleaks version | grep -q "8.30.1"; \
    opengrep --version | grep -q "1.16.5"; \
    trivy --version

# Create non-root user + writable cache dirs
RUN adduser -D -u 10001 -h /home/appuser appuser \
    && mkdir -p \
        /home/appuser/.cache/opengrep \
        /home/appuser/.cache/trivy \
        /workspace \
    && chown -R appuser:appuser /home/appuser /workspace \
    && chmod -R 755 /home/appuser \
    && git config --system --add safe.directory /workspace

USER appuser

WORKDIR /workspace