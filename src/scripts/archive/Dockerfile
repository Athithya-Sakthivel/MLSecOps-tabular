# syntax=docker/dockerfile:1.7

FROM ghcr.io/gitleaks/gitleaks:v8.30.1 AS gitleaks

FROM ghcr.io/athithya-sakthivel/trivy-safe-0.69.3@sha256:bcc376de8d77cfe086a917230e818dc9f8528e3c852f7b1aff648949b6258d1c

ARG OPENGREP_VERSION=v1.16.5
ARG OPENGREP_DIST=opengrep_musllinux_x86
ARG OPENGREP_SHA256

USER root

ENV PATH="/usr/local/bin:${PATH}"

RUN apk add --no-cache bash ca-certificates curl git openssh-client \
    && update-ca-certificates

COPY --from=gitleaks /usr/bin/gitleaks /usr/local/bin/gitleaks

RUN curl -fsSL --retry 5 --retry-all-errors \
      -o /usr/local/bin/opengrep \
      "https://github.com/opengrep/opengrep/releases/download/${OPENGREP_VERSION}/${OPENGREP_DIST}" \
    && if [ -n "${OPENGREP_SHA256:-}" ]; then echo "${OPENGREP_SHA256}  /usr/local/bin/opengrep" | sha256sum -c -; fi \
    && chmod 0755 /usr/local/bin/opengrep

RUN command -v gitleaks \
    && command -v opengrep \
    && command -v trivy \
    && gitleaks version | grep -q "8.30.1" \
    && opengrep --version | grep -q "1.16.5" \
    && trivy --version

RUN adduser -D -u 10001 -h /home/appuser appuser \
    && git config --system --add safe.directory /workspace

USER appuser

WORKDIR /workspace

HEALTHCHECK CMD trivy --version || exit 1