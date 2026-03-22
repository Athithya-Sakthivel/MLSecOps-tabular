#!/usr/bin/env bash

echo $GITHUB_TOKEN | docker login ghcr.io -u athithya-sakthivel --password-stdin

docker build -t ghcr.io/athithya-sakthivel/flyte-elt-spark-base:1.0.0 \
  -f src/workflows/ELT/Dockerfile.base .

docker push ghcr.io/athithya-sakthivel/flyte-elt-spark-base:1.0.0

# https://github.com/users/<GH_USERNAME>/packages/container/package/flyte-elt-spark-base

# GitHub Container Registry package URL for the image (replace <GH_USERNAME> with your account)

# Note: <GH_USERNAME> is case-insensitive, while the GHCR image repository name must be lowercase

# Ensure the repository "flyte-elt-spark-base:1.0.0" is set to public visibility

# docker pull ghcr.io/athithya-sakthivel/flyte-elt-spark-base@sha256:04ecd3631080e8627f0ae9308697ca0d96250c0b07a0b3a147fdcc22276585e5

