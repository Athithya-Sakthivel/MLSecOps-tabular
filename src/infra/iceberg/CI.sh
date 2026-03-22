#!/usr/bin/env bash

echo $GITHUB_TOKEN | docker login ghcr.io -u athithya-sakthivel --password-stdin

docker build -t ghcr.io/athithya-sakthivel/iceberg-rest-gravitino-postgres:1.1.0 \
  -f src/infra/iceberg/Dockerfile .


docker push ghcr.io/athithya-sakthivel/iceberg-rest-gravitino-postgres:1.1.0

# https://github.com/users/<GH_USERNAME>/packages/container/package/iceberg-rest-gravitino-postgres

# GitHub Container Registry package URL for the image (replace <GH_USERNAME> with your account)

# Note: <GH_USERNAME> is case-insensitive, while the GHCR image repository name must be lowercase

# Ensure the repository "iceberg-rest-gravitino-postgres" is set to public visibility
