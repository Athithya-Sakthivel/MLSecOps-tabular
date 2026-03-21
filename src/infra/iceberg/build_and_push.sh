
echo $GITHUB_TOKEN | docker login ghcr.io -u athithya-sakthivel --password-stdin

docker build -t ghcr.io/athithya-sakthivel/iceberg-rest-gravitino-postgres:1.1.0 \
  -f src/infra/iceberg/Dockerfile .


docker push ghcr.io/athithya-sakthivel/iceberg-rest-gravitino-postgres:1.1.0

# set the repo iceberg-rest-gravitino-postgres public
