# Base image provides Apache Gravitino Iceberg REST catalog used by engines to access Iceberg tables through a unified API for multi cloud
# PostgreSQL JDBC driver is added because the catalog backend stores metadata in Postgres and the upstream image does not ship DB drivers
FROM apache/gravitino-iceberg-rest:1.1.0
ADD https://jdbc.postgresql.org/download/postgresql-42.7.10.jar /root/gravitino-iceberg-rest-server/libs/postgresql.jar

# docker pull ghcr.io/athithya-sakthivel/iceberg-rest-postgres@sha256:55250a42cd067c92a27559a5bebab570586b34b057d7ca9adf42f45a8ebab1a4
