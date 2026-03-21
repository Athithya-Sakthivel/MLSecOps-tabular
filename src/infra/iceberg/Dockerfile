# Base image provides Apache Gravitino Iceberg REST catalog used by engines to access Iceberg tables through a unified API for multi cloud
# PostgreSQL JDBC driver is added because the catalog backend stores metadata in Postgres and the upstream image does not ship DB drivers

# docker pull ghcr.io/athithya-sakthivel/iceberg-rest-gravitino-postgres@sha256:bb2ac1c0328a94b65834aa0bae9e863baf8aaee7557810e7b79c71f3537d13ea

FROM apache/gravitino-iceberg-rest:1.1.0

# Add Postgres JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-42.7.10.jar \
    /root/gravitino-iceberg-rest-server/libs/postgresql.jar

# Fix permissions for non-root execution
RUN chmod -R a+rx /root/gravitino-iceberg-rest-server && \
    chmod -R a+rx /root

# Run as non-root (matches K8s securityContext)
USER 1001
