# Base image provides Apache Gravitino Iceberg REST catalog used by engines to access Iceberg tables through a unified API for multi 
# PostgreSQL JDBC driver is added because the catalog backend stores metadata in Postgres and the upstream image does not ship DB drivers

FROM apache/gravitino-iceberg-rest:1.1.0

# Add Postgres JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-42.7.10.jar \
    /root/gravitino-iceberg-rest-server/libs/postgresql.jar

# Fix everything at build time for non-root execution
RUN set -eux; \
    # Ensure /root is traversable by non-root
    chmod o+rx /root; \
    \
    # Ensure all scripts are executable
    find /root/gravitino-iceberg-rest-server/bin -type f -name "*.sh" -exec chmod 0755 {} \;; \
    \
    # Ensure app files are readable and dirs traversable
    chmod -R a+rX /root/gravitino-iceberg-rest-server; \
    \
    # Create required runtime dirs
    mkdir -p /root/gravitino-iceberg-rest-server/logs \
             /root/gravitino-iceberg-rest-server/tmp; \
    \
    # Make runtime dirs writable
    chmod -R 0775 /root/gravitino-iceberg-rest-server/logs \
                   /root/gravitino-iceberg-rest-server/tmp; \
    \
    # Assign ownership to runtime user
    chown -R 1001:1001 /root/gravitino-iceberg-rest-server

# Run as non-root (matches Kubernetes securityContext)
USER 1001