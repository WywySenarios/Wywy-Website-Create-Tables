FROM liquibase/liquibase:4.29.2

# The base image ends as USER liquibase:liquibase (uid 1001). Install as root,
# then restore the non-root runtime user.
USER root

# Pin the PostgreSQL JDBC driver (the base image bundles no postgres driver).
# Version verified against Maven Central (2026-08-06).
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar /liquibase/lib/postgresql-42.7.13.jar

# Build context is the master-db repo root, so every COPY source is prefixed
# with the submodule path apps/create_tables/...
COPY apps/create_tables/changelog/ /changelog/
COPY apps/create_tables/entrypoint.sh /entrypoint.sh
COPY apps/create_tables/seed.py /seed.py
COPY apps/create_tables/requirements.txt /requirements.txt

# Python + deps for seed.py (psycopg + argon2-cffi).
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r /requirements.txt

# COPY preserves 0644; the entrypoint must be executable.
RUN chmod +x /entrypoint.sh

USER 1001

ENTRYPOINT ["/entrypoint.sh"]
