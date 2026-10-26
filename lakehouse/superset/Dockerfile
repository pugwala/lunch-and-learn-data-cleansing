# Superset plus the one driver it needs, twice over: psycopg2 for its own metadata database on catalog-pg and for
# Cube's Postgres-wire SQL API. The official image ships without database drivers. Config and the start script are
# mounted by compose.yaml, so changing them needs a restart, not a rebuild.
ARG SUPERSET_TAG=6.1.0
FROM apache/superset:${SUPERSET_TAG}

USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python psycopg2-binary==2.9.13
USER superset
