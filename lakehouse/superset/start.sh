#!/bin/bash
# Superset start-up, run by compose on every start. Each step is idempotent, so restarts are safe:
#   1. bring the metadata database's schema up to date
#   2. make sure the admin user exists, with the password from .env (reset each start, like the Postgres roles)
#   3. sync Superset's built-in roles and permissions
#   4. register Cube as the database, by its SQL API (created if missing, updated if the password changed)
# then hand over to the image's own server script.
set -euo pipefail

superset db upgrade
superset fab create-admin --username admin --firstname Lakehouse --lastname Admin --email admin@example.com \
  --password "$SUPERSET_ADMIN_PASSWORD" || true   # "already exists" after the first start
superset fab reset-password --username admin --password "$SUPERSET_ADMIN_PASSWORD"
superset init
superset set-database-uri --database_name Cube --uri "postgresql+psycopg2://lake:${CUBE_SQL_PASSWORD}@cube:15432/cube"

exec /app/docker/entrypoints/run-server.sh
