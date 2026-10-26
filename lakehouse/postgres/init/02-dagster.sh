#!/bin/bash
# Dagster's run storage role. Runs automatically on a fresh catalog-pg volume, and is safe to run again by hand on
# an existing one (it only creates what's missing and resets the password):
#   docker compose --profile core exec -T catalog-pg bash /docker-entrypoint-initdb.d/02-dagster.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" -v pw="$DAGSTER_PG_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE dagster LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dagster') \gexec
ALTER ROLE dagster WITH LOGIN PASSWORD :'pw';
SELECT 'CREATE DATABASE dagster' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster') \gexec
ALTER DATABASE dagster OWNER TO dagster;
SQL
