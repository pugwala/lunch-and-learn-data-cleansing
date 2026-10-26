#!/bin/bash
# Superset's metadata role (users, charts, dashboards). 01-roles.sh creates the superset database; this gives it an
# owner. Runs automatically on a fresh catalog-pg volume, and is safe to run again by hand on an existing one (it
# only creates what's missing and resets the password):
#   docker compose --profile core exec -T catalog-pg bash /docker-entrypoint-initdb.d/03-superset.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" -v pw="$SUPERSET_PG_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE superset LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'superset') \gexec
ALTER ROLE superset WITH LOGIN PASSWORD :'pw';
SELECT 'CREATE DATABASE superset' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset') \gexec
ALTER DATABASE superset OWNER TO superset;
SQL
