#!/bin/bash
# OpenMetadata's metadata role and database (glossary, tables, tags, users). Runs automatically on a fresh
# catalog-pg volume, and is safe to run again by hand on an existing one (it only creates what's missing and resets
# the password):
#   docker compose --profile core exec -T catalog-pg bash /docker-entrypoint-initdb.d/04-openmetadata.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" -v pw="$OM_DB_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE openmetadata LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmetadata') \gexec
ALTER ROLE openmetadata WITH LOGIN PASSWORD :'pw';
SELECT 'CREATE DATABASE openmetadata_db OWNER openmetadata' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_db') \gexec
ALTER DATABASE openmetadata_db OWNER TO openmetadata;
SQL
