#!/bin/bash
# The role postgres-exporter logs in as: pg_monitor only, so it reads statistics views, never table data. Runs on a
# fresh catalog-pg volume, and is safe to run again by hand (it only creates what's missing and resets the password):
#   docker compose --profile core exec -T catalog-pg bash /docker-entrypoint-initdb.d/05-monitor.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" -v pw="$LAKE_MONITOR_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE lake_monitor LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'lake_monitor') \gexec
ALTER ROLE lake_monitor WITH LOGIN PASSWORD :'pw';
GRANT pg_monitor TO lake_monitor;
SQL
