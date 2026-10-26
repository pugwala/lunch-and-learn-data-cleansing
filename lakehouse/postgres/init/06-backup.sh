#!/bin/bash
# The role the nightly catalog backup logs in as: pg_read_all_data, so pg_dump can read every table in every
# database while the role can change nothing. ducklake doesn't let PUBLIC connect (01-roles.sh), so it gets CONNECT
# there by name. Runs on a fresh catalog-pg volume, and is safe to run again by hand:
#   docker compose --profile core exec -T catalog-pg bash /docker-entrypoint-initdb.d/06-backup.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" -v pw="$LAKE_BACKUP_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE lake_backup LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'lake_backup') \gexec
ALTER ROLE lake_backup WITH LOGIN PASSWORD :'pw';
GRANT pg_read_all_data TO lake_backup;
GRANT CONNECT ON DATABASE ducklake TO lake_backup;
SQL
