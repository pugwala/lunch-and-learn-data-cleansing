#!/bin/bash
# Runs once, on the first start of an empty catalog-pg volume.
# ducklake  - the DuckLake catalog. lake_rw owns it; lake_ro can only read it.
# superset, dagster - app databases for later phases, so they don't need a second Postgres.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" \
     -v rw_pw="$LAKE_RW_PASSWORD" -v ro_pw="$LAKE_RO_PASSWORD" <<'SQL'
CREATE ROLE lake_rw LOGIN PASSWORD :'rw_pw';
CREATE ROLE lake_ro LOGIN PASSWORD :'ro_pw';
CREATE DATABASE ducklake OWNER lake_rw;
CREATE DATABASE superset;
CREATE DATABASE dagster;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname ducklake <<'SQL'
REVOKE ALL ON DATABASE ducklake FROM PUBLIC;
GRANT CONNECT ON DATABASE ducklake TO lake_ro;
GRANT USAGE ON SCHEMA public TO lake_ro;
-- DuckLake creates its catalog tables on first attach, as lake_rw. Make every table it creates readable.
ALTER DEFAULT PRIVILEGES FOR ROLE lake_rw IN SCHEMA public GRANT SELECT ON TABLES TO lake_ro;
SQL
