#!/bin/bash
# The full restore drill: prove the latest off-machine backup restores into a working lake, not just into files.
#
#   bash lakehouse/backup/restore_drill.sh
#
# offsite-backup's monthly drill already proves every file comes back byte for byte; this goes further. It restores
# the latest snapshot into the offsite cache (on /lake), restores the newest catalog dump from it into a scratch
# database (ducklake_drill) on catalog-pg, and runs the 19 parity checks (lakehouse/tests) in a throwaway container
# whose lake is the restored catalog and the restored files, mounted read-only at /lake/data where the catalog
# expects them. It always drops the scratch database and deletes the restored files, also when a step fails.
# Run on the VM from the repo; it needs the stack up (offsite-backup, catalog-pg) and the lnl-jupyter image.
set -euo pipefail

cd "$(dirname "$0")/.."   # lakehouse/
REPO_ROOT=$(cd .. && pwd)
DB=ducklake_drill
NETWORK=lakehouse_default   # compose's network for the project in lakehouse/

# env_val KEY: KEY's value in .env (the last one, quotes stripped), or empty; never printed.
env_val() { { grep -E "^$1=" .env 2>/dev/null || true; } | tail -1 | cut -d= -f2- | sed -e "s/^'//" -e "s/'$//"; }
# dc ARGS: docker compose for the core profile.
dc() { docker compose --profile core "$@"; }
# fail MESSAGE: say what failed and stop; the EXIT trap still cleans up.
fail() { echo "Restore drill FAILED: $*" >&2; exit 1; }

LAKE_ROOT=$(env_val LAKE_ROOT)
LAKE_ROOT=${LAKE_ROOT:-/lake}
RESTORED=$LAKE_ROOT/offsite-cache/full-drill   # /cache/full-drill inside offsite-backup

# cleanup: drop the scratch database and delete the restored files, whatever happened.
cleanup() {
  dc exec -T catalog-pg dropdb -U postgres --if-exists "$DB" >/dev/null 2>&1 || true
  sudo rm -rf "$RESTORED"
}
trap cleanup EXIT

echo "Restore drill: restoring the latest snapshot"
sudo rm -rf "$RESTORED"
dc exec -T offsite-backup restic restore latest --host lakehouse --target /cache/full-drill >/dev/null \
  || fail "restic couldn't restore the latest snapshot"

stamp=$(sudo ls "$RESTORED/lake/catalog-backups" | grep -E '^[0-9]{8}T[0-9]{6}Z$' | sort | tail -1)
[ -n "$stamp" ] || fail "the snapshot holds no catalog backup"
dump=$RESTORED/lake/catalog-backups/$stamp/ducklake.dump
echo "Restore drill: restoring the catalog backup $stamp into the scratch database $DB"
dc exec -T catalog-pg dropdb -U postgres --if-exists "$DB" >/dev/null 2>&1
dc exec -T catalog-pg createdb -U postgres "$DB" || fail "couldn't create $DB"
sudo cat "$dump" | dc exec -T catalog-pg pg_restore -U postgres --no-owner --no-privileges -d "$DB" \
  || fail "pg_restore of $stamp/ducklake.dump"
dc exec -T catalog-pg psql -U postgres -d "$DB" -v ON_ERROR_STOP=1 -qc \
  "GRANT CONNECT ON DATABASE $DB TO lake_rw; GRANT USAGE ON SCHEMA public TO lake_rw;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO lake_rw;" >/dev/null || fail "grants on $DB"
snapshots=$(dc exec -T catalog-pg psql -U postgres -d "$DB" -tAc 'select count(*) from ducklake_snapshot' | tr -d '[:space:]')

echo "Restore drill: running the parity checks on the restored lake ($snapshots snapshots)"
PGPASSWORD=$(env_val LAKE_RW_PASSWORD) docker run --rm --network "$NETWORK" -e PGPASSWORD \
  -e LAKE_CATALOG="ducklake:postgres:dbname=$DB host=catalog-pg user=lake_rw" -e LAKE_DATA_PATH=/lake/data/ \
  -e LNL_DATA_DIR=/tmp/lnl -e PYTHONPATH=/course -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$REPO_ROOT:/course:ro" -v "$RESTORED/lake/data:/lake/data:ro" -w /course --entrypoint python \
  lnl-jupyter:local -m pytest -q -p no:cacheprovider lakehouse/tests \
  || fail "the parity checks failed on the restored lake"

echo "Restore drill: passed. The backup of $stamp restores into a lake that matches the course's numbers."
