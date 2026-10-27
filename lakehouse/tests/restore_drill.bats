#!/usr/bin/env bats
# Tests for backup/restore_drill.sh, the full restore drill. Each test runs the script in a throwaway repo under
# $BATS_TEST_TMPDIR with stub docker and sudo first on PATH, so nothing touches the real backup, catalog or lake.
#   bats lakehouse/tests/restore_drill.bats

# setup: before each test, a throwaway repo with the script, a lake folder, a .env, and stub commands on PATH.
setup() {
  T=$BATS_TEST_TMPDIR
  REPO=$T/repo
  mkdir -p "$REPO/lakehouse/backup" "$T/bin" "$T/lake/offsite-cache"
  cp "$BATS_TEST_DIRNAME/../backup/restore_drill.sh" "$REPO/lakehouse/backup/"
  printf 'LAKE_ROOT=%s\nLAKE_RW_PASSWORD=rw-secret-value\n' "$T/lake" > "$REPO/lakehouse/.env"
  export PATH="$T/bin:$PATH" FAKE_LOG=$T/calls.log FAKE_PARITY_EXIT=0 FAKE_RESTORE_EXIT=0 FAKE_LAKE=$T/lake
  : > "$FAKE_LOG"
  stub docker <<'EOF'
case "$*" in
  *"restic restore"*)
    [ "$FAKE_RESTORE_EXIT" = 0 ] || exit "$FAKE_RESTORE_EXIT"
    d=$FAKE_LAKE/offsite-cache/full-drill/lake
    mkdir -p "$d/catalog-backups/20261026T070000Z" "$d/catalog-backups/20261027T070000Z" "$d/data/main"
    echo old > "$d/catalog-backups/20261026T070000Z/ducklake.dump"
    echo newest > "$d/catalog-backups/20261027T070000Z/ducklake.dump"
    echo parquet > "$d/data/main/f.parquet"
    echo "restored snapshot 8455b540" ;;
  *"restic snapshots"*) echo '[{"short_id":"8455b540","time":"2026-10-27T15:15:50Z"}]' ;;
  *pg_restore*) cat > "$FAKE_LAKE/restored-dump" ;;
  *"select count(*) from ducklake_snapshot"*) echo 351 ;;
  *pytest*)
    [ -n "${PGPASSWORD:-}" ] && echo "PGPASSWORD reached docker's environment" >> "$FAKE_LOG"
    echo "19 passed in 0.6s"; exit "$FAKE_PARITY_EXIT" ;;
esac
exit 0
EOF
  stub sudo <<'EOF'
exec "$@"
EOF
}

# stub NAME < body: an executable in $T/bin that logs its arguments, then runs body.
stub() {
  { echo '#!/bin/bash'; echo "echo \"$1 \$*\" >> \"\$FAKE_LOG\""; cat; } > "$T/bin/$1"
  chmod +x "$T/bin/$1"
}

# run_drill: run the script under test.
run_drill() { run bash "$REPO/lakehouse/backup/restore_drill.sh" </dev/null; }
# called TEXT: true when any stub was called with TEXT in its arguments.
called() { grep -q -- "$1" "$FAKE_LOG"; }
# not_called TEXT: a function, so a failure fails the test.
not_called() { ! grep -q -- "$1" "$FAKE_LOG"; }
# line_of TEXT: the number of the first log line with TEXT, to check the order of steps.
line_of() { grep -n -- "$1" "$FAKE_LOG" | head -1 | cut -d: -f1; }

@test "restores the latest snapshot into the cache, then the newest catalog dump into a scratch database" {
  run_drill
  [ "$status" -eq 0 ]
  called "exec -T offsite-backup restic restore latest --host lakehouse --target /cache/full-drill"
  called "exec -T catalog-pg createdb -U postgres ducklake_drill"
  [ "$(cat "$T/lake/restored-dump")" = newest ]
  [ "$(line_of 'restic restore')" -lt "$(line_of 'createdb')" ]
}

@test "runs the parity checks on the restored lake: its catalog, its files mounted read-only at /lake/data" {
  run_drill
  [ "$status" -eq 0 ]
  called "dbname=ducklake_drill host=catalog-pg user=lake_rw"
  called "$T/lake/offsite-cache/full-drill/lake/data:/lake/data:ro"
  called "pytest -q -p no:cacheprovider lakehouse/tests"
  [[ $output == *"19 passed"* ]]
}

@test "the database password reaches docker only through its environment, never its arguments" {
  run_drill
  [ "$status" -eq 0 ]
  called "PGPASSWORD reached docker's environment"
  not_called "rw-secret-value"
}

@test "cleans up the scratch database and the restored files" {
  run_drill
  [ "$status" -eq 0 ]
  called "exec -T catalog-pg dropdb -U postgres --if-exists ducklake_drill"
  [ ! -e "$T/lake/offsite-cache/full-drill" ]
}

@test "a failed parity check fails the drill, and still cleans up" {
  FAKE_PARITY_EXIT=1 run_drill
  [ "$status" -ne 0 ]
  [[ $output == *"FAILED"* ]]
  called "dropdb -U postgres --if-exists ducklake_drill"
  [ ! -e "$T/lake/offsite-cache/full-drill" ]
}

@test "a failed restore stops before touching the catalog" {
  FAKE_RESTORE_EXIT=1 run_drill
  [ "$status" -ne 0 ]
  not_called "createdb"
  not_called "pytest"
}
