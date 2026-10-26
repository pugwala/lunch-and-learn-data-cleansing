#!/usr/bin/env bats
# Tests for scripts/bootstrap.sh. Each test runs the script in a throwaway repo under $BATS_TEST_TMPDIR, with stub
# docker, findmnt, blkid, lsblk, curl and sudo first on PATH, so nothing touches real disks, .env or containers.
#   bats lakehouse/tests/bootstrap.bats
# BOOTSTRAP=<path> tests a different copy of the script (used to prove each test fails when its behaviour breaks).

setup() {
  T=$BATS_TEST_TMPDIR
  REPO=$T/repo
  ENV=$REPO/lakehouse/.env
  mkdir -p "$REPO/lakehouse/scripts" "$T/bin" "$T/lake"
  cp "${BOOTSTRAP:-$BATS_TEST_DIRNAME/../scripts/bootstrap.sh}" "$REPO/lakehouse/scripts/bootstrap.sh"
  cp "$BATS_TEST_DIRNAME/../.env.example" "$REPO/lakehouse/"
  make_stubs
  export PATH="$T/bin:$PATH" FAKE_LOG=$T/calls.log
  export FAKE_MOUNT_SRC=/dev/fakelake FAKE_LAKE_DEV='' FAKE_MOUNTED_AT='' FAKE_BUILD_EXIT=0 FAKE_HAVE_DATA=yes
  export FAKE_CURL_DOWN=""
  : > "$FAKE_LOG"
  cat > "$ENV" <<EOF
POSTGRES_PASSWORD=pg-secret
LAKE_RW_PASSWORD=rw-secret
LAKE_RO_PASSWORD=ro-secret
DAGSTER_PG_PASSWORD=dagster-secret
JUPYTER_TOKEN=jupyter-secret
CUBEJS_API_SECRET=cube-api-secret
CUBE_SQL_PASSWORD=cube-sql-secret
SUPERSET_SECRET_KEY=superset-key
SUPERSET_PG_PASSWORD=superset-pg-secret
SUPERSET_ADMIN_PASSWORD=superset-admin-secret
DAGSTER_UI_USER=admin
DAGSTER_UI_HASH='\$2a\$14\$fakehash'
LAKE_ROOT=$T/lake
LAKE_TS_HOST=lakehouse.example.invalid
EOF
  chmod 600 "$ENV"
}

# make_stubs: write the fake commands each test runs against, so nothing real is touched.
make_stubs() {
  stub docker <<'EOF'
case "$*" in
  "compose version"|info)                    exit 0 ;;
  *" build "*)                               exit "$FAKE_BUILD_EXIT" ;;
  *" ps --format {{.Service}} {{.Status}}"*) for s in catalog-pg jupyter dagster-webserver dagster-daemon cube superset caddy; do
                                               echo "$s Up 1 minute (healthy)"; done ;;
  *" ps --format {{.Status}} "*)             echo "Up 1 minute (healthy)" ;;
  *"exec -T jupyter python"*)                cat >/dev/null; echo "$FAKE_HAVE_DATA" ;;
  *pytest*)                                  echo "18 passed in 0.10s" ;;
  *"run --rm load-lake"*)                    echo "Loaded run fake: 100,000 silver rows" ;;
esac
exit 0
EOF
  stub findmnt <<'EOF'
case "$*" in
  *"-o SOURCE --mountpoint "*) [ -n "$FAKE_MOUNT_SRC" ] && echo "$FAKE_MOUNT_SRC" || exit 1 ;;
  *"-o SIZE --mountpoint "*)   echo 200G ;;
  *"-o TARGET --source "*)     [ -n "$FAKE_MOUNTED_AT" ] && echo "$FAKE_MOUNTED_AT" || exit 1 ;;
esac
EOF
  stub blkid <<'EOF'
[ -n "$FAKE_LAKE_DEV" ] && echo "$FAKE_LAKE_DEV" || exit 2
EOF
  stub lsblk <<'EOF'
echo lake
EOF
  stub curl <<'EOF'
[ -n "$FAKE_CURL_DOWN" ] && { printf 000; exit 7; }   # like real curl when nothing answers: prints 000 and fails
case "$*" in
  *example.invalid/jupyter/*) printf 302 ;; *example.invalid/dagster/*) printf 401 ;; *example.invalid/superset/*) printf 302 ;; *) printf 000 ;;
esac
EOF
  stub tailscale <<'EOF'
echo '{"Self": {"DNSName": "lakehouse.example.invalid."}}'
EOF
  stub sudo <<'EOF'
case $1 in chown|apt-get) exit 0 ;; esac   # can't chown to 1000 without root; record it and move on
exec "$@"
EOF
}

# stub NAME < body: writes an executable to $T/bin that logs its arguments, then runs body.
stub() {
  { echo '#!/bin/bash'; echo "echo \"$1 \$*\" >> \"\$FAKE_LOG\""; cat; } > "$T/bin/$1"
  chmod +x "$T/bin/$1"
}

# run_bootstrap ARGS: run the copy of the script under test, with no terminal attached.
run_bootstrap() { run bash "$REPO/lakehouse/scripts/bootstrap.sh" "$@" </dev/null; }
# called TEXT: true when any stub was called with TEXT in its arguments.
called()        { grep -q -- "$1" "$FAKE_LOG"; }
# not_called TEXT: a function, so a failure fails the test (a bare `! cmd` line never does).
not_called()    { ! grep -q -- "$1" "$FAKE_LOG"; }
# set_env KEY [VALUE]: set KEY in the test .env, or remove it when no VALUE is given.
set_env()       { sed -i "/^$1=/d" "$ENV"; [ $# = 1 ] || echo "$1=$2" >> "$ENV"; chmod 600 "$ENV"; }

# --- data disk -------------------------------------------------------------------------------------------------

@test "stops when /lake isn't mounted and no disk is labeled lake" {
  FAKE_MOUNT_SRC=''
  run_bootstrap
  [ "$status" -eq 1 ]
  [[ $output == *"is NOT a mounted disk"* ]]
  not_called " build "
}

@test "says to fix LAKE_ROOT when the lake disk is mounted somewhere else" {
  FAKE_MOUNT_SRC='' FAKE_LAKE_DEV=/dev/sdz FAKE_MOUNTED_AT=/mnt/elsewhere
  run_bootstrap
  [ "$status" -eq 1 ]
  [[ $output == *"Set LAKE_ROOT=/mnt/elsewhere"* ]]
}

# --- secrets ---------------------------------------------------------------------------------------------------

@test "refuses duplicate keys in .env and leaves the file untouched" {
  echo "JUPYTER_TOKEN=a-second-value" >> "$ENV"
  before=$(sha256sum < "$ENV")
  run_bootstrap
  [ "$status" -eq 1 ]
  [[ $output == *"more than once in .env: JUPYTER_TOKEN"* ]]
  [ "$(sha256sum < "$ENV")" = "$before" ]
  not_called " build "
}

@test "generates only missing secrets and never overwrites a set one" {
  set_env LAKE_RW_PASSWORD CHANGE_ME
  set_env LAKE_RO_PASSWORD                     # key absent entirely
  run_bootstrap
  [ "$status" -eq 0 ]
  grep -qx 'POSTGRES_PASSWORD=pg-secret' "$ENV"
  grep -qE '^LAKE_RW_PASSWORD=[0-9a-f]{48}$' "$ENV"
  grep -qE '^LAKE_RO_PASSWORD=[0-9a-f]{48}$' "$ENV"
  [ "$(grep -c CHANGE_ME "$ENV")" = 0 ]
}

@test "generates the Cube secrets when an older .env lacks them" {
  set_env CUBEJS_API_SECRET
  set_env CUBE_SQL_PASSWORD
  run_bootstrap
  [ "$status" -eq 0 ]
  grep -qE '^CUBEJS_API_SECRET=[0-9a-f]{48}$' "$ENV"
  grep -qE '^CUBE_SQL_PASSWORD=[0-9a-f]{48}$' "$ENV"
}

@test "generates the Superset secrets when an older .env lacks them" {
  set_env SUPERSET_SECRET_KEY
  set_env SUPERSET_PG_PASSWORD
  set_env SUPERSET_ADMIN_PASSWORD
  run_bootstrap
  [ "$status" -eq 0 ]
  grep -qE '^SUPERSET_SECRET_KEY=[0-9a-f]{48}$' "$ENV"
  grep -qE '^SUPERSET_PG_PASSWORD=[0-9a-f]{48}$' "$ENV"
  grep -qE '^SUPERSET_ADMIN_PASSWORD=[0-9a-f]{48}$' "$ENV"
}

@test "builds both local images: jupyter and superset" {
  run_bootstrap
  [ "$status" -eq 0 ]
  called "build -q jupyter superset"
}

@test "creates both app roles on catalog-pg: dagster and superset" {
  run_bootstrap
  [ "$status" -eq 0 ]
  called "02-dagster.sh"
  called "03-superset.sh"
}

@test "fills in LAKE_TS_HOST from Tailscale when .env lacks it" {
  set_env LAKE_TS_HOST
  run_bootstrap
  [ "$status" -eq 0 ]
  grep -qx 'LAKE_TS_HOST=lakehouse.example.invalid' "$ENV"
}

@test "checks the Tailscale URLs with certificate verification on" {
  run_bootstrap
  [ "$status" -eq 0 ]
  called "https://lakehouse.example.invalid/jupyter/"
  called "https://lakehouse.example.invalid/dagster/"
  called "https://lakehouse.example.invalid/superset/"
  [ -z "$(grep 'example.invalid/' "$FAKE_LOG" | grep -E 'curl .*-[a-z]*k')" ]   # no -k: the certificate must be trusted
}

@test "no longer checks the retired lake.home.arpa names" {
  run_bootstrap
  [ "$status" -eq 0 ]
  not_called "lake.home.arpa"
}

@test "the closing message points at the Tailscale URLs" {
  run_bootstrap
  [ "$status" -eq 0 ]
  closing=$(grep 'All checks passed' <<< "$output")   # only the closing line: the URL checks above mention them too
  [[ $closing == *"https://lakehouse.example.invalid/jupyter/"*"https://lakehouse.example.invalid/dagster/"* ]]
}

@test "reports an unreachable site as answering 000, once" {
  FAKE_CURL_DOWN=1
  run_bootstrap
  [ "$status" -eq 1 ]
  [[ $output == *"answers 000, expected 302"* ]]
  [[ $output != *"000000"* ]]
}

@test ".env keeps mode 600, its inode, and no inherited ACL entries after a rewrite" {
  setfacl -d -m u:1000:rwX "$REPO/lakehouse"   # the repo's default ACL: new files here would inherit it
  set_env JUPYTER_TOKEN CHANGE_ME              # forces the script to rewrite .env
  inode=$(stat -c %i "$ENV")
  run_bootstrap
  [ "$status" -eq 0 ]
  [ "$(stat -c %a "$ENV")" = 600 ]
  [ "$(stat -c %i "$ENV")" = "$inode" ]
  [ -z "$(getfacl -pn "$ENV" | grep '^user:[0-9]')" ]
}

# --- repo permissions ------------------------------------------------------------------------------------------

@test "recognises existing repo ACLs by numeric uid, not user name" {
  [ "$(id -u)" != 1000 ] || skip "only meaningful when you aren't uid 1000"
  setfacl -d -m u:1000:rwx "$REPO"
  run_bootstrap --check
  [[ $output == *"repo ACLs grant uid 1000"* ]]
  [[ $output != *"repo has no ACL for uid 1000"* ]]
}

# --- image, services, data -------------------------------------------------------------------------------------

@test "a failed image build stops the script before any service starts" {
  FAKE_BUILD_EXIT=1
  run_bootstrap
  [ "$status" -eq 1 ]
  [[ $output == *"image build failed"* ]]
  not_called " up "
}

@test "a healthy install passes, starting the seven services by name and not running the loader" {
  run_bootstrap
  [ "$status" -eq 0 ]
  [[ $output == *"All checks passed"* ]]
  called "up -d catalog-pg jupyter dagster-webserver dagster-daemon cube superset caddy"
  not_called "run --rm load-lake"
}

@test "runs the loader when the lake is empty" {
  FAKE_HAVE_DATA=no
  run_bootstrap
  [ "$status" -eq 0 ]
  called "run --rm load-lake"
}

@test "--check changes nothing, even when something needs fixing" {
  set_env JUPYTER_TOKEN CHANGE_ME
  before=$(sha256sum < "$ENV")
  run_bootstrap --check
  [ "$status" -eq 1 ]
  [[ $output == *"JUPYTER_TOKEN is not set"* ]]
  [ "$(sha256sum < "$ENV")" = "$before" ]
  not_called " build "
  not_called " up "
  not_called "02-dagster.sh"
  not_called "load-lake"
}
