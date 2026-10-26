#!/bin/bash
# The nightly catalog backup (Phase 6), run by the catalog-backup service from the same postgres image as catalog-pg,
# so pg_dump matches the server. As lake_backup (pg_read_all_data: reads everything, writes nothing) it dumps every
# app database in custom format to /backups/<UTC stamp>/, checks each with pg_restore --list, writes manifest.json,
# keeps KEEP_DAYS days, and leaves metrics for node-exporter's textfile collector in /backups/metrics. It backs up at
# start when the newest backup is over 23 hours old, then every day at BACKUP_AT (local time in TZ). Everything is
# private (umask 077) except the folders, manifest and metrics: the dumps hold every app database, user password
# hashes included, so other users on the lake disk can see that a backup happened but not read one.
# Roles aren't dumped (that needs a superuser); postgres/init/ recreates them from .env.
set -uo pipefail   # no -e: a failed step is caught and recorded, and the loop goes on to the next night

: "${PGPASSWORD:?PGPASSWORD (LAKE_BACKUP_PASSWORD) is not set}"
export PGHOST=${PGHOST:-catalog-pg} PGUSER=${PGUSER:-lake_backup}
ROOT=/backups
METRICS=$ROOT/metrics
DATABASES=(ducklake dagster superset openmetadata_db)
umask 077

# write_prom FILE: write stdin to $METRICS/FILE in one rename, so node-exporter never reads half a file.
write_prom() {
  local tmp
  tmp=$(mktemp "$METRICS/.$1.XXXXXX") && cat > "$tmp" && chmod 644 "$tmp" && mv "$tmp" "$METRICS/$1"
}

# record_failure WHAT: log what failed, record the time for Prometheus, and drop the partial folder.
record_failure() {
  echo "catalog-backup: FAILED: $1" >&2
  printf '# HELP lakehouse_backup_last_failure_timestamp_seconds When a catalog backup last failed.\n# TYPE lakehouse_backup_last_failure_timestamp_seconds gauge\nlakehouse_backup_last_failure_timestamp_seconds %s\n' \
    "$(date +%s)" | write_prom failure.prom
  rm -rf "$PARTIAL"
}

# backup_once: dump, check and record one backup; returns 1 (after recording why) if any step fails.
backup_once() {
  local started stamp db list tables bytes sizes="" json=""
  started=$(date +%s) stamp=$(date -u +%Y%m%dT%H%M%SZ)
  PARTIAL="$ROOT/$stamp.partial"
  mkdir -p "$PARTIAL" || { record_failure "mkdir $PARTIAL"; return 1; }
  for db in "${DATABASES[@]}"; do
    pg_dump -Fc -d "$db" -f "$PARTIAL/$db.dump" || { record_failure "pg_dump $db"; return 1; }
    list=$(pg_restore --list "$PARTIAL/$db.dump") || { record_failure "pg_restore --list $db"; return 1; }
    tables=$(awk '$4 == "TABLE" && $5 == "DATA" { printf "%s\"%s\"", sep, $7; sep = "," }' <<< "$list")
    bytes=$(stat -c %s "$PARTIAL/$db.dump")
    json+="${json:+,}\"$db\": {\"bytes\": $bytes, \"entries\": $(grep -cv '^;' <<< "$list"), \"tables\": [$tables]}"
    sizes+="lakehouse_backup_bytes{database=\"$db\"} $bytes"$'\n'
  done
  printf '{"stamp": "%s", "databases": {%s}}\n' "$stamp" "$json" > "$PARTIAL/manifest.json" \
    && chmod 644 "$PARTIAL/manifest.json" && chmod 755 "$PARTIAL" \
    && mv "$PARTIAL" "$ROOT/$stamp" || { record_failure "manifest"; return 1; }
  find "$ROOT" -mindepth 1 -maxdepth 1 -type d -name '20*' -mmin +$(( KEEP_DAYS * 1440 )) -exec rm -rf {} +
  { printf '# HELP lakehouse_backup_last_success_timestamp_seconds When the last catalog backup finished.\n'
    printf '# TYPE lakehouse_backup_last_success_timestamp_seconds gauge\nlakehouse_backup_last_success_timestamp_seconds %s\n' "$(date +%s)"
    printf '# HELP lakehouse_backup_seconds How long the last catalog backup took.\n# TYPE lakehouse_backup_seconds gauge\n'
    printf 'lakehouse_backup_seconds %s\n' $(( $(date +%s) - started ))
    printf '# HELP lakehouse_backup_bytes Size of each database dump in the last backup.\n# TYPE lakehouse_backup_bytes gauge\n'
    printf '%s' "$sizes"; } | write_prom success.prom
  echo "catalog-backup: $stamp done: ${DATABASES[*]}"
}

# newest_age: seconds since the newest complete backup, or a very large number when there is none.
newest_age() {
  local m
  m=$(find "$ROOT" -mindepth 2 -maxdepth 2 -name manifest.json -printf '%T@\n' 2>/dev/null | sort -n | tail -1)
  if [ -n "$m" ]; then echo $(( $(date +%s) - ${m%.*} )); else echo 999999999; fi
}

# seconds_until HH:MM: seconds from now until the next HH:MM, local time.
seconds_until() {
  local next
  next=$(date -d "today $1" +%s)
  [ "$next" -gt "$(date +%s)" ] || next=$(date -d "tomorrow $1" +%s)
  echo $(( next - $(date +%s) ))
}

mkdir -p "$METRICS" && chmod 755 "$ROOT" "$METRICS"   # node-exporter (nobody) reads the metrics
[ "$(newest_age)" -lt $(( 23 * 3600 )) ] || backup_once
while true; do
  sleep "$(seconds_until "${BACKUP_AT:-02:00}")"
  backup_once
done
