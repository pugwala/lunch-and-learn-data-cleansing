#!/bin/sh
# The off-machine backup, run by the offsite-backup service from the restic image (BusyBox sh: no bash, no jq).
# Every night at BACKUP_AT (local time in TZ) it backs up the catalog dumps (/lake/catalog-backups, written by
# catalog-backup at 02:00) and the lake's data files (/lake/data) together into an encrypted, deduplicated restic
# repository on the backup target, keeps KEEP_DAILY, KEEP_WEEKLY and KEEP_MONTHLY snapshots, and checks the
# repository. Once every DRILL_DAYS it then runs a restore drill: it reads every byte of the repository back,
# restores the latest snapshot into the cache and compares each restored file with its original. Data files never
# change once written and dumps are kept 14 days, so every file must still match.
#
# The target must hold .lakehouse-backup-target, saying "local" or "truenas" (bootstrap.sh writes "local" for a
# folder on this VM). A share that didn't mount has no marker, so the backup fails loudly instead of filling the
# VM's own disk. Metrics go to node-exporter's textfile collector (/metrics), and /cache/status.json (on
# /lake/offsite-cache, readable by the tests) says what the last backup and drill did. It backs up at start when
# the last success is over 23 hours old.
set -u   # no -e: a failed step is recorded, and the loop goes on to the next night

MARKER=/target/.lakehouse-backup-target
export RESTIC_REPOSITORY=${RESTIC_REPOSITORY:-/target/restic}   # compose sets these too, for `exec ... restic`
export RESTIC_PASSWORD_FILE=${RESTIC_PASSWORD_FILE:-/run/secrets/restic_password} RESTIC_CACHE_DIR=${RESTIC_CACHE_DIR:-/cache/restic}
STATUS=/cache/status.json
umask 077

# write_prom FILE: write stdin to /metrics/FILE in one rename, so node-exporter never reads half a file.
write_prom() {
  tmp=$(mktemp "/metrics/.$1.XXXXXX") && cat > "$tmp" && chmod 644 "$tmp" && mv "$tmp" "/metrics/$1"
}

# gauge NAME HELP VALUE: one gauge in Prometheus's text format.
gauge() {
  printf '# HELP %s %s\n# TYPE %s gauge\n%s %s\n' "$1" "$2" "$1" "$1" "$3"
}

# last NAME FILE: the value /metrics/FILE holds for NAME, or 0 when there is none.
last() {
  v=$(sed -n "s/^$1 //p" "/metrics/$2" 2>/dev/null)
  echo "${v:-0}"
}

# backup_failed WHAT: log a failed backup step and record when, for OffsiteBackupFailed.
backup_failed() {
  echo "offsite-backup: FAILED: $1" >&2
  gauge lakehouse_offsite_backup_last_failure_timestamp_seconds "When an off-machine backup last failed." \
    "$(date +%s)" | write_prom offsite_failure.prom
}

# drill_failed WHAT: log a failed restore drill and record when, for RestoreDrillFailed.
drill_failed() {
  echo "offsite-backup: drill FAILED: $1" >&2
  gauge lakehouse_restore_drill_last_failure_timestamp_seconds "When a restore drill last failed." \
    "$(date +%s)" | write_prom drill_failure.prom
  rm -rf /cache/drill /cache/drill-files
}

# write_public FILE: write stdin to FILE (readable by all, like the metrics) in one rename.
write_public() {
  tmp=$(mktemp "$1.XXXXXX") && cat > "$tmp" && chmod 644 "$tmp" && mv "$tmp" "$1"
}

# drill: read every byte back, restore the latest snapshot, compare each restored file with its original.
drill() {
  rm -rf /cache/drill /cache/drill-files
  restic check --read-data || { drill_failed "restic check --read-data"; return; }
  restic restore latest --host lakehouse --target /cache/drill || { drill_failed "restic restore"; return; }
  find /cache/drill -type f > /cache/drill-files
  compared=0 mismatched=0 missing=0
  while read -r restored; do
    original=${restored#/cache/drill}
    if [ ! -f "$original" ]; then
      missing=$((missing + 1))
      echo "offsite-backup: drill: $original is gone" >&2
    elif cmp -s "$restored" "$original"; then
      compared=$((compared + 1))
    else
      mismatched=$((mismatched + 1))
      echo "offsite-backup: drill: $original differs" >&2
    fi
  done < /cache/drill-files
  rm -rf /cache/drill /cache/drill-files
  now=$(date +%s)
  printf '{"finished": %s, "compared": %s, "mismatched": %s, "missing": %s}\n' \
    "$now" "$compared" "$mismatched" "$missing" | write_public /cache/drill.json
  if [ "$compared" -gt 0 ] && [ "$mismatched" -eq 0 ] && [ "$missing" -eq 0 ]; then
    { gauge lakehouse_restore_drill_last_success_timestamp_seconds "When the last restore drill matched every file." "$now"
      gauge lakehouse_restore_drill_files "Files the last restore drill restored and found identical." "$compared"
    } | write_prom drill.prom
    echo "offsite-backup: drill: all $compared restored files identical"
  else
    drill_failed "$mismatched differ, $missing gone, $compared identical"
  fi
}

# backup_once: back up, keep the retention, check the repository, record the results; then a drill when one is due.
backup_once() {
  started=$(date +%s)
  [ -f "$MARKER" ] || { backup_failed "no $MARKER: the backup target isn't prepared, or the share didn't mount"; return; }
  kind=$(cat "$MARKER")
  if [ ! -f "$RESTIC_REPOSITORY/config" ]; then
    restic init || { backup_failed "restic init"; return; }
  fi
  restic backup --host lakehouse --tag nightly --exclude /lake/catalog-backups/metrics \
    /lake/catalog-backups /lake/data || { backup_failed "restic backup"; return; }
  restic forget --host lakehouse --keep-daily "${KEEP_DAILY:-7}" --keep-weekly "${KEEP_WEEKLY:-4}" \
    --keep-monthly "${KEEP_MONTHLY:-6}" --prune || { backup_failed "restic forget"; return; }
  restic check || { backup_failed "restic check"; return; }
  snapshots=$(restic snapshots --host lakehouse --json | grep -o '"short_id"' | wc -l)
  paths=$(restic snapshots latest --host lakehouse --json | sed -n 's/.*"paths":\(\[[^]]*\]\).*/\1/p')
  bytes=$(restic stats --mode raw-data --json | sed -n 's/.*"total_size":\([0-9]*\).*/\1/p')
  off=1
  [ "$kind" = local ] && off=0
  now=$(date +%s)
  { gauge lakehouse_offsite_backup_last_success_timestamp_seconds "When the last off-machine backup finished." "$now"
    gauge lakehouse_offsite_backup_seconds "How long the last off-machine backup took." "$((now - started))"
    gauge lakehouse_offsite_backup_snapshots "Snapshots the backup repository keeps." "$snapshots"
    gauge lakehouse_offsite_backup_repository_bytes "Bytes the repository holds (deduplicated, compressed)." "${bytes:-0}"
    gauge lakehouse_offsite_backup_off_machine "1 when the target is off this machine (TrueNAS), 0 for a local folder." "$off"
  } | write_prom offsite.prom
  echo "offsite-backup: backed up to $kind: $snapshots snapshots, ${bytes:-0} bytes"
  [ $((now - $(last lakehouse_restore_drill_last_success_timestamp_seconds drill.prom))) -lt \
    $(( ${DRILL_DAYS:-30} * 86400 )) ] || drill
  drill_json=$(cat /cache/drill.json 2>/dev/null)
  printf '{"target": "%s", "last_backup": %s, "paths": %s, "snapshots": %s, "repository_bytes": %s, "drill": %s}\n' \
    "$kind" "$now" "${paths:-[]}" "$snapshots" "${bytes:-0}" "${drill_json:-null}" | write_public "$STATUS"
}

# seconds_until HH:MM: seconds from now until the next HH:MM, local time (BusyBox date can't parse "today HH:MM").
seconds_until() {
  set -- $(date '+%H %M %S') "$1"
  now=$(( $(echo "$1" | sed 's/^0//') * 3600 + $(echo "$2" | sed 's/^0//') * 60 + $(echo "$3" | sed 's/^0//') ))
  at=$(( $(echo "${4%:*}" | sed 's/^0//') * 3600 + $(echo "${4#*:}" | sed 's/^0//') * 60 ))
  wait=$((at - now))
  [ "$wait" -gt 0 ] || wait=$((wait + 86400))
  echo "$wait"
}

mkdir -p /cache && chmod 755 /cache   # the status file is for the tests (other users on the lake disk)
[ $(( $(date +%s) - $(last lakehouse_offsite_backup_last_success_timestamp_seconds offsite.prom) )) -lt $((23 * 3600)) ] \
  || backup_once
while true; do
  sleep "$(seconds_until "${BACKUP_AT:-02:30}")"
  backup_once
done
