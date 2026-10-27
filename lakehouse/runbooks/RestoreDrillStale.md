# RestoreDrillStale

**Severity:** warning · **Fires when:** no restore drill has succeeded in 35 days, for an hour · **Comes from:** offsite-backup's monthly drill (lakehouse/backup/offsite.sh)

## What it means

The monthly drill (read every byte of the repository back, restore the latest snapshot, compare each file with its
original) hasn't passed in over a month, so nobody knows the backup can actually be restored.

## What to check

1. `cat /lake/offsite-cache/status.json`: the `drill` part says when it last ran and what it found.
2. `docker compose --profile core logs --tail 200 offsite-backup | grep -i drill`.

## How to fix it

- The drill runs after a backup once the last one is over 30 days old: if backups are failing, fix that first
  (OffsiteBackupFailed).
- To run one now: set `DRILL_DAYS: "0"` for offsite-backup in lakehouse/compose.yaml, `docker compose --profile core up -d offsite-backup`,
  let it back up and drill, then put `DRILL_DAYS` back to 30.
- The full drill (the catalog restored into a scratch database, the 19 parity checks on the restored lake):
  `bash lakehouse/backup/restore_drill.sh`.

## When it clears

When a drill passes.
