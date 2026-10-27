# RestoreDrillFailed

**Severity:** critical · **Fires when:** the latest restore drill failed · **Comes from:** offsite-backup's monthly drill (lakehouse/backup/offsite.sh)

## What it means

The drill couldn't read the repository back, couldn't restore the latest snapshot, or restored files that differ
from their originals (or whose originals are gone). The backup may not give back what was backed up.

## What to check

1. `docker compose --profile core logs --tail 200 offsite-backup | grep -i drill` lists each file that differed or was gone.
2. `cat /lake/offsite-cache/status.json`: compared, mismatched, missing.

## How to fix it

- `restic check --read-data` failed: data in the repository is damaged (often the disk or share underneath). Check
  TrueNAS's pool health, and don't prune; ask before repairing.
- Files differ: something changed the lake's data files, which should never happen once they're written; find what.
- Files gone: housekeeping or a manual cleanup removed them between the backup and the drill; the next drill, right
  after a backup, shows whether it was only timing.

## When it clears

When the next drill passes.
