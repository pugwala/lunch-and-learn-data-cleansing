# OffsiteBackupStale

**Severity:** critical · **Fires when:** no off-machine backup has succeeded in 26 hours, for 30 minutes · **Comes from:** offsite-backup (lakehouse/backup/offsite.sh), through node-exporter

## What it means

The nightly 02:30 copy of the catalog dumps and the lake's data to the backup target (TrueNAS, or a local folder until
that exists) hasn't succeeded in over a day. Lose the VM or its disk now and the newest copy is older than that.

## What to check

1. `docker compose --profile core ps offsite-backup` and `docker compose --profile core logs --tail 100 offsite-backup`.
2. What it last did: `cat /lake/offsite-cache/status.json`.
3. Is the target there? `ls -la "$(grep '^BACKUP_TARGET=' .env | cut -d= -f2-)"` must show `.lakehouse-backup-target`
   (and, for TrueNAS, `mountpoint` on that folder says it's a mount).

## How to fix it

- The share isn't mounted (no marker): mount it again (`sudo mount -a`), then `docker compose --profile core restart offsite-backup`, which backs
  up at once when the last success is over 23 hours old.
- The service isn't running: `docker compose --profile core up -d offsite-backup` (named).
- A failed step: see OffsiteBackupFailed.

## When it clears

When a backup succeeds.
