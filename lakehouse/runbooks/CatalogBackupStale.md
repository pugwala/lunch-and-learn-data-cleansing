# CatalogBackupStale

**Severity:** critical · **Fires when:** no catalog backup has succeeded in 26 hours, for 30 minutes · **Comes from:** catalog-backup (lakehouse/backup/backup.sh), through node-exporter

## What it means

The nightly 02:00 dump of every app database hasn't succeeded in over a day. If catalog-pg were lost now, the
Parquet files would be just files: the catalog is what makes them a lake.

## What to check

1. `docker compose --profile core ps catalog-backup` and `docker compose --profile core logs --tail 100 catalog-backup`.
2. The newest backup: `sudo ls -lt /lake/catalog-backups | head`.
3. Is `/lake` full? `df -h /lake`.

## How to fix it

- Restarting it takes a backup at once when the newest is over 23 hours old: `docker compose --profile core restart catalog-backup`.
- A password error for `lake_backup`: `bash scripts/bootstrap.sh` puts the role's password back in step with
  `.env`.

## When it clears

When a backup succeeds.
