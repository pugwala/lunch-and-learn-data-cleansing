# DiskFilling

**Severity:** warning · **Fires when:** `/` or `/lake` has been over 85% full for 30 minutes · **Comes from:** node-exporter

## What it means

A disk is getting full. At 95% DiskAlmostFull fires; at 100% Postgres, DuckLake writes, backups and the daily
load fail. `/lake` holds the data, the catalog, backups and monitoring; `/` holds Docker's images and the
system.

## What to check

1. How full: `df -h / /lake`.
2. What's big on `/lake`: `sudo du -sh /lake/* | sort -h`.
3. What's big on `/`: `docker system df`.

## How to fix it

- **DuckLake history** (`/lake/data`): history past 30 days is removed weekly; run it now with the Dagster job
  `lake_housekeeping` (Jobs, Launch), or see first what it would remove:
  `docker compose --profile core exec -T jupyter python lakehouse/housekeeping.py --dry-run`.
- **Executed notebooks** (`/lake/notebook-runs`): old dated folders can go.
- **Catalog backups** (`/lake/catalog-backups`): kept 14 days by `lakehouse/backup/backup.sh`; don't delete the
  newest.
- **Docker images** on `/`: `docker image prune` removes only dangling images (ask before anything wider).
- Out of room for good: grow the disk in Proxmox, then the filesystem.

## When it clears

When the disk is back under 85% (checked continuously).
