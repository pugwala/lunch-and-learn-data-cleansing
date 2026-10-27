# CatalogBackupFailed

**Severity:** critical · **Fires when:** the latest catalog backup failed · **Comes from:** catalog-backup (lakehouse/backup/backup.sh), through node-exporter

## What it means

The last backup attempt failed: a dump, the read-back check (`pg_restore --list`), or writing the files.

## What to check

`docker compose --profile core logs --tail 100 catalog-backup` names the database and the step that failed.

## How to fix it

- Fix the cause (a full disk, a role password out of step with `.env`: `bash scripts/bootstrap.sh`), then
  `docker compose --profile core restart catalog-backup` to try again at once.

## When it clears

When the next backup succeeds.
