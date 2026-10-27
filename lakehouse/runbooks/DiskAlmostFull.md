# DiskAlmostFull

**Severity:** critical · **Fires when:** `/` or `/lake` has been over 95% full for 5 minutes · **Comes from:** node-exporter

## What it means

A disk is nearly full. Writes are about to fail: the daily load, Postgres (the lake's catalog), backups, Prometheus.
Act now; the cleanups are the same as for DiskFilling.

## What to check

1. `df -h / /lake`.
2. `sudo du -sh /lake/* | sort -h` and `docker system df`.

## How to fix it

Follow lakehouse/runbooks/DiskFilling.md, quickest first: Docker's dangling images (`docker image prune`), old
`/lake/notebook-runs` folders, then DuckLake history (the `lake_housekeeping` job). If Postgres has already stopped,
see PostgresDown once there's room.

## When it clears

When the disk is back under 95% (DiskFilling stays until it's under 85%).
