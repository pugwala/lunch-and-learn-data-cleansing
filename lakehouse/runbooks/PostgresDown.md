# PostgresDown

**Severity:** critical · **Fires when:** catalog-pg hasn't answered postgres-exporter for 2 minutes · **Comes from:** postgres-exporter

## What it means

catalog-pg is down. It holds the lake's catalog (DuckLake), Dagster's runs, Superset and OpenMetadata, so almost
everything stops: no loads, no queries through Cube, no dashboards.

## What to check

1. `docker compose --profile core ps catalog-pg` and `docker compose --profile core logs --tail 100 catalog-pg`.
2. Is the disk full? `df -h /lake` (see DiskAlmostFull).

## How to fix it

- Start it again: `docker compose --profile core up -d catalog-pg` (named, so the loader doesn't run).
- After catalog-pg is recreated, Dagster holds dead connections and answers 500 until restarted:
  `docker compose --profile core restart dagster-webserver dagster-daemon`.
- `bash scripts/bootstrap.sh --check` then confirms everything else came back.

## When it clears

When postgres-exporter reaches it again.
