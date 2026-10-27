# TargetDown

**Severity:** critical · **Fires when:** a scrape target has answered nothing for 5 minutes · **Comes from:** Prometheus's own `up` metric

## What it means

Prometheus can't scrape one of its targets (the alert's `job` label): node-exporter, cAdvisor, postgres-exporter,
blackbox, the Pushgateway, Alertmanager, ntfy or Prometheus itself. While it's down, that part of the lakehouse
isn't being watched, and alerts that depend on its numbers can't fire.

## What to check

1. Which target: `https://<LAKE_TS_HOST>/prometheus/targets` (the Dagster login) lists each one with its last error.
2. Is the service running and healthy?
   ```bash
   docker compose --profile core ps
   docker compose --profile core logs --tail 100 <service>
   ```

## How to fix it

- Restart the one service: `docker compose --profile core restart <service>` (by name: never a bare `up`, which also runs the loader).
- If it keeps stopping, its logs say why; a full disk (DiskAlmostFull) or low memory (MemoryLow) are the usual
  causes.
- If the whole stack is unhappy, `bash scripts/bootstrap.sh --check` shows what's off without changing anything,
  and `bash scripts/bootstrap.sh` puts it right.

## When it clears

When the target answers a scrape again; the alert resolves on Alertmanager's next group interval.
