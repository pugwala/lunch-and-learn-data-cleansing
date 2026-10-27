# LakeHousekeepingStale

**Severity:** warning · **Fires when:** the weekly `lake_housekeeping` hasn't succeeded in 8 days · **Comes from:** Dagster's run sensors, through the Pushgateway

## What it means

The Sunday housekeeping (lakehouse/housekeeping.py) hasn't run successfully in over a week. Nothing breaks at once,
but DuckLake history beyond 30 days, and any personal data that changed, piles up on `/lake`.

## What to check

1. Dagster, Runs, `lake_housekeeping`: the last run and its log.
2. What it would do now:
   `docker compose --profile core exec -T jupyter python lakehouse/housekeeping.py --dry-run`.

## How to fix it

- Launch `lake_housekeeping` from Dagster (Jobs, Launch run). It needs catalog-pg up (see PostgresDown).
- If it never ran: in Dagster, Automation, `lake_housekeeping_schedule` must be on.

## When it clears

When `lake_housekeeping` next succeeds.
