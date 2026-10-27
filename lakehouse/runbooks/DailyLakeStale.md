# DailyLakeStale

**Severity:** critical · **Fires when:** `daily_lake` hasn't succeeded in 26 hours, for an hour · **Comes from:** Dagster's run sensors, through the Pushgateway

## What it means

The 05:00 load hasn't finished successfully in over a day, so gold, the marts, OpenMetadata and Fuseki are stale.
Either it's failing (JobFailing also fires) or it isn't running at all.

## What to check

1. Dagster, Runs: did today's `daily_lake` run, and how did it end?
2. Is the scheduler running? `docker compose --profile core ps dagster-daemon` and `docker compose --profile core logs --tail 100 dagster-daemon`.
3. In Dagster, Automation: `daily_schedule` must be on.

## How to fix it

- It failed: see JobFailing.
- It never started: `docker compose --profile core restart dagster-daemon`, and turn `daily_schedule` back on if it's off.
- Then launch `daily_lake` by hand rather than waiting for 05:00.

## When it clears

When `daily_lake` next succeeds.
