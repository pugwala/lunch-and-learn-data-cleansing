# JobFailing

**Severity:** critical · **Fires when:** a Dagster job's latest run failed (newer than its latest success) · **Comes from:** Dagster's run sensors, through the Pushgateway

## What it means

The job named in the `pipeline` label failed its latest run: `daily_lake` (the 05:00 load and everything after
it), `regenerate_lake`, `notebook_smoke` (weekly notebooks), or `lake_housekeeping` (weekly). Alerts for
`monitoring_selftest` are the tests failing it on purpose; they go to a test topic, never the phone.

## What to check

1. Dagster: `https://<LAKE_TS_HOST>/dagster/` (the Dagster login), Runs, the failed run: the step in red and its
   logs say what broke.
2. For `daily_lake`, a failed blocking check (`matches_course_numbers`) stops gold on purpose: the lake no longer
   matches the course's numbers.

## How to fix it

- Fix what the logs show, then launch the job again from Dagster (Jobs, the job, Launch run).
- `daily_lake` failing at a dbt test: that test's model is wrong or the data changed; `lakehouse/dbt/` holds both.
- `notebook_smoke`: the executed copies in `/lake/notebook-runs/<date>_<run>/` show the cell that failed.

## When it clears

When the job's next run succeeds (its success is then newer than the failure).
