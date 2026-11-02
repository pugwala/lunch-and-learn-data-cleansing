# NotebookSmokeStale

**Severity:** warning · **Fires when:** the weekly `notebook_smoke` hasn't succeeded in 8 days · **Comes from:** Dagster's run sensors, through the Pushgateway

## What it means

The Sunday run of the seven course notebooks and `08_from_lake.ipynb` hasn't passed in over a week, so nobody has
proved lately that the notebooks still run in the lakehouse's Jupyter.

The job is part of course 1's add-on (`lakehouse/course1_addon.py`). The alert only counts while the daily run
reports the add-on on (`lakehouse_course1_addon_enabled` 1); with `LAKEHOUSE_COURSE1=off` there is no job and no
alert.

## What to check

1. Dagster, Runs, `notebook_smoke`: the last one, and its log naming the notebook that failed.
2. The executed copies: `/lake/notebook-runs/<date>_<run>/` (the cell with the error).

## How to fix it

- Fix the notebook or the environment it needs, then launch `notebook_smoke` from Dagster.
- If it never ran: in Dagster, Automation, `notebook_smoke_schedule` must be on.

## When it clears

When `notebook_smoke` next succeeds.
