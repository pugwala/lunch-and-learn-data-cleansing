# DataQualityBelowFloor

**Severity:** warning · **Fires when:** a quality dimension's pass rate is under 95% · **Comes from:** the `monitoring/data_quality` asset (gold.dq_scorecard), through the Pushgateway

## What it means

One data-quality dimension (the `dimension` label: completeness, validity and so on) passes fewer than 95% of its
checks. Something reporting relies on is worse than it should be.

## What to check

In Jupyter: `SELECT * FROM gold.dq_scorecard WHERE score_pct < 95 ORDER BY score_pct` lists the rules dragging
it down.

## How to fix it

- Data that got worse: trace it to the load (see FlaggedShareHigh).
- A rule that's wrong: the scorecard is built by dbt in `lakehouse/dbt/models/gold/`; fix it there, then
  launch `daily_lake`.

## When it clears

After a daily run where the dimension is back at 95% or more.
