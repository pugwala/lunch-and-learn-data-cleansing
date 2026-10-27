# MoreRulesBelowFloor

**Severity:** warning · **Fires when:** more than one quality rule scores under 95% · **Comes from:** the `monitoring/data_quality` asset, through the Pushgateway

## What it means

One rule under 95% is expected (`disciplinary_points`, which the mock data makes messy on purpose). More than one
means another rule has slipped.

## What to check

In Jupyter: `SELECT * FROM gold.dq_scorecard WHERE score_pct < 95` shows which.

## How to fix it

As for DataQualityBelowFloor: trace data that got worse to the load, or fix a wrong rule in
`lakehouse/dbt/models/gold/`.

## When it clears

After a daily run with only the expected rule under the floor.
