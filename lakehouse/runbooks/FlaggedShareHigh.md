# FlaggedShareHigh

**Severity:** warning · **Fires when:** over 10% of records have a flagged value after a load · **Comes from:** the loader, through the Pushgateway

## What it means

Far more records than usual carry a flagged value (about 3% is normal). The load succeeded, but the data, or the
rules that check it, changed.

## What to check

1. Which rules are flagging: in Jupyter,
   `SELECT rule, column_name, count(*) FROM silver.hero_offenders_exceptions GROUP BY ALL ORDER BY 3 DESC`.
2. Did the generator or the course's rules change? `git log -5 -- generator lnl`.

## How to fix it

- A real change in the data: confirm with the data's owner; raise the threshold in
  lakehouse/monitoring/prometheus/rules/lakehouse.yml only if the new level is right.
- A rule that went wrong: fix it in `lnl/`, then launch `daily_lake`.

## When it clears

After a load with 10% or fewer records flagged.
