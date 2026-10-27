# SilverDroppedRows

**Severity:** critical · **Fires when:** silver holds fewer rows than bronze after a load · **Comes from:** the loader (`load_lake`), through the Pushgateway

## What it means

The load lost records between bronze and silver. That must never happen: the course's rule is flag, don't drop,
so every bad value stays in silver with a row in `silver.hero_offenders_exceptions`.

## What to check

1. The numbers per load: `SELECT * FROM ops.load_runs ORDER BY finished_at DESC LIMIT 5` (in Jupyter, `%sql`).
2. Did the course code (`lnl/`) or lakehouse/lake.py change since the last good load? `git log -5 -- lnl lakehouse/lake.py`.

## How to fix it

- Find and undo the change that drops rows, then launch `daily_lake` again.
- The parity check (19 checks against the Facilitator Guide) is blocking, so gold isn't rebuilt from a broken
  silver in the meantime.

## When it clears

When a load writes as many silver rows as bronze.
