# DataProductStale

**Severity:** critical · **Fires when:** a data product is older than its descriptor promises · **Comes from:** the `monitoring/data_products` asset, through the Pushgateway

## What it means

The product in the `product` label wasn't refreshed within its `max_age_hours` (lakehouse/products/<product>.yml).
A product's refresh time is the oldest of its tables' latest Dagster materializations, pushed by every daily run.
The custody products' tables are rebuilt by `daily_lake` (26 hours promised); the expanded world's
(`field-incidents`, `incident-damage`, `court-outcomes`) by `world_refresh` and `world_monthly_job` on the 1st
(768 hours, 32 days, promised).

## What to check

1. A custody product: Dagster, Runs: did today's `daily_lake` rebuild its marts? See JobFailing or DailyLakeStale,
   which usually fire too.
2. A world product: did this month's `world_refresh` (04:30 on the 1st) and `world_monthly_job` (05:30) succeed?
   Dagster, Assets, `world_gold`: which of the product's tables was built longest ago?
3. If neither, did today's `daily_lake` reach `monitoring/data_products`? Without it nothing is pushed, and the
   last pushed time ages.

## How to fix it

Get the failing job succeeding again and launch it (for the world: `world_refresh`, then `world_monthly_job` for
the month that just closed). The next `daily_lake` pushes the new refresh time.

## When it clears

When the product's refresh is newer than its max age.
