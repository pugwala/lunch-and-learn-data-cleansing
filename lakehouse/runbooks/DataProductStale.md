# DataProductStale

**Severity:** critical · **Fires when:** a data product is older than its descriptor promises · **Comes from:** the `monitoring/data_products` asset, through the Pushgateway

## What it means

The product in the `product` label wasn't refreshed within its `max_age_hours` (lakehouse/products/<product>.yml).
Its refresh is pushed at the end of the daily run, so a failed or missing `daily_lake` is almost always why.

## What to check

1. Dagster, Runs: did today's `daily_lake` reach `monitoring/data_products`?
2. See JobFailing or DailyLakeStale, which usually fire too.

## How to fix it

Get `daily_lake` succeeding again (JobFailing, DailyLakeStale), then launch it.

## When it clears

When the product's refresh is newer than its max age.
