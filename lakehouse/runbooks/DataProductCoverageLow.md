# DataProductCoverageLow

**Severity:** warning · **Fires when:** over 10% of records can't be placed in time for a product · **Comes from:** the `monitoring/data_products` asset, through the Pushgateway

## What it means

The custody snapshot places each record at the month-ends it's open; records with no sentence or release date
can't be placed. About 6% is normal; over 10% means the products undercount who's held.

## What to check

In Jupyter: `SELECT * FROM mart_custody.snapshot_coverage`, then, in silver, which records lack the dates and why.

## How to fix it

- Dates lost in loading or cleansing: trace it to the load (see FlaggedShareHigh).
- A real change in the data: confirm with the data's owner before raising the threshold in
  lakehouse/monitoring/prometheus/rules/lakehouse.yml.

## When it clears

After a daily run with 10% or fewer records undated.
