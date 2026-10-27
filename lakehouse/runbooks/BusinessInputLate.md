# BusinessInputLate

**Severity:** warning · **Fires when:** a business unit is past its deadline to close a month · **Comes from:** the `readiness_inputs` asset (lakehouse/readiness/closes.py), in the daily run

## What it means

The unit in the `unit` label hasn't reported its input (the `input` label) complete for a month within its
`closes_within_days` (lakehouse/products/inputs/<input>.yml). Every data product that depends on it stays
provisional for that month, and says so wherever it's queried.

## What to check

`docker compose --profile core exec -T jupyter python lakehouse/readiness/closes.py status` lists each input, what
it's complete through, who said so, and how late it is.

## How to fix it

- In real life: ask the unit to close the month in their system.
- Here the units are fictional and the daily run plays them; if `readiness_inputs` didn't run, launch `daily_lake`.
- To record a close by hand:
  `docker compose --profile core exec -T jupyter python lakehouse/readiness/closes.py report <input> <month-end> --by "<who>"`.

## When it clears

After the next daily run once the unit has closed the month.
