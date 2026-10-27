# Watchdog

**Severity:** none · **Fires when:** always, on purpose · **Comes from:** a rule that is always true (`vector(1)`)

## What it means

Watchdog proves alerting works end to end. It goes to ntfy's `lakehouse-heartbeat` topic every 4 hours at the
lowest priority, never to the phone's topic. It is never a problem in itself; its **absence** is.

## What to check

Nothing, unless the heartbeat stops: the acceptance tests (`lakehouse/acceptance/test_ntfy.py`) check that a
Watchdog notice reached `lakehouse-heartbeat` within 5 hours.

## How to fix it

If the heartbeat stops, some link in Prometheus, Alertmanager, ntfy is broken: check each with
`docker compose --profile core ps prometheus alertmanager ntfy`, then see TargetDown and AlertDeliveryFailing.

## When it clears

It never clears: it's meant to fire forever.
