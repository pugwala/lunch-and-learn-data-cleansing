# AlertDeliveryFailing

**Severity:** critical · **Fires when:** Alertmanager failed to deliver a notification in the last 15 minutes · **Comes from:** Alertmanager's own counters, scraped by Prometheus

## What it means

Alerts aren't reaching ntfy, so they may not reach the phone. If ntfy itself is down, this alert can't reach the
phone either: look in Alertmanager (`https://<LAKE_TS_HOST>/alertmanager/`) or Grafana.

## What to check

1. Why: `docker compose --profile core logs --tail 100 alertmanager | grep -i -E 'notify|error'`.
2. Is ntfy up? `docker compose --profile core ps ntfy` and `docker compose --profile core logs --tail 50 ntfy`.
3. A 401 means the `alertmanager` user's password differs between ntfy and Alertmanager.

## How to fix it

- ntfy down: `docker compose --profile core restart ntfy`.
- A 401 after `NTFY_ALERTMANAGER_PASSWORD` changed in `.env`: both must restart to read it:
  `docker compose --profile core up -d ntfy alertmanager` (named services), or `bash scripts/bootstrap.sh`.

## When it clears

After 15 minutes with no failed notification.
