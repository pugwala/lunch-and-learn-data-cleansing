# SiteDown

**Severity:** critical · **Fires when:** a site hasn't answered through Caddy for 5 minutes · **Comes from:** blackbox, probing each site on the Tailscale name (`monitoring/prometheus/sites.yml`)

## What it means

One of the sites (the `site` label: jupyter, dagster, superset, mcp, openmetadata, fuseki, prometheus, grafana,
alertmanager, ntfy) doesn't answer with the expected status through Caddy, on the tailnet, with a trusted
certificate. People and tools can't reach it.

## What to check

1. Which site, and what it answers: `bash scripts/bootstrap.sh --check` checks every site the same way and prints
   each status.
2. Is the service itself up? `docker compose --profile core ps <service>` and `docker compose --profile core logs --tail 100 <service>`.
3. Is Caddy up? `docker compose --profile core logs --tail 100 caddy`.
4. Is Tailscale up on the VM? `tailscale status`.

## How to fix it

- The service is down: `docker compose --profile core restart <service>`.
- Caddy is down or lost its config: `docker compose --profile core restart caddy`.
- Tailscale is logged out or stopped: `sudo tailscale up` (then check the name with `tailscale status`).
- A certificate problem shows as a TLS error in the probe: see CertificateExpiresSoon.

## When it clears

When the site answers through Caddy again, at the next probe (every 30 seconds).
