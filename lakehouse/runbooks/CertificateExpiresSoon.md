# CertificateExpiresSoon

**Severity:** warning · **Fires when:** the Tailscale certificate has under 14 days left, for an hour · **Comes from:** blackbox's TLS probe of each site

## What it means

Caddy gets its certificate from Tailscale (`get_certificate tailscale` in `lakehouse/caddy/Caddyfile`) and it
should renew well before expiry. Under 14 days means renewal isn't happening; when it expires, every site fails
with a certificate error.

## What to check

1. Caddy's view: `docker compose --profile core logs --tail 200 caddy | grep -i -E 'certif|tls|tailscale'`.
2. Tailscale's view: `tailscale status`; in the Tailscale admin console, HTTPS certificates must still be enabled
   for the tailnet.

## How to fix it

- Restart Caddy so it asks Tailscale again: `docker compose --profile core restart caddy`.
- If Tailscale itself can't issue one, re-enable HTTPS in the admin console, then restart Caddy.

## When it clears

When the probe sees a certificate with more than 14 days left.
