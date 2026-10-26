#!/bin/bash
# Dagster's web UI has no login of its own, so Caddy puts one in front of it. This asks for a password,
# hashes it with Caddy's bcrypt, and writes DAGSTER_UI_USER and DAGSTER_UI_HASH into .env. The password
# itself is never stored.  Run from lakehouse/:  bash scripts/set-dagster-ui-password.sh
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo "No .env here; copy .env.example first." >&2; exit 1; }

read -rp "Dagster UI user name [admin]: " user; user=${user:-admin}
read -rsp "Password: " pw; echo
read -rsp "Again: " pw2; echo
[ "$pw" = "$pw2" ] || { echo "Passwords don't match." >&2; exit 1; }
[ ${#pw} -ge 12 ] || { echo "Use at least 12 characters." >&2; exit 1; }

hash=$(docker run --rm "caddy:${CADDY_TAG:-2}" caddy hash-password --plaintext "$pw")
sed -i '/^DAGSTER_UI_USER=/d; /^DAGSTER_UI_HASH=/d' .env
printf "DAGSTER_UI_USER=%s\nDAGSTER_UI_HASH='%s'\n" "$user" "$hash" >> .env
setfacl -b .env 2>/dev/null || true; chmod 600 .env   # sed -i made a new file, which inherits the repo's default ACL
echo "Saved. Apply with: docker compose --profile core up -d caddy"
