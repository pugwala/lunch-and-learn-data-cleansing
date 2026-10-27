#!/bin/sh
# Start ntfy with its three users declared. The plain passwords come from .env (via Compose); each is hashed here,
# at start, so no hash is kept anywhere and changing a password in .env is a restart. server.yml holds the rest.
set -eu

# hash VAR: the bcrypt hash of the password in environment variable VAR, from ntfy's own hasher.
hash() {
  eval "pw=\${$1:?$1 is not set}"
  printf '%s\n%s\n' "$pw" "$pw" | ntfy user hash 2>/dev/null | tail -1
}

NTFY_AUTH_USERS="alertmanager:$(hash NTFY_ALERTMANAGER_PASSWORD):user,ian:$(hash NTFY_IAN_PASSWORD):user,checker:$(hash NTFY_CHECKER_PASSWORD):user"
export NTFY_AUTH_USERS
unset NTFY_ALERTMANAGER_PASSWORD NTFY_IAN_PASSWORD NTFY_CHECKER_PASSWORD pw
exec ntfy serve
