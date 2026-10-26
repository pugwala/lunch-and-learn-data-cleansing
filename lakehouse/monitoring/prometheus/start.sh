#!/bin/sh
# Starts Prometheus after filling in the probe targets (fill-targets.sh), so the Tailscale name never enters the repo.
set -eu
sh /etc/prometheus/fill-targets.sh
exec /bin/prometheus "$@"
