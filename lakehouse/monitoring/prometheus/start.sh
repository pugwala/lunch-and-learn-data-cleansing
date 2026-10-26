#!/bin/sh
# Starts Prometheus after filling in the probe targets: sites.yml names the host as LAKE_TS_HOST, and the filled-in
# copy goes to the data folder (/prometheus/sd), outside the repo. Prometheus rereads that file when it changes.
set -eu
: "${LAKE_TS_HOST:?LAKE_TS_HOST is not set}"
mkdir -p /prometheus/sd
sed "s|LAKE_TS_HOST|${LAKE_TS_HOST}|g" /etc/prometheus/sites.yml > /prometheus/sd/sites.yml
exec /bin/prometheus "$@"
