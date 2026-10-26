#!/bin/sh
# Fills in the probe targets: sites.yml names the host as LAKE_TS_HOST, and the filled-in copy goes to Prometheus's
# data folder (/prometheus/sd), outside the repo. start.sh runs it at start, bootstrap.sh on every run; Prometheus
# rereads the file when it changes.
set -eu
: "${LAKE_TS_HOST:?LAKE_TS_HOST is not set}"
mkdir -p /prometheus/sd
sed "s|LAKE_TS_HOST|${LAKE_TS_HOST}|g" /etc/prometheus/sites.yml > /prometheus/sd/sites.yml.new
mv /prometheus/sd/sites.yml.new /prometheus/sd/sites.yml
