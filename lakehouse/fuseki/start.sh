#!/bin/bash
# Starts Fuseki for the lakehouse. The writer's password arrives as FUSEKI_WRITER_PASSWORD and is written to a
# private file on the /run/fuseki tmpfs (compose.yaml), so it is never in the image, the repo or a command line.
# Served under /fuseki (Caddy keeps the path); /$/ping is on for the healthcheck; admin answers only inside it.
set -euo pipefail
: "${FUSEKI_WRITER_PASSWORD:?FUSEKI_WRITER_PASSWORD is not set}"
# Fuseki keeps run-time state in its base folder (default ./run, which is /run and not ours). Everything it needs
# comes from config.ttl, so that state can live on the tmpfs and vanish with the container.
export FUSEKI_BASE=/run/fuseki/base
umask 077
mkdir -p "$FUSEKI_BASE"
cp /opt/fuseki/conf/shiro.ini "$FUSEKI_BASE/shiro.ini"   # ours, with no users, before Fuseki writes its default
printf 'writer: %s\n' "$FUSEKI_WRITER_PASSWORD" > /run/fuseki/passwd
exec java ${JAVA_OPTIONS:-} -jar /opt/fuseki/fuseki-server.jar \
  --config=/opt/fuseki/conf/config.ttl --contextPath=/fuseki --ping
