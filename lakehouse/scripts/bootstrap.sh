#!/bin/bash
# Sets up or repairs the lakehouse on this VM, then proves it works. Safe to run again at any time: every step
# checks first and only does what's missing, so on a healthy install it changes nothing and prints a report.
#
#   bash lakehouse/scripts/bootstrap.sh            # from anywhere; runs as your user, uses sudo where needed
#   bash lakehouse/scripts/bootstrap.sh --reload   # also re-run the loader even if the lake already has data
#   bash lakehouse/scripts/bootstrap.sh --check    # verify only: no changes at all
#
# It never formats a disk. If /lake isn't the mounted data disk it stops and says what to do.
set -euo pipefail
cd "$(dirname "$0")/.."                      # lakehouse/
REPO=$(cd .. && pwd)

RELOAD=0 CHECK=0
for arg in "$@"; do
  case $arg in
    --reload) RELOAD=1 ;;
    --check)  CHECK=1 ;;
    -h|--help) sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $arg (try --help)" >&2; exit 2 ;;
  esac
done

SECRETS=(POSTGRES_PASSWORD LAKE_RW_PASSWORD LAKE_RO_PASSWORD DAGSTER_PG_PASSWORD JUPYTER_TOKEN
         CUBEJS_API_SECRET CUBE_SQL_PASSWORD SUPERSET_SECRET_KEY SUPERSET_PG_PASSWORD SUPERSET_ADMIN_PASSWORD
         MCP_TOKEN OM_DB_PASSWORD OM_ADMIN_PASSWORD OM_FERNET_KEY OM_MCP_PASSWORD FUSEKI_WRITER_PASSWORD GRAFANA_ADMIN_PASSWORD LAKE_MONITOR_PASSWORD LAKE_BACKUP_PASSWORD
         NTFY_ALERTMANAGER_PASSWORD NTFY_IAN_PASSWORD NTFY_CHECKER_PASSWORD)
SERVICES=(catalog-pg jupyter dagster-webserver dagster-daemon cube superset mcp opensearch openmetadata-server fuseki prometheus node-exporter cadvisor postgres-exporter blackbox pushgateway catalog-backup ntfy alertmanager grafana caddy)   # never a bare `up`: it also runs load-lake
ROLE_SCRIPTS=(02-dagster.sh 03-superset.sh 04-openmetadata.sh 05-monitor.sh 06-backup.sh)   # app roles on catalog-pg; postgres/init/ only runs on an empty volume
CONTAINER_UID=1000 CONTAINER_GID=100                                    # jovyan:users in the image
BACKUP_UID=999 BACKUP_GID=999   # catalog-backup's postgres user: the dumps hold every app database, so not uid 1000
FAILED=()

# step TITLE: print a step heading.
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
# ok MESSAGE: report a check that passed.
ok()   { printf '  \033[32mok\033[0m    %s\n' "$*"; }
# did MESSAGE: report something this run changed.
did()  { printf '  \033[36mdone\033[0m  %s\n' "$*"; }
# warn MESSAGE: report a problem that doesn't fail the run.
warn() { printf '  \033[33mwarn\033[0m  %s\n' "$*"; }
# bad MESSAGE: report a failed check; the run exits 1 at the end, after every check has had its say.
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; FAILED+=("$*"); }
# die MESSAGE: stop now, with the message on stderr.
die()  { printf '\n\033[31m%s\033[0m\n' "$*" >&2; exit 1; }
# dc ARGS: docker compose for the core profile.
dc()   { docker compose --profile core "$@"; }
# env_val KEY: KEY's value in .env (the last one, quotes stripped), or empty when it's missing.
env_val() { { grep -E "^$1=" .env 2>/dev/null || true; } | tail -1 | cut -d= -f2- | sed -e "s/^'//" -e "s/'$//"; }   # missing key = empty, not a pipefail exit

# .env must stay owner-only. Anything that rewrites it (sed -i, editors) makes a new file, and new files here
# inherit the repo's default ACL, which would let the container user read every secret.
lock_env() { setfacl -b .env 2>/dev/null || true; chmod 600 .env; }

# ---------------------------------------------------------------------------------------------------------------
step "1/7 Tools"
for c in docker openssl curl setfacl findmnt; do
  if command -v $c >/dev/null; then ok "$c"
  elif [ $c = setfacl ] && [ $CHECK = 0 ]; then sudo apt-get install -y -qq acl >/dev/null && did "installed acl (setfacl)"
  else die "$c is missing. Install it first (Docker: https://docs.docker.com/engine/install/debian/)."; fi
done
docker compose version >/dev/null 2>&1 || die "The Docker Compose plugin is missing (apt install docker-compose-plugin)."
docker info >/dev/null 2>&1 || die "Can't talk to Docker. Is your user in the docker group? (log out and in after adding it)"
ok "docker compose, and your user can reach the Docker daemon"

# ---------------------------------------------------------------------------------------------------------------
step "2/7 Data disk"
LAKE_ROOT=$(env_val LAKE_ROOT); LAKE_ROOT=${LAKE_ROOT:-/lake}
if src=$(findmnt -n -o SOURCE --mountpoint "$LAKE_ROOT" 2>/dev/null); then
  ok "$LAKE_ROOT is mounted from $src ($(findmnt -n -o SIZE --mountpoint "$LAKE_ROOT"), label: $(lsblk -dn -o LABEL "$src" 2>/dev/null || echo '?'))"
else
  dev=$(sudo blkid -L lake 2>/dev/null || true)
  if [ -n "$dev" ] && elsewhere=$(findmnt -n -o TARGET --source "$dev" 2>/dev/null); then
    die "The 'lake' disk ($dev) is mounted at $elsewhere, but LAKE_ROOT in .env is $LAKE_ROOT. Set LAKE_ROOT=$elsewhere in .env."
  elif [ -n "$dev" ]; then
    die "$LAKE_ROOT is NOT mounted, but $dev is labeled 'lake'. If $LAKE_ROOT is empty, run: sudo mount $LAKE_ROOT
If it already holds files, they're on the OS disk: move them onto the data disk first (see the Work Log, 2026-10-25)."
  fi
  die "$LAKE_ROOT is NOT a mounted disk, and no disk is labeled 'lake'. Data written now would land on the OS disk.
Find the data disk by size with: lsblk -o NAME,SIZE,FSTYPE,LABEL  (and its stable name in /dev/disk/by-id/).
Formatting erases a disk, so this script never does it. Once you're sure which disk it is:
  sudo mkfs.ext4 -L lake /dev/disk/by-id/<data disk>
  grep -q 'LABEL=lake' /etc/fstab || echo 'LABEL=lake $LAKE_ROOT ext4 defaults,noatime,nofail 0 2' | sudo tee -a /etc/fstab
  sudo mkdir -p $LAKE_ROOT && sudo mount $LAKE_ROOT
Then run this script again."
fi

for d in landing data notebook-runs catalog-backups models dagster openmetadata fuseki prometheus grafana pushgateway alertmanager ntfy; do
  p="$LAKE_ROOT/$d"
  [ -d "$p" ] || { [ $CHECK = 1 ] && { bad "$p is missing"; continue; }; sudo mkdir -p "$p"; did "created $p"; }
  owner=$CONTAINER_UID:$CONTAINER_GID
  [ $d = catalog-backups ] && owner=$BACKUP_UID:$BACKUP_GID
  if [ "$(stat -c %u:%g "$p")" = "$owner" ]; then ok "$p owned by $owner"
  elif [ $CHECK = 1 ]; then bad "$p is owned by $(stat -c %u:%g "$p"), want $owner"
  else sudo chown -R "$owner" "$p"; did "chown $owner $p"; fi
done

# ---------------------------------------------------------------------------------------------------------------
step "3/7 Repo permissions"
# The containers run as uid 1000 and write into the mounted repo (pytest cache, notebook saves). If you log in
# as a different uid, ACLs let both of you write. -d sets the default ACL that new files inherit.
me=$(id -u)
if [ "$me" = $CONTAINER_UID ]; then ok "you are uid $CONTAINER_UID, same as the containers: no ACLs needed"
elif getfacl -pn "$REPO" 2>/dev/null | grep -q "^default:user:$CONTAINER_UID:rwx"; then ok "repo ACLs grant uid $CONTAINER_UID and uid $me"   # -n: numeric ids, not names
elif [ $CHECK = 1 ]; then bad "repo has no ACL for uid $CONTAINER_UID"
else
  setfacl -R    -m u:$CONTAINER_UID:rwX -m u:"$me":rwX "$REPO"
  setfacl -R -d -m u:$CONTAINER_UID:rwX -m u:"$me":rwX "$REPO"
  did "granted uid $CONTAINER_UID and uid $me rwX on the repo"
fi

# ---------------------------------------------------------------------------------------------------------------
step "4/7 Secrets (.env)"
if [ ! -f .env ]; then
  [ $CHECK = 1 ] && die ".env is missing."
  cp .env.example .env; did "created .env from .env.example"
fi
lock_env

dups=$(grep -oE '^[A-Z_]+=' .env | sort | uniq -d | tr -d = | tr '\n' ' ')
[ -z "$dups" ] || die "These keys appear more than once in .env: $dups
Compose uses the LAST one, which can silently disagree with what Postgres has. Delete the stale line(s), then re-run."
ok "no duplicate keys"

# Image tags are pinned in compose.yaml and the Dockerfiles. An override in .env wins over them, so a floating one
# (latest, or a bare major such as 17) would quietly undo the pins.
floating=$(grep -E '^[A-Z_]+_TAG=(latest|v?[0-9]+)$' .env || true)
if [ -n "$floating" ]; then
  while read -r line; do bad "$line floats: set an exact release in .env, or delete the line to use the pin"; done <<< "$floating"
else ok "no floating image tags in .env"; fi

# set_env_key KEY VALUE: set KEY in .env, replacing its line or adding one. Rewrites through a temp file and cats it
# back, so .env keeps its inode, owner and mode (sed -i would not).
set_env_key() {
  local tmp; tmp=$(mktemp)
  if grep -qE "^$1=" .env; then awk -v k="$1" -v v="$2" -F= '$1==k {print k"="v; next} {print}' .env > "$tmp"
  else cat .env > "$tmp"; echo "$1=$2" >> "$tmp"; fi
  cat "$tmp" > .env; rm -f "$tmp"
}

for k in "${SECRETS[@]}"; do
  v=$(env_val "$k")
  if [ -n "$v" ] && [ "$v" != CHANGE_ME ]; then ok "$k is set"; continue; fi
  [ $CHECK = 1 ] && { bad "$k is not set"; continue; }
  case $k in   # most secrets are 48 hex characters; two need their own shape
    OM_FERNET_KEY)     val=$(openssl rand -base64 32 | tr '+/' '-_') ;;   # Fernet: 32 random bytes, base64url
    OM_ADMIN_PASSWORD|OM_MCP_PASSWORD) val="Lk$(openssl rand -hex 24)!" ;;   # meets OpenMetadata's password policy
    *)                 val=$(openssl rand -hex 24) ;;
  esac
  set_env_key "$k" "$val"
  did "generated $k"
done

# OpenMetadata signs its login tokens with an RSA key. The image ships default keys anyone can copy, so generate our
# own once, on the lake disk (never in the repo), readable only by the container user. sudo test: the folder is 700.
OM_KEYS="$LAKE_ROOT/openmetadata/keys"
if sudo test -s "$OM_KEYS/private_key.der" && sudo test -s "$OM_KEYS/public_key.der"; then ok "OpenMetadata signing keys present"
elif [ $CHECK = 1 ]; then bad "OpenMetadata signing keys missing in $OM_KEYS"
else
  tmp=$(mktemp -d)
  openssl genrsa -out "$tmp/key.pem" 2048 2>/dev/null
  openssl pkcs8 -topk8 -inform PEM -outform DER -in "$tmp/key.pem" -out "$tmp/private_key.der" -nocrypt
  openssl rsa -in "$tmp/key.pem" -pubout -outform DER -out "$tmp/public_key.der" 2>/dev/null
  sudo mkdir -p "$OM_KEYS" && sudo cp "$tmp/private_key.der" "$tmp/public_key.der" "$OM_KEYS/"
  rm -rf "$tmp"
  sudo chown -R $CONTAINER_UID:$CONTAINER_GID "$OM_KEYS" && sudo chmod 700 "$OM_KEYS" && sudo chmod 600 "$OM_KEYS/private_key.der"
  did "generated OpenMetadata signing keys in $OM_KEYS"
fi

# This VM's Tailscale name. Caddy serves the sites there with a real certificate from Tailscale. Not a secret.
TS_HOST=$(env_val LAKE_TS_HOST)
if [ -n "$TS_HOST" ]; then ok "Tailscale name: $TS_HOST"
elif [ $CHECK = 1 ]; then bad "LAKE_TS_HOST is not set"
else
  command -v tailscale >/dev/null || die "Tailscale isn't installed; Caddy serves the sites on this VM's Tailscale name.
Install it (https://tailscale.com/download/linux) and sign in with: sudo tailscale up --accept-dns=false"
  TS_HOST=$(tailscale status --json 2>/dev/null \
            | python3 -I -c 'import json, sys; print(json.load(sys.stdin)["Self"]["DNSName"].rstrip("."))' 2>/dev/null || true)
  [ -n "$TS_HOST" ] || die "Tailscale isn't signed in. Run: sudo tailscale up --accept-dns=false"
  set_env_key LAKE_TS_HOST "$TS_HOST"
  did "set LAKE_TS_HOST=$TS_HOST from Tailscale"
fi

if [ -n "$(env_val DAGSTER_UI_HASH)" ]; then ok "Dagster UI login is set (user: $(env_val DAGSTER_UI_USER))"
elif [ $CHECK = 1 ]; then bad "Dagster UI login is not set"
elif [ -t 0 ]; then echo "  The Dagster UI needs a login (Compose won't start without it):"; bash scripts/set-dagster-ui-password.sh
else die "The Dagster UI login isn't set, and this shell can't ask for a password. Run: bash scripts/set-dagster-ui-password.sh"; fi
lock_env
[ "$(stat -c %a .env)" = 600 ] && ! getfacl -pn .env 2>/dev/null | grep -q '^user:[^:]' && ok ".env is mode 600, owner only" \
  || bad ".env permissions are not owner-only"

# ---------------------------------------------------------------------------------------------------------------
step "5/7 Image and services"
if [ $CHECK = 0 ]; then
  echo "  building lnl-jupyter:local, lnl-superset:local and lnl-mcp:local (cached layers make this quick after the first time)..."
  dc build -q jupyter superset mcp || die "An image build failed (output above)."
  did "image built"
  # Postgres first, so the dagster role exists before Dagster tries to log in.
  pg_up=$(dc up -d --wait catalog-pg 2>&1) || die "catalog-pg didn't become healthy. See: docker compose --profile core logs catalog-pg"
  ok "catalog-pg healthy"
  # postgres/init/ only runs on an empty volume; these scripts are idempotent and re-sync each password from .env.
  for script in "${ROLE_SCRIPTS[@]}"; do
    for i in 1 2 3 4 5; do
      dc exec -T catalog-pg bash "/docker-entrypoint-initdb.d/$script" >/dev/null 2>&1 && break
      [ $i = 5 ] && die "$script failed. Try by hand: docker compose --profile core exec -T catalog-pg bash /docker-entrypoint-initdb.d/$script"
      sleep 3
    done
    did "$script: role present, password in sync with .env"
  done
  dc up -d "${SERVICES[@]}" >/dev/null 2>&1 || die "Starting the services failed. Run: docker compose --profile core up -d ${SERVICES[*]}"
  did "running: ${SERVICES[*]}"
  dc exec -T caddy caddy reload --config /etc/caddy/Caddyfile >/dev/null 2>&1 && did "caddy reloaded its Caddyfile" || warn "caddy reload failed"
  # Prometheus and Alertmanager read their config only at start. Refill the probe targets, then SIGHUP makes both
  # reread it without restarting, so the metrics have no gap.
  dc exec -T prometheus sh /etc/prometheus/fill-targets.sh >/dev/null 2>&1 \
    && dc kill -s SIGHUP prometheus alertmanager >/dev/null 2>&1 \
    && did "Prometheus and Alertmanager reread their config" || warn "Prometheus or Alertmanager didn't reread its config"
  # A recreated catalog-pg leaves Dagster holding dead pooled connections: its API answers 500 until they cycle.
  if grep -q 'catalog-pg.*Recreated' <<< "$pg_up"; then
    dc restart dagster-webserver dagster-daemon >/dev/null 2>&1 \
      && did "restarted Dagster (catalog-pg was recreated)" || warn "Dagster didn't restart"
  fi
fi

echo "  waiting for health checks (up to 3 minutes)..."
for i in $(seq 1 36); do
  waiting=$(dc ps --format '{{.Service}} {{.Status}}' "${SERVICES[@]}" | grep -c -E 'starting|Restarting' || true)
  [ "$waiting" = 0 ] && break; sleep 5
done
for s in "${SERVICES[@]}"; do
  st=$(dc ps --format '{{.Status}}' "$s" 2>/dev/null)
  case "$st" in
    *healthy*) [[ $st == *unhealthy* ]] && bad "$s: $st" || ok "$s: $st" ;;
    Up*)       ok "$s: $st" ;;                               # caddy has no healthcheck
    *)         bad "$s: ${st:-not running}" ;;
  esac
done

# ---------------------------------------------------------------------------------------------------------------
step "6/7 Lake data"
have=$(dc exec -T jupyter python - <<'PY' 2>/dev/null || echo error
import sys; sys.path.insert(0, "/home/jovyan/course")
from lakehouse.lake import lake_connect
con = lake_connect(read_only=True)
t = {r[0] for r in con.sql("select table_schema||'.'||table_name from information_schema.tables").fetchall()}
print("yes" if {"silver.hero_offenders", "ref.null_tokens"} <= t else "no")
PY
)
have=$(echo "$have" | tail -1)
if [ "$have" = yes ] && [ $RELOAD = 0 ]; then ok "silver and reference tables present (use --reload to load again)"
elif [ $CHECK = 1 ]; then [ "$have" = yes ] && ok "lake has data" || bad "lake is empty or unreadable ($have)"
else
  echo "  running the loader..."
  out=$(dc run --rm load-lake 2>&1) || { echo "$out" | tail -20; die "The loader failed (output above)."; }
  did "$(echo "$out" | grep -m1 '^Loaded run')"
fi

# Session 7's embedding model lives on the lake disk (HF_HOME) at a pinned revision, and the notebooks run offline.
# Only this one command may download, so it turns HF_HUB_OFFLINE off for itself.
if dc exec -T jupyter python lakehouse/scripts/stage_model.py --check >/dev/null 2>&1; then ok "embedding model staged"
elif [ $CHECK = 1 ]; then bad "embedding model not staged (run bootstrap.sh without --check)"
else model=$(dc exec -T -e HF_HUB_OFFLINE=0 jupyter python lakehouse/scripts/stage_model.py 2>&1) \
       && did "$(echo "$model" | tail -1)" || bad "embedding model: $(echo "$model" | tail -1)"
fi

# OpenMetadata starts with a well-known admin password; setup_admin.py replaces it with OM_ADMIN_PASSWORD.
if [ $CHECK = 1 ]; then om=$(dc exec -T jupyter python lakehouse/openmetadata/setup_admin.py --check 2>&1)
else om=$(dc exec -T jupyter python lakehouse/openmetadata/setup_admin.py 2>&1); fi \
  && ok "$(echo "$om" | tail -1)" || bad "$(echo "$om" | tail -1)"

# om_sync.py loads the glossary, the sensitivity tags and every lake table into OpenMetadata; --check only compares.
if [ $CHECK = 1 ]; then sync=$(dc exec -T jupyter python lakehouse/openmetadata/om_sync.py --check 2>&1)
else sync=$(dc exec -T jupyter python lakehouse/openmetadata/om_sync.py 2>&1); fi \
  && ok "$(echo "$sync" | tail -1)" || bad "$(echo "$sync" | tail -1)"

# om_to_rdf.py publishes what OpenMetadata now holds (glossary as SKOS, column sensitivity and terms) to Fuseki as
# one named graph, replaced whole; --check only compares.
if [ $CHECK = 1 ]; then rdf=$(dc exec -T jupyter python lakehouse/openmetadata/om_to_rdf.py --check 2>&1)
else rdf=$(dc exec -T jupyter python lakehouse/openmetadata/om_to_rdf.py 2>&1); fi \
  && ok "$(echo "$rdf" | tail -1)" || bad "$(echo "$rdf" | tail -1)"

# sync_masks.py makes Cube mask exactly the columns OpenMetadata tags Sensitivity.Restricted, a steward's tag from
# the UI included. It edits cube/model/offender_records.yml, so it runs as the repo's owner (jupyter's own user
# can't write the repo) in the users group (the only way into /home/jovyan, where the repo is mounted), and Cube
# rereads its model only on restart. Review that change with git diff, then commit.
if [ $CHECK = 1 ]; then masks=$(dc exec -T jupyter python lakehouse/cube/sync_masks.py --check 2>&1)
else masks=$(dc exec -T -u "$(id -u):users" jupyter python lakehouse/cube/sync_masks.py 2>&1); fi \
  && ok "$(echo "$masks" | tail -1)" || bad "$(echo "$masks" | tail -1)"
if [[ $masks == "Cube masks: updated"* ]]; then
  dc restart cube >/dev/null 2>&1 && did "restarted cube with the new masks" || bad "cube didn't restart"
fi

# Grafana's Home Lakehouse dashboard is provisioned from the repo; set_home.py makes it the home page (an org
# preference, which provisioning can't set); --check only compares.
if [ $CHECK = 1 ]; then home=$(dc exec -T jupyter python lakehouse/monitoring/grafana/set_home.py --check 2>&1)
else home=$(dc exec -T jupyter python lakehouse/monitoring/grafana/set_home.py 2>&1); fi \
  && ok "$(echo "$home" | tail -1)" || bad "$(echo "$home" | tail -1)"

# Dashboards live in Superset's metadata database. dashboards.py creates or updates them through Superset's API,
# so a fresh install gets them and a re-run changes nothing; --check only reports what's missing.
if [ $CHECK = 1 ]; then dash=$(dc exec -T jupyter python lakehouse/superset/dashboards.py --check 2>&1)
else dash=$(dc exec -T jupyter python lakehouse/superset/dashboards.py 2>&1); fi \
  && ok "dashboards: $(echo "$dash" | tail -1)" || bad "dashboards: $(echo "$dash" | tail -1)"

# ---------------------------------------------------------------------------------------------------------------
step "7/7 Proof"
res=$(dc exec -T jupyter pytest -q lakehouse/tests 2>&1 | tail -1)
[[ $res == *passed* && $res != *failed* ]] && ok "parity tests: $res" || bad "parity tests: $res"

# The Tailscale name has a real certificate, so no -k here: a self-signed or wrong certificate fails the check.
for pair in "jupyter:302" "dagster:401" "superset:302" "mcp:401" "openmetadata:200" "fuseki:200" "prometheus:401" "grafana:302" "alertmanager:401" "ntfy:200"; do   # 302 = a login redirect, 401 = basic auth or no MCP token
  site=${pair%%:*} want=${pair##*:} port=443 url="https://$TS_HOST/${pair%%:*}/"
  [ "$site" = ntfy ] && port=8443 url="https://$TS_HOST:8443/v1/health"   # ntfy can't sit under a path: its own port
  got=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 --resolve "$TS_HOST:$port:127.0.0.1" "$url" || true); got=${got:-000}   # curl prints 000 itself when nothing answers
  [ "$got" = "$want" ] && ok "$url answers $got (trusted certificate)" || bad "$url answers $got, expected $want"
done

echo
if [ ${#FAILED[@]} = 0 ]; then
  printf '\033[32mAll checks passed.\033[0m Open https://%s/jupyter/, https://%s/dagster/, https://%s/superset/, https://%s/openmetadata/, https://%s/fuseki/ and https://%s/grafana/ (from any device on your tailnet).\n' "$TS_HOST" "$TS_HOST" "$TS_HOST" "$TS_HOST" "$TS_HOST" "$TS_HOST"
else
  printf '\033[31m%d check(s) failed:\033[0m\n' ${#FAILED[@]}; printf '  - %s\n' "${FAILED[@]}"; exit 1
fi
