"""Superset configuration for the home-lab lakehouse, mounted at /app/lakehouse (SUPERSET_CONFIG_PATH points here).

Secrets come from the environment compose.yaml passes in, never from this file. Superset serves under /superset
(SUPERSET_APP_ROOT, set in compose.yaml) behind Caddy at https://<LAKE_TS_HOST>/superset/.
"""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Users, charts and dashboards live in catalog-pg's superset database, owned by the superset role
# (postgres/init/03-superset.sh). The data Superset charts comes only from Cube, registered by start.sh.
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://superset:{os.environ['SUPERSET_PG_PASSWORD']}@catalog-pg:5432/superset"

# Caddy terminates TLS in front of Superset: trust its X-Forwarded-* headers so links and redirects keep https.
ENABLE_PROXY_FIX = True

# One gunicorn worker and no Redis yet, so an in-process cache is enough.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG

# No usage analytics pixel, in keeping with telemetry being off everywhere else in the stack.
SCARF_ANALYTICS = False
