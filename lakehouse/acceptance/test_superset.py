"""Acceptance test for Superset, the dashboards and SQL Lab front end (Phase 4).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_superset.py

Superset runs under /superset (SUPERSET_APP_ROOT) behind Caddy at https://<LAKE_TS_HOST>/superset/. Its only
database is Cube, reached over Cube's Postgres-wire SQL API, so every chart goes through the governed semantic
layer and nothing in Superset can read DuckLake directly. Its own metadata (users, charts, dashboards) lives in
the superset database on catalog-pg.
"""
import os
from pathlib import Path

import pytest
import requests

from test_daily_run import TRUST_TEST

LAKEHOUSE = Path(__file__).resolve().parents[1]
SUPERSET = os.environ.get("SUPERSET_URL", "http://superset:8088/superset")
ADMIN = {"username": "admin", "password": os.environ.get("SUPERSET_ADMIN_PASSWORD", ""), "provider": "db",
         "refresh": False}


@pytest.fixture(scope="module")
def api():
    """A requests session logged in to Superset's REST API as admin, with a CSRF token for write endpoints."""
    s = requests.Session()
    r = s.post(f"{SUPERSET}/api/v1/security/login", json=ADMIN, timeout=30)
    r.raise_for_status()
    s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    csrf = s.get(f"{SUPERSET}/api/v1/security/csrf_token/", timeout=30)
    csrf.raise_for_status()
    s.headers["X-CSRFToken"] = csrf.json()["result"]
    s.headers["Referer"] = SUPERSET
    yield s
    s.close()


def databases(api):
    """Every database connection Superset knows, as {name: id}."""
    r = api.get(f"{SUPERSET}/api/v1/database/", params={"q": "(page_size:100)"}, timeout=30)
    r.raise_for_status()
    return {d["database_name"]: d["id"] for d in r.json()["result"]}


# --- service ---------------------------------------------------------------------------------------------------

def test_superset_is_healthy_under_its_app_root():
    """Superset answers its health check at /superset/health, so it runs under the /superset path."""
    r = requests.get(f"{SUPERSET}/health", timeout=15)
    assert r.status_code == 200 and r.text.strip() == "OK"


def test_superset_keeps_its_metadata_in_postgres():
    """Users, charts and dashboards live in catalog-pg's superset database as the superset role, not in SQLite."""
    config = (LAKEHOUSE / "superset" / "superset_config.py").read_text()
    assert "postgresql+psycopg2://superset:" in config and "@catalog-pg:5432/superset" in config
    assert "sqlite" not in config.lower()


# --- governance ------------------------------------------------------------------------------------------------

def test_superset_only_database_is_cube(api):
    """Cube is Superset's one database: there is no direct connection to DuckLake to bypass governance with."""
    assert set(databases(api)) == {"Cube"}


def test_superset_reaches_cube_over_its_sql_api(api):
    """The Cube connection points at Cube's Postgres-wire SQL API on the Compose network."""
    db = api.get(f"{SUPERSET}/api/v1/database/{databases(api)['Cube']}/connection", timeout=30)   # URI, password masked
    db.raise_for_status()
    uri = db.json()["result"]["sqlalchemy_uri"]
    assert uri.startswith("postgresql") and "@cube:15432/" in uri


# --- answers ---------------------------------------------------------------------------------------------------

def test_trust_test_dashboard_shows_the_scorecard(api):
    """A "Trust Test" dashboard exists, and its chart returns the course's 8 questions and answers through Cube."""
    q = "(filters:!((col:dashboard_title,opr:eq,value:'Trust Test')))"
    found = api.get(f"{SUPERSET}/api/v1/dashboard/", params={"q": q}, timeout=30)
    found.raise_for_status()
    dashboards = found.json()["result"]
    assert len(dashboards) == 1, "there should be exactly one Trust Test dashboard"
    charts = api.get(f"{SUPERSET}/api/v1/dashboard/{dashboards[0]['id']}/charts", timeout=30)
    charts.raise_for_status()
    chart_ids = [c["id"] for c in charts.json()["result"]]
    assert chart_ids, "the Trust Test dashboard has no charts"
    data = api.get(f"{SUPERSET}/api/v1/chart/{chart_ids[0]}/data/", timeout=60)
    data.raise_for_status()
    got = sorted((r["question_id"], r["as_loaded"], r["cleansed"]) for r in data.json()["result"][0]["data"])
    assert got == [(q, as_loaded, cleansed) for q, _, as_loaded, cleansed in TRUST_TEST]


def test_sql_lab_counts_every_record_through_cube(api):
    """A SQL Lab query through Superset returns the same 100,000 records as gold."""
    r = api.post(f"{SUPERSET}/api/v1/sqllab/execute/", timeout=60,
                 json={"database_id": databases(api)["Cube"], "sql": "SELECT MEASURE(record_count) FROM offender_records",
                       "runAsync": False})
    r.raise_for_status()
    rows = r.json()["data"]
    assert len(rows) == 1 and list(rows[0].values()) == [100_000]
