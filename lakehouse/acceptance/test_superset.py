"""Acceptance test for Superset, the dashboards and SQL Lab front end (Phase 4).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_superset.py

Superset runs under /superset (SUPERSET_APP_ROOT) behind Caddy at https://<LAKE_TS_HOST>/superset/. Its only
database is Cube, reached over Cube's Postgres-wire SQL API, so every chart goes through the governed semantic
layer and nothing in Superset can read DuckLake directly. Its own metadata (users, charts, dashboards) lives in
the superset database on catalog-pg. Two dashboards: the Trust Test, and Custody, whose four charts read only the
custody data products' views and must give Cube's own numbers.
"""
import json
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


# --- the Custody dashboard (data products) ---------------------------------------------------------------------

CUSTODY_VIEWS = {"custody_population", "custody_offenses", "custody_admissions", "period_readiness"}


def latest_month_end():
    """The snapshot's latest month-end, as YYYY-MM-DD (mart_custody.snapshot_coverage)."""
    from lakehouse import lake
    con = lake.lake_ro_connect()
    return str(con.execute("SELECT last_month_end FROM mart_custody.snapshot_coverage").fetchone()[0])


def custody_charts(api):
    """{chart name: chart data rows} for every chart on the Custody dashboard."""
    q = "(filters:!((col:dashboard_title,opr:eq,value:'Custody')))"
    found = api.get(f"{SUPERSET}/api/v1/dashboard/", params={"q": q}, timeout=30).json()["result"]
    assert len(found) == 1 and found[0]["published"], "there should be one published Custody dashboard"
    charts = api.get(f"{SUPERSET}/api/v1/dashboard/{found[0]['id']}/charts", timeout=30).json()["result"]
    out = {}
    for c in charts:
        data = api.get(f"{SUPERSET}/api/v1/chart/{c['id']}/data/", timeout=120)
        assert data.ok, (c["slice_name"], data.status_code, data.text[:300])
        out[c["slice_name"]] = data.json()["result"][0]["data"]
    return out


def month_value(rows, month, column, value):
    """The value column of the row whose time column falls in YYYY-MM (Superset returns epoch milliseconds)."""
    import datetime as dt
    for r in rows:
        when = r[column]
        when = (dt.datetime.fromtimestamp(when / 1000, dt.timezone.utc) if isinstance(when, (int, float))
                else dt.datetime.fromisoformat(str(when)))
        if when.strftime("%Y-%m") == month:
            return r[value]
    raise AssertionError(f"no {month} row")


def test_custody_dashboard_reads_only_the_product_views(api):
    """Its five charts all sit on custody product views (and the readiness view): the governed path, nothing
    straight from the lake."""
    q = "(filters:!((col:dashboard_title,opr:eq,value:'Custody')))"
    dash = api.get(f"{SUPERSET}/api/v1/dashboard/", params={"q": q}, timeout=30).json()["result"][0]
    charts = api.get(f"{SUPERSET}/api/v1/dashboard/{dash['id']}/charts", timeout=30).json()["result"]
    assert len(charts) == 5
    for c in charts:
        chart = api.get(f"{SUPERSET}/api/v1/chart/{c['id']}", timeout=30).json()["result"]
        ds_id = json.loads(chart["params"])["datasource"].split("__")[0]   # params name the dataset: "<id>__table"
        ds = api.get(f"{SUPERSET}/api/v1/dataset/{ds_id}", timeout=30).json()["result"]
        assert ds["table_name"] in CUSTODY_VIEWS and ds["database"]["database_name"] == "Cube", (c["slice_name"], ds["table_name"])


def test_custody_charts_match_cube(api):
    """People in custody in September 2026 (81,409), by custody level and by offense category on the latest
    month-end, and September's 1,557 admissions: the same numbers Cube gives directly."""
    latest = latest_month_end()
    charts = custody_charts(api)
    people = charts["People in custody, by month-end"]
    assert month_value(people, "2026-09", "month_end", "People in custody") == 81_409
    admissions = charts["New sentences per month"]
    assert month_value(admissions, "2026-09", "sentence_date", "New sentences") == 1_557
    by_level = {r["custody_level"]: r["People in custody"] for r in charts["People in custody by custody level, latest month-end"]}
    by_cat = {r["offense_against"]: r["People in custody"] for r in charts["People in custody by offense category, latest month-end"]}
    import psycopg2
    from test_cube import CUBE_SQL
    with psycopg2.connect(**CUBE_SQL) as conn, conn.cursor() as cur:
        cur.execute("SELECT custody_level, MEASURE(people_in_custody) FROM custody_population "
                    f"WHERE month_end = CAST('{latest}' AS DATE) GROUP BY 1")
        assert by_level == dict(cur.fetchall())
        cur.execute("SELECT offense_against, MEASURE(people_in_custody) FROM custody_offenses "
                    f"WHERE month_end = CAST('{latest}' AS DATE) GROUP BY 1")
        assert by_cat == dict(cur.fetchall())


def month_of(value):
    """YYYY-MM of a date as Superset returns it (epoch milliseconds, or ISO text)."""
    import datetime as dt
    when = (dt.datetime.fromtimestamp(value / 1000, dt.timezone.utc) if isinstance(value, (int, float))
            else dt.datetime.fromisoformat(str(value)))
    return when.strftime("%Y-%m")


def test_custody_dashboard_says_which_months_are_final(api):
    """"Period status by month-end" lists the latest month-ends for both products, final or provisional and what
    each waits on, exactly as mart_readiness.period_readiness says."""
    from lakehouse import lake
    rows = custody_charts(api)["Period status by month-end"]
    con = lake.lake_ro_connect()
    latest = con.execute("SELECT max(month_end) FROM mart_readiness.period_readiness").fetchone()[0]
    want = set(con.execute("SELECT product, period_status, waiting_on FROM mart_readiness.period_readiness "
                           "WHERE month_end = ?", [latest]).fetchall())
    got = {(r["product"], r["period_status"], r["waiting_on"]) for r in rows if month_of(r["month_end"]) == latest.strftime("%Y-%m")}
    assert got == want

