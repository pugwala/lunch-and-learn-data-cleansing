"""Acceptance test for the expanded world's data products (dataset expansion, chunk 9).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_world_products.py

Needs the stack with the world built (world_refresh, world_monthly_job), then daily_lake, om_sync.py, om_to_rdf.py
and superset/dashboards.py run after the descriptors were added. Three products, declared in files like the custody
ones (lakehouse/products/), in two new domains:

  Field Operations   field-incidents   fights by month, place, kind and outcome; who took part (distinct people)
                     incident-damage   what fights destroyed, what insurance paid and what it didn't
  Justice            court-outcomes    charges decided each month, by outcome and offense; convictions

Each depends on business inputs (products/inputs/): Field Operations' incident reports, Insurance's claims, and the
Courts liaison's court decisions, so each month is final or provisional like the custody products'. The tests check
that the descriptors, Cube's views, the readiness view, the MCP agent, OpenMetadata, Fuseki, Superset and the
freshness metrics all tell the same story, with every number checked against the lake itself.
"""
import datetime as dt
import time

import psycopg2
import pytest
import requests

from lakehouse import lake
from test_cube import CUBE_SQL
from test_custody_cube import meta
from test_daily_run import graphql
from test_glossary_rdf import ask, select
from test_mcp import call
from test_products import PRODUCT_PREFIXES, PRODUCTS, dbt_models, get, om, prom  # noqa: F401  (om: fixture)
from test_readiness import INPUTS, TESTER, closes, readiness
from test_superset import SUPERSET, api, month_of  # noqa: F401  (api: fixture)

WORLD = {"field-incidents": "Field Operations", "incident-damage": "Field Operations", "court-outcomes": "Justice"}
DEPENDS = {"field-incidents": ["field-reports"], "incident-damage": ["field-reports", "insurance-claims"],
           "court-outcomes": ["courts"]}
VIEWS = {"field_incidents": "field-incidents", "fight_participants": "field-incidents",
         "incident_damage": "incident-damage", "court_outcomes": "court-outcomes"}
HIDDEN = {"world_incidents", "world_participation", "world_damage", "world_charges"}
DASHBOARD = "Fights, Damage and Courts"
YEAR = 2025


def lake_one(sql, *params):
    """The first row of a query on the lake, read-only."""
    con = lake.lake_ro_connect()
    try:
        return con.execute(sql, list(params)).fetchone()
    finally:
        con.close()


def cube_rows(sql):
    """All rows of a query through Cube's SQL API (what Superset and BI tools use)."""
    with psycopg2.connect(**CUBE_SQL, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


IN_YEAR = f"BETWEEN CAST('{YEAR}-01-01' AS DATE) AND CAST('{YEAR}-12-31' AS DATE)"


# --- what the files declare ------------------------------------------------------------------------------------

def test_the_world_has_three_products_in_two_new_domains():
    """field-incidents and incident-damage in Field Operations, court-outcomes in Justice, each domain a file."""
    assert {name: PRODUCTS[name]["domain"] for name in WORLD} == WORLD
    from test_products import LAKEHOUSE
    import yaml
    domains = {yaml.safe_load(f.read_text())["name"] for f in (LAKEHOUSE / "products" / "domains").glob("*.yml")}
    assert {"Field Operations", "Justice"} <= domains


def test_each_depends_on_the_units_that_feed_it():
    """Fights wait on Field Operations' reports; damage also on Insurance's claims; court outcomes on the Courts
    liaison, the same input custody offenses already wait on."""
    assert {name: PRODUCTS[name]["depends_on"] for name in WORLD} == DEPENDS
    units = {name: i["unit"] for name, i in INPUTS.items()}
    assert (units["field-reports"], units["insurance-claims"], units["courts"]) == ("Field Operations", "Insurance",
                                                                                   "Courts liaison")


def test_world_products_promise_a_monthly_refresh():
    """The world is rebuilt monthly (world_refresh, then world_monthly_job, on the 1st), so its products promise a
    month, not a day: a 32-day maximum age."""
    for name in WORLD:
        p = PRODUCTS[name]
        assert (p["refresh"]["every"], p["refresh"]["unit"]) == (1, "month") and p["max_age_hours"] == 32 * 24, name


# --- Cube: the views and their numbers -------------------------------------------------------------------------

def test_the_views_are_public_tagged_and_the_cubes_under_them_hidden():
    """Consumers see four views, each tagged with its product; the cubes over world_gold stay hidden."""
    cubes = {c["name"]: c for c in meta()}
    assert {v: (cubes.get(v, {}).get("meta") or {}).get("product") for v in VIEWS} == VIEWS
    assert not HIDDEN & set(cubes)
    for name in WORLD:
        assert {v for v, p in VIEWS.items() if p == name} == set(PRODUCTS[name]["cube_views"]), name


def test_fights_match_the_lake():
    """2025's fights and bystanders hurt, through Cube, are the lake's own counts."""
    got = cube_rows(f"SELECT MEASURE(fights), MEASURE(bystanders_injured), MEASURE(bystanders_killed) "
                    f"FROM field_incidents WHERE incident_month {IN_YEAR}")
    want = lake_one("SELECT count(*), sum(bystanders_injured), sum(bystanders_killed) FROM world_gold.fct_incident "
                    "WHERE year(incident_month) = ?", YEAR)
    assert [tuple(int(v) for v in got[0])] == [tuple(int(v) for v in want)] and want[0] > 10_000


def test_people_in_fights_are_counted_once_never_summed():
    """People who fought in 2025 are distinct person keys, counted by Cube for the whole year; adding up the
    months would count anyone who fought twice more than once."""
    year = cube_rows(f"SELECT MEASURE(people_involved) FROM fight_participants WHERE incident_month {IN_YEAR}")[0][0]
    want, months = lake_one("""SELECT (SELECT count(DISTINCT person_key) FROM world_gold.fct_participation
                                       WHERE year(incident_month) = $1),
                                      (SELECT sum(n) FROM (SELECT count(DISTINCT person_key) AS n
                                       FROM world_gold.fct_participation WHERE year(incident_month) = $1
                                       GROUP BY incident_month))""".replace("$1", str(YEAR)))
    assert year == want and months > want


def test_damage_and_insurance_match_the_lake():
    """2025's damage: assessed cost, what insurance paid, and the cost nobody insured, as the lake sums them."""
    got = cube_rows(f"SELECT MEASURE(assessments), MEASURE(damage_cost_usd), MEASURE(insurance_paid_usd), "
                    f"MEASURE(uninsured_cost_usd) FROM incident_damage WHERE incident_month {IN_YEAR}")
    want = lake_one("SELECT count(*), sum(cost_usd), sum(insurance_paid_usd), sum(cost_usd) FILTER (WHERE NOT insured) "
                    "FROM world_gold.fct_damage WHERE year(incident_month) = ?", YEAR)
    assert tuple(int(v) for v in got[0]) == tuple(int(v) for v in want) and 0 < want[3] < want[1]


def test_court_outcomes_match_the_lake():
    """Charges decided in 2025, by outcome; convictions; and the distinct people charged, as the lake counts them."""
    got = dict(cube_rows(f"SELECT outcome, MEASURE(charges) FROM court_outcomes WHERE decided_on {IN_YEAR} GROUP BY 1"))
    con = lake.lake_ro_connect()
    want = dict(con.execute("SELECT outcome, count(*) FROM world_gold.fct_charge WHERE year(decided_on) = ? GROUP BY 1",
                            [YEAR]).fetchall())
    convicted, people = con.execute("SELECT count(*) FILTER (WHERE is_conviction), count(DISTINCT person_key) "
                                    "FROM world_gold.fct_charge WHERE year(decided_on) = ?", [YEAR]).fetchone()
    con.close()
    assert got == want and len(want) >= 3
    assert cube_rows(f"SELECT MEASURE(convictions), MEASURE(people_charged) FROM court_outcomes "
                     f"WHERE decided_on {IN_YEAR}") == [(convicted, people)]


def test_every_view_says_whether_its_month_is_final():
    """period_status and waiting_on ride along in each view, as mart_readiness.period_readiness says for its
    product, without changing the counts."""
    month = "2026-09"
    want = {p: tuple(r) for p, *r in lake.lake_ro_connect().execute(
        "SELECT product, period_status, waiting_on FROM mart_readiness.period_readiness "
        "WHERE month_end = DATE '2026-09-30'").fetchall()}
    time_column = {"field_incidents": "incident_month", "fight_participants": "incident_month",
                   "incident_damage": "incident_month", "court_outcomes": "decided_on"}
    for view, product in VIEWS.items():
        rows = cube_rows(f"SELECT period_status, waiting_on FROM {view} WHERE {time_column[view]} "
                         f"BETWEEN CAST('{month}-01' AS DATE) AND CAST('{month}-30' AS DATE) GROUP BY 1, 2")
        assert rows == [want[product]], (view, rows, want[product])


# --- readiness -------------------------------------------------------------------------------------------------

def test_the_readiness_calendar_starts_with_the_world():
    """Every product has a row for every month-end from January 1996 (the world's first fights) to the custody
    snapshot's last: the same calendar for all, with no gaps."""
    got = readiness()
    first, last = dt.date(1996, 1, 31), lake_one("SELECT max(month_end) FROM mart_custody.custody_snapshot_monthly")[0]
    months, m = [], first
    while m <= last:
        months.append(m)
        m = (m + dt.timedelta(days=1)).replace(day=28) + dt.timedelta(days=4)
        m = m - dt.timedelta(days=m.day)
    assert set(got) == {(p, month) for p in PRODUCTS for month in months}


def test_field_operations_reopening_a_month_holds_back_fights_and_damage_but_not_courts():
    """Live: Field Operations reopening September makes field-incidents and incident-damage provisional, waiting
    on Field Operations, while court-outcomes doesn't move; closing it again restores them. The test's own reports
    are deleted after."""
    before = readiness()
    sept, aug = dt.date(2026, 9, 30), dt.date(2026, 8, 31)
    try:
        assert closes("report", "field-reports", str(aug), "--by", TESTER).returncode == 0
        now = readiness()
        for product in ("field-incidents", "incident-damage"):
            assert now[(product, sept)]["period_status"] == "provisional", product
            assert "Field Operations" in now[(product, sept)]["waiting_on"], product
        assert now[("court-outcomes", sept)] == before[("court-outcomes", sept)]
    finally:
        con = lake.lake_connect()
        con.execute("DELETE FROM lake.ops.input_closes WHERE reported_by = ?", [TESTER])
        con.close()
    assert readiness() == before


# --- the MCP agent ---------------------------------------------------------------------------------------------

def test_the_agent_answers_world_questions_with_their_readiness():
    """Ask for fights by month in 2025: the agent picks field_incidents, names its product, and says whether its
    latest month is final (the readiness block names the product), all from the descriptors and Cube, with no change to the server."""
    is_error, result, text = call("query_metric", metric="fights", date_from=f"{YEAR}-01-01", date_to=f"{YEAR}-12-31",
                                  granularity="month")
    assert not is_error, text
    assert len(result["rows"]) == 12 and result["readiness"]["product"] == "field-incidents"
    want = lake_one("SELECT count(*) FROM world_gold.fct_incident WHERE year(incident_month) = ?", YEAR)[0]
    assert sum(r["fights"] for r in result["rows"]) == want


# --- OpenMetadata and Fuseki -----------------------------------------------------------------------------------

def test_openmetadata_has_the_domains_products_and_monthly_contracts(om):
    """Both domains, each product with exactly its delivered tables, and a data contract on each contracted
    world table with dbt's columns and a monthly SLA (05:30 Central, at most 768 hours old)."""
    for domain in ("Field Operations", "Justice"):
        assert get(om, f"domains/name/{domain}")["domainType"] == "Consumer-aligned"
    from lakehouse.openmetadata.om_sync import dbt_contracts
    contracts = dbt_contracts()   # dbt's columns, typed as OpenMetadata names them (INTEGER is INT there)
    for name in WORLD:
        p = PRODUCTS[name]
        product = get(om, f"dataProducts/name/{name}", fields="domains")
        assert [d["name"] for d in product["domains"]] == [p["domain"]]
        assets = {a["fullyQualifiedName"].removeprefix("lakehouse.lake.")
                  for a in get(om, f"dataProducts/name/{name}/assets", limit=1000)["data"]}
        assert assets == set(p["tables"])
        for table in p["tables"]:
            contract = get(om, f"dataContracts/name/lakehouse.lake.{table}.dataContract_{table.split('.')[1]}-contract",
                           fields="sla")
            assert [(c["name"], c["dataType"]) for c in contract["schema"]] == contracts[table], table
            assert contract["sla"]["refreshFrequency"] == {"interval": 1, "unit": "month"}
            assert contract["sla"]["maxLatency"] == {"value": 768, "unit": "hour"}


def test_world_products_are_dcat_datasets_that_speak_in_world_terms():
    """Each is a dcat:Dataset in its domain, and impact analysis works: Field Incidents and Incident Damage speak
    in Incident, and Court Outcomes speaks in Charge, which is about Incident, so a path (lh:usesTerm, then
    lh:isAbout zero or one times) finds all three a change to Incident would touch."""
    rows = select(PRODUCT_PREFIXES + "SELECT ?title ?domain WHERE { ?p a dcat:Dataset ; dct:title ?title ; "
                  "lh:domain ?domain }")
    got = {r["title"]: r["domain"] for r in rows}
    assert {PRODUCTS[n]["title"]: d for n, d in WORLD.items()}.items() <= got.items()
    direct = select(PRODUCT_PREFIXES + "SELECT ?title WHERE { ?p a dcat:Dataset ; dct:title ?title ; "
                    "lh:usesTerm term:incident }")
    assert {r["title"] for r in direct} == {"Field Incidents", "Incident Damage"}
    rows = select(PRODUCT_PREFIXES + "SELECT DISTINCT ?title WHERE { ?p a dcat:Dataset ; dct:title ?title ; "
                  "lh:usesTerm/lh:isAbout? term:incident }")
    assert {r["title"] for r in rows} == {PRODUCTS[n]["title"] for n in WORLD}
    assert ask(PRODUCT_PREFIXES + 'ASK { prod:court-outcomes lh:dependsOn ?i . ?i lh:unit "Courts liaison" }')


# --- Superset --------------------------------------------------------------------------------------------------

def world_charts(api):
    """{chart name: (dataset table, chart data rows)} for every chart on the world dashboard."""
    q = f"(filters:!((col:dashboard_title,opr:eq,value:'{DASHBOARD}')))"
    found = api.get(f"{SUPERSET}/api/v1/dashboard/", params={"q": q}, timeout=30).json()["result"]
    assert len(found) == 1 and found[0]["published"], f"there should be one published {DASHBOARD} dashboard"
    out = {}
    for c in api.get(f"{SUPERSET}/api/v1/dashboard/{found[0]['id']}/charts", timeout=30).json()["result"]:
        chart = api.get(f"{SUPERSET}/api/v1/chart/{c['id']}", timeout=30).json()["result"]
        import json
        ds_id = json.loads(chart["params"])["datasource"].split("__")[0]
        ds = api.get(f"{SUPERSET}/api/v1/dataset/{ds_id}", timeout=30).json()["result"]
        data = api.get(f"{SUPERSET}/api/v1/chart/{c['id']}/data/", timeout=120)
        assert data.ok, (c["slice_name"], data.status_code, data.text[:300])
        out[c["slice_name"]] = (ds["table_name"], ds["database"]["database_name"], data.json()["result"][0]["data"])
    return out


def test_the_world_dashboard_reads_only_product_views_and_matches_cube(api):
    """Seven charts, all on the world products' views (or the readiness view) through the Cube database; fights
    in September 2026 and charges by outcome are Cube's own numbers; the status table lists only world products."""
    charts = world_charts(api)
    assert len(charts) == 7
    for name, (table, database, _) in charts.items():
        assert table in set(VIEWS) | {"period_readiness"} and database == "Cube", (name, table)
    fights = {month_of(r["incident_month"]): r["Fights"] for r in charts["Fights per month"][2]
              if r["incident_month"] is not None}   # fights whose date couldn't be read have no month
    assert fights["2026-09"] == cube_rows("SELECT MEASURE(fights) FROM field_incidents WHERE incident_month = "
                                          "CAST('2026-09-01' AS DATE)")[0][0]
    by_outcome = {r["outcome"]: r["Charges"] for r in charts["Charges by court outcome"][2]}
    assert by_outcome == dict(cube_rows("SELECT outcome, MEASURE(charges) FROM court_outcomes GROUP BY 1"))
    assert {r["product"] for r in charts["World period status by month-end"][2]} == set(WORLD)


# --- freshness -------------------------------------------------------------------------------------------------

def latest_materialization(key):
    """When Dagster last materialized an asset (any partition), in epoch seconds; 0 if never."""
    path = ", ".join(f'"{p}"' for p in key)
    node = graphql(f"{{ assetNodeOrError(assetKey: {{path: [{path}]}}) {{ ... on AssetNode {{ "
                   "assetMaterializations(limit: 1) { timestamp } } } }")["assetNodeOrError"]
    found = node.get("assetMaterializations") or []
    return float(found[0]["timestamp"]) / 1000 if found else 0.0


def test_world_freshness_is_when_their_tables_were_built_not_when_the_check_ran():
    """A product is as fresh as its stalest table: the pushed refresh time is the oldest of its tables' latest
    Dagster materializations (world_gold models are world_gold/wg_<table>), and it is within the promise."""
    pushed = prom("lakehouse_product_refreshed_timestamp_seconds")
    for name in WORLD:
        want = min(latest_materialization(["world_gold", f"wg_{t.split('.')[1]}"]) for t in PRODUCTS[name]["tables"])
        assert want > 0 and abs(pushed[name] - want) < 1, (name, pushed.get(name), want)
        assert time.time() - pushed[name] < PRODUCTS[name]["max_age_hours"] * 3600, name


@pytest.mark.parametrize("name", sorted(WORLD))
def test_contracted_world_tables_are_dbt_contracts(name):
    """Every table a world product delivers is a world_gold model whose contract dbt enforces."""
    models = {((m.get("config") or {}).get("alias") or m["name"]): m for m in dbt_models().values()
              if m["name"].startswith("wg_")}
    for table in PRODUCTS[name]["tables"]:
        assert table.startswith("world_gold.") and models[table.split(".")[1]]["config"]["contract"]["enforced"], table
