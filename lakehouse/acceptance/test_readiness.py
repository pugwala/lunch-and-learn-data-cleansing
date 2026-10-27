"""Acceptance test for data readiness: is a data product's number for a month final, or still waiting on an input?

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_readiness.py

A number for a month is only right once every business process that feeds it has finished, each run by its own
unit on its own schedule. Everything a person adds is a file, so it can be taught without touching code:

- products/inputs/<input>.yml: a business input, the unit that owns it, its system, and how many days after a
  month ends it should close (simulate: plays the fictional unit, closing each month on a set day).
- depends_on: in a product descriptor (products/<product>.yml): the inputs its numbers need.

readiness/closes.py syncs those files to ops.business_inputs and ops.product_inputs, and records each close a unit
reports in ops.input_closes ("complete through" a month-end; append-only, the latest report wins, so an earlier
date reopens a month). The dbt view mart_readiness.period_readiness says, per product and month-end, final or
provisional and what it is waiting on; being a view, a close shows on the very next query. Cube carries
period_status and waiting_on in the product views, the MCP server's answers include the product's readiness,
Superset's Custody dashboard shows it, the daily run pushes each input's lateness, and BusinessInputLate alerts.
"""
import datetime as dt
import os
import subprocess
import time
from pathlib import Path

import psycopg2
import requests
import yaml

from lakehouse import lake
from test_cube import CUBE_SQL
from test_glossary_rdf import ask, select
from test_mcp import call

LAKEHOUSE = Path(__file__).resolve().parents[1]
PRODUCTS = {p.stem: yaml.safe_load(p.read_text()) for p in sorted((LAKEHOUSE / "products").glob("*.yml"))}
INPUTS = {p.stem: yaml.safe_load(p.read_text()) for p in sorted((LAKEHOUSE / "products" / "inputs").glob("*.yml"))}
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
TESTER = "acceptance-test"


def closes(*args):
    """Run readiness/closes.py; returns the finished process."""
    return subprocess.run(["python", str(LAKEHOUSE / "readiness" / "closes.py"), *args],
                          capture_output=True, text=True, timeout=300)


def readiness():
    """{(product, month_end): row} from mart_readiness.period_readiness, as lake_ro reads it now."""
    con = lake.lake_ro_connect()
    rows = con.execute("SELECT product, month_end, inputs_required, inputs_closed, is_final, period_status, waiting_on "
                       "FROM mart_readiness.period_readiness").fetchall()
    con.close()
    keys = ("product", "month_end", "inputs_required", "inputs_closed", "is_final", "period_status", "waiting_on")
    return {(r[0], r[1]): dict(zip(keys, r)) for r in rows}


def cube_sql(sql):
    """All rows of a query through Cube's SQL API."""
    conn = psycopg2.connect(**CUBE_SQL, connect_timeout=10)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


# --- what people declare ---------------------------------------------------------------------------------------

def test_every_product_depends_on_inputs_that_are_declared():
    """Each product names at least one input in depends_on, and every input it names has its own file."""
    for name, p in PRODUCTS.items():
        assert p.get("depends_on"), name
        assert set(p["depends_on"]) <= set(INPUTS), (name, set(p["depends_on"]) - set(INPUTS))


def test_every_input_says_who_owns_it_and_when_it_closes():
    """An input has a title, an owning unit, a system, a description and a whole number of days to close; a
    simulated unit closes on time (before its deadline)."""
    assert len(INPUTS) >= 3
    for stem, i in INPUTS.items():
        assert i["name"] == stem and i["title"] and i["unit"] and i["system"] and len(i["description"]) >= 40, stem
        assert isinstance(i["closes_within_days"], int) and i["closes_within_days"] >= 0, stem
        assert 0 <= i["simulate"]["closes_after_days"] <= i["closes_within_days"], stem


def test_the_lake_holds_what_the_files_declare():
    """closes.py sync has put the inputs and dependencies in ops, and --check finds nothing to change."""
    con = lake.lake_ro_connect()
    inputs = {r[0]: r[1:] for r in con.execute(
        "SELECT input, unit, system, closes_within_days FROM ops.business_inputs").fetchall()}
    deps = set(con.execute("SELECT product, input FROM ops.product_inputs").fetchall())
    con.close()
    assert inputs == {s: (i["unit"], i["system"], i["closes_within_days"]) for s, i in INPUTS.items()}
    assert deps == {(name, i) for name, p in PRODUCTS.items() for i in p["depends_on"]}
    r = closes("sync", "--check")
    assert r.returncode == 0, r.stdout + r.stderr


# --- readiness follows the closes ------------------------------------------------------------------------------

def test_readiness_follows_the_watermarks():
    """For every product and month-end of the snapshot: final exactly when each input it depends on has been
    reported complete through that month-end (the latest report counts); otherwise provisional, waiting on the
    units of the inputs still open."""
    con = lake.lake_ro_connect()
    marks = dict(con.execute("SELECT input, arg_max(complete_through, reported_at) FROM ops.input_closes "
                             "GROUP BY input").fetchall())
    months = [r[0] for r in con.execute("SELECT DISTINCT month_end FROM mart_custody.custody_snapshot_monthly").fetchall()]
    con.close()
    got = readiness()
    assert set(got) == {(name, m) for name in PRODUCTS for m in months}
    for (name, month), row in got.items():
        open_inputs = [i for i in PRODUCTS[name]["depends_on"] if marks.get(i) is None or marks[i] < month]
        assert row["is_final"] == (not open_inputs) and row["period_status"] == ("provisional" if open_inputs else "final")
        assert row["waiting_on"] == (", ".join(sorted(INPUTS[i]["unit"] for i in open_inputs)) or None), (name, month)
        assert (row["inputs_required"], row["inputs_closed"]) == (len(PRODUCTS[name]["depends_on"]),
                                                                  len(PRODUCTS[name]["depends_on"]) - len(open_inputs))


def test_a_reopened_input_makes_the_month_provisional_and_a_close_makes_it_final():
    """Live, with no rebuild: Classification reopening the latest month-end makes Custody Population provisional,
    waiting on Classification, in the lake and in Cube, while Custody Offenses (which doesn't need Classification)
    stays as it was; reporting the month closed makes it final again. The test's own reports are deleted after."""
    latest = max(m for _, m in readiness())
    before = readiness()
    previous = latest.replace(day=1) - dt.timedelta(days=1)
    try:
        assert closes("report", "classification", str(previous), "--by", TESTER).returncode == 0
        reopened = readiness()
        assert reopened[("custody-population", latest)]["period_status"] == "provisional"
        assert "Classification" in reopened[("custody-population", latest)]["waiting_on"]
        assert reopened[("custody-offenses", latest)] == before[("custody-offenses", latest)]
        deadline = time.time() + 90   # Cube re-reads the view once its cache refreshes
        while cube_sql(f"SELECT period_status FROM custody_population WHERE month_end = CAST('{latest}' AS DATE) "
                       "GROUP BY 1") != [("provisional",)]:
            assert time.time() < deadline, "Cube never showed the reopened month as provisional"
            time.sleep(5)
        assert closes("report", "classification", str(latest), "--by", TESTER).returncode == 0
        closed = readiness()[("custody-population", latest)]
        assert closed["inputs_closed"] == reopened[("custody-population", latest)]["inputs_closed"] + 1
    finally:
        con = lake.lake_connect()
        con.execute("DELETE FROM lake.ops.input_closes WHERE reported_by = ?", [TESTER])
        con.close()
    assert readiness() == before


def test_cube_carries_the_status_without_changing_the_numbers():
    """period_status rides along in both product views without fanning out the counts (81,409 people on
    2026-09-30), and the period_readiness view lists every product's months."""
    sept = "month_end = CAST('2026-09-30' AS DATE)"
    pop = cube_sql(f"SELECT MEASURE(people_in_custody), period_status FROM custody_population WHERE {sept} GROUP BY 2")
    assert [p for p, _ in pop] == [81_409]
    off = cube_sql(f"SELECT MEASURE(people_in_custody), period_status FROM custody_offenses WHERE {sept} GROUP BY 2")
    assert [p for p, _ in off] == [81_409]
    rows = cube_sql("SELECT product, period_status FROM period_readiness WHERE month_end = CAST('2026-09-30' AS DATE)")
    assert {p for p, _ in rows} == set(PRODUCTS)


def test_the_mcp_answer_says_whether_the_month_is_final():
    """query_metric on a product view returns the product's readiness: its latest month-end, final or provisional
    and what it waits on, plus any month still provisional."""
    is_error, result, text = call("query_metric", metric="people_in_custody", group_by=["custody_level"])
    assert not is_error, text
    r = result["readiness"]
    assert r["product"] == "custody-population"
    assert r["latest"]["month_end"] == str(max(m for _, m in readiness()))
    assert r["latest"]["status"] in ("final", "provisional")
    assert all(p["status"] == "provisional" and p["waiting_on"] for p in r["provisional"])


def test_a_metric_outside_any_product_has_no_readiness():
    """record_count isn't part of a product, so its answer carries no readiness block."""
    is_error, result, text = call("query_metric", metric="record_count")
    assert not is_error and result.get("readiness") is None, text


# --- the ontology, the daily run and the alert -----------------------------------------------------------------

def test_the_ontology_knows_who_each_product_waits_on():
    """Fuseki: each product lh:dependsOn its inputs, each an lh:BusinessInput with its unit and deadline."""
    rows = select("""PREFIX dcat: <http://www.w3.org/ns/dcat#> PREFIX dct: <http://purl.org/dc/terms/>
        SELECT ?product ?input ?unit WHERE { ?p a dcat:Dataset ; dct:title ?product ; lh:dependsOn ?i .
        ?i a lh:BusinessInput ; dct:title ?input ; lh:unit ?unit ; lh:closesWithinDays ?days }""")
    want = {(p["title"], INPUTS[i]["title"], INPUTS[i]["unit"]) for p in PRODUCTS.values() for i in p["depends_on"]}
    assert {(r["product"], r["input"], r["unit"]) for r in rows} == want
    assert not ask("PREFIX dcat: <http://www.w3.org/ns/dcat#> ASK { ?p a dcat:Dataset FILTER NOT EXISTS { ?p lh:dependsOn ?i } }")


def test_the_daily_run_reports_each_input_and_product():
    """The daily run pushes each input's days late (0 today) and whether each product's latest month is final;
    BusinessInputLate loads."""
    def query(q):
        """An instant PromQL query as {label value: value}."""
        r = requests.get(f"{PROMETHEUS}/api/v1/query", params={"query": q}, timeout=30).json()["data"]["result"]
        return r
    late = {x["metric"]["input"]: float(x["value"][1]) for x in query("lakehouse_input_days_late")}
    assert late == {name: 0.0 for name in INPUTS}
    final = {x["metric"]["product"] for x in query("lakehouse_product_latest_period_final")}
    assert final == set(PRODUCTS)
    groups = requests.get(f"{PROMETHEUS}/api/v1/rules", timeout=30).json()["data"]["groups"]
    rule = next(r for g in groups for r in g["rules"] if r["name"] == "BusinessInputLate")
    assert rule["health"] == "ok" and "lakehouse_input_days_late" in rule["query"]


# --- the date rules, on fixed dates ----------------------------------------------------------------------------

def test_days_late_counts_from_the_first_month_not_closed():
    """An input complete through July with 10 days to close is 0 days late on Sept 10 (August is due that day),
    and 17 days late on Sept 27. Complete through September, it isn't late before October ends + 10 days."""
    from lakehouse.readiness import closes as c
    july, sept = dt.date(2026, 7, 31), dt.date(2026, 9, 30)
    assert c.days_late(july, 10, dt.date(2026, 9, 10)) == 0
    assert c.days_late(july, 10, dt.date(2026, 9, 27)) == 17
    assert c.days_late(sept, 10, dt.date(2026, 11, 10)) == 0
    assert c.days_late(sept, 10, dt.date(2026, 11, 12)) == 2


def test_a_simulated_unit_closes_each_month_on_its_day():
    """A unit that closes 8 days after a month ends reports September on Oct 8, not Oct 7, and never twice."""
    from lakehouse.readiness import closes as c
    aug, sept = dt.date(2026, 8, 31), dt.date(2026, 9, 30)
    assert c.simulated_close(aug, 8, dt.date(2026, 10, 7)) is None
    assert c.simulated_close(aug, 8, dt.date(2026, 10, 8)) == sept
    assert c.simulated_close(sept, 8, dt.date(2026, 10, 27)) is None
    assert c.simulated_close(None, 8, dt.date(2026, 10, 27)) == sept


def test_a_report_must_name_a_known_input_and_a_month_end():
    """closes.py refuses a close for an input nobody declared, or a date that isn't a month-end."""
    assert closes("report", "no-such-input", "2026-09-30", "--by", TESTER).returncode != 0
    assert closes("report", "classification", "2026-09-15", "--by", TESTER).returncode != 0
