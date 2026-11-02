"""Acceptance test for the MCP server's data_product_info and data_quality tools.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_mcp_products.py

data_product_info answers "what is this data product, can I trust this month's number, and how fresh is it?" in one
call: the descriptor's promise as the catalog publishes it (Fuseki's DCAT: title, domain, question, tables, views,
terms, and the business inputs it waits on), the data contracts OpenMetadata holds for its tables, the metrics its
Cube views offer, whether its months are final (Cube's period_readiness), and how fresh it is against its promise
(Prometheus). data_quality answers "how clean is the data?": the course data's quality scorecard by dimension and
the rules under the floor (Cube's dq_scorecard), the expanded world's flagged values by silver table and rule (Cube's
world_exceptions: counts only, never a value), and which data-quality alerts are firing. Every number is checked
against its own source here (the lake, the descriptors, Prometheus), never against the tool's other answers.
"""
import time

import pytest
import yaml

from lakehouse import lake
from test_daily_run import EXPECTED_DIMENSIONS
from test_mcp import LAKEHOUSE, call, prometheus_now

PRODUCTS = {p.stem: yaml.safe_load(p.read_text()) for p in sorted((LAKEHOUSE / "products").glob("*.yml"))}
INPUTS = {p.stem: yaml.safe_load(p.read_text()) for p in sorted((LAKEHOUSE / "products" / "inputs").glob("*.yml"))}
DQ_ALERTS = {"DataQualityBelowFloor", "MoreRulesBelowFloor", "FlaggedShareHigh", "SilverDroppedRows"}


def products(**arguments):
    """data_product_info's products, after checking the call succeeded."""
    is_error, result, text = call("data_product_info", **arguments)
    assert not is_error, text
    return {p["name"]: p for p in result["products"]}


def quality(**arguments):
    """data_quality's result, after checking the call succeeded."""
    is_error, result, text = call("data_quality", **arguments)
    assert not is_error, text
    return result


def lake_rows(sql, *params):
    """All rows of a query on the lake, read-only."""
    con = lake.lake_ro_connect()
    try:
        return con.execute(sql, list(params)).fetchall()
    finally:
        con.close()


# --- data_product_info -----------------------------------------------------------------------------------------

def test_data_product_info_lists_every_product_as_its_descriptor_describes_it():
    """With no product named: all five, each with the title, domain, question, tables, views and glossary terms its
    descriptor declares, and the business inputs it waits on (title, unit, system, days to close)."""
    got = products()
    assert set(got) == set(PRODUCTS)
    for name, p in PRODUCTS.items():
        g = got[name]
        assert (g["title"], g["domain"], g["question"]) == (p["title"], p["domain"], p["question"]), name
        assert sorted(g["tables"]) == sorted(p["tables"]) and sorted(g["views"]) == sorted(p["cube_views"]), name
        assert sorted(g["terms"]) == sorted(p["glossary_terms"]), name
        want = sorted((i, INPUTS[i]["title"], INPUTS[i]["unit"], INPUTS[i]["system"], INPUTS[i]["closes_within_days"])
                      for i in p["depends_on"])
        assert sorted((i["name"], i["title"], i["unit"], i["system"], i["closes_within_days"])
                      for i in g["inputs"]) == want, name


def test_a_product_is_found_by_name_in_any_case_and_an_unknown_one_lists_them():
    """"court-outcomes", "Court Outcomes" and "court outcomes" find the same product (case and hyphens don't
    matter); a made-up one is an error that names the real ones."""
    for ask in ("court-outcomes", "Court Outcomes", "court outcomes"):
        assert set(products(product=ask)) == {"court-outcomes"}, ask
    is_error, _, text = call("data_product_info", product="no such product")
    assert is_error and "field-incidents" in text and "custody-population" in text


def test_data_product_info_names_the_metrics_its_views_offer():
    """Court Outcomes offers charges, convictions and people_charged through court_outcomes; Custody Population
    offers people_in_custody through custody_population: what query_metric can then be asked for."""
    assert {"court_outcomes.charges", "court_outcomes.convictions", "court_outcomes.people_charged"} <= \
        set(products(product="court-outcomes")["court-outcomes"]["metrics"])
    assert "custody_population.people_in_custody" in products(product="custody-population")["custody-population"]["metrics"]


def test_data_product_info_reports_the_contracts_openmetadata_holds():
    """Each delivered table with a data contract in OpenMetadata, with its refresh promise and column count (the
    dbt contract's columns); a table without one (gold.dim_unit) is listed as uncontracted, not left out."""
    from lakehouse.openmetadata.om_sync import dbt_contracts
    columns = {t: len(c) for t, c in dbt_contracts().items()}
    court = products(product="court-outcomes")["court-outcomes"]
    assert [(c["table"], c["refresh_every"], c["refresh_unit"], c["max_latency_hours"], c["columns"])
            for c in court["contracts"]] == [("world_gold.fct_charge", 1, "month", 768, columns["world_gold.fct_charge"])]
    pop = products(product="custody-population")["custody-population"]
    assert {c["table"] for c in pop["contracts"]} == {"mart_custody.custody_snapshot_monthly",
                                                       "mart_custody.snapshot_coverage"}
    assert all(c["refresh_unit"] == "day" and c["max_latency_hours"] == 26 for c in pop["contracts"])
    assert pop["uncontracted_tables"] == ["gold.dim_unit"] and court["uncontracted_tables"] == []


def test_data_product_info_says_which_months_are_final():
    """Readiness as mart_readiness.period_readiness says: the latest month-end with its status and who it waits
    on, and every provisional month."""
    for name in ("incident-damage", "custody-offenses"):
        got = products(product=name)[name]["readiness"]
        rows = lake_rows("SELECT month_end, period_status, waiting_on FROM mart_readiness.period_readiness "
                         "WHERE product = ? ORDER BY month_end DESC", name)
        assert (got["latest"]["month_end"], got["latest"]["status"]) == (str(rows[0][0]), rows[0][1]), name
        assert [p["month_end"] for p in got["provisional"]] == [str(r[0]) for r in rows if r[1] == "provisional"], name


def test_data_product_info_says_how_fresh_against_its_promise():
    """Refreshed time and maximum age as the daily run pushed them to Prometheus, the age in hours, and fresh while
    the age is within the promise (a world product promises 768 hours, a custody product 26)."""
    pushed = {x["metric"]["product"]: float(x["value"][1])
              for x in prometheus_now("lakehouse_product_refreshed_timestamp_seconds")}
    for name, info in products().items():
        f = info["freshness"]
        assert f["max_age_hours"] == PRODUCTS[name]["max_age_hours"], name
        assert abs(f["refreshed_epoch"] - pushed[name]) < 1, name
        assert abs(f["age_hours"] - (time.time() - pushed[name]) / 3600) < 0.1, name
        assert f["status"] == ("fresh" if f["age_hours"] <= f["max_age_hours"] else "stale"), name


def test_data_product_info_reports_coverage_only_where_the_product_has_it():
    """The custody products say how many records the snapshot can't place in time (5,860 of 100,000); the world's
    products have no such measure, so coverage is empty rather than made up."""
    got = products()
    for name in ("custody-population", "custody-offenses"):
        assert got[name]["coverage"] == {"records_total": 100_000, "records_without_dates": 5_860}, name
    for name in ("field-incidents", "incident-damage", "court-outcomes"):
        assert got[name]["coverage"] is None, name


# --- data_quality ----------------------------------------------------------------------------------------------

def test_data_quality_scores_the_course_data_by_dimension():
    """The course data's pass rate per quality dimension, as the daily run checks them (completeness 96.89 ...),
    with how many rules each has, against the 95% floor."""
    course = quality(area="course")["course"]
    assert course["floor"] == 95
    assert {d["dimension"]: d["pass_rate"] for d in course["dimensions"]} == EXPECTED_DIMENSIONS
    rules = dict(lake_rows("SELECT dimension, count(*) FROM gold.dq_scorecard GROUP BY 1"))
    assert {d["dimension"]: d["rules"] for d in course["dimensions"]} == rules


def test_data_quality_lists_the_rules_under_the_floor():
    """Every rule scoring under 95%, worst first, exactly as gold.dq_scorecard has them."""
    below = quality(area="course")["course"]["below_floor"]
    want = lake_rows("SELECT dimension, column_name, rule, score_pct FROM gold.dq_scorecard WHERE score_pct < 95 "
                     "ORDER BY score_pct, dimension, column_name, rule")
    assert [(r["dimension"], r["column"], r["rule"], r["score_pct"]) for r in below] == [tuple(w) for w in want]


def test_data_quality_counts_the_worlds_flagged_values_by_table_and_rule():
    """The expanded world's flagged values, per silver table and rule, and the rows they touch, as
    world_silver.exceptions counts them; the totals add up."""
    world = quality(area="world")["world"]
    want = lake_rows("SELECT table_name, rule, count(*) FROM world_silver.exceptions GROUP BY 1, 2")
    got = {(t["table"], rule): n for t in world["tables"] for rule, n in t["by_rule"].items()}
    assert got == {(t, r): n for t, r, n in want}
    rows = dict(lake_rows("SELECT table_name, count(DISTINCT id) FROM world_silver.exceptions GROUP BY 1"))
    assert {t["table"]: t["rows_flagged"] for t in world["tables"]} == rows
    assert world["flagged_values"] == sum(n for *_, n in want) == sum(t["flagged_values"] for t in world["tables"])
    assert [t["flagged_values"] for t in world["tables"]] == sorted((t["flagged_values"] for t in world["tables"]),
                                                                     reverse=True)


def test_data_quality_for_one_world_table_and_never_a_value():
    """table="damage" gives only damage's flags; nothing in the answer is a flagged value or a row id, only counts
    (flagged values can be names), and an unknown table names the real ones."""
    world = quality(area="world", table="damage")["world"]
    assert [t["table"] for t in world["tables"]] == ["damage"]
    assert set(world["tables"][0]) == {"table", "flagged_values", "rows_flagged", "by_rule"}
    assert all(isinstance(n, int) for n in world["tables"][0]["by_rule"].values())
    is_error, _, text = call("data_quality", area="world", table="no_such_table")
    assert is_error and "damage" in text and "person" in text


def test_data_quality_says_which_quality_alerts_are_firing():
    """Both areas by default, and the data-quality alerts Prometheus has firing now (none today); a made-up area is
    an error that names the real ones."""
    both = quality()
    assert both["course"] and both["world"]
    firing = {x["metric"]["alertname"] for x in prometheus_now('ALERTS{alertstate="firing"}')} & DQ_ALERTS
    assert set(both["alerts_firing"]) == firing
    is_error, _, text = call("data_quality", area="everything")
    assert is_error and "course" in text and "world" in text


@pytest.mark.parametrize("tool", ["data_product_info", "data_quality"])
def test_the_new_tools_are_listed(tool):
    """Both tools are offered alongside the four others."""
    from test_mcp import rpc
    r = rpc("tools/list")
    assert tool in {t["name"] for t in r.json()["result"]["tools"]}
