"""Acceptance test for the MCP server (Phase 4): list_metrics and query_metric over Cube (filters and time ranges
too), define_term over the glossary, business_rules over the rulebook in Fuseki.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_mcp.py

An agent asks for a governed metric by name instead of writing SQL against the lake: query_metric goes through
Cube's REST API, so it sees only Cube's model. define_term reads lakehouse/glossary/glossary.yml until OpenMetadata
(Phase 5) holds the glossary. The server is reached at https://<LAKE_TS_HOST>/mcp through Caddy, and on the
Compose network at mcp:8000/mcp; every request needs MCP_TOKEN as a bearer token.
"""
import os
from pathlib import Path

import pytest
import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
MCP = os.environ.get("MCP_URL", "http://mcp:8000/mcp")
TOKEN = os.environ.get("MCP_TOKEN", "")
PLAN_TERMS = {"Offender Record", "Offender Number", "SID Number", "Unit", "Custody Level", "Threat Level",
              "STG Affiliation", "Offense Code", "Offense-Against Category", "Violence Code", "Restitution",
              "Escape Risk", "Protective Custody", "Conduct Note"}


def rpc(method, params=None, token=TOKEN):
    """One JSON-RPC request to the MCP server; returns the HTTP response."""
    headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.post(MCP, headers=headers, timeout=60,
                         json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}})


def call(tool, **arguments):
    """Call a tool; returns (is_error, structured result or None, text)."""
    r = rpc("tools/call", {"name": tool, "arguments": arguments})
    r.raise_for_status()
    result = r.json()["result"]
    text = " ".join(c.get("text", "") for c in result.get("content", []))
    return bool(result.get("isError")), result.get("structuredContent"), text


# --- access ----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("token", ["", "wrong-token"])
def test_refuses_requests_without_the_right_token(token):
    """No token, or a wrong one, gets 401 before any tool runs."""
    assert rpc("tools/list", token=token).status_code == 401


def test_offers_exactly_its_six_tools():
    """The server lists list_metrics, query_metric, define_term, business_rules, data_product_info and data_quality
    (those two are tested in test_mcp_products.py), and nothing else."""
    r = rpc("tools/list")
    r.raise_for_status()
    assert {t["name"] for t in r.json()["result"]["tools"]} == {"list_metrics", "query_metric", "define_term",
                                                                "business_rules", "data_product_info", "data_quality"}


def test_server_holds_no_lake_or_admin_credentials():
    """The MCP container gets its token, Cube's API secret and the read-only mcp-reader's password for
    OpenMetadata: no lake credentials, no OpenMetadata admin, and no glossary file (define_term reads OpenMetadata)."""
    svc = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]["mcp"]
    env = svc["environment"]
    assert {"MCP_TOKEN", "CUBEJS_API_SECRET", "OM_MCP_PASSWORD"} <= set(env)
    assert not any(k in env for k in ("PGPASSWORD", "LAKE_RO_PASSWORD", "LAKE_CATALOG", "LAKE_RW_PASSWORD",
                                      "OM_ADMIN_PASSWORD"))
    assert not any("glossary" in str(v) for v in svc.get("volumes", []))


# --- query_metric ----------------------------------------------------------------------------------------------

def test_query_metric_counts_every_record():
    """record_count through Cube is all 100,000 offender records."""
    is_error, result, _ = call("query_metric", metric="record_count")
    assert not is_error
    assert result["rows"] == [{"record_count": 100_000}]


def test_query_metric_groups_by_a_dimension():
    """Grouped by custody level, the counts add back up to every record."""
    is_error, result, _ = call("query_metric", metric="record_count", group_by=["custody_level"])
    assert not is_error and len(result["rows"]) >= 5
    assert sum(r["record_count"] for r in result["rows"]) == 100_000


def test_query_metric_answers_the_trust_test():
    """The Trust Test cube is there too: all 8 answers changed by cleansing."""
    is_error, result, _ = call("query_metric", metric="answers_changed")
    assert not is_error and result["rows"] == [{"answers_changed": 8}]


def test_unknown_metric_lists_the_real_ones():
    """Asking for a metric Cube doesn't have is an error that names the metrics it does have."""
    is_error, _, text = call("query_metric", metric="no_such_metric")
    assert is_error and "record_count" in text


def test_restricted_columns_come_back_masked():
    """The MCP server's token has no groups, so a restricted column comes back masked: no race codes leak."""
    is_error, result, _ = call("query_metric", metric="record_count", group_by=["race_code"])
    assert not is_error and result["rows"] == [{"race_code": "***", "record_count": 100_000}]


# --- query_metric: filters and time ranges ---------------------------------------------------------------------

def rows(**arguments):
    """query_metric's rows, after checking the call succeeded."""
    is_error, result, text = call("query_metric", **arguments)
    assert not is_error, text
    return result["rows"]


def test_query_metric_filters_by_a_dimension():
    """A filter keeps only the matching rows: G5 records filtered are the G5 group's count, and a notEquals filter
    has every other record (Cube counts an empty custody level as not G5)."""
    by_level = {r["custody_level"]: r["record_count"] for r in rows(metric="record_count", group_by=["custody_level"])}
    g5 = rows(metric="record_count", filters=[{"dimension": "custody_level", "operator": "equals", "values": ["G5"]}])
    rest = rows(metric="record_count", filters=[{"dimension": "custody_level", "operator": "notEquals", "values": ["G5"]}])
    assert g5 == [{"record_count": by_level["G5"]}] and 0 < by_level["G5"] < 100_000
    assert rest[0]["record_count"] == 100_000 - by_level["G5"]


def test_a_filter_never_sees_through_a_mask():
    """Filtering on a restricted column compares the masked value, so it can't be used to count a race: W finds
    nothing, *** finds every record."""
    w = rows(metric="record_count", filters=[{"dimension": "race_code", "operator": "equals", "values": ["W"]}])
    masked = rows(metric="record_count", filters=[{"dimension": "race_code", "operator": "equals", "values": ["***"]}])
    assert w in ([], [{"record_count": 0}]) and masked == [{"record_count": 100_000}]


def test_query_metric_over_a_time_range_by_month():
    """A data product's metric by month over 2025: twelve month-ends in order, each a count of people, the time
    dimension found on its own (the view has only one)."""
    got = rows(metric="people_in_custody", date_from="2025-01-01", date_to="2025-12-31", granularity="month")
    assert [r["month_end"][:7] for r in got] == [f"2025-{m:02d}" for m in range(1, 13)]
    assert all(isinstance(r["people_in_custody"], int) and r["people_in_custody"] > 0 for r in got)


def test_a_time_range_counts_the_same_by_year_or_in_one_sum():
    """Records sentenced 2020 to 2022: three yearly rows that add up to the same range asked as one number."""
    window = {"metric": "record_count", "time_dimension": "sentence_date", "date_from": "2020-01-01",
              "date_to": "2022-12-31"}
    by_year = rows(**window, granularity="year")
    total = rows(**window)
    assert [r["sentence_date"][:4] for r in by_year] == ["2020", "2021", "2022"]
    assert sum(r["record_count"] for r in by_year) == total[0]["record_count"] > 0


@pytest.mark.parametrize("arguments,expect", [
    ({"filters": [{"dimension": "no_such_dimension", "values": ["x"]}]}, "custody_level"),
    ({"filters": [{"dimension": "custody_level", "operator": "between", "values": ["G1"]}]}, "notEquals"),
    ({"time_dimension": "custody_level", "date_from": "2020-01-01", "date_to": "2020-12-31"}, "sentence_date"),
    ({"time_dimension": "sentence_date", "granularity": "fortnight"}, "quarter"),
    ({"time_dimension": "sentence_date", "date_from": "last spring", "date_to": "2020-12-31"}, "YYYY-MM-DD"),
])
def test_bad_filters_and_time_ranges_say_what_would_work(arguments, expect):
    """An unknown dimension, operator, time dimension, granularity or date is an error naming what would work."""
    is_error, _, text = call("query_metric", metric="record_count", **arguments)
    assert is_error and expect in text, text


# --- business_rules --------------------------------------------------------------------------------------------

RULES = yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text())["rules"]


def rulebook(**arguments):
    """business_rules' rules, after checking the call succeeded."""
    is_error, result, text = call("business_rules", **arguments)
    assert not is_error, text
    return result["rules"]


def test_business_rules_lists_every_rule_with_what_enforces_it():
    """Every rule in the rulebook, its statement, its terms and its enforcers, as glossary.yml has them."""
    got = {r["name"]: r for r in rulebook()}
    assert set(got) == {r["name"] for r in RULES}
    for r in RULES:
        assert " ".join(r["statement"].split()) == got[r["name"]]["statement"]
        assert set(r.get("about", [])) == set(got[r["name"]]["about"])
        assert {e["kind"] + ":" + e["name"] for e in got[r["name"]]["enforced_by"]} == set(r["enforced_by"])


def test_business_rules_about_a_term_or_its_synonym():
    """About Charge gives the rules about charges; "fight", a synonym of Incident, finds the rule that a charge follows
    its fight; an unknown term is an error that names the terms rules are about."""
    charge = {r["name"] for r in rulebook(about="charge")}
    assert charge == {r["name"] for r in RULES if "Charge" in r.get("about", [])} and len(charge) >= 2
    assert "A charge follows its fight, and its outcome follows the charge" in {r["name"] for r in rulebook(about="Fight")}
    is_error, _, text = call("business_rules", about="no such term")
    assert is_error and "Person" in text


def prometheus_now(query):
    """The series a PromQL query returns now, asked of Prometheus directly."""
    r = requests.get(f"{os.environ.get('PROMETHEUS_URL', 'http://prometheus:9090/prometheus')}/api/v1/query",
                     params={"query": query}, timeout=30)
    r.raise_for_status()
    return r.json()["data"]["result"]


def test_business_rules_says_which_rules_hold_today():
    """A shape's status agrees with the daily check's violations in Prometheus (today none, so it holds), an alert's
    with whether Prometheus has it firing; the others say they aren't checked here, and what checks them."""
    violations = {x["metric"]["shape"]: float(x["value"][1]) for x in prometheus_now("lakehouse_business_rule_violations")}
    firing = {x["metric"]["alertname"] for x in prometheus_now('ALERTS{alertstate="firing"}')}
    for r in rulebook():
        for e in r["enforced_by"]:
            if e["kind"] == "shacl":
                assert e["status"] == ("holds" if violations[e["name"]] == 0 else "broken") == "holds", (r["name"], e)
            elif e["kind"] == "prometheus":
                assert e["status"] == ("broken" if e["name"] in firing else "holds"), (r["name"], e)
            else:
                assert e["status"] == "not checked here" and e["detail"], (r["name"], e)


# --- list_metrics ----------------------------------------------------------------------------------------------

def metrics():
    """list_metrics' entries, after checking the call succeeded."""
    is_error, result, text = call("list_metrics")
    assert not is_error, text
    return result["metrics"]


def test_list_metrics_names_every_metric_query_metric_knows():
    """list_metrics offers exactly the metrics query_metric accepts: the ones its unknown-metric error names."""
    _, _, text = call("query_metric", metric="no_such_metric")
    known = {name.strip() for name in text.split("Metrics:", 1)[1].split(",")}
    assert {m["name"] for m in metrics()} == known


def test_list_metrics_says_where_each_metric_lives():
    """Each entry names its view, data product and dimensions: people_in_custody is in both custody views, each
    its own product's; record_count is in offender_records, with custody_level, and belongs to no product."""
    by_view = {(m["name"], m["view"]): m for m in metrics()}
    assert by_view[("people_in_custody", "custody_population")]["product"] == "custody-population"
    assert by_view[("people_in_custody", "custody_offenses")]["product"] == "custody-offenses"
    record_count = by_view[("record_count", "offender_records")]
    assert record_count["product"] is None and "custody_level" in record_count["dimensions"]
    assert record_count["title"] == "Record Count" and record_count["description"]


def test_list_metrics_flags_counts_of_distinct_people():
    """A count of distinct people says so (countDistinct): its values never add up across groups."""
    by_view = {(m["name"], m["view"]): m for m in metrics()}
    assert by_view[("people_in_custody", "custody_population")]["aggregation"] == "countDistinct"
    assert by_view[("record_count", "offender_records")]["aggregation"] == "count"


# --- define_term -----------------------------------------------------------------------------------------------

def test_define_term_gives_definition_and_relations():
    """Offender Number has a definition, its column, and identifies an Offender Record."""
    is_error, result, _ = call("define_term", term="Offender Number")
    assert not is_error
    assert result["term"] == "Offender Number" and result["definition"]
    assert "offender_number" in result["columns"]
    assert {"type": "identifies", "term": "Offender Record"} in result["relations"]


def test_define_term_finds_synonyms_in_any_case():
    """A synonym, typed in any case, finds the term it belongs to."""
    is_error, result, _ = call("define_term", term="RECORD ID")
    assert not is_error and result["term"] == "Offender Number"


def test_unknown_term_lists_the_glossary():
    """A term the glossary doesn't have is an error that names the terms it does have."""
    is_error, _, text = call("define_term", term="no such term")
    assert is_error and "Offender Record" in text


def test_glossary_holds_the_plans_terms_and_consistent_relations():
    """glossary.yml has every term the plan lists, and every relation points at a term that exists."""
    terms = yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text())["terms"]
    names = {t["name"] for t in terms}
    assert PLAN_TERMS <= names
    for t in terms:
        assert t["definition"].strip(), t["name"]
        for rel in t.get("relations", []):
            assert rel["term"] in names, f"{t['name']} -> {rel['term']}"
