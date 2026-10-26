"""Acceptance test for the MCP server (Phase 4): query_metric over Cube, define_term over the glossary.

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


def test_offers_exactly_query_metric_and_define_term():
    """The server lists the two tools the plan calls for, and nothing else."""
    r = rpc("tools/list")
    r.raise_for_status()
    assert {t["name"] for t in r.json()["result"]["tools"]} == {"query_metric", "define_term"}


def test_server_holds_no_lake_credentials():
    """The MCP container gets the MCP token and Cube's API secret only: Cube is its one way to the data."""
    env = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]["mcp"]["environment"]
    assert {"MCP_TOKEN", "CUBEJS_API_SECRET"} <= set(env)
    assert not any(k in env for k in ("PGPASSWORD", "LAKE_RO_PASSWORD", "LAKE_CATALOG", "LAKE_RW_PASSWORD"))


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


def test_restricted_columns_are_out_of_reach():
    """Race isn't in Cube's model (masking comes in Phase 5), so query_metric can't group by it."""
    is_error, _, _ = call("query_metric", metric="record_count", group_by=["race"])
    assert is_error


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
