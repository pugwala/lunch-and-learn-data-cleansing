"""Acceptance test for om_sync.py: OpenMetadata holds the lakehouse's meaning (Phase 5).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_om_sync.py

om_sync.py loads, from the repo and the lake: the Lakehouse glossary (glossary/glossary.yml, with typed relations),
the Sensitivity.Restricted tag, and every lake table and column (descriptions from dbt), tagging the columns dbt
marks restricted and linking columns to their glossary terms. Re-running it changes nothing.
"""
import base64
import html
import os
import subprocess
from pathlib import Path

import pytest
import requests
import yaml

from lakehouse import lake

LAKEHOUSE = Path(__file__).resolve().parents[1]
OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata") + "/api/v1"
GLOSSARY = yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text())["terms"]


def restricted_columns():
    """Columns of gold.fct_offender_record that dbt tags meta.sensitivity: restricted."""
    doc = yaml.safe_load((LAKEHOUSE / "dbt" / "models" / "gold" / "schema.yml").read_text())
    fct = next(m for m in doc["models"] if m["name"] == "fct_offender_record")
    return {c["name"] for c in fct["columns"] if (c.get("meta") or {}).get("sensitivity") == "restricted"}


@pytest.fixture(scope="module")
def om():
    """A requests session with an admin token."""
    r = requests.post(f"{OM}/users/login", timeout=30, json={
        "email": "admin@open-metadata.org",
        "password": base64.b64encode(os.environ["OM_ADMIN_PASSWORD"].encode()).decode()})
    r.raise_for_status()
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"
    return s


def get(om, path, **params):
    """GET an OpenMetadata API path; returns its JSON."""
    r = om.get(f"{OM}/{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def terms(om):
    """Every term in the Lakehouse glossary, by name, with relations, synonyms and custom properties."""
    data = get(om, "glossaryTerms", glossary=get(om, "glossaries/name/Lakehouse")["id"],
               fields="relatedTerms,synonyms,extension,tags", limit=100)["data"]
    return {t["name"]: t for t in data}


# --- glossary --------------------------------------------------------------------------------------------------

def test_glossary_has_every_term_with_its_definition(om):
    """Every term in glossary.yml is in OpenMetadata, with the same definition."""
    got = terms(om)
    for t in GLOSSARY:
        assert t["name"] in got, t["name"]
        assert html.unescape(got[t["name"]]["description"]).split() == t["definition"].split(), t["name"]   # stored HTML-escaped


def test_glossary_relations_are_typed(om):
    """Relations keep their type: Offender Number identifies Offender Record; Offense Code is broader-linked."""
    got = terms(om)
    rels = {(r["relationType"], r["term"]["name"]) for r in got["Offender Number"]["relatedTerms"]}
    assert ("identifies", "Offender Record") in rels
    rels = {(r["relationType"], r["term"]["name"]) for r in got["Offense Code"]["relatedTerms"]}
    assert ("broader", "Offense-Against Category") in rels


def test_glossary_keeps_synonyms_columns_and_metrics(om):
    """Synonyms, and the columns and metrics a term maps to, travel with it."""
    t = terms(om)["Offender Record"]
    assert "record" in t["synonyms"]
    assert t["extension"]["lakehouseColumns"] == "offender_number"
    assert t["extension"]["lakehouseMetrics"] == "record_count, records_with_exceptions"


# --- tables and tags -------------------------------------------------------------------------------------------

def test_every_lake_table_is_in_openmetadata(om):
    """Each table in the lake (ref, bronze, silver, gold, ops) has an entity under lakehouse.lake.<schema>."""
    con = lake.lake_ro_connect()
    tables = con.sql("SELECT table_schema, table_name FROM information_schema.tables "
                     "WHERE table_catalog = 'lake' AND table_schema IN ('ref','bronze','silver','gold','ops')").fetchall()
    assert tables
    for schema, table in tables:
        r = om.get(f"{OM}/tables/name/lakehouse.lake.{schema}.{table}", timeout=30)
        assert r.status_code == 200, f"{schema}.{table}"


def test_restricted_columns_carry_the_restricted_tag(om):
    """In gold.fct_offender_record, the columns dbt tags restricted carry Sensitivity.Restricted. dbt is the floor:
    a steward can tag more in OpenMetadata, and om_sync keeps those."""
    table = get(om, "tables/name/lakehouse.lake.gold.fct_offender_record", fields="columns,tags")
    tagged = {c["name"] for c in table["columns"]
              if any(t["tagFQN"] == "Sensitivity.Restricted" for t in c.get("tags", []))}
    assert tagged >= restricted_columns()


def test_columns_link_to_their_glossary_terms(om):
    """The record key column is linked to the Offender Number glossary term."""
    table = get(om, "tables/name/lakehouse.lake.gold.fct_offender_record", fields="columns,tags")
    col = next(c for c in table["columns"] if c["name"] == "offender_number")
    assert "Lakehouse.Offender Number" in {t["tagFQN"] for t in col.get("tags", [])}


def test_columns_carry_dbt_descriptions(om):
    """Column descriptions come from dbt's schema.yml."""
    table = get(om, "tables/name/lakehouse.lake.gold.fct_offender_record", fields="columns")
    col = next(c for c in table["columns"] if c["name"] == "offender_number")
    assert col.get("description", "").strip() == "Record ID, BIGINT."


def test_sync_is_idempotent():
    """A second run finds nothing to change: --check passes."""
    r = subprocess.run(["python", str(LAKEHOUSE / "openmetadata" / "om_sync.py"), "--check"],
                       capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]


def test_raw_and_personal_columns_are_tagged_where_they_live(om):
    """Silver and bronze keep the raw race, religion and STG values and the personal fields: dbt's source metadata
    marks them, and OpenMetadata tags them Restricted or Personal there too."""
    silver = get(om, "tables/name/lakehouse.lake.silver.hero_offenders", fields="columns,tags")
    tags = {c["name"]: {t["tagFQN"] for t in c.get("tags", [])} for c in silver["columns"]}
    assert "Sensitivity.Restricted" in tags["race"] and "Sensitivity.Restricted" in tags["stg_affiliation"]
    assert "Sensitivity.Personal" in tags["conduct_notes"] and "Sensitivity.Personal" in tags["date_of_birth"]
    bronze = get(om, "tables/name/lakehouse.lake.bronze.hero_offenders_raw", fields="columns,tags")
    assert "Sensitivity.Personal" in {t["tagFQN"] for c in bronze["columns"] if c["name"] == "last_name" for t in c.get("tags", [])}


# --- the daily run ---------------------------------------------------------------------------------------------

from test_daily_run import graphql   # noqa: E402  (Dagster's GraphQL helper)

OM_ASSET = '{path: ["openmetadata", "lake"]}'


def test_dagster_can_reach_openmetadata():
    """The Dagster services get the OpenMetadata passwords om_sync.py and sync_masks.py log in with."""
    env = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["x-dagster"]["environment"]
    assert "OM_ADMIN_PASSWORD" in env and "OM_MCP_PASSWORD" in env


def test_daily_lake_syncs_openmetadata_after_gold():
    """daily_lake (the 05:00 job) includes the openmetadata/lake asset, downstream of gold."""
    node = graphql(f'{{ assetNodeOrError(assetKey: {OM_ASSET}) {{ ... on AssetNode {{ jobNames '
                   f'dependencyKeys {{ path }} }} }} }}')["assetNodeOrError"]
    assert "daily_lake" in node.get("jobNames", []), node
    assert ["gold", "fct_offender_record"] in [k["path"] for k in node["dependencyKeys"]]


def test_daily_run_checks_cube_masks_without_blocking():
    """A non-blocking check on the asset reports whether Cube's masks still match OpenMetadata, and it passed."""
    node = graphql(f'{{ assetNodeOrError(assetKey: {OM_ASSET}) {{ ... on AssetNode {{ assetChecksOrError '
                   f'{{ ... on AssetChecks {{ checks {{ name blocking }} }} }} }} }} }}')["assetNodeOrError"]
    checks = {c["name"]: c for c in node["assetChecksOrError"]["checks"]}
    assert checks.get("cube_masks_match_openmetadata", {}).get("blocking") is False, checks
    runs = graphql(f'{{ assetCheckExecutions(assetKey: {OM_ASSET}, checkName: "cube_masks_match_openmetadata", '
                   f'limit: 1) {{ evaluation {{ success }} }} }}')["assetCheckExecutions"]
    assert runs and runs[0]["evaluation"]["success"], runs
