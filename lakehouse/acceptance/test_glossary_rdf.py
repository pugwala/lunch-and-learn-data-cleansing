"""Acceptance test for the glossary as RDF in Fuseki (Phase 5, optional piece).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_glossary_rdf.py

openmetadata/om_to_rdf.py reads OpenMetadata (as the read-only mcp-reader) and replaces one named graph in Fuseki:
the Lakehouse glossary as SKOS (concepts, definitions, synonyms, typed relations) and every lake column with its
sensitivity tag and the term it means. SPARQL can then join meaning, relations and sensitivity in one question.
"""
import os
import subprocess
from pathlib import Path

import requests
import yaml

from test_masks import model_masks

LAKEHOUSE = Path(__file__).resolve().parents[1]
FUSEKI = os.environ.get("FUSEKI_URL", "http://fuseki:3030/fuseki") + "/lakehouse"
WRITER = ("writer", os.environ.get("FUSEKI_WRITER_PASSWORD", ""))
GLOSSARY = yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text())["terms"]
GRAPH = "https://lakehouse.invalid/graph/openmetadata"
PREFIXES = """PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX lh:   <https://lakehouse.invalid/ontology#>
PREFIX term: <https://lakehouse.invalid/glossary/>
"""
FCT = "gold.fct_offender_record"


def select(query):
    """Run a SPARQL SELECT (with the prefixes above) on Fuseki; returns a list of {variable: value} rows."""
    r = requests.post(f"{FUSEKI}/sparql", data={"query": PREFIXES + query}, timeout=30,
                      headers={"Accept": "application/sparql-results+json"})
    r.raise_for_status()
    return [{k: v["value"] for k, v in row.items()} for row in r.json()["results"]["bindings"]]


def ask(query):
    """Run a SPARQL ASK (with the prefixes above) on Fuseki; returns True or False."""
    r = requests.post(f"{FUSEKI}/sparql", data={"query": PREFIXES + query}, timeout=30,
                      headers={"Accept": "application/sparql-results+json"})
    r.raise_for_status()
    return r.json()["boolean"]


def om_to_rdf(*args):
    """Run openmetadata/om_to_rdf.py; returns the finished process."""
    return subprocess.run(["python", str(LAKEHOUSE / "openmetadata" / "om_to_rdf.py"), *args],
                          capture_output=True, text=True, timeout=300)


def test_every_glossary_term_is_a_skos_concept_with_its_definition():
    """Each glossary term is a skos:Concept in the Lakehouse scheme, with its name and definition."""
    rows = select("SELECT ?label ?definition WHERE { ?t a skos:Concept ; skos:inScheme term:Lakehouse ; "
                  "skos:prefLabel ?label ; skos:definition ?definition }")
    got = {r["label"]: " ".join(r["definition"].split()) for r in rows}
    assert got == {t["name"]: " ".join(t["definition"].split()) for t in GLOSSARY}


def test_relations_keep_their_type():
    """SKOS's own relations where they fit, the lakehouse's own properties where they don't."""
    assert ask("ASK { term:offender-number lh:identifies term:offender-record }")
    assert ask("ASK { term:offense-code skos:broader term:offense-against-category }")
    assert ask("ASK { term:violence-code skos:related term:offense-code }")
    parts = select("SELECT ?t WHERE { ?t lh:partOf term:offender-record }")
    assert len(parts) == sum(1 for t in GLOSSARY for r in t.get("relations", []) if r["type"] == "partOf")


def test_synonyms_are_alternative_labels():
    """A term's synonyms are its skos:altLabel values."""
    rows = select("SELECT ?alt WHERE { term:offender-record skos:altLabel ?alt }")
    assert {"record", "offender record"} <= {r["alt"] for r in rows}


def test_columns_link_to_the_terms_they_mean():
    """The record key column of the fact table means the Offender Number term."""
    assert ask(f'ASK {{ ?c lh:table "{FCT}" ; lh:name "offender_number" ; lh:means term:offender-number }}')


def test_restricted_columns_agree_with_cube_masks():
    """One meaning, three systems: the columns the graph marks Restricted in the fact table are exactly the ones
    Cube masks (and those come from OpenMetadata's tags, test_masks.py)."""
    rows = select(f'SELECT ?name WHERE {{ ?c lh:table "{FCT}" ; lh:name ?name ; lh:sensitivity lh:Restricted }}')
    assert {r["name"] for r in rows} == model_masks()[0]


def test_one_question_across_meaning_relations_and_sensitivity():
    """Which restricted columns hold part of an offender record? Only the graph answers this in one query."""
    rows = select(f'SELECT ?name WHERE {{ ?c lh:table "{FCT}" ; lh:name ?name ; lh:sensitivity lh:Restricted ; '
                  f'lh:means ?t . ?t lh:partOf term:offender-record }}')
    assert {r["name"] for r in rows} == {"stg_group", "stg_status"}


def test_check_is_in_step():
    """A second look finds nothing to change: --check passes."""
    r = om_to_rdf("--check")
    assert r.returncode == 0, r.stdout[-600:] + r.stderr[-600:]


def test_a_load_replaces_the_graph():
    """A stray triple in the graph makes --check fail, and the next load removes it: the graph is replaced, not
    added to."""
    stray = f"INSERT DATA {{ GRAPH <{GRAPH}> {{ <urn:stray> <urn:p> 'x' }} }}"
    requests.post(f"{FUSEKI}/update", data={"update": stray}, auth=WRITER, timeout=30).raise_for_status()
    try:
        assert om_to_rdf("--check").returncode == 1
        r = om_to_rdf()
        assert r.returncode == 0, r.stdout[-600:] + r.stderr[-600:]
        assert not ask("ASK { <urn:stray> ?p ?o }")
    finally:
        requests.post(f"{FUSEKI}/update", timeout=30, auth=WRITER,
                      data={"update": f"DELETE DATA {{ GRAPH <{GRAPH}> {{ <urn:stray> <urn:p> 'x' }} }}"})
    assert om_to_rdf("--check").returncode == 0


# --- the daily run ---------------------------------------------------------------------------------------------

def test_dagster_can_write_fuseki():
    """The Dagster services get the Fuseki writer password the daily load uses."""
    env = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["x-dagster"]["environment"]
    assert "FUSEKI_WRITER_PASSWORD" in env


def test_daily_lake_refreshes_the_graph_after_openmetadata():
    """daily_lake includes the fuseki/glossary asset, right after openmetadata/lake."""
    from test_daily_run import graphql
    node = graphql('{ assetNodeOrError(assetKey: {path: ["fuseki", "glossary"]}) { ... on AssetNode { jobNames '
                   'dependencyKeys { path } } } }')["assetNodeOrError"]
    assert "daily_lake" in node.get("jobNames", []), node
    assert [k["path"] for k in node["dependencyKeys"]] == [["openmetadata", "lake"]]
