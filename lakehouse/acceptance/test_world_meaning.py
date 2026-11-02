"""Acceptance test for the expanded world's meaning (dataset expansion, chunk 8): glossary, sensitivity, rules.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_world_meaning.py

Needs the stack, with the world built (world_refresh and world_monthly_job) and om_sync.py and om_to_rdf.py run
after it. The tests check that:
  - OpenMetadata's glossary has the world's terms (Incident, Location, Participant, Power, Power Use, Damage
    Assessment, Charge, Court Outcome, Team, Mission, Person Key, Person Role) with their typed relations, the new
    occursAt among them (Incident occurs at Location);
  - every world_bronze, world_silver and world_gold table is in OpenMetadata, names and birth dates are tagged
    Personal where they live (bronze and silver), a team's STG group Restricted, and columns link to the terms they
    mean, a column qualified by table where its name means different things in different tables (a court's
    outcome is not a fight's);
  - Fuseki has the same: the relations, the columns' terms, person_key deduced pseudonymous like the SID;
  - the rulebook holds the world's rules, and the shape that keeps personal data out of gold catches one planted
    in a scratch graph.
"""
import requests
import yaml

from test_business_rules import FUSEKI, GLOSSARY, WRITER
from test_glossary_rdf import ask, select
from test_om_sync import LAKEHOUSE, OM, get, om  # noqa: F401  (om is a fixture)

from lakehouse import lake

WORLD_TERMS = {"Incident", "Location", "Participant", "Power", "Power Use", "Damage Assessment", "Charge",
               "Court Outcome", "Team", "Mission", "Person Key", "Person Role"}
WORLD_SCHEMAS = ("world_bronze", "world_silver", "world_gold")
TERMS = {t["name"]: t for t in GLOSSARY["terms"]}


def tags(om, fqn):
    """{column: its tag FQNs} for one OpenMetadata table."""
    table = get(om, f"tables/name/lakehouse.lake.{fqn}", fields="columns,tags")
    return {c["name"]: {t["tagFQN"] for t in c.get("tags") or []} for c in table["columns"]}


# --- the glossary ----------------------------------------------------------------------------------------------

def test_the_glossary_defines_the_worlds_terms():
    """Each world term is in glossary.yml with a definition of at least 40 characters and columns that say where
    it lives."""
    assert WORLD_TERMS <= set(TERMS)
    for name in WORLD_TERMS:
        assert len(" ".join(TERMS[name]["definition"].split())) >= 40 and TERMS[name]["columns"], name


def test_openmetadata_has_the_worlds_terms_and_typed_relations(om):
    """OpenMetadata holds every world term, with relations typed as in glossary.yml: Incident occurs at Location,
    Charge is about Incident, Person Key identifies Person, Participant is part of Incident."""
    for name in WORLD_TERMS:
        assert get(om, f"glossaryTerms/name/Lakehouse.{name}")["name"] == name
    rels = lambda name: {(r["relationType"], r["term"]["name"])  # noqa: E731
                         for r in get(om, f"glossaryTerms/name/Lakehouse.{name}", fields="relatedTerms")["relatedTerms"]}
    assert ("occursAt", "Location") in rels("Incident")
    assert ("isAbout", "Incident") in rels("Charge")
    assert ("identifies", "Person") in rels("Person Key")
    assert ("partOf", "Incident") in rels("Participant")


# --- tables, tags and links in OpenMetadata --------------------------------------------------------------------

def test_every_world_table_is_in_openmetadata(om):
    """Each table in world_bronze, world_silver and world_gold has an entity under lakehouse.lake.<schema>."""
    con = lake.lake_ro_connect()
    tables = con.sql("SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = 'lake' "
                     f"AND table_schema IN {WORLD_SCHEMAS}").fetchall()
    assert len(tables) >= 40
    for schema, table in tables:
        assert requests.get(f"{OM}/tables/name/lakehouse.lake.{schema}.{table}", headers=om.headers,
                            timeout=30).status_code == 200, f"{schema}.{table}"


def test_personal_and_restricted_columns_are_tagged_where_they_live(om):
    """Names and birth dates are Personal in world_bronze.person and world_silver.person; a team's STG group is
    Restricted in silver and gold, as the course's STG columns are."""
    for fqn in ("world_bronze.person", "world_silver.person"):
        got = tags(om, fqn)
        for col in ("last_name", "first_name", "alias", "birth_date"):
            assert "Sensitivity.Personal" in got[col], (fqn, col)
    for fqn in ("world_silver.team", "world_gold.dim_team"):
        assert "Sensitivity.Restricted" in tags(om, fqn)["stg_group"], fqn


def test_columns_link_to_the_terms_they_mean_qualified_where_names_clash(om):
    """Keys link everywhere they appear (charge_id means Charge in bronze, silver and gold); a name that means
    different things in different tables links only where the glossary qualifies it: fct_charge.outcome is a Court
    Outcome, fct_incident.outcome is not. offender_number in the world means the course's Offender Number."""
    for schema in WORLD_SCHEMAS:
        table = "fct_charge" if schema == "world_gold" else "charge"
        assert "Lakehouse.Charge" in tags(om, f"{schema}.{table}")["charge_id"], schema
    assert "Lakehouse.Court Outcome" in tags(om, "world_gold.fct_charge")["outcome"]
    assert "Lakehouse.Court Outcome" not in tags(om, "world_gold.fct_incident")["outcome"]
    assert "Lakehouse.Offender Number" in tags(om, "world_gold.fct_charge")["offender_number"]
    assert "Lakehouse.Person Key" in tags(om, "world_gold.dim_person")["person_key"]


# --- Fuseki ----------------------------------------------------------------------------------------------------

def test_fuseki_has_the_worlds_relations_and_column_meanings():
    """The graph says Incident occurs at Location (lh:occursAt), and which world columns mean which term."""
    assert ask("ASK { term:incident lh:occursAt term:location }")
    assert ask('ASK { ?c lh:table "world_gold.fct_damage" ; lh:name "cost_usd" ; lh:means term:damage-assessment }')
    assert ask('ASK { ?c lh:table "world_silver.person" ; lh:name "birth_date" ; lh:sensitivity lh:Personal }')


def test_person_key_is_deduced_pseudonymous_like_the_sid():
    """Person Key identifies Person, so the pseudonymous-key deduction marks every person_key column, with no change
    to the rule."""
    tables = {r["table"] for r in select("""SELECT ?table WHERE { GRAPH ?g { ?c lh:sensitivity lh:Pseudonymous }
        ?c lh:table ?table ; lh:name "person_key" }""")}
    assert {"world_gold.dim_person", "world_gold.fct_charge", "world_silver.person"} <= tables


def test_one_question_across_the_worlds_meaning():
    """Which world_gold columns hold a key that identifies a person? Meaning, relations and deduction, in one query."""
    rows = select("""SELECT DISTINCT ?table ?name WHERE { ?c lh:table ?table ; lh:name ?name ; lh:means ?t .
        ?t lh:identifies term:person . FILTER(STRSTARTS(?table, "world_gold.")) }""")
    assert {(r["table"], r["name"]) for r in rows} >= {("world_gold.dim_person", "person_key"),
                                                       ("world_gold.dim_person", "sid_number")}


# --- rules -----------------------------------------------------------------------------------------------------

def test_the_rulebook_holds_the_worlds_rules():
    """The world's rules are in the rulebook, each about world terms and enforced by a dbt test or a shape."""
    names = {r["name"] for r in GLOSSARY["rules"]}
    for rule in ("A charge follows its fight, and its outcome follows the charge", "Only a captured villain is charged",
                 "A power is used only while its holder holds it", "A record's conviction is decided on its sentence date",
                 "People in the expanded world are counted by Person Key", "Personal data never reaches gold"):
        assert rule in names, rule
    rows = select("""PREFIX dct: <http://purl.org/dc/terms/>
        SELECT ?title WHERE { GRAPH <https://lakehouse.invalid/graph/rulebook> { ?r dct:title ?title ; lh:about term:charge } }""")
    assert {"A charge follows its fight, and its outcome follows the charge"} <= {r["title"] for r in rows}


def test_personal_data_in_gold_breaks_the_rule():
    """The shape catches it: a gold column tagged Personal, in a scratch graph, is reported (and the graph is
    removed whatever happens)."""
    from lakehouse.ontology import check_rules
    scratch = "https://lakehouse.invalid/graph/scratch-world-test"
    bad = """@prefix lh: <https://lakehouse.invalid/ontology#> .
<https://lakehouse.invalid/column/world_gold/dim_person/last_name> a lh:Column ; lh:table "world_gold.dim_person" ;
    lh:name "last_name" ; lh:sensitivity lh:Personal ."""
    requests.put(f"{FUSEKI}/data", params={"graph": scratch}, data=bad.encode(), auth=WRITER, timeout=30,
                 headers={"Content-Type": "text/turtle"}).raise_for_status()
    try:
        found = check_rules.violations(graph=scratch)
    finally:
        requests.delete(f"{FUSEKI}/data", params={"graph": scratch}, auth=WRITER, timeout=30)
    assert [v["shape"] for v in found] == ["PersonalStaysOutOfGoldShape"]
    assert not ask(f"ASK {{ GRAPH <{scratch}> {{ ?s ?p ?o }} }}")
