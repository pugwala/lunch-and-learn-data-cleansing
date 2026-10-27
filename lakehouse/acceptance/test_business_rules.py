"""Acceptance test for business rules in the ontology: the rulebook, deduced facts and SHACL shapes.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_business_rules.py

Three kinds of rule, all kept as files in the repo and published to Fuseki by openmetadata/om_to_rdf.py, because
Fuseki's graphs are rebuilt on every run and anything typed into it by hand would be lost:

- The rulebook: the rules: section of glossary/glossary.yml. Each rule says in English what must hold, which terms
  it is about and what enforces it (a Cube measure, a dbt test, a Prometheus alert, a SHACL shape, a deduction rule
  or an acceptance test). It is published as lh:BusinessRule resources in graph/rulebook, so SPARQL can answer
  "which rules involve Person, and where is each enforced?". Every enforced_by must name something that exists.
- Deductions: ontology/rules/<name>.rq, one SPARQL CONSTRUCT each, whose results go to graph/inferred/<name>, so a
  deduced fact is never mistaken for a stated one and says which rule made it. The first one: a column that holds a
  person's identifier is a pseudonymous key (lh:Pseudonymous): fine in tables, for joining and counting people,
  never shown to a consumer, so no Cube member may expose it.
- Constraints: ontology/shapes.ttl, SHACL shapes Fuseki validates (its /shacl endpoint). ontology/check_rules.py
  reports any violation; the daily run pushes the count to Prometheus, and BusinessRuleViolated alerts on it.
"""
import os
import re
from pathlib import Path

import requests
import yaml

from test_custody_cube import meta
from test_glossary_rdf import ask, select

LAKEHOUSE = Path(__file__).resolve().parents[1]
FUSEKI = os.environ.get("FUSEKI_URL", "http://fuseki:3030/fuseki") + "/lakehouse"
WRITER = ("writer", os.environ.get("FUSEKI_WRITER_PASSWORD", ""))
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
GLOSSARY = yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text())
RULES = GLOSSARY["rules"]
ONTOLOGY = LAKEHOUSE / "ontology"
PSEUDONYMOUS_RULE = "identifier-of-a-person-is-pseudonymous"
INFERRED = "https://lakehouse.invalid/graph/inferred/"
RULE_PREFIXES = """PREFIX dct: <http://purl.org/dc/terms/>
PREFIX rule: <https://lakehouse.invalid/rule/>
"""


def shape_names():
    """The SHACL shapes in ontology/shapes.ttl, by local name (lh:TermShape -> TermShape)."""
    return set(re.findall(r"(?m)^lh:(\w+)\s+a\s+sh:NodeShape", (ONTOLOGY / "shapes.ttl").read_text()))


# --- the rulebook ----------------------------------------------------------------------------------------------

def test_every_rule_states_itself_and_is_about_real_terms():
    """Each rule in glossary.yml has a name, an English statement, terms that exist, and at least one enforcer."""
    terms = {t["name"] for t in GLOSSARY["terms"]}
    assert len(RULES) >= 5
    for r in RULES:
        assert r["name"] and len(r["statement"]) >= 40 and r["enforced_by"], r["name"]
        assert set(r.get("about", [])) <= terms, (r["name"], set(r["about"]) - terms)


def test_every_enforcer_named_exists():
    """enforced_by can't point at nothing: each Cube member, dbt test, alert, shape, deduction and test is real."""
    cube = {m["name"] for c in meta() for kind in ("dimensions", "measures") for m in c[kind]}
    alerts = {r["alert"] for g in yaml.safe_load((LAKEHOUSE / "monitoring" / "prometheus" / "rules" /
                                                  "lakehouse.yml").read_text())["groups"] for r in g["rules"]}
    for r in RULES:
        for ref in r["enforced_by"]:
            kind, name = ref.split(":", 1)
            if kind == "cube":
                assert name in cube, ref
            elif kind == "dbt":
                assert (LAKEHOUSE / "dbt" / "tests" / f"{name}.sql").exists(), ref
            elif kind == "prometheus":
                assert name in alerts, ref
            elif kind == "shacl":
                assert name in shape_names(), ref
            elif kind == "sparql":
                assert (ONTOLOGY / "rules" / f"{name}.rq").exists(), ref
            elif kind == "test":
                path, func = name.split("::")
                assert f"def {func}(" in (LAKEHOUSE / "acceptance" / path).read_text(), ref
            else:
                raise AssertionError(f"{ref}: unknown kind {kind!r}")


def test_the_rulebook_is_in_fuseki():
    """Every rule is an lh:BusinessRule with its title, statement, terms and enforcers, in graph/rulebook."""
    rows = select(RULE_PREFIXES + """SELECT ?title (COUNT(DISTINCT ?by) AS ?n) WHERE {
        GRAPH <https://lakehouse.invalid/graph/rulebook> { ?r a lh:BusinessRule ; dct:title ?title ;
        dct:description ?statement ; lh:enforcedBy ?by } } GROUP BY ?title""")
    assert {r["title"]: int(r["n"]) for r in rows} == {r["name"]: len(set(r["enforced_by"])) for r in RULES}


def test_one_query_finds_every_rule_about_person_and_its_enforcement():
    """The point of a rulebook: which rules involve Person, and where is each enforced, in one SPARQL query."""
    rows = select(RULE_PREFIXES + """SELECT ?title ?by WHERE { ?r a lh:BusinessRule ; dct:title ?title ;
        lh:about term:person ; lh:enforcedBy ?by }""")
    want = {(r["name"], by) for r in RULES if "Person" in r.get("about", []) for by in r["enforced_by"]}
    assert want and {(r["title"], r["by"]) for r in rows} == want


# --- deductions ------------------------------------------------------------------------------------------------

def test_every_sid_column_is_deduced_pseudonymous():
    """The rule finds every column that means a term identifying Person (today, sid_number in every table that has
    it) and marks it lh:Pseudonymous, in its own graph."""
    meaning = {r["table"] for r in select("""SELECT ?table WHERE { ?c lh:means ?t ; lh:table ?table .
        ?t lh:identifies term:person }""")}
    deduced = {r["table"] for r in select(f"""SELECT ?table WHERE {{ GRAPH <{INFERRED}{PSEUDONYMOUS_RULE}> {{
        ?c lh:sensitivity lh:Pseudonymous }} ?c lh:table ?table ; lh:name "sid_number" }}""")}
    assert len(meaning) >= 6 and deduced == meaning


def test_deduced_facts_stay_out_of_the_stated_graph():
    """Nothing in OpenMetadata's own graph says Pseudonymous: only the deduction's graph does."""
    assert not ask("ASK { GRAPH <https://lakehouse.invalid/graph/openmetadata> { ?c lh:sensitivity lh:Pseudonymous } }")


def test_no_cube_member_exposes_a_pseudonymous_column():
    """A pseudonymous key is never shown to a consumer: no public Cube member is a column the rule deduced."""
    keys = {r["name"] for r in select(f"""SELECT DISTINCT ?name WHERE {{ GRAPH <{INFERRED}{PSEUDONYMOUS_RULE}> {{
        ?c lh:sensitivity lh:Pseudonymous }} ?c lh:name ?name }}""")}
    assert keys == {"sid_number"}
    exposed = {m["name"] for c in meta() for kind in ("dimensions", "measures") for m in c[kind]
               if m["name"].split(".", 1)[1] in keys}
    assert not exposed, exposed


# --- constraints (SHACL) ---------------------------------------------------------------------------------------

def test_the_catalog_follows_every_shape_today():
    """check_rules.py validates every shape against all of Fuseki's graphs: today, nothing breaks a rule."""
    from lakehouse.ontology import check_rules
    assert check_rules.violations() == []
    assert {"TermShape", "RuleShape"} <= shape_names()


def test_a_placeholder_definition_breaks_the_term_rule():
    """The shape does catch a lazy definition: a term defined as "TBD", in a scratch graph, is reported."""
    from lakehouse.ontology import check_rules
    scratch = "https://lakehouse.invalid/graph/scratch-test"
    bad = """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
<https://lakehouse.invalid/glossary/scratch-term> a skos:Concept ; skos:prefLabel "Scratch term" ; skos:definition "TBD" ."""
    put = requests.put(f"{FUSEKI}/data", params={"graph": scratch}, data=bad.encode(), auth=WRITER, timeout=30,
                       headers={"Content-Type": "text/turtle"})
    put.raise_for_status()
    try:
        found = check_rules.violations(graph=scratch)
    finally:
        requests.delete(f"{FUSEKI}/data", params={"graph": scratch}, auth=WRITER, timeout=30)
    assert [(v["shape"], v["focus"]) for v in found] == [("TermShape", "https://lakehouse.invalid/glossary/scratch-term")]


def test_the_daily_run_reports_each_shape_to_prometheus():
    """The daily run pushes lakehouse_business_rule_violations per shape (all 0), and the alert rule loads."""
    r = requests.get(f"{PROMETHEUS}/api/v1/query", params={"query": "lakehouse_business_rule_violations"}, timeout=30)
    got = {x["metric"]["shape"]: float(x["value"][1]) for x in r.json()["data"]["result"]}
    assert got == {name: 0.0 for name in shape_names()}
    groups = requests.get(f"{PROMETHEUS}/api/v1/rules", timeout=30).json()["data"]["groups"]
    rule = next(r for g in groups for r in g["rules"] if r["name"] == "BusinessRuleViolated")
    assert rule["health"] == "ok" and "lakehouse_business_rule_violations" in rule["query"]


def test_a_deduction_whose_rule_was_deleted_is_removed():
    """Deleting a rule file must take its facts with it: an inferred graph with no rule file makes --check fail, and
    the next load deletes that graph."""
    from test_glossary_rdf import om_to_rdf
    orphan = f"{INFERRED}a-rule-that-was-deleted"
    requests.put(f"{FUSEKI}/data", params={"graph": orphan}, auth=WRITER, timeout=30, headers={"Content-Type": "text/turtle"},
                 data=b"<urn:orphan> <https://lakehouse.invalid/ontology#sensitivity> <urn:x> .").raise_for_status()
    try:
        assert om_to_rdf("--check").returncode == 1
        assert om_to_rdf().returncode == 0
        assert not ask("ASK { <urn:orphan> ?p ?o }")
    finally:
        requests.delete(f"{FUSEKI}/data", params={"graph": orphan}, auth=WRITER, timeout=30)
    assert om_to_rdf("--check").returncode == 0
