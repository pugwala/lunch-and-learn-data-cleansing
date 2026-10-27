"""Publish OpenMetadata's meaning to Fuseki as RDF: the glossary as SKOS, and every lake column's sensitivity and term.

    docker compose --profile core exec -T jupyter python lakehouse/openmetadata/om_to_rdf.py [--check]

Reads OpenMetadata as the read-only mcp-reader and builds one graph, <https://lakehouse.invalid/graph/openmetadata>:

- term:Lakehouse, a skos:ConceptScheme, and each glossary term as a skos:Concept with skos:prefLabel, skos:definition
  and skos:altLabel (synonyms). Relations keep their type: broader, narrower and relatedTo become SKOS's own
  skos:broader, skos:narrower and skos:related; the rest (partOf, hasPart, identifies, ...) become lh:<type>.
- every lake column as an lh:Column with lh:table ("schema.table"), lh:name, lh:sensitivity (lh:Restricted or
  lh:Personal, from the Sensitivity tags) and lh:means (the glossary term it is linked to).
- every data product as a dcat:Dataset (W3C DCAT, the data-catalog vocabulary) with dct:title, dct:description,
  lh:domain, lh:question, lh:usesTable for each table it delivers, lh:usesTerm for each glossary term it speaks in,
  and dcat:accessService for each Cube view that serves it (a dcat:DataService).

Beside it, graphs that come from the repo rather than OpenMetadata (acceptance/test_business_rules.py and
test_readiness.py):

- <https://lakehouse.invalid/graph/rulebook>: the rules: section of glossary/glossary.yml, each rule an
  lh:BusinessRule with dct:title, dct:description (its statement), lh:about (its terms) and lh:enforcedBy.
- <https://lakehouse.invalid/graph/readiness>: each business input in products/inputs/ as an lh:BusinessInput
  (dct:title, lh:unit, lh:system, lh:closesWithinDays), and each data product lh:dependsOn its depends_on: inputs.
- <https://lakehouse.invalid/graph/inferred/<name>>: what each deduction rule ontology/rules/<name>.rq (a SPARQL
  CONSTRUCT) derives from the graphs above, run until nothing new appears. Deduced facts stay apart from stated
  ones, and the graph's name says which rule made them. A graph whose rule file is gone is deleted.

Each graph is replaced in one Graph Store PUT as the writer user, so a re-run never piles up old triples. --check
compares every graph in Fuseki with what it should hold now and exits 1 if any differs. IRIs live under
lakehouse.invalid, a name reserved never to resolve, so nothing here points at a real host.
"""
import base64
import html
import os
import re
import sys
from pathlib import Path

import requests
import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.compare import isomorphic, to_isomorphic
from rdflib.namespace import DCAT, DCTERMS, RDF, SKOS

OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata") + "/api/v1"
FUSEKI = os.environ.get("FUSEKI_URL", "http://fuseki:3030/fuseki") + "/lakehouse"
GRAPH = "https://lakehouse.invalid/graph/openmetadata"
LH = Namespace("https://lakehouse.invalid/ontology#")
TERM = Namespace("https://lakehouse.invalid/glossary/")
COLUMN = "https://lakehouse.invalid/lake/"
PRODUCT = Namespace("https://lakehouse.invalid/product/")
CUBE_VIEW = Namespace("https://lakehouse.invalid/cube/")
RULEBOOK = "https://lakehouse.invalid/graph/rulebook"
READINESS = "https://lakehouse.invalid/graph/readiness"
INPUT = Namespace("https://lakehouse.invalid/input/")
INFERRED = "https://lakehouse.invalid/graph/inferred/"
RULE = Namespace("https://lakehouse.invalid/rule/")
LAKEHOUSE = Path(__file__).resolve().parents[1]
GLOSSARY = "Lakehouse"
SERVICE, DATABASE = "lakehouse", "lake"   # OpenMetadata names lake tables lakehouse.lake.<schema>.<table>
SKOS_RELATIONS = {"broader": SKOS.broader, "narrower": SKOS.narrower, "relatedTo": SKOS.related}
SENSITIVITY = {"Sensitivity.Restricted": LH.Restricted, "Sensitivity.Personal": LH.Personal}


def slug(name):
    """A term's IRI-safe name: lower case, runs of other characters as one hyphen (Offender Number -> offender-number)."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def text(value):
    """Description text as a person reads it (OpenMetadata returns it HTML-escaped), on one line."""
    return " ".join(html.unescape(value or "").split())


def om_session():
    """A requests session logged in to OpenMetadata as the read-only mcp-reader."""
    r = requests.post(f"{OM}/users/login", timeout=30, json={
        "email": "mcp-reader@open-metadata.org",
        "password": base64.b64encode(os.environ["OM_MCP_PASSWORD"].encode()).decode()})
    r.raise_for_status()
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"
    return s


def listing(om, path, **params):
    """Every entity of an OpenMetadata list endpoint, following its paging cursor."""
    out, after = [], None
    while True:
        r = om.get(f"{OM}/{path}", params={**params, "limit": 100, **({"after": after} if after else {})}, timeout=60)
        r.raise_for_status()
        page = r.json()
        out += page["data"]
        after = page.get("paging", {}).get("after")
        if not after:
            return out


def build(om):
    """The graph OpenMetadata describes now: glossary concepts and relations, then every lake column."""
    g = Graph()
    g.bind("skos", SKOS), g.bind("lh", LH), g.bind("term", TERM)
    g.add((TERM[GLOSSARY], RDF.type, SKOS.ConceptScheme))
    g.add((TERM[GLOSSARY], SKOS.prefLabel, Literal(GLOSSARY)))
    glossary = om.get(f"{OM}/glossaries/name/{GLOSSARY}", timeout=30).json()
    for t in listing(om, "glossaryTerms", glossary=glossary["id"], fields="relatedTerms,synonyms"):
        node = TERM[slug(t["name"])]
        g.add((node, RDF.type, SKOS.Concept))
        g.add((node, SKOS.inScheme, TERM[GLOSSARY]))
        g.add((node, SKOS.prefLabel, Literal(t["name"])))
        g.add((node, SKOS.definition, Literal(text(t.get("description")))))
        for synonym in t.get("synonyms") or []:
            g.add((node, SKOS.altLabel, Literal(synonym)))
        for rel in t.get("relatedTerms") or []:
            kind = rel.get("relationType") or "relatedTo"
            g.add((node, SKOS_RELATIONS.get(kind, LH[kind]), TERM[slug(rel["term"]["name"])]))
    for table in listing(om, "tables", database="lakehouse.lake", fields="columns,tags"):
        schema = table["fullyQualifiedName"].split(".")[2]
        for c in table["columns"]:
            node = URIRef(f"{COLUMN}{schema}/{table['name']}/{c['name']}")
            g.add((node, RDF.type, LH.Column))
            g.add((node, LH.table, Literal(f"{schema}.{table['name']}")))
            g.add((node, LH.name, Literal(c["name"])))
            for tag in c.get("tags") or []:
                if tag["tagFQN"] in SENSITIVITY:
                    g.add((node, LH.sensitivity, SENSITIVITY[tag["tagFQN"]]))
                elif tag.get("source") == "Glossary" and tag["tagFQN"].startswith(f"{GLOSSARY}."):
                    g.add((node, LH.means, TERM[slug(tag["tagFQN"].split(".", 1)[1])]))
    g.bind("dcat", DCAT), g.bind("dct", DCTERMS), g.bind("prod", PRODUCT)
    for p in listing(om, "dataProducts", fields="domains,extension"):
        node, ext = PRODUCT[p["name"]], p.get("extension") or {}
        g.add((node, RDF.type, DCAT.Dataset))
        g.add((node, DCTERMS.title, Literal(p.get("displayName") or p["name"])))
        g.add((node, DCTERMS.description, Literal(text(p.get("description")))))
        for d in p.get("domains") or []:
            g.add((node, LH.domain, Literal(d["name"])))
        if ext.get("lakehouseQuestion"):
            g.add((node, LH.question, Literal(ext["lakehouseQuestion"])))
        for asset in listing(om, f"dataProducts/name/{p['name']}/assets"):
            g.add((node, LH.usesTable, Literal(asset["fullyQualifiedName"].removeprefix(f"{SERVICE}.{DATABASE}."))))
        for term in filter(None, (ext.get("lakehouseTerms") or "").split(", ")):
            g.add((node, LH.usesTerm, TERM[slug(term)]))
        for view in filter(None, (ext.get("lakehouseCubeViews") or "").split(", ")):
            g.add((node, DCAT.accessService, CUBE_VIEW[view]))
            g.add((CUBE_VIEW[view], RDF.type, DCAT.DataService))
            g.add((CUBE_VIEW[view], DCTERMS.title, Literal(view)))
    return g


def rulebook():
    """The rulebook graph: each rule in glossary.yml's rules: section as an lh:BusinessRule."""
    g = Graph()
    g.bind("lh", LH), g.bind("term", TERM), g.bind("dct", DCTERMS), g.bind("rule", RULE)
    for r in yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text()).get("rules", []):
        node = RULE[slug(r["name"])]
        g.add((node, RDF.type, LH.BusinessRule))
        g.add((node, DCTERMS.title, Literal(r["name"])))
        g.add((node, DCTERMS.description, Literal(" ".join(r["statement"].split()))))
        for term in r.get("about", []):
            g.add((node, LH.about, TERM[slug(term)]))
        for ref in r["enforced_by"]:
            g.add((node, LH.enforcedBy, Literal(ref)))
    return g


def readiness():
    """The readiness graph: each business input (products/inputs/<name>.yml) as an lh:BusinessInput with its unit,
    system and days to close a month, and each data product lh:dependsOn the inputs in its depends_on:."""
    g = Graph()
    g.bind("lh", LH), g.bind("dct", DCTERMS), g.bind("prod", PRODUCT), g.bind("input", INPUT)
    for f in sorted((LAKEHOUSE / "products" / "inputs").glob("*.yml")):
        i = yaml.safe_load(f.read_text())
        node = INPUT[i["name"]]
        g.add((node, RDF.type, LH.BusinessInput))
        g.add((node, DCTERMS.title, Literal(i["title"])))
        g.add((node, DCTERMS.description, Literal(" ".join(i["description"].split()))))
        g.add((node, LH.unit, Literal(i["unit"])))
        g.add((node, LH.system, Literal(i["system"])))
        g.add((node, LH.closesWithinDays, Literal(int(i["closes_within_days"]))))
    for f in sorted((LAKEHOUSE / "products").glob("*.yml")):
        p = yaml.safe_load(f.read_text())
        for name in p.get("depends_on", []):
            g.add((PRODUCT[p["name"]], LH.dependsOn, INPUT[name]))
    return g


def deduce(*stated):
    """{graph IRI: what that rule deduced}: every ontology/rules/*.rq run over the stated graphs and each other's
    results until nothing new appears. A fact goes to the first rule that derived it."""
    rules = {f.stem: f.read_text() for f in sorted((LAKEHOUSE / "ontology" / "rules").glob("*.rq"))}
    known, out = Graph(), {INFERRED + name: Graph() for name in rules}
    for g in stated:
        known += g
    while True:
        new = 0
        for name, query in rules.items():
            for triple in known.query(query).graph:
                if triple not in known:
                    known.add(triple), out[INFERRED + name].add(triple)
                    new += 1
        if not new:
            return out


def current(graph=GRAPH):
    """A graph as Fuseki holds it now (empty if it was never loaded)."""
    r = requests.get(f"{FUSEKI}/get", params={"graph": graph}, headers={"Accept": "text/turtle"}, timeout=60)
    if r.status_code == 404:
        return Graph()
    r.raise_for_status()
    return Graph().parse(data=r.text, format="turtle")


def inferred_in_fuseki():
    """The IRIs of the inferred graphs Fuseki holds now, so ones whose rule file was deleted can be found."""
    r = requests.post(f"{FUSEKI}/sparql", timeout=60, headers={"Accept": "application/sparql-results+json"}, data={
        "query": f'SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{ ?s ?p ?o }} FILTER(STRSTARTS(STR(?g), "{INFERRED}")) }}'})
    r.raise_for_status()
    return {b["g"]["value"] for b in r.json()["results"]["bindings"]}


def gap(want, have):
    """How two graphs differ, in words ("3 to add, 0 to remove"); None when they are the same."""
    if isomorphic(want, have):
        return None
    a, b = to_isomorphic(want), to_isomorphic(have)
    return f"{len(a - b)} to add, {len(b - a)} to remove"


def label(graph):
    """A graph's short name for messages: openmetadata, rulebook, or inferred/<rule>."""
    return graph.removeprefix("https://lakehouse.invalid/graph/")


def main(argv):
    """Compare (--check) or replace every graph in Fuseki; print one summary line. Exits 1 when --check finds a gap."""
    stated, book, ready = build(om_session()), rulebook(), readiness()
    wanted = {GRAPH: stated, RULEBOOK: book, READINESS: ready, **deduce(stated, book, ready)}
    gaps = {g: gap(want, current(g)) for g, want in wanted.items()}
    gaps = {g: d for g, d in gaps.items() if d} | {g: "rule file deleted" for g in inferred_in_fuseki() - set(wanted)}
    sizes = f"{len(stated)} + {len(book)} rulebook + {len(ready)} readiness + {sum(len(wanted[g]) for g in wanted if g.startswith(INFERRED))} deduced triples"
    if not gaps:
        print(f"Fuseki graphs: in step with OpenMetadata and the rules ({sizes})")
        return 0
    what = "; ".join(f"{label(g)}: {d}" for g, d in sorted(gaps.items()))
    if "--check" in argv:
        print(f"Fuseki graphs: behind ({what}); run openmetadata/om_to_rdf.py")
        return 1
    auth = ("writer", os.environ["FUSEKI_WRITER_PASSWORD"])
    for g in gaps:
        if g in wanted:
            r = requests.put(f"{FUSEKI}/data", params={"graph": g}, data=wanted[g].serialize(format="turtle").encode(),
                             headers={"Content-Type": "text/turtle"}, timeout=120, auth=auth)
        else:
            r = requests.delete(f"{FUSEKI}/data", params={"graph": g}, timeout=60, auth=auth)
        r.raise_for_status()
    print(f"Fuseki graphs: loaded ({what}); {sizes}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
