"""Check the business rules in ontology/shapes.ttl against what Fuseki holds now.

    docker compose --profile core exec -T jupyter python lakehouse/ontology/check_rules.py

Fuseki validates the SHACL shapes itself (its /shacl endpoint), against all of its graphs at once: OpenMetadata's
glossary and columns, the rulebook and the deduced facts. Prints each violation, then one summary line; exits 1 if
any rule is broken. The daily run calls violations() and pushes the count per shape to Prometheus.
"""
import os
import re
import sys
from pathlib import Path

import requests
from rdflib import Graph, Namespace
from rdflib.namespace import RDF

FUSEKI = os.environ.get("FUSEKI_URL", "http://fuseki:3030/fuseki") + "/lakehouse"
SHAPES = Path(__file__).resolve().parent / "shapes.ttl"
SH = Namespace("http://www.w3.org/ns/shacl#")
LH = "https://lakehouse.invalid/ontology#"


def shapes():
    """The shapes' local names (lh:TermShape -> TermShape), in file order."""
    return re.findall(r"(?m)^lh:(\w+)\s+a\s+sh:NodeShape", SHAPES.read_text())


def owners():
    """{shape IRI: the node shape it belongs to}: each node shape owns itself and its named property shapes."""
    g = Graph().parse(SHAPES, format="turtle")
    out = {}
    for node in g.subjects(RDF.type, SH.NodeShape):
        out[str(node)] = str(node)
        out.update({str(p): str(node) for p in g.objects(node, SH.property)})
    return out


def violations(graph="union"):
    """Each violation Fuseki reports for the shapes against a graph (default: all of them), as a dict with the
    shape's local name, the focus node (what broke the rule) and the message."""
    r = requests.post(f"{FUSEKI}/shacl", params={"graph": graph}, data=SHAPES.read_bytes(), timeout=120,
                      headers={"Content-Type": "text/turtle", "Accept": "text/turtle"})
    r.raise_for_status()
    report, owner = Graph().parse(data=r.text, format="turtle"), owners()
    found = []
    for result in report.subjects(SH.focusNode, None):
        shape = owner.get(str(report.value(result, SH.sourceShape)), "?")
        found.append({"shape": shape.removeprefix(LH),
                      "focus": str(report.value(result, SH.focusNode)),
                      "message": str(report.value(result, SH.resultMessage) or "")})
    return sorted(found, key=lambda v: (v["shape"], v["focus"], v["message"]))


def main():
    """Print each violation and a summary line; exit 1 if any rule is broken."""
    names, found = shapes(), violations()
    for v in found:
        print(f"  {v['shape']}: {v['focus']}: {v['message']}")
    if not found:
        print(f"Business rules: all {len(names)} shapes hold")
        return 0
    broken = sorted({v["shape"] for v in found})
    print(f"Business rules: {len(found)} violation{'s' * (len(found) != 1)} of {len(names)} shapes ({', '.join(broken)})")
    return 1


if __name__ == "__main__":
    sys.exit(main())
