"""Make OpenMetadata hold the lakehouse's meaning: the glossary, sensitivity tags, and every lake table. Idempotent.

    python lakehouse/openmetadata/om_sync.py           # create or update whatever differs
    python lakehouse/openmetadata/om_sync.py --check   # change nothing; exit 1 and list what differs

Sources, all in the repo or the lake:
    glossary/glossary.yml          terms, synonyms, typed relations, the columns and metrics each maps to
    dbt models/*.yml               table and column descriptions; meta.sensitivity (restricted or personal)
    the lake itself                every table, column and type, read through the read-only lake_ro connection
OpenMetadata has no DuckLake connector, so this pushes the lake's tables as a CustomDatabase service named
lakehouse (lakehouse.lake.<schema>.<table>). bootstrap.sh runs it; it needs OM_ADMIN_PASSWORD.
"""
import base64
import html
import os
import re
import sys
from pathlib import Path

import requests
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lakehouse import lake  # noqa: E402

OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata") + "/api/v1"
LAKEHOUSE = Path(__file__).resolve().parents[1]
SERVICE, DATABASE, GLOSSARY, CLASSIFICATION = "lakehouse", "lake", "Lakehouse", "Sensitivity"
SCHEMAS = ("ref", "bronze", "silver", "gold", "ops", "mart_custody", "mart_readiness")
SENSITIVITY_TAGS = {"restricted": "Restricted", "personal": "Personal"}
TYPES = {"VARCHAR": "STRING", "BIGINT": "BIGINT", "INTEGER": "INT", "SMALLINT": "SMALLINT", "DOUBLE": "DOUBLE",
         "FLOAT": "FLOAT", "DATE": "DATE", "TIME": "TIME", "BOOLEAN": "BOOLEAN", "TIMESTAMP": "TIMESTAMP",
         "TIMESTAMP WITH TIME ZONE": "TIMESTAMPZ"}
# OpenMetadata names a contract's timezone by its own labels (a fixed GMT offset plus the zone), not IANA names.
OM_TIMEZONES = {"America/Chicago": "GMT-06:00 (America/Chicago)"}
PRODUCT_PROPERTIES = {"lakehouseQuestion": "The question this data product answers",
                      "lakehouseCubeViews": "Cube views that serve it (comma-separated)",
                      "lakehouseTerms": "Glossary terms it speaks in (comma-separated)"}
CUSTOM_PROPERTIES = {"lakehouseColumns": "Lake columns this term maps to (comma-separated)",
                     "lakehouseMetrics": "Cube measures to ask query_metric for (comma-separated)"}
CUSTOM_RELATIONS = [   # relation types glossary.yml uses that OpenMetadata doesn't define out of the box
    {"name": "identifies", "displayName": "Identifies", "inverseRelation": "identifiedBy",
     "description": "This term's value picks out exactly one instance of the other term."},
    {"name": "identifiedBy", "displayName": "Identified by", "inverseRelation": "identifies",
     "description": "Instances of this term are picked out by the other term's value."},
    {"name": "isAbout", "displayName": "Is about", "inverseRelation": "subjectOf",
     "description": "Each instance of this term describes one instance of the other (a record is about a person)."},
    {"name": "subjectOf", "displayName": "Subject of", "inverseRelation": "isAbout",
     "description": "Instances of this term are described by instances of the other."},
]


def text(value):
    """Description text as a person reads it: OpenMetadata returns descriptions HTML-escaped (' as &#39;)."""
    return " ".join(html.unescape(value or "").split())


class Sync:
    """One sync run: an admin session, plus the list of differences found (and fixed, unless check)."""

    def __init__(self, check):
        """Log in as admin; check=True means report differences without writing."""
        r = requests.post(f"{OM}/users/login", timeout=30, json={
            "email": "admin@open-metadata.org",
            "password": base64.b64encode(os.environ["OM_ADMIN_PASSWORD"].encode()).decode()})
        r.raise_for_status()
        self.http = requests.Session()
        self.http.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"
        self.check, self.drift = check, []

    def get(self, path, **params):
        """GET a path; returns its JSON, or None for 404."""
        r = self.http.get(f"{OM}/{path}", params=params, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def write(self, method, path, body, what):
        """Record a difference; unless checking, send the write and return its JSON."""
        self.drift.append(what)
        if self.check:
            return None
        r = self.http.request(method, f"{OM}/{path}", json=body, timeout=60)
        if not r.ok:
            raise RuntimeError(f"{method} {path} ({what}): {r.status_code} {r.text[:400]}")
        return r.json()

    def patch(self, path, ops, what):
        """Record a difference; unless checking, send a JSON Patch (OpenMetadata's partial update) to path."""
        self.drift.append(what)
        if self.check:
            return None
        r = self.http.patch(f"{OM}/{path}", json=ops, timeout=60,
                            headers={"Content-Type": "application/json-patch+json"})
        if not r.ok:
            raise RuntimeError(f"PATCH {path} ({what}): {r.status_code} {r.text[:400]}")
        return r.json()

    def ensure(self, collection, fqn, body, compare, fields=""):
        """Create or update an entity with PUT unless the fields in compare(current) already match body's."""
        current = self.get(f"{collection}/name/{fqn}", **({"fields": fields} if fields else {}))
        if current is not None and compare(current) == compare(body):
            return current
        return self.write("PUT", collection, body, f"{collection} {fqn}") or current


# ---- Sources -------------------------------------------------------------------------------------------------
def glossary_terms():
    """The terms in glossary/glossary.yml."""
    return yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text())["terms"]


def dbt_model_schemas():
    """{models/ subfolder: schema} from dbt_project.yml's +schema settings, e.g. gold -> gold, marts/custody ->
    mart_custody: where dbt writes the models in that folder."""
    out = {}

    def walk(node, prefix):
        """Record each folder's +schema, then descend into its subfolders."""
        for key, value in node.items():
            if isinstance(value, dict):
                folder = f"{prefix}/{key}" if prefix else key
                if "+schema" in value:
                    out[folder] = value["+schema"]
                walk(value, folder)

    walk(yaml.safe_load((LAKEHOUSE / "dbt" / "dbt_project.yml").read_text())["models"]["lakehouse_gold"], "")
    return out


def dbt_docs():
    """{(schema, table): {"description": str, "columns": {name: {"description": str, "sensitivity": str}}}} from dbt.
    A model's schema is its folder's +schema in dbt_project.yml (the deepest folder that sets one)."""
    docs, schemas = {}, dbt_model_schemas()
    models_dir = LAKEHOUSE / "dbt" / "models"
    for path in models_dir.rglob("*.yml"):
        doc = yaml.safe_load(path.read_text()) or {}
        folder = path.parent.relative_to(models_dir).as_posix()
        owner = max((f for f in schemas if folder == f or folder.startswith(f + "/")), key=len, default=None)
        entries = [(schemas.get(owner, "gold"), m) for m in doc.get("models", [])]
        entries += [(src["schema"], t) for src in doc.get("sources", []) for t in src.get("tables", [])]
        for schema, entry in entries:
            cols = {c["name"]: {"description": " ".join((c.get("description") or "").split()),
                                "sensitivity": (c.get("meta") or {}).get("sensitivity")}
                    for c in entry.get("columns", [])}
            docs[(schema, entry["name"])] = {"description": " ".join((entry.get("description") or "").split()),
                                             "columns": cols}
    return docs


def lake_tables():
    """{(schema, table): [(column, duckdb type)]} for every table in the lake's own schemas, in column order."""
    con = lake.lake_ro_connect()
    rows = con.sql("SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
                   f"WHERE table_catalog = 'lake' AND table_schema IN {SCHEMAS} "
                   "ORDER BY table_schema, table_name, ordinal_position").fetchall()
    tables = {}
    for schema, table, column, dtype in rows:
        tables.setdefault((schema, table), []).append((column, dtype))
    return tables


def column_entity(name, dtype, doc, glossary_links):
    """An OpenMetadata column: mapped type, dbt description, sensitivity tag and glossary-term links."""
    col = {"name": name, "dataTypeDisplay": dtype.lower(), "description": doc.get("description", "")}
    m = re.fullmatch(r"DECIMAL\((\d+),\s*(\d+)\)", dtype)
    if m:
        col.update(dataType="DECIMAL", precision=int(m.group(1)), scale=int(m.group(2)))
    else:
        col["dataType"] = TYPES.get(dtype, "UNKNOWN")
    tags = []
    if doc.get("sensitivity") in SENSITIVITY_TAGS:
        tags.append({"tagFQN": f"{CLASSIFICATION}.{SENSITIVITY_TAGS[doc['sensitivity']]}", "source": "Classification",
                     "labelType": "Manual", "state": "Confirmed"})
    tags += [{"tagFQN": f"{GLOSSARY}.{term}", "source": "Glossary", "labelType": "Manual", "state": "Confirmed"}
             for term in sorted(glossary_links.get(name, []))]
    col["tags"] = tags
    return col


# ---- Steps ---------------------------------------------------------------------------------------------------
def relation_types(s):
    """Add the custom relation types glossary.yml needs (identifies / identifiedBy, isAbout / subjectOf) to
    OpenMetadata's settings."""
    current = s.get("system/settings/glossaryTermRelationSettings")
    types = (current or {}).get("config_value", {}).get("relationTypes", [])
    missing = [t for t in CUSTOM_RELATIONS if t["name"] not in {x["name"] for x in types}]
    if missing:
        s.write("PUT", "system/settings", {"config_type": "glossaryTermRelationSettings",
                                           "config_value": {"relationTypes": types + [
                                               {**t, "category": "associative", "isSymmetric": False,
                                                "isTransitive": False, "isCrossGlossaryAllowed": True,
                                                "isSystemDefined": False} for t in missing]}},
                f"relation types {', '.join(t['name'] for t in missing)}")


def sensitivity(s):
    """The Sensitivity classification and its Restricted and Personal tags."""
    s.ensure("classifications", CLASSIFICATION,
             {"name": CLASSIFICATION, "description": "How carefully a column must be handled. Restricted columns are "
              "masked in Cube; personal columns never leave silver.", "mutuallyExclusive": True},
             lambda e: (e["name"], text(e.get("description"))))
    for name, about in [("Restricted", "Race, religion and security threat group: masked for everyone except "
                        "restricted_reader."),
                       ("Personal", "Names, aliases, birth dates and conduct notes: kept out of gold and Cube.")]:
        s.ensure("tags", f"{CLASSIFICATION}.{name}", {"classification": CLASSIFICATION, "name": name,
                                                      "description": about},
                 lambda e: (e["name"], text(e.get("description"))))


def custom_properties(s):
    """Custom string properties: on glossary terms, the columns and metrics each maps to; on data products, the
    question, Cube views and glossary terms from the product's descriptor."""
    string_type = s.get("metadata/types/name/string")
    for entity, props in (("glossaryTerm", CUSTOM_PROPERTIES), ("dataProduct", PRODUCT_PROPERTIES)):
        typ = s.get(f"metadata/types/name/{entity}", fields="customProperties")
        have = {p["name"] for p in typ.get("customProperties") or []}
        for name, text in props.items():
            if name not in have:
                s.write("PUT", f"metadata/types/{typ['id']}", {"name": name, "description": text,
                                                               "propertyType": {"id": string_type["id"], "type": "type"}},
                        f"custom property {entity}.{name}")


def glossary(s):
    """The Lakehouse glossary, its terms (definition, synonyms, columns, metrics, sensitivity) and typed relations."""
    s.ensure("glossaries", GLOSSARY,
             {"name": GLOSSARY, "displayName": GLOSSARY, "description": "What the lakehouse's mock offender data "
              "means. Loaded from lakehouse/glossary/glossary.yml by om_sync.py."},
             lambda e: (e["name"], text(e.get("description"))))
    terms = glossary_terms()
    for t in terms:
        tags = ([{"tagFQN": f"{CLASSIFICATION}.Restricted", "source": "Classification", "labelType": "Manual",
                  "state": "Confirmed"}] if t.get("sensitivity") == "restricted" else [])
        body = {"glossary": GLOSSARY, "name": t["name"], "displayName": t["name"],
                "description": " ".join(t["definition"].split()), "synonyms": t.get("synonyms", []), "tags": tags,
                "extension": {"lakehouseColumns": ", ".join(t.get("columns", [])),
                              "lakehouseMetrics": ", ".join(t.get("metrics", []))}}
        s.ensure("glossaryTerms", f"{GLOSSARY}.{t['name']}", body, fields="synonyms,extension,tags",
                 compare=lambda e: (text(e["description"]), sorted(e.get("synonyms") or []),
                                    {k: v for k, v in (e.get("extension") or {}).items() if k in CUSTOM_PROPERTIES},
                                    sorted(x["tagFQN"] for x in e.get("tags") or [])))
    if s.check and s.drift:
        return   # terms are missing or stale; relations can't be compared yet
    ids = {t["name"]: (s.get(f"glossaryTerms/name/{GLOSSARY}.{t['name']}", fields="relatedTerms") or {})
           for t in terms}
    for t in terms:
        have = {(r.get("relationType"), r["term"]["name"]) for r in ids[t["name"]].get("relatedTerms") or []}
        for rel in t.get("relations", []):
            if (rel["type"], rel["term"]) not in have:
                s.write("POST", f"glossaryTerms/{ids[t['name']]['id']}/relations",
                        {"relationType": rel["type"], "term": {"id": ids[rel["term"]]["id"], "type": "glossaryTerm"}},
                        f"relation {t['name']} {rel['type']} {rel['term']}")


def tables(s):
    """The lakehouse service, the lake database, its schemas, and every table with its columns."""
    s.ensure("services/databaseServices", SERVICE,
             {"name": SERVICE, "serviceType": "CustomDatabase", "description": "The home-lab DuckLake lake, pushed "
              "by om_sync.py (OpenMetadata has no DuckLake connector).",
              "connection": {"config": {"type": "CustomDatabase"}}},
             lambda e: (e["name"], text(e.get("description"))))
    s.ensure("databases", f"{SERVICE}.{DATABASE}",
             {"name": DATABASE, "service": SERVICE, "description": "DuckLake catalog on catalog-pg, Parquet under "
              "/lake/data."}, lambda e: (e["name"], text(e.get("description"))))
    for schema in SCHEMAS:
        s.ensure("databaseSchemas", f"{SERVICE}.{DATABASE}.{schema}",
                 {"name": schema, "database": f"{SERVICE}.{DATABASE}"}, lambda e: e["name"])
    docs, links = dbt_docs(), {}
    for t in glossary_terms():
        for col in t.get("columns", []):
            links.setdefault(col, []).append(t["name"])
    for (schema, table), columns in sorted(lake_tables().items()):
        doc = docs.get((schema, table), {})
        description = doc.get("description") or lake.TABLE_COMMENTS.get(f"{schema}.{table}", "")
        cols = [column_entity(n, d, doc.get("columns", {}).get(n, {}), links) for n, d in columns]
        body = {"name": table, "databaseSchema": f"{SERVICE}.{DATABASE}.{schema}", "tableType": "Regular",
                "description": description, "columns": cols}
        wanted = {c["name"]: {x["tagFQN"] for x in c["tags"]} for c in cols}
        # Tags the repo wants are a floor, not a ceiling: a steward's tag added in OpenMetadata (PUT merges tags,
        # so it survives) and the tags a glossary term passes on (labelType Derived) aren't differences.
        s.ensure("tables", f"{SERVICE}.{DATABASE}.{schema}.{table}", body, fields="columns,tags",
                 compare=lambda e, wanted=wanted: (text(e.get("description")),
                                                   [(c["name"], c["dataType"], text(c.get("description")),
                                                     sorted({x["tagFQN"] for x in c.get("tags") or []}
                                                            & wanted.get(c["name"], set()))) for c in e["columns"]]))


def product_descriptors():
    """The data products' descriptors (lakehouse/products/*.yml) and their domains (products/domains/*.yml)."""
    folder = LAKEHOUSE / "products"
    return ([yaml.safe_load(f.read_text()) for f in sorted(folder.glob("*.yml"))],
            [yaml.safe_load(f.read_text()) for f in sorted((folder / "domains").glob("*.yml"))])


def dbt_contracts():
    """{model name: [(column, DATA_TYPE)]} for every dbt model whose contract is enforced, columns in order."""
    out = {}
    for path in (LAKEHOUSE / "dbt" / "models").rglob("*.yml"):
        for m in (yaml.safe_load(path.read_text()) or {}).get("models", []):
            if ((m.get("config") or {}).get("contract") or {}).get("enforced"):
                out[m["name"]] = [(c["name"], c["data_type"].upper()) for c in m["columns"]]
    return out


def products(s):
    """Each domain, then each data product (its delivered tables as assets, its question, views and terms as custom
    properties), then a data contract on every delivered table dbt enforces a contract for: dbt's columns and the
    descriptor's SLA. A table in two products gets one contract."""
    descriptors, domains = product_descriptors()
    for d in domains:
        s.ensure("domains", d["name"], {"name": d["name"], "domainType": d["type"],
                                        "description": " ".join(d["description"].split())},
                 lambda e: (e["name"], e["domainType"], text(e.get("description"))))
    contracts, tables, done = dbt_contracts(), {}, set()
    for p in descriptors:
        refs, domain = [], s.get(f"domains/name/{p['domain']}")
        for table in p["tables"]:
            entity = s.get(f"tables/name/{SERVICE}.{DATABASE}.{table}", fields="domains")
            tables[table] = entity
            refs.append({"id": entity["id"], "type": "table"})
            # A data product's assets must sit in its domain; adding keeps any other domain the table serves.
            have = [{"id": d["id"], "type": "domain"} for d in entity.get("domains") or []]
            if domain and domain["id"] not in {d["id"] for d in have}:
                s.patch(f"tables/{entity['id']}", [{"op": "add", "path": "/domains",
                                                    "value": have + [{"id": domain["id"], "type": "domain"}]}],
                        f"table {table} into domain {p['domain']}")
        body = {"name": p["name"], "displayName": p["title"], "description": " ".join(p["description"].split()),
                "domains": [p["domain"]],
                "extension": {"lakehouseQuestion": p["question"], "lakehouseCubeViews": ", ".join(p["cube_views"]),
                              "lakehouseTerms": ", ".join(p["glossary_terms"])}}
        s.ensure("dataProducts", p["name"], body, fields="domains,extension",
                 compare=lambda e: (e.get("displayName"), text(e.get("description")),
                                    sorted(d["name"] if isinstance(d, dict) else d for d in e.get("domains") or []),
                                    {k: (e.get("extension") or {}).get(k) for k in PRODUCT_PROPERTIES}))
        # Assets have their own endpoints (a create body's assets are ignored): add what's missing, drop extras.
        found = s.get(f"dataProducts/name/{p['name']}/assets", limit=1000)
        have = {a["id"] for a in (found or {}).get("data", [])}
        want = {r["id"] for r in refs}
        if want - have:
            s.write("PUT", f"dataProducts/{p['name']}/assets/add",
                    {"assets": [r for r in refs if r["id"] in want - have]}, f"assets added to {p['name']}")
        if have - want:
            s.write("PUT", f"dataProducts/{p['name']}/assets/remove",
                    {"assets": [{"id": i, "type": "table"} for i in have - want]}, f"assets removed from {p['name']}")
    for p in descriptors:
        r = p["refresh"]
        for table in p["tables"]:
            model = table.split(".")[1]
            if model not in contracts or table in done:   # a table in two products gets one contract
                continue
            done.add(table)
            name = f"{model}-contract"
            body = {"name": name, "description": f"The shape and freshness {table} promises: its columns as dbt "
                    "enforces them, refreshed daily by daily_lake.",
                    "entity": {"id": tables[table]["id"], "type": "table"},
                    "schema": [{"name": c, "dataType": t} for c, t in contracts[model]],
                    "sla": {"refreshFrequency": {"interval": r["every"], "unit": r["unit"]},
                            "maxLatency": {"value": p["max_age_hours"], "unit": "hour"},
                            "availabilityTime": r["at"], "timezone": OM_TIMEZONES[r["timezone"]]}}
            s.ensure("dataContracts", f"{SERVICE}.{DATABASE}.{table}.dataContract_{name}", body, fields="sla",
                     compare=lambda e: ([(c["name"], c["dataType"]) for c in e.get("schema") or []], e.get("sla")))


def main(argv):
    """Sync (or --check) everything; print what differed and a summary line."""
    s = Sync(check="--check" in argv)
    for step in (relation_types, sensitivity, custom_properties, glossary, tables, products):
        step(s)
    for what in s.drift:
        print(f"  {'differs' if s.check else 'updated'}: {what}")
    verb = "differences" if s.check else "updates"
    print(f"OpenMetadata sync: {len(s.drift)} {verb}" if s.drift else "OpenMetadata sync: in step with the repo and lake")
    return 1 if (s.check and s.drift) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
