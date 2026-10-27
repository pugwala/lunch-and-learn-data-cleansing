"""Acceptance test for the data products' descriptors, catalog entries and contracts (data products, chunk 4).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_products.py

Each product is one file in lakehouse/products/: its domain, owner, question, the tables it delivers (none may hold
a restricted or personal column), the tables its data comes from, its Cube views, the glossary terms it speaks in,
and how fresh it promises to be. om_sync.py publishes it to OpenMetadata as a Domain and a Data Product, with a
Data Contract on every product table that dbt enforces a contract for (columns copied from dbt, the SLA from the
descriptor); om_to_rdf.py publishes it to Fuseki as a DCAT dataset. These tests hold every one of those to the file.
"""
import base64
import os
from pathlib import Path

import pytest
import requests
import yaml

from lakehouse import lake
from test_custody_cube import meta
from test_glossary_rdf import ask, select

LAKEHOUSE = Path(__file__).resolve().parents[1]
PRODUCTS = {p.stem: yaml.safe_load(p.read_text()) for p in sorted((LAKEHOUSE / "products").glob("*.yml"))}
GLOSSARY = {t["name"] for t in yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text())["terms"]}
OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata") + "/api/v1"
REQUIRED = {"name", "title", "domain", "owner", "question", "description", "tables", "sources", "cube_views",
            "glossary_terms", "refresh", "max_age_hours"}


def dbt_models():
    """{model name: model entry} across the dbt project's schema files."""
    out = {}
    for f in (LAKEHOUSE / "dbt" / "models").rglob("*.yml"):
        out.update({m["name"]: m for m in (yaml.safe_load(f.read_text()) or {}).get("models", [])})
    return out


def tagged_columns():
    """Every column name dbt tags with a sensitivity (restricted or personal), anywhere in the project."""
    tagged = set()
    for f in (LAKEHOUSE / "dbt" / "models").rglob("*.yml"):
        doc = yaml.safe_load(f.read_text()) or {}
        tables = [t for s in doc.get("sources", []) for t in s.get("tables", [])] + doc.get("models", [])
        tagged |= {c["name"] for t in tables for c in t.get("columns", []) if (c.get("meta") or {}).get("sensitivity")}
    return tagged


@pytest.fixture(scope="module")
def om():
    """A requests session with an OpenMetadata admin token."""
    r = requests.post(f"{OM}/users/login", timeout=30, json={
        "email": "admin@open-metadata.org",
        "password": base64.b64encode(os.environ["OM_ADMIN_PASSWORD"].encode()).decode()})
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"
    return s


def get(om, path, **params):
    """GET an OpenMetadata path; returns its JSON, failing the test on an error."""
    r = om.get(f"{OM}/{path}", params=params, timeout=30)
    assert r.ok, f"{path}: {r.status_code} {r.text[:200]}"
    return r.json()


# --- the descriptors tell the truth ----------------------------------------------------------------------------

def test_both_products_have_a_full_descriptor():
    """custody-population and custody-offenses exist, each with every field, named after its file."""
    assert set(PRODUCTS) == {"custody-population", "custody-offenses"}
    for stem, p in PRODUCTS.items():
        assert REQUIRED <= set(p), (stem, REQUIRED - set(p))
        assert p["name"] == stem and p["max_age_hours"] > 0


def test_every_table_view_and_term_named_exists():
    """The tables (delivered and source) are in the lake, the views are in Cube, the terms are in the glossary."""
    con = lake.lake_ro_connect()
    tables = {f"{s}.{t}" for s, t in con.execute("SELECT table_schema, table_name FROM information_schema.tables "
                                                 "WHERE table_catalog = 'lake'").fetchall()}
    views = {c["name"] for c in meta()}
    for stem, p in PRODUCTS.items():
        assert set(p["tables"]) | set(p["sources"]) <= tables, (stem, set(p["tables"]) - tables)
        assert set(p["cube_views"]) <= views, (stem, set(p["cube_views"]) - views)
        assert set(p["glossary_terms"]) <= GLOSSARY, (stem, set(p["glossary_terms"]) - GLOSSARY)


def test_delivered_tables_hold_no_sensitive_column():
    """No table a product delivers has a restricted or personal column (sources may: views keep those out)."""
    con = lake.lake_ro_connect()
    tagged = tagged_columns()
    for stem, p in PRODUCTS.items():
        for table in p["tables"]:
            schema, name = table.split(".")
            cols = {r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE "
                                              "table_catalog = 'lake' AND table_schema = ? AND table_name = ?",
                                              [schema, name]).fetchall()}
            assert cols and not cols & tagged, (stem, table, cols & tagged)


# --- OpenMetadata: domain, data products, data contracts -------------------------------------------------------

def test_the_domain_is_consumer_aligned(om):
    """Custody Operations is a Consumer-aligned domain: built for the people who use it."""
    domain = get(om, "domains/name/Custody Operations")
    assert domain["domainType"] == "Consumer-aligned" and domain.get("description")


@pytest.mark.parametrize("stem", ["custody-population", "custody-offenses"])
def test_each_data_product_matches_its_descriptor(om, stem):
    """The data product sits in its domain, its assets are exactly the delivered tables, and its custom
    properties carry the question, views and terms."""
    p = PRODUCTS[stem]
    product = get(om, f"dataProducts/name/{stem}", fields="domains,extension")
    assert product["displayName"] == p["title"]
    assert [d["name"] for d in product["domains"]] == [p["domain"]]
    found = get(om, f"dataProducts/name/{stem}/assets", limit=1000)["data"]   # assets have their own endpoint
    assets = {a["fullyQualifiedName"].removeprefix("lakehouse.lake.") for a in found}
    assert assets == set(p["tables"])
    ext = product.get("extension") or {}
    assert ext.get("lakehouseCubeViews") == ", ".join(p["cube_views"])
    assert ext.get("lakehouseTerms") == ", ".join(p["glossary_terms"])
    assert ext.get("lakehouseQuestion") == p["question"]


def test_contracted_tables_carry_dbts_columns_and_the_sla(om):
    """Every delivered table with a dbt-enforced contract has an OpenMetadata data contract: the same columns and
    types as dbt, and the SLA the descriptors promise (daily, at 05:00 Central, at most 26 hours old)."""
    models = dbt_models()
    contracted = {t for p in PRODUCTS.values() for t in p["tables"]
                  if ((models.get(t.split(".")[1]) or {}).get("config") or {}).get("contract", {}).get("enforced")}
    assert contracted == {"mart_custody.custody_snapshot_monthly", "mart_custody.snapshot_coverage"}
    for table in contracted:
        model = table.split(".")[1]   # a contract's name sits under its table: <table FQN>.dataContract_<name>
        contract = get(om, f"dataContracts/name/lakehouse.lake.{table}.dataContract_{model}-contract", fields="sla")
        assert contract["entity"]["id"] == get(om, f"tables/name/lakehouse.lake.{table}")["id"]
        want = [(c["name"], c["data_type"].upper()) for c in models[table.split(".")[1]]["columns"]]
        assert [(c["name"], c["dataType"]) for c in contract["schema"]] == want
        sla = contract["sla"]
        assert sla["refreshFrequency"] == {"interval": 1, "unit": "day"}
        assert sla["maxLatency"] == {"value": 26, "unit": "hour"}
        assert sla["availabilityTime"] == "05:00" and sla["timezone"] == "GMT-06:00 (America/Chicago)"   # OM's label


# --- Fuseki: the products as DCAT datasets ---------------------------------------------------------------------

PRODUCT_PREFIXES = """PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX prod: <https://lakehouse.invalid/product/>
"""


def test_products_are_dcat_datasets_in_their_domain():
    """Each product is a dcat:Dataset with its title, domain, tables and Cube views as dcat:DataService."""
    rows = select(PRODUCT_PREFIXES + "SELECT ?title ?domain WHERE { ?p a dcat:Dataset ; dct:title ?title ; "
                  "lh:domain ?domain } ORDER BY ?title")
    assert rows == [{"title": "Custody Offenses", "domain": "Custody Operations"},
                    {"title": "Custody Population", "domain": "Custody Operations"}]
    assert ask(PRODUCT_PREFIXES + 'ASK { prod:custody-population lh:usesTable "mart_custody.custody_snapshot_monthly" ; '
               'dcat:accessService ?v . ?v a dcat:DataService ; dct:title "custody_population" }')


def test_a_change_to_person_touches_both_products():
    """Impact analysis in one query: both products speak in the Person term."""
    rows = select(PRODUCT_PREFIXES + "SELECT ?title WHERE { ?p a dcat:Dataset ; dct:title ?title ; "
                  "lh:usesTerm term:person } ORDER BY ?title")
    assert [r["title"] for r in rows] == ["Custody Offenses", "Custody Population"]


def test_no_product_delivers_a_restricted_column():
    """A governance check in one query: no product's tables hold a column tagged Restricted."""
    assert not ask(PRODUCT_PREFIXES + "ASK { ?p a dcat:Dataset ; lh:usesTable ?t . "
                   "?c lh:table ?t ; lh:sensitivity lh:Restricted }")

