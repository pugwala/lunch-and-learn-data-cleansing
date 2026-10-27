"""Acceptance test for modeling the person (data products, chunk 1).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_person.py

A person can have several offender records (86,879 people hold the 100,000 records), and some hold two at once, so
counting records is not counting people. The glossary gains Person, identified by SID Number, with every Offender
Record about one Person; "inmate" and "offender" are roles a person has during a record, so they are synonyms, not
entities. gold.dim_person has one row per known SID and only facts derived from records: no names, birth dates or
anything else tagged Personal, which stay in silver.
"""
import base64
import os
from pathlib import Path

import pytest
import requests
import yaml

from lakehouse import lake
from test_glossary_rdf import ask
from test_mcp import call

LAKEHOUSE = Path(__file__).resolve().parents[1]
GLOSSARY = {t["name"]: t for t in yaml.safe_load((LAKEHOUSE / "glossary" / "glossary.yml").read_text())["terms"]}
OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata") + "/api/v1"
DIM_PERSON_COLUMNS = {"sid_number", "record_count", "first_sentence_date", "latest_sentence_date", "is_repeat",
                      "has_concurrent_records"}


@pytest.fixture(scope="module")
def con():
    """A read-only connection to the lake, shared by this module's tests."""
    c = lake.lake_ro_connect()
    yield c
    c.close()


def scalar(con, sql):
    """First column of the first row of a query."""
    return con.execute(sql).fetchone()[0]


# --- the glossary ----------------------------------------------------------------------------------------------

def test_glossary_defines_person_identified_by_sid():
    """Person is a term; SID Number identifies it; every Offender Record is about one; inmate is a synonym."""
    person = GLOSSARY["Person"]
    assert "inmate" in person["synonyms"] and "offender" in person["synonyms"]
    assert {"type": "identifies", "term": "Person"} in GLOSSARY["SID Number"]["relations"]
    assert {"type": "isAbout", "term": "Person"} in GLOSSARY["Offender Record"]["relations"]


def test_openmetadata_holds_person_and_its_relations():
    """The relations reach OpenMetadata typed, with the custom isAbout type registered."""
    r = requests.post(f"{OM}/users/login", timeout=30, json={
        "email": "admin@open-metadata.org",
        "password": base64.b64encode(os.environ["OM_ADMIN_PASSWORD"].encode()).decode()})
    headers = {"Authorization": f"Bearer {r.json()['accessToken']}"}
    record = requests.get(f"{OM}/glossaryTerms/name/Lakehouse.Offender Record", params={"fields": "relatedTerms"},
                          headers=headers, timeout=30).json()
    assert ("isAbout", "Person") in {(x["relationType"], x["term"]["name"]) for x in record["relatedTerms"]}


def test_the_graph_links_records_and_sids_to_person():
    """In Fuseki: a record is about a Person, and a SID identifies one."""
    assert ask("ASK { term:offender-record lh:isAbout term:person }")
    assert ask("ASK { term:sid-number lh:identifies term:person }")


def test_define_term_finds_person_by_inmate():
    """An agent asking what an inmate is gets the Person term."""
    is_error, result, _ = call("define_term", term="Inmate")
    assert not is_error and result["term"] == "Person"


# --- gold.dim_person -------------------------------------------------------------------------------------------

def test_one_row_per_known_person(con):
    """dim_person has a row for every distinct SID in the fact table, and only those."""
    assert scalar(con, "SELECT count(*) FROM gold.dim_person") == \
        scalar(con, "SELECT count(DISTINCT sid_number) FROM gold.fct_offender_record") == 86_879


def test_record_counts_add_up(con):
    """Every record with a SID is counted once; records with no SID (2,997) belong to no known person."""
    assert scalar(con, "SELECT sum(record_count) FROM gold.dim_person") == \
        scalar(con, "SELECT count(*) FROM gold.fct_offender_record WHERE sid_number IS NOT NULL") == 97_003


def test_repeat_and_concurrent_people(con):
    """8,499 people have more than one record; has_concurrent_records marks those with two at once."""
    assert scalar(con, "SELECT count(*) FROM gold.dim_person WHERE is_repeat") == 8_499
    concurrent = scalar(con, """SELECT count(DISTINCT a.sid_number) FROM gold.fct_offender_record a
        JOIN gold.fct_offender_record b ON a.sid_number = b.sid_number AND a.offender_number <> b.offender_number
         AND a.sentence_date < b.projected_release_date AND b.sentence_date < a.projected_release_date""")
    assert scalar(con, "SELECT count(*) FROM gold.dim_person WHERE has_concurrent_records") == concurrent > 0


def test_dim_person_holds_no_personal_data(con):
    """Only derived facts: exactly the agreed columns, none of them tagged Personal or Restricted anywhere."""
    cols = {r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_catalog = "
                                      "'lake' AND table_schema = 'gold' AND table_name = 'dim_person'").fetchall()}
    assert cols == DIM_PERSON_COLUMNS
    tagged = set()
    for f in (LAKEHOUSE / "dbt" / "models").rglob("*.yml"):
        doc = yaml.safe_load(f.read_text()) or {}
        tables = [t for s in doc.get("sources", []) for t in s.get("tables", [])] + doc.get("models", [])
        tagged |= {c["name"] for t in tables for c in t.get("columns", []) if (c.get("meta") or {}).get("sensitivity")}
    assert not cols & tagged, cols & tagged
