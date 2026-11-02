"""Acceptance test for automatic tagging (lakehouse/tagging, Priority 4b): the daily run tags the lake as data lands,
and OpenMetadata shows the tags where people search.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_tags.py

The daily run's governance/tags step writes ops.table_tags and ops.column_tags (tagging/tags.py), and om_sync.py
publishes them: Governance (CJIS, PII), Source, Layer and Refresh on tables, PIIType (plus OpenMetadata's own
PII.Sensitive) on columns. Hand-set Sensitivity tags stay as they were.
"""
import sys
from pathlib import Path

from lakehouse import lake
from lakehouse.tagging import tags
from test_om_sync import OM, get, om  # noqa: F401  (om is a fixture)

FQN = "lakehouse.lake"


def lake_rows(sql: str) -> list:
    """All rows of a query on the lake, read-only."""
    con = lake.lake_ro_connect()
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def table(om, name: str) -> dict:
    """An OpenMetadata table (schema.table) with its columns and tags."""
    return get(om, f"tables/name/{FQN}.{name}", fields="columns,tags")


def tag_set(entity: dict) -> set:
    """The tag names on an OpenMetadata table or column."""
    return {t["tagFQN"] for t in entity.get("tags") or []}


def test_every_column_people_tagged_by_hand_is_tagged_in_the_lake():
    """ops.column_tags gives a PIIType tag to every one of the 42 columns dbt's docs mark restricted or personal,
    across the course's and the world's tables."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "openmetadata"))
    import om_sync
    hand = {(s, t, c) for (s, t), d in om_sync.dbt_docs().items() for c, cd in d.get("columns", {}).items()
            if cd.get("sensitivity")}
    tagged = set(lake_rows("SELECT DISTINCT table_schema, table_name, column_name FROM lake.ops.column_tags "
                           "WHERE tag LIKE 'PIIType.%'"))
    assert len(hand) == 42
    assert sorted(hand - tagged) == []


def test_openmetadata_shows_a_landed_tables_tags(om):
    """bronze.hero_offenders_raw: CJIS, personal data, where it came from, its layer and refresh; its date_of_birth
    keeps the hand tag and gains PIIType.BirthDate and PII.Sensitive; gender, which nobody tagged, is found too."""
    t = table(om, "bronze.hero_offenders_raw")
    assert {"Governance.CJIS", "Governance.PII", "Source.Course1Cleansing", "Layer.Bronze", "Refresh.Daily"} <= tag_set(t)
    cols = {c["name"]: tag_set(c) for c in t["columns"]}
    assert {"PIIType.BirthDate", "PII.Sensitive", "Sensitivity.Personal"} <= cols["date_of_birth"]
    assert {"PIIType.Demographic", "PII.Sensitive"} <= cols["gender"]
    assert not any(x.startswith(("PIIType.", "PII.")) for x in cols["custody_level"])


def test_tags_follow_lineage_into_gold(om):
    """Tables built from course 1's records or the world's deliveries inherit CJIS and their source; a gold table
    built only from reference data doesn't."""
    assert {"Governance.CJIS", "Source.Course1Cleansing", "Layer.Gold"} <= tag_set(table(om, "gold.dim_person"))
    assert {"Governance.CJIS", "Source.WorldGenerator", "Layer.Gold"} <= tag_set(table(om, "world_gold.dim_person"))
    offense = tag_set(table(om, "gold.dim_offense"))
    assert "Source.Course1Reference" in offense and "Governance.CJIS" not in offense


def search(om, tag: str) -> set:
    """Fully qualified names of the tables OpenMetadata's search finds with a tag."""
    import json
    flt = json.dumps({"query": {"bool": {"must": [{"term": {"tags.tagFQN": tag}}]}}})
    hits = get(om, "search/query", q="*", index="table_search_index", query_filter=flt, size=500)["hits"]["hits"]
    return {h["_source"]["fullyQualifiedName"] for h in hits}


def test_people_can_find_tables_by_tag(om):
    """OpenMetadata's search, filtered on Governance.CJIS, finds exactly the tables the tagger marked CJIS."""
    want = {f"{FQN}.{s}.{t}" for s, t in lake_rows("SELECT table_schema, table_name FROM lake.ops.table_tags "
                                                   "WHERE tag = 'Governance.CJIS'")}
    assert len(want) > 20
    assert search(om, "Governance.CJIS") == want


def test_the_tag_vocabulary_is_in_openmetadata(om):
    """Every family and tag in rules.yml and sources.yml is a classification and tag in OpenMetadata, described."""
    for family, f in tags.families().items():
        found = {t["name"]: t.get("description") for t in get(om, "tags", parent=family, limit=100)["data"]}
        assert set(f["tags"]) <= set(found), family
        assert all(found[name] for name in f["tags"]), family
