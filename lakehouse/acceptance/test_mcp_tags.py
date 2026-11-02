"""Acceptance test for the MCP server's find_by_tags: find lake tables and columns by the tags they carry.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_mcp_tags.py

find_by_tags reads every lakehouse table and its tags from OpenMetadata (as mcp-reader): the tagger's (Governance,
PIIType, Source, Layer, Refresh), the hand-set Sensitivity tags, OpenMetadata's PII.Sensitive and the glossary terms
(Lakehouse.*). One tag, several (any of them), or all of them, on tables, columns or both. Answers are checked against
the lake's own record of the tagger's tags (ops.table_tags, ops.column_tags), never against the tool's other answers.
"""
import time

from lakehouse import lake
from test_mcp import call

PII_FAMILY = "PIIType."


def find(**arguments) -> dict:
    """find_by_tags' result, after checking the call succeeded."""
    is_error, result, text = call("find_by_tags", **arguments)
    assert not is_error, text
    return result


def tables(result) -> set:
    """The schema.table names in a result."""
    return {t["table"] for t in result["tables"]}


def columns(result) -> set:
    """(schema.table, column) for every column in a result."""
    return {(t["table"], c["column"]) for t in result["tables"] for c in t["columns"]}


def lake_set(sql: str) -> set:
    """A query's rows on the lake, read-only, as a set of tuples (or of values, for one column)."""
    con = lake.lake_ro_connect()
    try:
        rows = con.execute(sql).fetchall()
    finally:
        con.close()
    return {r[0] if len(r) == 1 else tuple(r) for r in rows}


def tagged_tables(tag: str) -> set:
    """Tables the tagger gave a table-level tag."""
    return lake_set(f"SELECT table_schema || '.' || table_name FROM lake.ops.table_tags WHERE tag = '{tag}'")


def tagged_columns(tag: str) -> set:
    """(schema.table, column) pairs the tagger gave a column tag."""
    return lake_set(f"SELECT table_schema || '.' || table_name, column_name FROM lake.ops.column_tags WHERE tag = '{tag}'")


def test_one_table_tag_finds_every_table_that_carries_it():
    """Governance.CJIS: exactly the 54 tables the tagger marked CJIS, each saying it matched on the table itself."""
    got = find(tags=["Governance.CJIS"])
    assert tables(got) == tagged_tables("Governance.CJIS") and got["table_count"] == 54
    assert all(t["table_tags"] == ["Governance.CJIS"] and t["matched"] == ["Governance.CJIS"] for t in got["tables"])


def test_one_column_tag_finds_its_columns():
    """PIIType.BirthDate: the four birth-date columns, under their tables."""
    got = find(tags=["PIIType.BirthDate"])
    assert columns(got) == tagged_columns("PIIType.BirthDate") and got["column_count"] == 4


def test_several_tags_find_any_of_them():
    """Birth dates or free text: every table with either, each saying which of the two it has."""
    got = find(tags=["PIIType.BirthDate", "PIIType.FreeText"])
    want = {t for t, _ in tagged_columns("PIIType.BirthDate") | tagged_columns("PIIType.FreeText")}
    assert tables(got) == want
    free = {t for t, _ in tagged_columns("PIIType.FreeText")}
    assert all(("PIIType.FreeText" in t["matched"]) == (t["table"] in free) for t in got["tables"])


def test_all_finds_only_tables_carrying_every_tag():
    """CJIS and a birth date and free text: only the tables with all three, on the table or any of its columns."""
    asked = ["Governance.CJIS", "PIIType.BirthDate", "PIIType.FreeText"]
    got = find(tags=asked, match="all")
    want = (tagged_tables("Governance.CJIS") & {t for t, _ in tagged_columns("PIIType.BirthDate")}
            & {t for t, _ in tagged_columns("PIIType.FreeText")})
    assert tables(got) == want == {"bronze.hero_offenders_raw", "silver.hero_offenders"}
    assert all(sorted(t["matched"]) == sorted(asked) for t in got["tables"])


def test_all_on_columns_means_one_column_carries_them_all():
    """level=column with match=all: a column must carry every tag itself. Every birth date is also hand-tagged
    Personal; no column is both a birth date and a name, though some tables have both."""
    both = find(tags=["PIIType.BirthDate", "Sensitivity.Personal"], match="all", level="column")
    assert columns(both) == tagged_columns("PIIType.BirthDate")
    assert find(tags=["PIIType.BirthDate", "PIIType.PersonName"], match="all", level="column")["tables"] == []
    assert find(tags=["PIIType.BirthDate", "PIIType.PersonName"], match="all")["table_count"] == 4


def test_level_table_or_column_only_looks_there():
    """A column tag isn't on any table, and a table tag isn't on any column."""
    assert find(tags=["PIIType.BirthDate"], level="table")["tables"] == []
    assert find(tags=["Governance.CJIS"], level="column")["tables"] == []
    assert tables(find(tags=["Governance.CJIS"], level="table")) == tagged_tables("Governance.CJIS")


def test_names_are_forgiving_and_families_count_as_one():
    """Case, spaces and hyphens don't matter; a bare name works when one family has it; PIIType (or PIIType.*) is
    any PIIType tag, which is exactly the tables the tagger rolled up as Governance.PII. A bare PII is the tag
    Governance.PII; PII.* is OpenMetadata's own PII family."""
    want = tagged_tables("Governance.CJIS")
    for spelling in ("governance.cjis", "CJIS", " Governance . CJIS "):
        assert tables(find(tags=[spelling])) == want, spelling
    assert find(tags=["lakehouse.person-key"])["searched"] == ["Lakehouse.Person Key"]
    pii = tagged_tables("Governance.PII")
    assert tables(find(tags=["PIIType"])) == tables(find(tags=["piitype.*"])) == pii
    assert find(tags=["PII"])["searched"] == ["Governance.PII"]
    assert find(tags=["PII.*"])["searched"] == ["PII.Sensitive"]


def test_no_tags_lists_every_tag_in_use():
    """With no tags: every tag on a lakehouse table or column, its family and description, and how many tables and
    columns carry it, so an agent can choose."""
    got = find()
    assert got["tables"] == [] and len(got["tags"]) >= 40
    by = {t["tag"]: t for t in got["tags"]}
    assert by["Governance.CJIS"]["tables"] == 54 and by["Governance.CJIS"]["family"] == "Governance"
    assert by["PIIType.BirthDate"]["columns"] == 4 and by["PIIType.BirthDate"]["description"]
    assert by["Lakehouse.SID Number"]["columns"] > 0 and by["Lakehouse.SID Number"]["description"]


def test_a_crazy_number_of_tags_still_answers():
    """Every tag in use at once: any of them finds every tagged table, all of them finds none, quickly."""
    every = [t["tag"] for t in find()["tags"]]
    started = time.time()
    anything = find(tags=every)
    assert tables(anything) == lake_set("SELECT DISTINCT table_schema || '.' || table_name FROM lake.ops.table_tags")
    assert find(tags=every, match="all")["tables"] == []
    assert time.time() - started < 10


def test_unknown_tags_and_options_are_errors_that_help():
    """An unknown tag names the families in use and close matches; a wrong match or level lists the right ones."""
    is_error, _, text = call("find_by_tags", tags=["Governance.CJSI"])
    assert is_error and "Governance.CJIS" in text and "PIIType" in text
    is_error, _, text = call("find_by_tags", tags=["CJIS"], match="most")
    assert is_error and "any" in text and "all" in text
    is_error, _, text = call("find_by_tags", tags=["CJIS"], level="row")
    assert is_error and "table" in text and "column" in text and "both" in text
