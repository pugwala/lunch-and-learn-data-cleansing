"""Governance and metadata tags attached automatically when a source lands (lakehouse/tagging, Priority 4b).

    pytest lakehouse/tests/test_tagging.py      # inside the jupyter container (or the CI sandbox); no live stack

Until now a person tagged each sensitive column by hand (dbt meta.sensitivity). The tagger does it as data lands:
each source declares what its tables are (tagging/sources.yml: source system, governance such as CJIS, refresh),
column rules find personal data by name or by its values (tagging/rules.yml), tags follow dbt's lineage downstream,
and every table gets its layer. These tests check the rules, the sources, how a table's tags combine, and that on
course 1's real handoff the tagger finds every column people tagged by hand, and what they missed.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse.handoff import receive  # noqa: E402
from lakehouse.tagging import tags  # noqa: E402


def names(pairs) -> set:
    """Just the tag names from [(tag, why)]."""
    return {t for t, _ in pairs}


# --- column rules ----------------------------------------------------------------------------------------------

@pytest.mark.parametrize("column, want", [
    ("last_name", "PIIType.PersonName"), ("first_name_std", "PIIType.PersonName"), ("alias", "PIIType.PersonName"),
    ("date_of_birth", "PIIType.BirthDate"), ("birth_date", "PIIType.BirthDate"),
    ("sid_number", "PIIType.PersonIdentifier"), ("offender_number", "PIIType.PersonIdentifier"),
    ("person_key", "PIIType.Pseudonym"), ("person_id", "PIIType.Pseudonym"),
    ("race_omb", "PIIType.Demographic"), ("religion_family", "PIIType.Demographic"), ("gender", "PIIType.Demographic"),
    ("stg_status", "PIIType.Affiliation"), ("stg_affiliation", "PIIType.Affiliation"),
    ("value_as_written", "PIIType.AsWritten"),
])
def test_a_column_name_rule_tags_the_column(column, want):
    """Each name rule tags the columns it's for, with one tag."""
    assert names(tags.column_tags(column, "VARCHAR", [])) == {want}


@pytest.mark.parametrize("column", ["name", "unit_name", "city_name", "codename", "known_aliases", "team_id",
                                    "offense_code", "dob_flag", "first_sentence_date"])
def test_near_misses_are_left_alone(column):
    """Names of places, units, missions and groups, ids of things, and flags about a value aren't personal data."""
    assert tags.column_tags(column, "VARCHAR", []) == []


def sentences(n, distinct=True):
    """n sentences of eight words or more: all different, or three repeated."""
    return [f"Per Sgt. Brooks, {i if distinct else i % 3} awaiting classification review, no powers used."
            for i in range(n)]


def test_free_text_is_found_by_its_values():
    """Text people wrote (mostly six words or more, and varied) is tagged PIIType.FreeText whatever the column is called;
    templated text, short codes, a handful of rows or a column that isn't text are not."""
    assert names(tags.column_tags("remarks", "VARCHAR", sentences(200))) == {"PIIType.FreeText"}
    assert tags.column_tags("reason", "VARCHAR", sentences(200, distinct=False)) == []
    assert tags.column_tags("code", "VARCHAR", ["13A", "13B", "90Z"] * 100) == []
    assert tags.column_tags("remarks", "VARCHAR", sentences(50)) == []
    assert tags.column_tags("remarks", "BIGINT", sentences(200)) == []


def test_every_tag_says_why():
    """A tag comes with the rule that set it, in words a steward can check."""
    (tag, why), = tags.column_tags("date_of_birth", "DATE", [])
    assert tag == "PIIType.BirthDate" and why.startswith("rule birth_date:")


# --- sources, lineage and how a table's tags combine -----------------------------------------------------------

def test_each_source_names_the_tables_it_lands():
    """Course 1's handoff lands bronze and silver, its reference data ref.*, the world generator world_bronze.*;
    gold is built, not landed, so it has no source of its own. Every pinned handoff table has a source."""
    assert tags.source_of("bronze.hero_offenders_raw")["name"] == "course1-handoff"
    assert tags.source_of("silver.trust_test")["name"] == "course1-handoff"
    assert tags.source_of("ref.race_crosswalk")["name"] == "course1-reference"
    assert tags.source_of("world_bronze.person")["name"] == "world-generator"
    assert tags.source_of("gold.dim_person") is None
    assert all(tags.source_of(t) for t in receive.read_lock()["tables"])


def test_reference_data_and_what_is_built_only_from_it_describe_no_one():
    """Column rules only look at tables whose rows describe people: a source says whether it does (course 1's
    reference data doesn't: race_code there is a code list, not someone's race), and a built table does if anything
    it's built from does. A table nobody registered is looked at anyway: safer to look than to assume."""
    assert tags.about_people("ref.race_crosswalk", set()) is False
    assert tags.about_people("gold.dim_stg_group", {"ref.stg_groups"}) is False
    assert tags.about_people("gold.dim_person", {"gold.fct_offender_record", "silver.hero_offenders"}) is True
    assert tags.about_people("bronze.hero_offenders_raw", set()) is True
    assert tags.about_people("ops.load_runs", set()) is True


MANIFEST = {
    "nodes": {"model.p.fct_offender_record": {"schema": "gold", "alias": "fct_offender_record"},
              "model.p.dim_person": {"schema": "gold", "alias": "dim_person"},
              "model.p.dim_offense": {"schema": "gold", "alias": "dim_offense"}},
    "sources": {"source.p.silver.hero_offenders": {"schema": "silver", "identifier": "hero_offenders"},
                "source.p.ref.nibrs_offenses": {"schema": "ref", "identifier": "nibrs_offenses"}},
    "parent_map": {"model.p.fct_offender_record": ["source.p.silver.hero_offenders"],
                   "model.p.dim_person": ["model.p.fct_offender_record"],
                   "model.p.dim_offense": ["source.p.ref.nibrs_offenses"],
                   "source.p.silver.hero_offenders": [], "source.p.ref.nibrs_offenses": []},
}


def test_lineage_runs_all_the_way_up():
    """A model's upstream is every table it's built from, directly or not."""
    up = tags.upstream(MANIFEST)
    assert up["gold.dim_person"] == {"gold.fct_offender_record", "silver.hero_offenders"}
    assert up["gold.dim_offense"] == {"ref.nibrs_offenses"}


def test_a_landed_table_gets_its_sources_tags_its_layer_and_a_pii_rollup():
    """bronze.hero_offenders_raw: course 1's handoff says CJIS, where it came from and how often it lands; it's
    bronze; and a personal column makes the whole table Governance.PII."""
    got = tags.table_tags("bronze.hero_offenders_raw", {"last_name": [("PIIType.PersonName", "rule")]}, set())
    assert names(got) == {"Governance.CJIS", "Source.Course1Cleansing", "Refresh.Daily", "Layer.Bronze",
                          "Governance.PII"}


def test_a_built_table_inherits_governance_and_source_through_lineage():
    """gold.dim_person is built from course 1's records, so it is CJIS and from course 1 too (with the table it got
    that from); it has its own layer, no refresh of its own, and no personal columns, so no PII rollup. A gold
    table built only from reference data inherits no CJIS."""
    got = dict(tags.table_tags("gold.dim_person", {}, {"gold.fct_offender_record", "silver.hero_offenders"}))
    assert set(got) == {"Governance.CJIS", "Source.Course1Cleansing", "Layer.Gold"}
    assert "silver.hero_offenders" in got["Governance.CJIS"]
    assert names(tags.table_tags("gold.dim_offense", {}, {"ref.nibrs_offenses"})) == {"Source.Course1Reference",
                                                                                     "Layer.Gold"}


# --- on course 1's real handoff --------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def handoff_tags():
    """The tagger run on the pinned handoff (read as DuckDB views): {table: {column: {tags}}}."""
    if not receive.pinned_dir().exists():
        pytest.skip("no published handoff here")
    con = receive.connect(receive.pinned_dir())
    tables = {t: [(r[0], r[1]) for r in con.execute(f"DESCRIBE {t}").fetchall()] for t in receive.read_lock()["tables"]}
    _, columns = tags.classify(con, tables)
    out = {}
    for schema, table, column, tag, _how, _why in columns:
        out.setdefault(f"{schema}.{table}", {}).setdefault(column, set()).add(tag)
    return out


def test_the_tagger_finds_every_column_people_tagged_by_hand(handoff_tags):
    """Every column dbt's docs mark restricted or personal, in the tables course 1 hands over, gets a PII tag."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "openmetadata"))
    import om_sync
    hand = {(f"{s}.{t}", c) for (s, t), d in om_sync.dbt_docs().items() for c, cd in d.get("columns", {}).items()
            if cd.get("sensitivity") and f"{s}.{t}" in receive.read_lock()["tables"]}
    assert len(hand) == 25   # of the 42 hand-tagged columns; the rest are in the world's tables
    missed = sorted(f"{t}.{c}" for t, c in hand
                    if not any(x.startswith("PIIType.") for x in handoff_tags.get(t, {}).get(c, set())))
    assert missed == []


def test_the_tagger_finds_what_people_missed(handoff_tags):
    """Exactly what the tagger sees in the raw file and the flagged values: the hand tags' columns, plus the two
    identifiers, gender, and the raw values the exceptions table copies (birth dates and race codes among them)."""
    raw = handoff_tags["bronze.hero_offenders_raw"]
    assert raw == {"offender_number": {"PIIType.PersonIdentifier"}, "sid_number": {"PIIType.PersonIdentifier"},
                   "last_name": {"PIIType.PersonName"}, "first_name": {"PIIType.PersonName"}, "alias": {"PIIType.PersonName"},
                   "date_of_birth": {"PIIType.BirthDate"}, "gender": {"PIIType.Demographic"}, "race": {"PIIType.Demographic"},
                   "religion": {"PIIType.Demographic"}, "stg_affiliation": {"PIIType.Affiliation"},
                   "conduct_notes": {"PIIType.FreeText"}}
    assert handoff_tags["silver.hero_offenders_exceptions"] == {
        "offender_number": {"PIIType.PersonIdentifier"}, "sid_number": {"PIIType.PersonIdentifier"},
        "value_as_written": {"PIIType.AsWritten"}}
    assert "ref.race_crosswalk" not in handoff_tags
