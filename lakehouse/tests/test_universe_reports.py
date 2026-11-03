"""Acceptance tests for the expanded world's incident reports, the free text (Priority 5).

    pytest lakehouse/tests/test_universe_reports.py     # inside the jupyter container; needs the lake (silver, ref)

One scale-S build (no mess), then the incident reports' mess and mention key alone, checked:
  - the scale's row count, one report at most per fight, every link real, and a chunk rebuilt alone matches;
  - who wrote it: a field report by a hero who was there, a witness statement by a civilian who was there, a desk
    report by nobody; filed after the fight ended, and never after today unless the fight itself ran past it;
  - it is free text: most reports are long enough for the tagger's free-text rule, varied, with a little junk;
  - the mention key is exact: every mention is the text at its place, it belongs to someone who was in the fight,
    and its truth is that person's own name, alias, SID, birth date or phone number;
  - the messy copy writes the narrative exactly as the clean table has it, so the key's places hold in the lake.
"""
import sys
from pathlib import Path

import pyarrow.csv as pacsv
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.universe import build, manifest, mess, reports, scale, writer  # noqa: E402
from lakehouse.universe.world import TODAY  # noqa: E402

T = "incident_report"
KINDS = {"person_name", "alias", "sid", "birth_date", "phone"}


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A scale-S build of every table, the incident reports' mess and mention key, and a DuckDB connection with the
    lake attached read-only and a view per table (u_<table>, u_mention)."""
    out = tmp_path_factory.mktemp("universe") / "S"
    con = lake.lake_connect(read_only=True)
    built = build.build(out, scale_name="S", seed=2026, workers=4, with_mess=False)
    made = mess.build_mess(out, build.context(out, "S", 2026), {T: built[T]}, workers=1)
    for t in build.ORDER:
        con.execute(f"CREATE OR REPLACE TEMP VIEW u_{t} AS SELECT * FROM read_parquet('{out}/{t}/*.parquet')")
    con.execute(f"CREATE OR REPLACE TEMP VIEW u_mention AS SELECT * FROM read_parquet('{out}/{reports.MENTIONS}/*.parquet')")
    con.execute("""CREATE OR REPLACE TEMP VIEW r AS
                   SELECT r.*, i.started_at, i.started_at + to_minutes(i.duration_min) AS ended_at
                   FROM u_incident_report r JOIN u_incident i USING (incident_id)""")
    return out, con, made


def one(con, sql):
    """The single value a query returns."""
    return con.execute(sql).fetchone()[0]


# --- shape -------------------------------------------------------------------------------------------------------

def test_the_scales_rows_one_report_per_fight_at_most_and_the_manifest_verifies(world):
    """Rows are exactly scale.rows("incident_report", "S"), ids are keys, no fight has two reports, and every file
    matches the manifest."""
    out, con, _ = world
    assert T in build.ORDER
    assert one(con, f"SELECT count(*) FROM u_{T}") == scale.rows(T, "S") == 20_000
    assert one(con, f"SELECT count(*) - count(DISTINCT {T}_id) FROM u_{T}") == 0
    assert one(con, f"SELECT count(*) - count(DISTINCT incident_id) FROM u_{T}") == 0
    assert manifest.verify(out) == []


def test_every_link_points_at_a_row_that_exists(world):
    """Each report's fight exists, and so does its author (when it has one)."""
    _, con, _ = world
    assert one(con, f"SELECT count(*) FROM u_{T} r ANTI JOIN u_incident USING (incident_id)") == 0
    assert one(con, f"""SELECT count(*) FROM u_{T} r WHERE author_person_id IS NOT NULL
                        AND author_person_id NOT IN (SELECT person_id FROM u_person)""") == 0


def test_a_chunk_rebuilt_alone_matches_the_full_build(world, tmp_path):
    """Built in 7,000-row chunks, the last chunk rebuilt on its own in a fresh folder has the same bytes. (The
    fixture's world supplies every table the reports read.)"""
    ctx = build.context(world[0], "S", 2026)
    rows = scale.rows(T, "S")
    full = writer.build_table(T, rows, build.factory(T, ctx), tmp_path / "full", seed=2026, chunk_rows=7_000)
    last = writer.chunk_plan(rows, 7_000)[-1][0]
    alone = writer.build_table(T, rows, build.factory(T, ctx), tmp_path / "alone", seed=2026, chunk_rows=7_000,
                               only_chunks={last})
    assert len(full["files"]) == 3
    assert alone["files"][0]["sha256"] == full["files"][-1]["sha256"]


# --- who wrote it, and when ----------------------------------------------------------------------------------------

def test_authors_were_there_and_match_the_kind_of_report(world):
    """A field report is written by a hero in the fight, a witness statement by a civilian in it, a desk report by
    nobody; and all three kinds occur."""
    _, con, _ = world
    kinds = dict(con.execute(f"SELECT report_kind, count(*) FROM u_{T} GROUP BY 1").fetchall())
    assert set(kinds) == {"field report", "witness statement", "desk report"}
    assert kinds["field report"] > kinds["witness statement"] > 0 and kinds["desk report"] > 0
    wrong = one(con, f"""SELECT count(*) FROM u_{T} r LEFT JOIN u_participant pa
                           ON pa.incident_id = r.incident_id AND pa.person_id = r.author_person_id
                         WHERE (r.report_kind = 'field report' AND pa.side IS DISTINCT FROM 'hero')
                            OR (r.report_kind = 'witness statement' AND pa.side IS DISTINCT FROM 'civilian')
                            OR (r.report_kind = 'desk report' AND r.author_person_id IS NOT NULL)""")
    assert wrong == 0


def test_reports_are_filed_after_the_fight_and_not_after_today(world):
    """filed_at is on or after the fight's end, and on or before the end of today unless the fight ended later."""
    _, con, _ = world
    assert one(con, "SELECT count(*) FROM r WHERE filed_at IS NULL OR filed_at < ended_at") == 0
    today_end = f"(DATE '1970-01-01' + INTERVAL {TODAY + 1} DAY - INTERVAL 1 MICROSECOND)"
    assert one(con, f"SELECT count(*) FROM r WHERE filed_at > greatest(ended_at, {today_end})") == 0


def test_bigger_fights_are_written_up_more_often(world):
    """Reports favour bigger fights: the reported fights have more participants on average than fights overall."""
    _, con, _ = world
    sizes = """SELECT incident_id, count(*) AS n FROM u_participant GROUP BY 1"""
    every = one(con, f"SELECT avg(n) FROM ({sizes})")
    reported = one(con, f"SELECT avg(n) FROM ({sizes}) s SEMI JOIN u_{T} USING (incident_id)")
    assert reported > every * 1.1


# --- free text ---------------------------------------------------------------------------------------------------

def test_it_is_free_text_with_a_little_junk(world):
    """Most narratives are six words or more and they rarely repeat (what the tagger's free-text rule looks for);
    1-4% are junk (null, SEE ATTACHED, TBD, ...); some are all capitals, and some carry typos."""
    _, con, _ = world
    n = scale.rows(T, "S")
    long_ = one(con, f"SELECT count(*) FROM u_{T} WHERE len(string_split(trim(narrative), ' ')) >= 6")
    assert long_ / n > 0.9
    assert one(con, f"SELECT count(DISTINCT narrative) FROM u_{T}") / n > 0.9
    junk = one(con, f"SELECT count(*) FROM u_{T} WHERE narrative IS NULL OR narrative IN {tuple(reports.JUNK[1:])}")
    assert 0.01 <= junk / n <= 0.04
    shouting = one(con, f"SELECT count(*) FROM u_{T} WHERE narrative = upper(narrative) AND len(narrative) > 80")
    assert 0.05 <= shouting / n <= 0.15


# --- the mention key ---------------------------------------------------------------------------------------------

def test_every_mention_is_the_text_at_its_place(world):
    """For every mention, the narrative's characters start..end (end excluded, counted from 0) are as_written; mentions
    in a report don't overlap; every kind occurs; and most reports name someone."""
    _, con, _ = world
    assert one(con, "SELECT count(*) FROM u_mention") > scale.rows(T, "S")
    assert {k for (k,) in con.execute("SELECT DISTINCT kind FROM u_mention").fetchall()} == KINDS
    assert one(con, f"""SELECT count(*) FROM u_mention m JOIN u_{T} r USING ({T}_id)
                        WHERE substr(r.narrative, m.start_pos + 1, m.end_pos - m.start_pos) IS DISTINCT FROM m.as_written""") == 0
    assert one(con, f"""SELECT count(*) FROM (SELECT start_pos, lag(end_pos) OVER (PARTITION BY {T}_id ORDER BY start_pos) AS prev
                        FROM u_mention) WHERE start_pos < prev""") == 0
    named = one(con, f"SELECT count(DISTINCT {T}_id) FROM u_mention")
    assert named / scale.rows(T, "S") > 0.9


def test_mentions_belong_to_people_who_were_in_the_fight(world):
    """Each mention's person took part in the report's fight."""
    _, con, _ = world
    assert one(con, f"""SELECT count(*) FROM u_mention m JOIN u_{T} r USING ({T}_id)
                        ANTI JOIN u_participant pa ON pa.incident_id = r.incident_id AND pa.person_id = m.person_id""") == 0


def test_a_mentions_truth_is_that_persons_own_value(world):
    """sid is the person's SID; birth_date their birth date (ISO); person_name their LAST, FIRST; alias their alias;
    phone a fictional 555-01xx number, the same for a person in every report."""
    _, con, _ = world
    wrong = one(con, """SELECT count(*) FROM u_mention m JOIN u_person p USING (person_id) WHERE CASE m.kind
                          WHEN 'sid' THEN m.truth IS DISTINCT FROM p.sid_number::VARCHAR
                          WHEN 'birth_date' THEN m.truth IS DISTINCT FROM strftime(p.birth_date, '%Y-%m-%d')
                          WHEN 'person_name' THEN m.truth IS DISTINCT FROM p.last_name || ', ' || p.first_name
                          WHEN 'alias' THEN m.truth IS DISTINCT FROM p.alias
                          WHEN 'phone' THEN NOT regexp_full_match(m.truth, '\\d{3}-555-01\\d{2}') END""")
    assert wrong == 0
    assert one(con, "SELECT count(*) FROM (SELECT person_id FROM u_mention WHERE kind = 'phone' GROUP BY 1 HAVING count(DISTINCT truth) > 1)") == 0
    # The same value is written more than one way: SIDs, birth dates and names each come in several spellings.
    for kind in ("sid", "birth_date", "person_name", "phone"):
        assert one(con, f"SELECT count(DISTINCT as_written) - count(DISTINCT truth) FROM u_mention WHERE kind = '{kind}'") > 0, kind


def test_what_is_written_spells_the_truth(world):
    """A mention's words are its truth written another way, so a span in the wrong place can't pass: the SID's and
    the phone's digits, the alias (any case), the surname (any case) and the birth year (four digits or the last two, as a
    number of its own)."""
    _, con, _ = world
    wrong = one(con, """SELECT count(*) FROM u_mention WHERE NOT CASE kind
                          WHEN 'sid' THEN regexp_replace(as_written, '[^0-9]', '', 'g') = truth
                          WHEN 'phone' THEN regexp_replace(as_written, '[^0-9]', '', 'g') = replace(truth, '-', '')
                          WHEN 'alias' THEN upper(as_written) = truth
                          WHEN 'person_name' THEN contains(upper(as_written), split_part(truth, ', ', 1))
                          WHEN 'birth_date' THEN regexp_matches(as_written, '(^|[^0-9])(' || left(truth, 4) || '|'
                                                                || substr(truth, 3, 2) || ')([^0-9]|$)') END""")
    assert wrong == 0


def test_a_report_is_never_filed_after_today_unless_the_fight_ran_past_it():
    """filed_time caps a filing at 23:59:59 today, but never before the fight's end."""
    hour = 3_600_000_000
    assert reports.filed_time(reports.TODAY_END_US - hour, 5 * hour) == reports.TODAY_END_US
    assert reports.filed_time(reports.TODAY_END_US - 5 * hour, hour) == reports.TODAY_END_US - 4 * hour
    assert reports.filed_time(reports.TODAY_END_US + hour, hour) == reports.TODAY_END_US + hour


# --- the messy copy ------------------------------------------------------------------------------------------------

def test_the_messy_copy_keeps_the_narrative_as_written(world):
    """The incident reports get a messy copy, an answer key and a mention key in the manifest, and the messy CSV's
    narrative is the clean narrative exactly (blank where it is null), so the mention key's places hold in the lake."""
    out, con, made = world
    assert {f"messy/{T}", f"answer_key/{T}", reports.MENTIONS} <= set(made)
    assert one(con, f"SELECT count(*) FROM u_mention") == made[reports.MENTIONS]["rows"]
    opts = pacsv.ConvertOptions(column_types={c: "string" for c in ("incident_report_id", "narrative")},
                                strings_can_be_null=False)
    messy = pacsv.read_csv(next((out / "messy" / T).glob("*.csv")), convert_options=opts)
    con.register("messy_r", messy)
    assert one(con, f"""SELECT count(*) FROM messy_r m JOIN u_{T} r ON r.{T}_id = m.{T}_id::BIGINT
                        WHERE m.narrative IS DISTINCT FROM coalesce(r.narrative, '')""") == 0
    assert one(con, f"SELECT count(*) FROM read_parquet('{out}/answer_key/{T}/*.parquet') WHERE column_name = 'narrative'") == 0
