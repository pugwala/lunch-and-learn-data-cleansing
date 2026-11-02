"""Acceptance tests for the expanded world's charges, court outcomes and missions (dataset expansion, chunk 5).

    pytest lakehouse/tests/test_universe_cases.py     # inside the jupyter container; needs the lake (silver, ref)

One scale-S build is made in 40,000-row chunks (so charges and mission members straddle files) and checked:
  - every table has the scale's rows, the manifest verifies, and chunks rebuilt alone match;
  - every link points at a row that exists; offense codes are NIBRS codes from ref.nibrs_offenses;
  - only captured villains are charged; every charge has exactly one disposition;
  - each of today's 94,120 linkable offender records is charge i on fight i's villain, with the record's offense
    code, and a conviction on the sentence date carrying the record's offender number; any other conviction is of
    a new person (today's people are only ever convicted through their records);
  - time only moves forward: a charge is filed on or after the fight, decided after it's filed, pending only when
    undecided; a mission ends after it starts, and its fights happen while it runs;
  - missions are staffed by heroes who held a power when it started, one lead each, nobody twice.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.universe import build, manifest, scale, writer  # noqa: E402

CASES = ["charge", "disposition", "mission", "mission_member", "mission_incident"]
CHUNK_ROWS = 40_000
CONVICTED = "('convicted', 'pleaded guilty')"


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A scale-S build of every table, and a DuckDB connection with the lake attached read-only and a view per
    built table."""
    out = tmp_path_factory.mktemp("universe") / "S"
    con = lake.lake_connect(read_only=True)
    build.build(out, scale_name="S", seed=2026, workers=4, con=con, chunk_rows=CHUNK_ROWS, with_mess=False)
    for t in build.ORDER:
        con.execute(f"CREATE OR REPLACE TEMP VIEW u_{t} AS SELECT * FROM read_parquet('{out}/{t}/*.parquet')")
    con.execute("""CREATE OR REPLACE TEMP VIEW charged AS
                   SELECT c.*, d.outcome, d.decided_on, d.offender_number, pa.person_id, pa.side, pa.status,
                          pa.incident_id AS participant_incident_id, i.started_at::DATE AS day, pe.is_core, pe.sid_number
                   FROM u_charge c JOIN u_disposition d USING (charge_id) JOIN u_participant pa USING (participant_id)
                   JOIN u_incident i ON i.incident_id = c.incident_id JOIN u_person pe USING (person_id)""")
    return out, con


def one(con, sql):
    """The single value a query returns."""
    return con.execute(sql).fetchone()[0]


# --- shape -------------------------------------------------------------------------------------------------------

def test_every_table_has_the_scales_row_count_and_the_manifest_verifies(world):
    """Rows per table are exactly scale.rows(table, "S"), ids are keys, and every file matches the manifest."""
    out, con = world
    for t in CASES:
        assert one(con, f"SELECT count(*) FROM u_{t}") == scale.rows(t, "S"), t
        assert one(con, f"SELECT count(*) - count(DISTINCT {t}_id) FROM u_{t}") == 0, t
    assert manifest.verify(out) == []
    assert set(manifest.read(out)["tables"]) == set(build.ORDER)


@pytest.mark.parametrize("table", ["charge", "disposition", "mission_member"])
def test_a_chunk_rebuilt_alone_matches_the_full_build(world, tmp_path, table):
    """A table's last chunk, rebuilt on its own in a fresh folder, has the same bytes as in the full build. (The
    last, because the first charges are today's records, which don't use the shared-out charge plan.)"""
    out, _ = world
    last = writer.chunk_plan(scale.rows(table, "S"), CHUNK_ROWS)[-1][0]
    entry = writer.build_table(table, scale.rows(table, "S"), build.factory(table, build.context(out, "S", 2026)),
                               tmp_path, seed=2026, chunk_rows=CHUNK_ROWS, only_chunks={last})
    full = {f["name"]: f["sha256"] for f in manifest.read(out)["tables"][table]["files"]}
    assert entry["files"][0]["sha256"] == full[f"part-{last:05d}.parquet"]


# --- links -------------------------------------------------------------------------------------------------------

def test_every_link_points_at_a_row_that_exists(world):
    """Charges, dispositions, missions, members and mission fights point at real rows of the right kind."""
    _, con = world
    checks = {
        "charge.participant_id": "SELECT count(*) FROM u_charge c LEFT JOIN u_participant p USING (participant_id) WHERE p.incident_id IS DISTINCT FROM c.incident_id",
        "charge.offense_code": "SELECT count(*) FROM u_charge WHERE offense_code NOT IN (SELECT code FROM lake.ref.nibrs_offenses)",
        "disposition.charge_id": "SELECT count(*) FROM u_charge c FULL JOIN (SELECT charge_id, count(*) n FROM u_disposition GROUP BY 1) d USING (charge_id) WHERE d.n IS DISTINCT FROM 1 OR c.incident_id IS NULL",
        "mission.team_id": "SELECT count(*) FROM u_mission WHERE team_id NOT IN (SELECT team_id FROM u_team WHERE side = 'hero')",
        "mission.target_team_id": "SELECT count(*) FROM u_mission WHERE target_team_id NOT IN (SELECT team_id FROM u_team WHERE side = 'villain')",
        "mission.location_id": "SELECT count(*) FROM u_mission WHERE location_id NOT IN (SELECT location_id FROM u_location)",
        "mission_member.mission_id": "SELECT count(*) FROM u_mission_member WHERE mission_id NOT IN (SELECT mission_id FROM u_mission)",
        "mission_member.person_id": "SELECT count(*) FROM u_mission_member WHERE person_id NOT IN (SELECT person_id FROM u_person)",
        "mission_incident.mission_id": "SELECT count(*) FROM u_mission_incident WHERE mission_id NOT IN (SELECT mission_id FROM u_mission)",
        "mission_incident.incident_id": "SELECT count(*) FROM u_mission_incident WHERE incident_id NOT IN (SELECT incident_id FROM u_incident)",
    }
    assert {k: one(con, q) for k, q in checks.items()} == {k: 0 for k in checks}
    assert one(con, "SELECT count(*) - count(DISTINCT (mission_id, incident_id)) FROM u_mission_incident") == 0


# --- charges and court outcomes ----------------------------------------------------------------------------------

def test_only_captured_villains_are_charged(world):
    """Every charge is on a villain who was captured in that fight."""
    _, con = world
    assert one(con, "SELECT count(*) FROM charged WHERE side <> 'villain' OR status <> 'captured'") == 0


def test_each_of_todays_records_is_a_conviction_on_its_own_fight(world):
    """Charge i (i < 94,120) is on fight i's villain, the record's SID, with the record's offense code; it ends in a
    conviction on the sentence date that carries the record's offender number. No other charge carries one."""
    _, con = world
    matched = one(con, """
        WITH r AS (SELECT row_number() OVER (ORDER BY offender_number) - 1 AS charge_id, offender_number, sid_number,
                          sentence_date, offense_code_std
                   FROM lake.silver.hero_offenders WHERE sid_number IS NOT NULL AND sentence_date IS NOT NULL)
        SELECT count(*) FROM r JOIN charged c USING (charge_id)
        WHERE c.incident_id = c.charge_id AND c.sid_number = r.sid_number AND c.offender_number = r.offender_number
          AND c.offense_code IS NOT DISTINCT FROM r.offense_code_std AND c.decided_on = r.sentence_date
          AND c.outcome IN """ + CONVICTED)
    assert matched == scale.CORE_RECORDS == 94_120
    assert one(con, f"SELECT count(*) FROM charged WHERE offender_number IS NOT NULL AND charge_id >= {scale.CORE_RECORDS}") == 0


def test_todays_people_are_convicted_only_through_their_records(world):
    """A conviction of one of today's people always carries an offender number; new people are convicted too."""
    _, con = world
    assert one(con, f"SELECT count(*) FROM charged WHERE is_core AND outcome IN {CONVICTED} AND offender_number IS NULL") == 0
    assert one(con, f"SELECT count(*) FROM charged WHERE NOT is_core AND outcome IN {CONVICTED}") > 0


def test_convictions_outnumber_dismissals(world):
    """New villains fight often enough (fights.NEW_VILLAIN_SHARE) that at least half the charges after today's
    records are on new people, so convictions (convicted or pleaded guilty) outnumber dismissals (chunk 5b, Ian's
    option B: today's people, who are only convicted through their records, no longer swamp the courts)."""
    _, con = world
    assert one(con, f"SELECT avg((NOT is_core)::INT) FROM charged WHERE charge_id >= {scale.CORE_RECORDS}") >= 0.5
    assert one(con, f"SELECT count(*) FILTER (outcome IN {CONVICTED}) - count(*) FILTER (outcome = 'dismissed') FROM charged") > 0


def test_court_time_only_moves_forward(world):
    """Filed on or after the fight; decided after filing and by today; pending exactly when undecided."""
    _, con = world
    assert one(con, """SELECT count(*) FROM charged WHERE filed_on < day OR decided_on < filed_on
                       OR filed_on > DATE '2026-09-24' OR decided_on > DATE '2026-09-24'
                       OR (decided_on IS NULL) <> (outcome = 'pending')""") == 0


# --- missions ----------------------------------------------------------------------------------------------------

def test_missions_run_forward_and_their_fights_happen_during_them(world):
    """A mission ends after it starts and by today; it has no end exactly when ongoing; a linked fight falls inside
    it."""
    _, con = world
    assert one(con, """SELECT count(*) FROM u_mission WHERE ended_on < started_on OR ended_on > DATE '2026-09-24'
                       OR started_on < DATE '1996-01-01' OR (ended_on IS NULL) <> (status = 'ongoing')""") == 0
    assert one(con, """SELECT count(*) FROM u_mission_incident JOIN u_mission USING (mission_id) JOIN u_incident USING (incident_id)
                       WHERE started_at::DATE < started_on OR started_at::DATE > coalesce(ended_on, DATE '2026-09-24')""") == 0


def test_missions_are_staffed_by_powered_heroes(world):
    """Members are heroes or secondary heroes who held a power on the start day; one lead per mission; nobody
    twice in a mission."""
    _, con = world
    assert one(con, """SELECT count(*) FROM u_mission_member mm JOIN u_mission m USING (mission_id) JOIN u_person p USING (person_id)
                       WHERE p.role NOT IN ('hero', 'secondary_hero')
                          OR NOT EXISTS (SELECT 1 FROM u_person_power pp WHERE pp.person_id = mm.person_id
                                         AND pp.acquired_on <= m.started_on)""") == 0
    assert one(con, "SELECT count(*) FROM (SELECT mission_id FROM u_mission_member GROUP BY 1 HAVING count(*) FILTER (role = 'lead') <> 1)") == 0
    assert one(con, "SELECT count(*) FROM u_mission WHERE mission_id NOT IN (SELECT mission_id FROM u_mission_member)") == 0
    assert one(con, "SELECT count(*) - count(DISTINCT (mission_id, person_id)) FROM u_mission_member") == 0
