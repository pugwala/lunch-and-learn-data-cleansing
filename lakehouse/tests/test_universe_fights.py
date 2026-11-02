"""Acceptance tests for the expanded world's fights (dataset expansion, chunk 4).

    pytest lakehouse/tests/test_universe_fights.py     # inside the jupyter container; needs the lake (silver)

One scale-S build (about 1.3M fight rows) is made into a temporary folder, in quarter-million-row chunks so a fight's
participants can straddle two files, and checked:
  - every table has exactly the rows the scale knob says, the manifest verifies, and a chunk rebuilt alone matches;
  - every link points at a row that exists; nobody fights twice in one fight; sides match people's roles;
  - an incident's outcome agrees with what happened to its villains;
  - time only moves forward: fights fall in 1996 to today, a power is held when it's used, new people are born
    before they fight, a repair comes after the damage;
  - each of today's 94,120 offender records with a SID and a sentence date has its own fight, before the sentence,
    in which that villain was captured (chunk 5 hangs the charge and the conviction on it).
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.universe import build, core, fights, manifest, scale, writer  # noqa: E402

FIGHTS = ["incident", "participant", "power_use", "damage"]
CHUNK_ROWS = 250_000


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A scale-S build of every table so far, and a DuckDB connection with the lake attached read-only and a view
    per built table."""
    out = tmp_path_factory.mktemp("universe") / "S"
    con = lake.lake_connect(read_only=True)
    build.build(out, scale_name="S", seed=2026, workers=4, chunk_rows=CHUNK_ROWS, with_mess=False)
    for t in build.ORDER:
        con.execute(f"CREATE OR REPLACE TEMP VIEW u_{t} AS SELECT * FROM read_parquet('{out}/{t}/*.parquet')")
    con.execute("""CREATE OR REPLACE TEMP VIEW fought AS
                   SELECT pa.*, i.started_at::DATE AS day, pe.role AS person_role, pe.is_core, pe.birth_date
                   FROM u_participant pa JOIN u_incident i USING (incident_id) JOIN u_person pe USING (person_id)""")
    return out, con


def one(con, sql):
    """The single value a query returns."""
    return con.execute(sql).fetchone()[0]


# --- shape -------------------------------------------------------------------------------------------------------

def test_every_table_has_the_scales_row_count_and_the_manifest_verifies(world):
    """Rows per fight table are exactly scale.rows(table, "S"), and every file of every table matches the manifest."""
    out, con = world
    for t in FIGHTS:
        assert one(con, f"SELECT count(*) FROM u_{t}") == scale.rows(t, "S"), t
    assert manifest.verify(out) == []
    assert set(manifest.read(out)["tables"]) == set(build.ORDER)


def test_ids_are_unique(world):
    """Each fight table's id column is a key."""
    _, con = world
    for t in FIGHTS:
        assert one(con, f"SELECT count(*) - count(DISTINCT {t}_id) FROM u_{t}") == 0, t


def test_a_chunk_rebuilt_alone_matches_the_full_build(world, tmp_path):
    """Participant chunk 1, rebuilt on its own in a fresh folder, has the same bytes as in the full build: the fight
    plan every chunk works from doesn't depend on which chunks were built, or where."""
    out, _ = world
    ctx = build.context(out, "S", 2026)
    entry = writer.build_table("participant", scale.rows("participant", "S"),
                               build.factory("participant", ctx), tmp_path, seed=2026, chunk_rows=CHUNK_ROWS,
                               only_chunks={1})
    full = {f["name"]: f["sha256"] for f in manifest.read(out)["tables"]["participant"]["files"]}
    assert entry["files"][0]["sha256"] == full["part-00001.parquet"]


# --- links -------------------------------------------------------------------------------------------------------

def test_every_link_points_at_a_row_that_exists(world):
    """Fights happen at ordinary sites; participants, power uses and damages point at real rows, and a power use's
    incident is its participant's."""
    _, con = world
    checks = {
        "incident.location_id": """SELECT count(*) FROM u_incident WHERE location_id NOT IN
                                   (SELECT location_id FROM u_location WHERE kind = 'site' AND unit_code IS NULL)""",
        "participant.incident_id": "SELECT count(*) FROM u_participant WHERE incident_id NOT IN (SELECT incident_id FROM u_incident)",
        "participant.person_id": "SELECT count(*) FROM u_participant WHERE person_id NOT IN (SELECT person_id FROM u_person)",
        "power_use.participant_id": """SELECT count(*) FROM u_power_use u LEFT JOIN u_participant p USING (participant_id)
                                       WHERE p.incident_id IS DISTINCT FROM u.incident_id""",
        "power_use.power_id": "SELECT count(*) FROM u_power_use WHERE power_id NOT IN (SELECT power_id FROM u_power)",
        "damage.incident_id": "SELECT count(*) FROM u_damage WHERE incident_id NOT IN (SELECT incident_id FROM u_incident)",
    }
    assert {k: one(con, q) for k, q in checks.items()} == {k: 0 for k in checks}


def test_every_fight_has_a_villain_and_nobody_twice(world):
    """Each incident has at least one villain and two participants, and no person appears twice in one incident."""
    _, con = world
    assert one(con, """SELECT count(*) FROM u_incident WHERE incident_id NOT IN
                       (SELECT incident_id FROM u_participant WHERE side = 'villain')""") == 0
    assert one(con, "SELECT count(*) FROM (SELECT incident_id FROM u_participant GROUP BY 1 HAVING count(*) < 2)") == 0
    assert one(con, "SELECT count(*) - count(DISTINCT (incident_id, person_id)) FROM u_participant") == 0


def test_sides_match_peoples_roles(world):
    """Villains fight for the villains, heroes and secondary heroes for the heroes, civilians are civilians."""
    _, con = world
    assert one(con, """SELECT count(*) FROM fought WHERE NOT (
                         (side = 'villain' AND person_role = 'villain')
                      OR (side = 'hero' AND person_role IN ('hero', 'secondary_hero'))
                      OR (side = 'civilian' AND person_role = 'civilian'))""") == 0


def test_the_outcome_agrees_with_what_happened_to_the_villains(world):
    """'captured': every villain captured; 'escaped': none; 'partial': some but not all. Only villains are captured
    or escape."""
    _, con = world
    assert one(con, """WITH v AS (SELECT incident_id, count(*) FILTER (status = 'captured') AS caught, count(*) AS n
                                  FROM u_participant WHERE side = 'villain' GROUP BY 1)
                       SELECT count(*) FROM u_incident JOIN v USING (incident_id) WHERE NOT (
                            (outcome = 'captured' AND caught = n) OR (outcome = 'escaped' AND caught = 0)
                         OR (outcome = 'partial' AND caught BETWEEN 1 AND n - 1))""") == 0
    assert one(con, """SELECT count(*) FROM u_participant WHERE (side = 'villain') <>
                       (status IN ('captured', 'escaped'))""") == 0


# --- time --------------------------------------------------------------------------------------------------------

def test_time_only_moves_forward(world):
    """Fights fall between 1996-01-01 and the course's today; new people are born before they fight (today's
    people keep the course's planted birth-date mess); a repair comes after the fight, and only when repaired."""
    _, con = world
    assert one(con, "SELECT count(*) FROM u_incident WHERE started_at NOT BETWEEN TIMESTAMP '1996-01-01' AND TIMESTAMP '2026-09-24 23:59:59'") == 0
    assert one(con, "SELECT count(*) FROM fought WHERE NOT is_core AND birth_date >= day") == 0
    assert one(con, """SELECT count(*) FROM u_damage d JOIN u_incident i USING (incident_id)
                       WHERE d.repaired_on <= i.started_at::DATE OR d.repaired_on > DATE '2026-09-24'
                          OR (d.repaired_on IS NOT NULL) <> (d.repair_status = 'repaired')""") == 0


def test_a_power_is_held_when_its_used(world):
    """Every power use is of a power the participant's person held on the day of the fight (gained on or before it,
    not yet lost); so civilians, who hold none, never use one."""
    _, con = world
    assert one(con, """SELECT count(*) FROM u_power_use u JOIN fought f USING (participant_id)
                       WHERE NOT EXISTS (SELECT 1 FROM u_person_power pp
                                         WHERE pp.person_id = f.person_id AND pp.power_id = u.power_id
                                           AND (pp.acquired_on IS NULL OR pp.acquired_on <= f.day)
                                           AND (pp.lost_on IS NULL OR pp.lost_on > f.day))""") == 0
    assert one(con, "SELECT count(*) FROM u_power_use WHERE effectiveness NOT BETWEEN 0 AND 1") == 0


# --- today's records ---------------------------------------------------------------------------------------------

def test_each_of_todays_records_has_its_own_fight_before_the_sentence(world):
    """Incident i (i < 94,120) is the fight behind the i-th offender record (by offender number) with a SID and a
    sentence date: its villain is that SID, captured, and it happened before the sentence date."""
    _, con = world
    assert fights.CORE_RECORDS == 94_120
    matched = one(con, """
        WITH r AS (SELECT row_number() OVER (ORDER BY offender_number) - 1 AS incident_id, sid_number, sentence_date
                   FROM lake.silver.hero_offenders WHERE sid_number IS NOT NULL AND sentence_date IS NOT NULL)
        SELECT count(*) FROM r JOIN fought f USING (incident_id) JOIN u_person p USING (person_id)
        WHERE p.sid_number = r.sid_number AND f.side = 'villain' AND f.status = 'captured' AND f.day < r.sentence_date""")
    assert matched == fights.CORE_RECORDS


# --- money -------------------------------------------------------------------------------------------------------

def test_damage_costs_and_insurance_add_up(world):
    """Every damage costs something; an insurer pays only for insured damage and never more than it cost."""
    _, con = world
    assert one(con, "SELECT count(*) FROM u_damage WHERE cost_usd <= 0") == 0
    assert one(con, """SELECT count(*) FROM u_damage WHERE insurance_paid_usd > cost_usd
                       OR (insurance_paid_usd IS NOT NULL AND NOT insured)""") == 0
    assert one(con, "SELECT count(*) FROM u_damage WHERE insured") > 0
