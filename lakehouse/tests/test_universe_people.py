"""Acceptance tests for the expanded world's people, powers and places (dataset expansion, chunk 3).

    pytest lakehouse/tests/test_universe_people.py     # inside the jupyter container; needs the lake (silver)

One scale-S build (about 140,000 people) is made into a temporary folder and checked against the lake:
  - every table has exactly the rows the scale knob says, and the manifest verifies;
  - today's 86,879 people are all there, once each, as villains, with the lake's names, birth dates and genders;
  - their 140,795 powers and 9,065 gang memberships are all there, and nobody new gets a SID;
  - every link points at a row that exists, and time only moves forward (a power is held before it's used up,
    a villain held theirs before their first sentence);
  - places cover the course's cities and every custody unit; teams include the twelve STG groups.
"""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.universe import build, core, manifest, scale  # noqa: E402

TABLES = ["location", "power", "team", "person", "person_power", "team_member"]


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A scale-S build of chunk 3's tables, and a DuckDB connection with the lake attached read-only and a view
    per built table."""
    out = tmp_path_factory.mktemp("universe") / "S"
    con = lake.lake_connect(read_only=True)
    build.build(out, scale_name="S", seed=2026, workers=4, tables=TABLES, con=con)
    for t in TABLES:
        con.execute(f"CREATE OR REPLACE TEMP VIEW u_{t} AS SELECT * FROM read_parquet('{out}/{t}/*.parquet')")
    return out, con


def one(con, sql):
    """The single value a query returns."""
    return con.execute(sql).fetchone()[0]


# --- shape -------------------------------------------------------------------------------------------------------

def test_every_table_has_the_scales_row_count_and_the_manifest_verifies(world):
    """Rows per table are exactly scale.rows(table, "S"), and every file matches the manifest."""
    out, con = world
    for t in TABLES:
        assert one(con, f"SELECT count(*) FROM u_{t}") == scale.rows(t, "S"), t
    assert manifest.verify(out) == []
    assert set(manifest.read(out)["tables"]) == set(TABLES)


def test_ids_are_unique(world):
    """Each table's id column is a key."""
    _, con = world
    for t, key in [("location", "location_id"), ("power", "power_id"), ("team", "team_id"), ("person", "person_id"),
                   ("person_power", "person_power_id"), ("team_member", "team_member_id")]:
        assert one(con, f"SELECT count(*) - count(DISTINCT {key}) FROM u_{t}") == 0, t


# --- today's people ----------------------------------------------------------------------------------------------

def test_todays_people_are_all_villains_once_each(world):
    """All 86,879 SIDs in silver are people, once each, marked core and villain; nobody else has a SID."""
    _, con = world
    assert one(con, "SELECT count(DISTINCT sid_number) FROM lake.silver.hero_offenders") == scale.CORE_PEOPLE
    assert one(con, """SELECT count(*) FROM u_person p JOIN (SELECT DISTINCT sid_number FROM lake.silver.hero_offenders
                       WHERE sid_number IS NOT NULL) s USING (sid_number)
                       WHERE p.is_core AND p.role = 'villain'""") == scale.CORE_PEOPLE
    assert one(con, "SELECT count(*) - count(DISTINCT sid_number) FROM u_person WHERE sid_number IS NOT NULL") == 0
    assert one(con, "SELECT count(*) FROM u_person WHERE NOT is_core AND sid_number IS NOT NULL") == 0
    assert one(con, "SELECT count(*) FROM u_person WHERE is_core") == scale.CORE_PEOPLE


def test_todays_people_keep_the_lakes_names_birth_dates_and_genders(world):
    """For every core person, name, alias, birth date and gender are the ones the lake's latest record holds."""
    _, con = world
    mismatched = one(con, """
        WITH latest AS (SELECT * FROM lake.silver.hero_offenders WHERE sid_number IS NOT NULL
                        QUALIFY row_number() OVER (PARTITION BY sid_number ORDER BY sentence_date DESC, offender_number DESC) = 1)
        SELECT count(*) FROM u_person p JOIN latest l USING (sid_number)
        WHERE p.last_name IS DISTINCT FROM l.last_name_std OR p.first_name IS DISTINCT FROM l.first_name_std
           OR p.alias IS DISTINCT FROM l.alias_std OR p.birth_date IS DISTINCT FROM l.date_of_birth
           OR p.gender IS DISTINCT FROM l.gender""")
    assert mismatched == 0


def test_todays_powers_and_gang_memberships_are_all_there(world):
    """Every (SID, power) pair and every (SID, STG group) affiliation in silver appears in the new tables."""
    _, con = world
    assert one(con, """
        WITH want AS (SELECT DISTINCT sid_number, trim(upper(unnest(string_split(superpower, ';')))) AS power
                      FROM lake.silver.hero_offenders WHERE sid_number IS NOT NULL),
             have AS (SELECT p.sid_number, w.name AS power FROM u_person_power pp JOIN u_person p USING (person_id)
                      JOIN u_power w USING (power_id) WHERE p.is_core)
        SELECT count(*) FROM (SELECT * FROM want WHERE power <> '' EXCEPT SELECT * FROM have)""") == 0
    assert one(con, "SELECT count(*) FROM u_person_power pp JOIN u_person p USING (person_id) WHERE p.is_core") \
        == scale.CORE_PERSON_POWERS == 140_795
    assert one(con, """
        WITH want AS (SELECT DISTINCT sid_number, stg_group FROM lake.silver.hero_offenders
                      WHERE sid_number IS NOT NULL AND stg_group IS NOT NULL AND stg_group <> 'None'),
             have AS (SELECT p.sid_number, t.stg_group FROM u_team_member m JOIN u_person p USING (person_id)
                      JOIN u_team t USING (team_id) WHERE p.is_core)
        SELECT count(*) FROM (SELECT * FROM want EXCEPT SELECT * FROM have)""") == 0
    assert one(con, "SELECT count(*) FROM u_team_member m JOIN u_person p USING (person_id) WHERE p.is_core") \
        == scale.CORE_TEAM_MEMBERS == 9_065


# --- links and time ----------------------------------------------------------------------------------------------

def test_every_link_points_at_a_row_that_exists(world):
    """Foreign keys: person homes, power holders and powers, team bases, members and teams, place parents."""
    _, con = world
    checks = {
        "person.home_location_id": "SELECT count(*) FROM u_person WHERE home_location_id NOT IN (SELECT location_id FROM u_location WHERE kind = 'city')",
        "person_power.person_id": "SELECT count(*) FROM u_person_power WHERE person_id NOT IN (SELECT person_id FROM u_person)",
        "person_power.power_id": "SELECT count(*) FROM u_person_power WHERE power_id NOT IN (SELECT power_id FROM u_power)",
        "team.base_location_id": "SELECT count(*) FROM u_team WHERE base_location_id NOT IN (SELECT location_id FROM u_location)",
        "team_member.team_id": "SELECT count(*) FROM u_team_member WHERE team_id NOT IN (SELECT team_id FROM u_team)",
        "team_member.person_id": "SELECT count(*) FROM u_team_member WHERE person_id NOT IN (SELECT person_id FROM u_person)",
        "location.parent_id": """SELECT count(*) FROM u_location c LEFT JOIN u_location p ON c.parent_id = p.location_id
                                 WHERE (c.kind = 'city' AND c.parent_id IS NOT NULL)
                                    OR (c.kind = 'district' AND p.kind IS DISTINCT FROM 'city')
                                    OR (c.kind = 'site' AND p.kind IS DISTINCT FROM 'district')""",
    }
    assert {k: one(con, q) for k, q in checks.items()} == {k: 0 for k in checks}


def test_time_only_moves_forward(world):
    """A power is gained after birth and lost after it's gained; today's villains held theirs before their first
    sentence; membership ends after it starts."""
    _, con = world
    assert one(con, """SELECT count(*) FROM u_person_power pp JOIN u_person p USING (person_id)
                       WHERE pp.acquired_on < p.birth_date OR pp.lost_on <= pp.acquired_on""") == 0
    assert one(con, """SELECT count(*) FROM u_person_power pp JOIN u_person p USING (person_id)
                       JOIN lake.gold.dim_person d USING (sid_number) WHERE pp.acquired_on > d.first_sentence_date""") == 0
    assert one(con, "SELECT count(*) FROM u_team_member WHERE left_on <= joined_on") == 0


def test_every_date_is_real_or_unknown(world):
    """Dates the world makes fall between 1900 and the course's today (2026-09-24), or are null where the lake
    doesn't know enough: a person with no sentence date on record gets no power or joining date, not a made-up one."""
    _, con = world
    for t, col in [("person_power", "acquired_on"), ("person_power", "lost_on"), ("team_member", "joined_on"),
                   ("team_member", "left_on"), ("team", "founded_on")]:
        assert one(con, f"SELECT count(*) FROM u_{t} WHERE {col} NOT BETWEEN DATE '1900-01-01' AND DATE '2026-09-24'") == 0, col
    unknown = "(SELECT sid_number FROM lake.gold.dim_person WHERE first_sentence_date IS NULL)"
    assert one(con, f"""SELECT count(*) FROM u_person_power JOIN u_person USING (person_id)
                        WHERE sid_number IN {unknown} AND acquired_on IS NOT NULL""") == 0
    assert one(con, f"""SELECT count(*) FROM u_team_member JOIN u_person USING (person_id)
                        WHERE sid_number IN {unknown} AND joined_on IS NOT NULL""") == 0


def test_roles_hold_the_powers_and_teams_they_should(world):
    """Civilians hold no powers; a secondary hero's power is weak, temporary or borrowed; hero teams have only
    heroes, villain teams only villains."""
    _, con = world
    assert one(con, "SELECT count(*) FROM u_person_power JOIN u_person USING (person_id) WHERE role = 'civilian'") == 0
    assert one(con, """SELECT count(*) FROM u_person_power pp JOIN u_person p USING (person_id) JOIN u_power w USING (power_id)
                       WHERE p.role = 'secondary_hero' AND w.tier <> 'DELTA' AND pp.source NOT IN ('artifact', 'borrowed', 'temporary')""") == 0
    assert one(con, """SELECT count(*) FROM u_team_member m JOIN u_team t USING (team_id) JOIN u_person p USING (person_id)
                       WHERE (t.side = 'hero' AND p.role NOT IN ('hero', 'secondary_hero'))
                          OR (t.side = 'villain' AND p.role <> 'villain')""") == 0
    roles = dict(con.execute("SELECT role, count(*) FROM u_person GROUP BY role").fetchall())
    assert set(roles) == {"villain", "hero", "secondary_hero", "civilian"}


# --- catalogs ----------------------------------------------------------------------------------------------------

def test_powers_include_every_power_held_today(world):
    """400 distinct powers, each with a known tier and permanence, including all 88 held in the course data."""
    _, con = world
    assert one(con, "SELECT count(DISTINCT name) FROM u_power") == 400
    assert one(con, """SELECT count(*) FROM (SELECT DISTINCT trim(upper(unnest(string_split(superpower, ';')))) AS name
                       FROM lake.silver.hero_offenders) s WHERE name <> '' AND name NOT IN (SELECT name FROM u_power)""") == 0
    assert one(con, "SELECT count(*) FROM u_power WHERE tier NOT IN ('DELTA', 'BETA', 'ALPHA', 'OMEGA')") == 0


def test_places_cover_the_course_cities_and_every_custody_unit(world):
    """20,000 places; every course city is a city, and every custody unit is a site carrying its unit code."""
    _, con = world
    assert one(con, """SELECT count(*) FROM (SELECT DISTINCT unit_code FROM lake.silver.hero_offenders WHERE unit_code IS NOT NULL)
                       WHERE unit_code NOT IN (SELECT unit_code FROM u_location WHERE kind = 'site' AND unit_code IS NOT NULL)""") == 0
    assert one(con, "SELECT count(*) FROM u_location WHERE unit_code IS NOT NULL") == 106
    cities = {r[0] for r in con.execute("SELECT name FROM u_location WHERE kind = 'city'").fetchall()}
    assert set(core.course_constants()["CITIES"]) <= cities
    assert one(con, "SELECT count(*) FROM u_location WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180") == 0


def test_teams_include_the_twelve_stg_groups_as_villain_teams(world):
    """300 teams; each of the course's 12 STG groups is a villain team under the same name."""
    _, con = world
    groups = set(core.course_constants()["STG_GROUPS"])
    assert len(groups) == 12
    assert {r[0] for r in con.execute("SELECT stg_group FROM u_team WHERE side = 'villain' AND stg_group IS NOT NULL").fetchall()} == groups
