"""Acceptance test for the custody data products on Cube (data products, chunk 3).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_custody_cube.py

Three views, one per product question, over a hidden custody_snapshot cube (mart_custody.custody_snapshot_monthly)
and the existing offender_records cube: custody_population (who is held, by unit and level), custody_offenses (what
they are held for) and custody_admissions (new sentences by offense). People are counted as distinct SIDs at query
time, so a person held under two records is one person at any breakdown. No view exposes a restricted or personal
member. The MCP server's query_metric picks the view that has both the metric and the dimensions asked for.
"""
import os
from pathlib import Path

import psycopg2
import pytest
import requests

from test_cube import CUBE_SQL, rest_load
from test_daily_run import PERSONAL_COLUMNS
from test_mcp import call

LAKEHOUSE = Path(__file__).resolve().parents[1]
CUBE_REST = os.environ.get("CUBE_REST", "http://cube:4000/cubejs-api/v1")
VIEWS = {"custody_population", "custody_offenses", "custody_admissions"}
RESTRICTED = {"race_code", "race_omb", "religion_group", "religion_family", "stg_group", "stg_status"}
SEPT = "month_end = CAST('2026-09-30' AS DATE)"


@pytest.fixture(scope="module")
def cube():
    """A connection to Cube's SQL API, as Superset uses it."""
    conn = psycopg2.connect(**CUBE_SQL, connect_timeout=10)
    conn.autocommit = True
    yield conn
    conn.close()


def rows(conn, sql):
    """All rows of a query through Cube's SQL API."""
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def meta():
    """Cube's /meta, as a plain-token REST client sees it."""
    import jwt
    import time
    token = jwt.encode({"exp": int(time.time()) + 300}, os.environ["CUBEJS_API_SECRET"], algorithm="HS256")
    return requests.get(f"{CUBE_REST}/meta", headers={"Authorization": token}, timeout=30).json()["cubes"]


def test_the_three_views_are_public_and_the_snapshot_cube_is_not():
    """Consumers see the product views; the cube under them stays hidden."""
    names = {c["name"] for c in meta()}
    assert VIEWS <= names
    assert "custody_snapshot" not in names


def test_population_counts_people_not_records(cube):
    """On 2026-09-30: 84,106 open records, held by 81,409 people."""
    got = rows(cube, f"SELECT MEASURE(records_in_custody), MEASURE(people_in_custody) FROM custody_population WHERE {SEPT}")
    assert got == [(84_106, 81_409)]


def test_people_by_unit_are_distinct_not_summed(cube):
    """By unit, a person held in two units counts in each (81,413 in all), yet the total stays 81,409: Cube
    counts distinct SIDs at query time instead of adding up stored counts."""
    by_unit = rows(cube, f"SELECT unit_code, unit_name, MEASURE(people_in_custody) FROM custody_population "
                         f"WHERE {SEPT} GROUP BY 1, 2")
    assert sum(n for _, _, n in by_unit) == 81_413
    # every unit code has its name; the one nameless group is records whose unit couldn't be mapped (no code)
    assert [(c, n) for c, name, n in by_unit if not name] == [(None, 2_611)]


def test_offenses_profile_the_same_population(cube):
    """By offense category, the records add up to the population's 84,106; Property is the largest category."""
    got = dict(rows(cube, f"SELECT offense_against, MEASURE(records_in_custody) FROM custody_offenses WHERE {SEPT} GROUP BY 1"))
    assert sum(got.values()) == 84_106
    assert got["Property"] == 30_212 and got["Person"] == 29_483


def test_admissions_by_sentence_month(cube):
    """September 2026 saw 1,557 new sentences, for 1,510 people."""
    got = rows(cube, "SELECT MEASURE(admissions), MEASURE(people_admitted) FROM custody_admissions "
                     "WHERE sentence_date >= CAST('2026-09-01' AS DATE) AND sentence_date < CAST('2026-10-01' AS DATE)")
    assert got == [(1_557, 1_510)]


def test_no_view_exposes_restricted_or_personal_members():
    """The products carry no race, religion, STG or personal members, so no masking is needed to share them."""
    for view in (c for c in meta() if c["name"] in VIEWS):
        members = {m["name"].split(".", 1)[1] for kind in ("dimensions", "measures") for m in view[kind]}
        assert not members & (RESTRICTED | PERSONAL_COLUMNS | {"sid_number"}), (view["name"], members)


def test_a_restricted_reader_token_still_reads_the_products():
    """The views work for every group (rest_load with restricted_reader still answers record counts)."""
    assert sum(rest_load(["restricted_reader"], "custody_level").values()) == 100_000


def test_mcp_picks_the_view_that_has_the_dimension():
    """people_in_custody lives in two views; asked by offense category, the MCP server uses custody_offenses, and by
    unit, custody_population."""
    is_error, result, text = call("query_metric", metric="people_in_custody", group_by=["offense_against"])
    assert not is_error, text
    assert {r["offense_against"] for r in result["rows"]} >= {"Person", "Property", "Society", "Group B"}
    is_error, result, text = call("query_metric", metric="people_in_custody", group_by=["unit_name"])
    assert not is_error and len(result["rows"]) > 50, text


def test_mcp_still_finds_record_count():
    """Adding views didn't make the existing short names ambiguous."""
    is_error, result, text = call("query_metric", metric="record_count")
    assert not is_error and result["rows"] == [{"record_count": 100_000}], text
