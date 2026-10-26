"""Acceptance test for Cube, the governed query path over gold (Phase 4).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_cube.py

Cube reads DuckLake read-only, as the lake_ro role, with the DuckDB 1.5.5 bundled in Cube v1.8.3 (a read-only
exception to "DuckDB 1.5.6 everywhere"). Its SQL API speaks the Postgres wire protocol at cube:15432, which is how
Superset and other tools will query it. Columns dbt tags restricted are in the model but masked (***) for everyone
except the restricted_reader group (Phase 5); the personal fields gold leaves out never appear.
"""
import os
import time
from pathlib import Path

import jwt

import psycopg2
import pytest
import requests
import yaml

from test_daily_run import EXPECTED_DIMENSIONS, PERSONAL_COLUMNS, TRUST_TEST

LAKEHOUSE = Path(__file__).resolve().parents[1]
CUBE_SQL = {"host": os.environ.get("CUBE_SQL_HOST", "cube"), "port": int(os.environ.get("CUBE_SQL_PORT", "15432")),
            "user": os.environ.get("CUBE_SQL_USER", "lake"), "password": os.environ.get("CUBE_SQL_PASSWORD", ""),
            "dbname": "cube"}


def restricted_columns():
    """Columns of gold.fct_offender_record that dbt tags meta.sensitivity: restricted."""
    doc = yaml.safe_load((LAKEHOUSE / "dbt" / "models" / "gold" / "schema.yml").read_text())
    fct = next(m for m in doc["models"] if m["name"] == "fct_offender_record")
    return {c["name"] for c in fct["columns"] if (c.get("meta") or {}).get("sensitivity") == "restricted"}


@pytest.fixture(scope="module")
def cube():
    """A connection to Cube's Postgres-wire SQL API, shared by this module's tests."""
    conn = psycopg2.connect(**CUBE_SQL, connect_timeout=10)
    conn.autocommit = True
    yield conn
    conn.close()


def rows(conn, sql):
    """All rows of a query, as a list of tuples."""
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


# --- configuration ---------------------------------------------------------------------------------------------

def test_cube_reads_the_lake_read_only_as_lake_ro():
    """Compose hands Cube only the read-only password, and cube.js attaches DuckLake READ_ONLY as lake_ro."""
    env = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]["cube"]["environment"]
    assert "LAKE_RO_PASSWORD" in str(env["PGPASSWORD"])
    assert "LAKE_RW" not in str(env)
    config = (LAKEHOUSE / "cube" / "cube.js").read_text()
    assert "user=lake_ro" in config and "READ_ONLY" in config
    assert "lake_rw" not in config


# --- answers ---------------------------------------------------------------------------------------------------

def test_cube_counts_every_gold_record(cube):
    """The record count through Cube equals gold: all 100,000 offenders."""
    assert rows(cube, "SELECT MEASURE(record_count) FROM offender_records") == [(100_000,)]


def test_cube_scorecard_pass_rates_match_gold(cube):
    """Cube's per-dimension pass rate gives the same numbers as gold.dq_scorecard."""
    got = rows(cube, "SELECT dimension, MEASURE(pass_rate) FROM dq_scorecard GROUP BY 1")
    assert {d: round(float(p), 2) for d, p in got} == EXPECTED_DIMENSIONS


def test_cube_shows_only_disciplinary_points_under_the_floor(cube):
    """The one rule under the 95% quality floor is visible through Cube, as in gold."""
    below = rows(cube, "SELECT dimension, column_name, score_pct FROM dq_scorecard WHERE score_pct < 95 GROUP BY 1, 2, 3")
    assert [(d, c, float(s)) for d, c, s in below] == [("completeness", "disciplinary_points", 94.15)]


def test_cube_serves_the_trust_test(cube):
    """The Trust Test scorecard reaches Cube unchanged, so dashboards can show the payoff of sessions 1 to 4."""
    got = rows(cube, "SELECT question_id, question, as_loaded, cleansed, changed FROM trust_test "
                     "GROUP BY 1, 2, 3, 4, 5 ORDER BY 1")
    assert [tuple(r) for r in got] == [(*row, True) for row in TRUST_TEST]


# --- governance ------------------------------------------------------------------------------------------------

MASK = "***"
CUBE_REST = os.environ.get("CUBE_REST", "http://cube:4000/cubejs-api/v1")


def masked_members():
    """The offender_records members every group gets masked: the "*" policy's member_masking includes."""
    doc = yaml.safe_load((LAKEHOUSE / "cube" / "model" / "offender_records.yml").read_text())
    cube_ = next(c for c in doc["cubes"] if c["name"] == "offender_records")
    policy = next(p for p in cube_.get("access_policy", []) if p.get("group") == "*")
    return set(policy["member_masking"]["includes"])


def rest_load(groups, dimension):
    """record_count by one dimension through Cube's REST API, with a token for the given groups."""
    claims = {"exp": int(time.time()) + 300, **({"groups": groups} if groups is not None else {})}
    token = jwt.encode(claims, os.environ["CUBEJS_API_SECRET"], algorithm="HS256")
    r = requests.post(f"{CUBE_REST}/load", headers={"Authorization": token}, timeout=60,
                      json={"query": {"measures": ["offender_records.record_count"],
                                      "dimensions": [f"offender_records.{dimension}"]}})
    r.raise_for_status()
    return {row[f"offender_records.{dimension}"]: int(row["offender_records.record_count"]) for row in r.json()["data"]}


def test_cube_exposes_no_personal_columns(cube):
    """Names, aliases, birth dates and notes are never in Cube's model (gold leaves them out)."""
    cols = {c for (c,) in rows(cube, "SELECT column_name FROM information_schema.columns "
                                     "WHERE table_name = 'offender_records'")}
    assert cols, "Cube has no offender_records cube"
    assert not cols & PERSONAL_COLUMNS


def test_masks_cover_the_restricted_columns():
    """Cube masks at least the columns dbt tags restricted. OpenMetadata can restrict more (a steward's tag), and
    test_masks.py checks the masks equal OpenMetadata's tags exactly."""
    assert masked_members() >= restricted_columns()


@pytest.mark.parametrize("column", sorted(restricted_columns()))
def test_restricted_columns_are_masked_through_the_sql_api(cube, column):
    """Through the SQL API (Superset's way in), a restricted column shows only its mask: one group, every record."""
    assert rows(cube, f"SELECT {column}, MEASURE(record_count) FROM offender_records GROUP BY 1") == [(MASK, 100_000)]


def test_a_plain_rest_token_sees_masks():
    """A REST token without groups (the MCP server's kind) gets the mask too."""
    assert rest_load(None, "race_code") == {MASK: 100_000}


def test_restricted_reader_sees_real_values():
    """A restricted_reader token gets the real race codes: several groups adding up to every record."""
    got = rest_load(["restricted_reader"], "race_code")
    assert MASK not in got and len(got) >= 5 and sum(got.values()) == 100_000
