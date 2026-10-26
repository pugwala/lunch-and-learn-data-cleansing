"""Acceptance test for Cube, the governed query path over gold (Phase 4).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_cube.py

Cube reads DuckLake read-only, as the lake_ro role, with the DuckDB 1.5.5 bundled in Cube v1.8.3 (a read-only
exception to "DuckDB 1.5.6 everywhere"). Its SQL API speaks the Postgres wire protocol at cube:15432, which is how
Superset and other tools will query it. Until Phase 5 adds masking by role, Cube exposes no column that dbt tags
as restricted, and never the personal fields gold already leaves out.
"""
import os
from pathlib import Path

import psycopg2
import pytest
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

def test_cube_exposes_no_personal_or_restricted_columns(cube):
    """Until masking exists (Phase 5), restricted and personal columns are not in Cube's model at all."""
    cols = {c for (c,) in rows(cube, "SELECT column_name FROM information_schema.columns "
                                     "WHERE table_name = 'offender_records'")}
    assert cols, "Cube has no offender_records cube"
    assert not cols & (PERSONAL_COLUMNS | restricted_columns())
