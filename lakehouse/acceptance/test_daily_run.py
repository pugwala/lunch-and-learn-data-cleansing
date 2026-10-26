"""Acceptance test for a finished daily_lake run: did the scheduled pipeline load the lake and build gold correctly?

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance

Run it after a daily_lake run, from the jupyter container (it reaches Dagster at dagster-webserver:3000). It lives
outside lakehouse/tests on purpose: the pipeline's blocking parity check runs everything in lakehouse/tests during
the run, before gold exists, so a post-run test there would fail and block the run it's meant to check.

Expected numbers are for the default 100,000 rows and seed 2026, as in tests/test_parity.py.
"""
import json
import os
import sys
import urllib.request
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lnl import config  # noqa: E402

pytestmark = pytest.mark.skipif((config.ROWS, config.SEED) != (100_000, 2026),
                                reason="expected numbers are for the default 100,000 rows, seed 2026")

DAGSTER = os.environ.get("DAGSTER_GRAPHQL", "http://dagster-webserver:3000/dagster/graphql")

# Overall pass rate per dimension, as built in the sandbox against the same data (handoff, Reference numbers).
EXPECTED_DIMENSIONS = {"completeness": 96.89, "conformity": 99.79, "consistency": 99.81,
                       "validity": 99.99, "uniqueness": 100.00, "timeliness": 100.00}
PERSONAL_COLUMNS = {"last_name", "first_name", "alias", "date_of_birth", "conduct_notes",
                    "last_name_std", "first_name_std", "alias_std"}
# The Trust Test scorecard (lnl.trust, notebook 4): question, answer from a default load, answer cleansed and typed.
TRUST_TEST = [
    ("Q1", "Records missing a custody level", "1536", "2991"),
    ("Q2", "Distinct gender values", "19", "3"),
    ("Q3", "Records with last name KENT", "0", "3"),
    ("Q4", "Earliest projected release date", "'\\t '", "1999-11-02"),
    ("Q5", "Does offender #02381457 exist?", "False", "True"),
    ("Q6", "Average age at sentencing", "can't compute (dates are text)", "39.0"),
    ("Q7", "Records flagged as escape risk", "5635", "14194"),
    ("Q8", "Records in protective custody", "5168", "6878"),
]


def graphql(query):
    """POST a query to Dagster's GraphQL API and return its data, failing the test on any GraphQL error."""
    req =urllib.request.Request(DAGSTER, data=json.dumps({"query": query}).encode(),
                                 headers={"Content-Type": "application/json"})
    body = json.loads(urllib.request.urlopen(req, timeout=15).read())
    assert "errors" not in body, body["errors"]
    return body["data"]


@pytest.fixture(scope="module")
def run():
    """The latest daily_lake run, as Dagster reports it."""
    data = graphql('{ runsOrError(filter: {pipelineName: "daily_lake"}, limit: 1) '
                   '{ ... on Runs { results { runId status } } } }')
    results = data["runsOrError"]["results"]
    assert results, "no daily_lake run yet: launch one from the Dagster UI (Jobs > daily_lake > Launch run)"
    return results[0]


def last_materialization_run(*path):
    """Run ID that last materialized the asset at path (e.g. "gold", "dq_scorecard"), or None if Dagster never has."""
    key =json.dumps(list(path))
    data = graphql(f'{{ assetOrError(assetKey: {{path: {key}}}) '
                   f'{{ ... on Asset {{ assetMaterializations(limit: 1) {{ runId }} }} }} }}')
    mats = (data["assetOrError"] or {}).get("assetMaterializations") or []
    return mats[0]["runId"] if mats else None


@pytest.fixture(scope="module")
def con():
    """A read-only connection to the lake, shared by this module's tests."""
    c = lake.lake_connect(read_only=True)
    yield c
    c.close()


def scalar(con, sql):
    """First column of the first row of a query."""
    return con.execute(sql).fetchone()[0]


# --- the run ---------------------------------------------------------------------------------------------------

def test_latest_daily_lake_run_succeeded(run):
    """The most recent daily_lake run finished successfully (so the blocking parity check passed too)."""
    assert run["status"] == "SUCCESS"


@pytest.mark.parametrize("asset", [("silver", "hero_offenders"), ("gold", "fct_offender_record"),
                                   ("gold", "dq_scorecard"), ("gold", "trust_test"), ("openmetadata", "lake"),
                                   ("fuseki", "glossary")])
def test_run_materialized(run, asset):
    """That same run loaded silver and built gold, rather than Dagster reporting success on stale tables."""
    assert last_materialization_run(*asset) == run["runId"], f"{'/'.join(asset)} was not built by the latest run"


# --- gold ------------------------------------------------------------------------------------------------------

def test_gold_is_built_from_the_latest_load(con):
    """The scorecard describes the newest load in ops.load_runs, not an earlier one."""
    latest_load = scalar(con, "SELECT run_id FROM ops.load_runs ORDER BY finished_at DESC LIMIT 1")
    assert scalar(con, "SELECT any_value(run_id) FROM gold.dq_scorecard") == latest_load


def test_fact_holds_every_silver_record(con):
    """Gold keeps all 100,000 records, one per offender: records are flagged, never dropped."""
    assert scalar(con, "SELECT count(*) FROM gold.fct_offender_record") == 100_000
    assert scalar(con, "SELECT count(DISTINCT offender_number) FROM gold.fct_offender_record") == 100_000


def test_gold_trust_test_is_the_course_scorecard(con):
    """gold.trust_test holds the course's Trust Test: 8 questions, every answer changed by sessions 2 to 4."""
    got = con.execute("SELECT question_id, question, as_loaded, cleansed, changed FROM gold.trust_test "
                      "ORDER BY question_id").fetchall()
    assert got == [(*row, True) for row in TRUST_TEST]


@pytest.mark.parametrize("dim", ["dim_unit", "dim_offense", "dim_stg_group"])
def test_dimensions_have_rows(con, dim):
    """Each dimension built from reference data is populated."""
    assert scalar(con, f"SELECT count(*) FROM gold.{dim}") > 0


def test_fact_leaves_out_personal_fields(con):
    """Reporting gets keys, codes and measures only: no names, aliases, birth dates or conduct notes."""
    cols = {r[0] for r in con.execute("SELECT column_name FROM information_schema.columns "
                                      "WHERE table_schema = 'gold' AND table_name = 'fct_offender_record'").fetchall()}
    assert cols, "gold.fct_offender_record has no columns"
    assert not cols & PERSONAL_COLUMNS


# --- data quality ----------------------------------------------------------------------------------------------

def test_scorecard_pass_rate_by_dimension(con):
    """Each quality dimension's overall pass rate matches what the same data scored in the sandbox."""
    rows = con.execute("SELECT dimension, round(100.0 * sum(passed) / sum(checked), 2) "
                       "FROM gold.dq_scorecard GROUP BY dimension").fetchall()
    assert {d: float(p) for d, p in rows} == EXPECTED_DIMENSIONS


def test_only_disciplinary_points_completeness_trips_the_95_floor(con):
    """Exactly what dbt's dq_score_floor test warns about: one rule, at 94.15%."""
    below = con.execute("SELECT dimension, column_name, score_pct FROM gold.dq_scorecard "
                        "WHERE score_pct < 95").fetchall()
    assert [(d, c, float(s)) for d, c, s in below] == [("completeness", "disciplinary_points", 94.15)]
