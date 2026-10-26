"""The lake must show the same numbers the class sees (FACILITATOR_GUIDE.md). Run after a load:

    pytest lakehouse/tests            # inside the jupyter container, or anywhere LAKE_CATALOG is set

A failure means the lakehouse and the course disagree, so fix that before trusting anything downstream.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lnl import config  # noqa: E402

pytestmark = pytest.mark.skipif((config.ROWS, config.SEED) != (100_000, 2026),
                                reason="expected numbers are for the default 100,000 rows, seed 2026")


@pytest.fixture(scope="module")
def con():
    """A read-only connection to the lake, shared by this module's tests."""
    c = lake.lake_connect(read_only=True)
    yield c
    c.close()


def scalar(con, sql):
    """First column of the first row of a query."""
    return con.execute(sql).fetchone()[0]


def test_row_counts(con):
    """Bronze and silver each hold all 100,000 records: flagged values are flagged, never dropped."""
    assert scalar(con, "SELECT count(*) FROM bronze.hero_offenders_raw") == 100_000
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders") == 100_000


def test_record_key_is_unique_bigint(con):
    """offender_number is unique across silver and typed BIGINT."""
    assert scalar(con, "SELECT count(DISTINCT offender_number) FROM silver.hero_offenders") == 100_000
    assert scalar(con, "SELECT data_type FROM information_schema.columns "
                       "WHERE table_schema = 'silver' AND table_name = 'hero_offenders' "
                       "AND column_name = 'offender_number'") == "BIGINT"


@pytest.mark.parametrize("column, expected", [
    ("offense_code", 1379), ("unit_location", 266), ("violence_code", 104),
    ("religion", 86), ("stg_affiliation", 77), ("race", 54),
])
def test_unmapped_coded_values(con, column, expected):
    """Coded values with no mapping in the reference tables, per column, as the Facilitator Guide counts them."""
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders_exceptions "
                       f"WHERE rule = 'domain_unmapped' AND column_name = '{column}'") == expected


@pytest.mark.parametrize("column, expected", [
    ("disciplinary_points", 206), ("restitution_owed", 183), ("intake_time", 185),
    ("escape_risk", 220), ("protective_custody", 0), ("dampener_required", 292),
])
def test_failed_conversions(con, column, expected):
    """Values that failed type conversion, per column, as the Facilitator Guide counts them."""
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders_exceptions "
                       f"WHERE rule = 'conversion_failed' AND column_name = '{column}'") == expected


def test_offense_violence_conflicts(con):
    """Records whose offense and violence codes disagree: 228."""
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders_exceptions "
                       "WHERE rule = 'offense_violence_conflict'") == 228


def test_birth_dates_flagged(con):
    """Implausible birth dates flagged by the date-of-birth check: 8."""
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders_exceptions WHERE rule = 'dob_check'") == 8


def test_restitution_total_to_the_cent(con):
    """Total restitution owed, exactly to the cent: 1,333,587,950.77."""
    assert str(scalar(con, "SELECT sum(restitution_owed) FROM silver.hero_offenders")) == "1333587950.77"


def test_confirmed_stg_records(con):
    """Records with a Confirmed STG status: 3,612."""
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders WHERE stg_status = 'Confirmed'") == 3612


def test_trust_test_all_eight_answers_change(con):
    """Session 4's contract: all 8 Trust Test answers change between a default load and the cleansed, typed data.
    Q1 and Q2 are the guide's own numbers (1,536 to 2,991 and 19 to 3)."""
    got = {q: (a, b, changed) for q, a, b, changed in
           con.execute("SELECT question_id, as_loaded, cleansed, changed FROM silver.trust_test").fetchall()}
    assert sorted(got) == [f"Q{i}" for i in range(1, 9)]
    assert all(changed for _, _, changed in got.values())
    assert got["Q1"][:2] == ("1536", "2991") and got["Q2"][:2] == ("19", "3")
