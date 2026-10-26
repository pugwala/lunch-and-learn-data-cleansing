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
    c = lake.lake_connect(read_only=True)
    yield c
    c.close()


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def test_row_counts(con):
    assert scalar(con, "SELECT count(*) FROM bronze.hero_offenders_raw") == 100_000
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders") == 100_000


def test_record_key_is_unique_bigint(con):
    assert scalar(con, "SELECT count(DISTINCT offender_number) FROM silver.hero_offenders") == 100_000
    assert scalar(con, "SELECT data_type FROM information_schema.columns "
                       "WHERE table_schema = 'silver' AND table_name = 'hero_offenders' "
                       "AND column_name = 'offender_number'") == "BIGINT"


@pytest.mark.parametrize("column, expected", [
    ("offense_code", 1379), ("unit_location", 266), ("violence_code", 104),
    ("religion", 86), ("stg_affiliation", 77), ("race", 54),
])
def test_unmapped_coded_values(con, column, expected):
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders_exceptions "
                       f"WHERE rule = 'domain_unmapped' AND column_name = '{column}'") == expected


@pytest.mark.parametrize("column, expected", [
    ("disciplinary_points", 206), ("restitution_owed", 183), ("intake_time", 185),
    ("escape_risk", 220), ("protective_custody", 0), ("dampener_required", 292),
])
def test_failed_conversions(con, column, expected):
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders_exceptions "
                       f"WHERE rule = 'conversion_failed' AND column_name = '{column}'") == expected


def test_offense_violence_conflicts(con):
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders_exceptions "
                       "WHERE rule = 'offense_violence_conflict'") == 228


def test_birth_dates_flagged(con):
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders_exceptions WHERE rule = 'dob_check'") == 8


def test_restitution_total_to_the_cent(con):
    assert str(scalar(con, "SELECT sum(restitution_owed) FROM silver.hero_offenders")) == "1333587950.77"


def test_confirmed_stg_records(con):
    assert scalar(con, "SELECT count(*) FROM silver.hero_offenders WHERE stg_status = 'Confirmed'") == 3612
