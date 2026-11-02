"""The lake must show the same numbers the class sees (FACILITATOR_GUIDE.md). Run after a load:

    pytest lakehouse/tests            # inside the jupyter container, or anywhere LAKE_CATALOG is set

A failure means the lakehouse and the course disagree, so fix that before trusting anything downstream.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.handoff import parity  # noqa: E402
from lnl import config  # noqa: E402

pytestmark = pytest.mark.skipif((config.ROWS, config.SEED) != (100_000, 2026),
                                reason="expected numbers are for the default 100,000 rows, seed 2026")


@pytest.fixture(scope="module")
def con():
    """A read-only connection to the lake, shared by this module's tests."""
    c = lake.lake_connect(read_only=True)
    yield c
    c.close()


scalar = parity.scalar


def test_bronze_holds_every_record(con):
    """Bronze holds all 100,000 raw records too: flagged values are flagged, never dropped."""
    assert scalar(con, "SELECT count(*) FROM bronze.hero_offenders_raw") == 100_000


@pytest.mark.parametrize("name, measure, expected", parity.CHECKS, ids=[c[0] for c in parity.CHECKS])
def test_the_lake_matches_the_course(con, name, measure, expected):
    """One of the 19 numbers the class sees (lakehouse/handoff/parity.py: unmapped codes and failed conversions per
    column, conflicts, flagged birth dates, restitution to the cent, Confirmed STG records, the Trust Test), measured
    on the lake's silver tables. The handoff's publish step runs the same 19 on its files."""
    assert measure(con) == expected
