"""Acceptance test for the custody mart (data products, chunk 2).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_custody_marts.py

mart_custody.custody_snapshot_monthly is a periodic snapshot: one row per offender record per month-end it is open
(sentence_date on or before the month-end, projected_release_date after it), carrying the SID, so the semantic layer
can count distinct people at any breakdown without counting a person with two open records twice. People counts are
never stored, because they don't add up across units or levels. mart_custody.snapshot_coverage says what the
snapshot leaves out: records with no sentence or release date can't be placed in time. Built by dbt from gold,
with an enforced contract, and no restricted or personal columns.
"""
import base64
import datetime as dt
import os
from pathlib import Path

import pytest
import requests
import yaml

from lakehouse import lake

LAKEHOUSE = Path(__file__).resolve().parents[1]
MARTS = LAKEHOUSE / "dbt" / "models" / "marts" / "custody"
OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata") + "/api/v1"
SNAPSHOT = "mart_custody.custody_snapshot_monthly"
SNAPSHOT_COLUMNS = ["month_end", "offender_number", "sid_number", "unit_code", "custody_level", "threat_level",
                    "offense_code", "offense_against", "violence_code"]


@pytest.fixture(scope="module")
def con():
    """A read-only connection to the lake (as lake_ro, like Cube), shared by this module's tests."""
    c = lake.lake_ro_connect()
    yield c
    c.close()


def scalar(con, sql):
    """First column of the first row of a query."""
    return con.execute(sql).fetchone()[0]


def last_complete_month_end():
    """The last month-end before today: the snapshot covers complete months only."""
    first = dt.date.today().replace(day=1)
    return first - dt.timedelta(days=1)


def test_one_row_per_record_per_open_month_end(con):
    """The grain is (month_end, offender_number), and it holds exactly the record-months gold says are open."""
    assert scalar(con, f"SELECT count(*) FROM {SNAPSHOT}") == scalar(con, f"""
        SELECT count(*) FROM (SELECT DISTINCT month_end FROM {SNAPSHOT}) m JOIN gold.fct_offender_record f
          ON f.sentence_date <= m.month_end AND f.projected_release_date > m.month_end""")
    assert scalar(con, f"SELECT count(*) - count(DISTINCT (month_end, offender_number)) FROM {SNAPSHOT}") == 0


def test_month_ends_run_from_the_first_record_to_the_last_complete_month(con):
    """Every month-end from the first sentence's month to the last complete month, and nothing else."""
    first, last, n, not_ends = con.execute(f"""SELECT min(month_end), max(month_end), count(DISTINCT month_end),
        count(*) FILTER (WHERE month_end <> last_day(month_end)) FROM {SNAPSHOT}""").fetchone()
    assert first == scalar(con, "SELECT last_day(min(sentence_date)) FROM gold.fct_offender_record")
    assert last == last_complete_month_end()
    assert n == (last.year - first.year) * 12 + last.month - first.month + 1
    assert not_ends == 0


def test_people_and_records_differ_as_they_should(con):
    """On 2026-09-30, 84,106 records were open, held by 81,409 people: counting records would overcount."""
    records, people = con.execute(f"SELECT count(*), count(DISTINCT sid_number) FROM {SNAPSHOT} "
                                  "WHERE month_end = DATE '2026-09-30'").fetchone()
    assert (records, people) == (84_106, 81_409)


def test_coverage_says_what_the_snapshot_leaves_out(con):
    """snapshot_coverage: every record is either placed in time or counted as undatable; unknown SIDs counted."""
    row = con.execute("SELECT records_total, records_placed, records_without_dates, records_without_sid, "
                      "first_month_end, last_month_end FROM mart_custody.snapshot_coverage").fetchall()
    assert len(row) == 1
    total, placed, undated, no_sid, first, last = row[0]
    assert total == 100_000 and undated == 5_860 and placed + undated == total
    assert no_sid == scalar(con, "SELECT count(*) FROM gold.fct_offender_record WHERE sid_number IS NULL")
    assert (first, last) == con.execute(f"SELECT min(month_end), max(month_end) FROM {SNAPSHOT}").fetchone()


def test_contract_is_enforced_with_a_type_for_every_column():
    """dbt enforces the snapshot's contract: its columns, in order, each with a declared type."""
    doc = yaml.safe_load((MARTS / "schema.yml").read_text())
    model = next(m for m in doc["models"] if m["name"] == "custody_snapshot_monthly")
    assert model["config"]["contract"]["enforced"] is True
    assert [c["name"] for c in model["columns"]] == SNAPSHOT_COLUMNS
    assert all(c.get("data_type") for c in model["columns"])


def test_no_restricted_or_personal_columns(con):
    """The mart carries no race, religion, STG or personal columns, so anyone may use it."""
    cols = {r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_catalog = "
                                      "'lake' AND table_schema = 'mart_custody'").fetchall()}
    tagged = set()
    for f in (LAKEHOUSE / "dbt" / "models").rglob("*.yml"):
        doc = yaml.safe_load(f.read_text()) or {}
        tables = [t for s in doc.get("sources", []) for t in s.get("tables", [])] + doc.get("models", [])
        tagged |= {c["name"] for t in tables for c in t.get("columns", []) if (c.get("meta") or {}).get("sensitivity")}
    assert cols and not cols & tagged, cols & tagged


def test_openmetadata_catalogs_the_mart():
    """om_sync.py covers the new schema: both mart tables are in OpenMetadata, described."""
    r = requests.post(f"{OM}/users/login", timeout=30, json={
        "email": "admin@open-metadata.org",
        "password": base64.b64encode(os.environ["OM_ADMIN_PASSWORD"].encode()).decode()})
    headers = {"Authorization": f"Bearer {r.json()['accessToken']}"}
    for table in ("custody_snapshot_monthly", "snapshot_coverage"):
        t = requests.get(f"{OM}/tables/name/lakehouse.lake.mart_custody.{table}", headers=headers, timeout=30)
        assert t.status_code == 200 and t.json().get("description"), table
