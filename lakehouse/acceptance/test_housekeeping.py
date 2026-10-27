"""Acceptance test for DuckLake housekeeping: 30 days of time travel, and no more.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_housekeeping.py

Every daily load replaces each table, and DuckLake keeps every replaced file so you can query the lake as it was.
Without housekeeping that history grows forever (two days of loads left 302 of 320 Parquet files, 533 of 571 MB,
as history). lakehouse/housekeeping.py, run weekly by the Dagster job lake_housekeeping (Sundays 03:30 Central),
expires snapshots older than 30 days (Ian's choice), merges small files, deletes the files no snapshot needs once
they have waited a day, and deletes orphaned files (written but never committed) older than a week. Personal data
that changed or was deleted is therefore gone from history within a month too. The destructive steps are tested
here on a scratch DuckLake in a temporary folder, never on the lake itself.
"""
import datetime as dt
import os
import subprocess
from pathlib import Path

import duckdb
import pytest
import requests

from lakehouse import housekeeping

LAKEHOUSE = Path(__file__).resolve().parents[1]
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
FUTURE = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)   # "older than" this = everything so far


@pytest.fixture
def scratch(tmp_path):
    """A DuckLake of its own in a temporary folder, attached as `lake` like the real one: one table replaced three
    times the way the daily load does it, so it has history (old snapshots and their files) to clean up."""
    data = tmp_path / "data"
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:{tmp_path / 'catalog.ducklake'}' AS lake (DATA_PATH '{data}/')")
    con.execute("USE lake")
    con.execute("CREATE SCHEMA silver")
    for load in range(3):
        con.execute("DROP TABLE IF EXISTS silver.records")
        con.execute(f"CREATE TABLE silver.records AS SELECT range AS id, {load} AS load FROM range(1000)")
    yield con, data
    con.close()


def parquet_files(folder):
    """How many Parquet files are on disk under a folder."""
    return sum(1 for _ in Path(folder).rglob("*.parquet"))


# --- what housekeeping does (scratch lake) ---------------------------------------------------------------------

def test_housekeeping_removes_history_and_keeps_todays_data(scratch):
    """Past the retention date, every old snapshot goes and so do its files; the current data is untouched."""
    con, data = scratch
    before = housekeeping.stats(con)
    assert before["snapshots"] > 3 and before["total_files"] > before["live_files"]
    result = housekeeping.housekeep(con, keep_since=FUTURE, files_before=FUTURE, orphans_before=FUTURE)
    after = housekeeping.stats(con)
    assert after["snapshots"] == 1                                   # the current snapshot is never expired
    assert after["total_files"] == after["live_files"] == parquet_files(data)
    assert result["snapshots_expired"] == before["snapshots"] - 1 and result["files_deleted"] > 0
    assert con.execute("SELECT count(*), min(load), max(load) FROM silver.records").fetchone() == (1000, 2, 2)


def test_recent_history_is_kept(scratch):
    """Snapshots newer than the retention date stay, so time travel within 30 days still works."""
    con, _ = scratch
    before = housekeeping.stats(con)
    long_ago = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)
    result = housekeeping.housekeep(con, keep_since=long_ago, files_before=long_ago, orphans_before=long_ago)
    assert result["snapshots_expired"] == 0 and housekeeping.stats(con)["snapshots"] >= before["snapshots"]


def test_dry_run_changes_nothing(scratch):
    """--dry-run reports what each step would remove, and removes nothing."""
    con, data = scratch
    before, on_disk = housekeeping.stats(con), parquet_files(data)
    result = housekeeping.housekeep(con, keep_since=FUTURE, files_before=FUTURE, orphans_before=FUTURE, dry_run=True)
    assert result["snapshots_expired"] == before["snapshots"] - 1   # what it would expire
    assert housekeeping.stats(con) == before and parquet_files(data) == on_disk


def test_an_orphaned_file_is_deleted(scratch):
    """A Parquet file in the data path that the catalog never recorded (a write that failed half way) is deleted."""
    con, data = scratch
    stray = data / "silver" / "records" / "orphan-from-a-failed-write.parquet"
    stray.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT 1 AS id) TO '{stray}' (FORMAT parquet)")
    result = housekeeping.housekeep(con, keep_since=FUTURE, files_before=FUTURE, orphans_before=FUTURE)
    assert not stray.exists() and result["orphans_deleted"] >= 1


# --- the real lake ---------------------------------------------------------------------------------------------

def test_the_command_line_dry_run_touches_nothing():
    """`housekeeping.py --dry-run` on the real lake reports, changes nothing, and exits 0."""
    r = subprocess.run(["python", str(LAKEHOUSE / "housekeeping.py"), "--dry-run"], capture_output=True, text=True,
                       timeout=300)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "dry run" in r.stdout.lower()


def test_the_job_runs_weekly_before_the_daily_load():
    """lake_housekeeping is a Dagster job, scheduled Sundays 03:30 Central and running, so it never overlaps 05:00."""
    from test_daily_run import graphql
    nodes = graphql('{ repositoriesOrError { ... on RepositoryConnection { nodes { schedules { name cronSchedule '
                    'executionTimezone pipelineName scheduleState { status } } } } } }')["repositoriesOrError"]["nodes"]
    sched = [s for n in nodes for s in n["schedules"]]
    mine = [s for s in sched if s["pipelineName"] == "lake_housekeeping"]
    assert len(mine) == 1, sched
    assert (mine[0]["cronSchedule"], mine[0]["executionTimezone"], mine[0]["scheduleState"]["status"]) == \
        ("30 3 * * 0", "America/Chicago", "RUNNING")


def test_no_snapshot_is_older_than_the_retention_allows():
    """On the real lake the oldest snapshot is at most 30 days old, plus the week until the next weekly run."""
    from lakehouse import lake
    con = lake.lake_ro_connect()
    oldest = con.execute("SELECT min(snapshot_time) FROM ducklake_snapshots('lake')").fetchone()[0]
    con.close()
    assert dt.datetime.now(dt.timezone.utc) - oldest <= dt.timedelta(days=housekeeping.RETENTION_DAYS + 8)


def test_housekeeping_reports_the_lake_to_prometheus():
    """The job pushes the lake's snapshots and its live and total size; LakeHousekeepingStale loads."""
    def value(q):
        """An instant PromQL query's single value, or None."""
        r = requests.get(f"{PROMETHEUS}/api/v1/query", params={"query": q}, timeout=30).json()["data"]["result"]
        return float(r[0]["value"][1]) if r else None
    assert value("lakehouse_lake_snapshots") >= 1
    live, total = value("lakehouse_lake_live_bytes"), value("lakehouse_lake_total_bytes")
    assert 0 < live <= total
    assert value('lakehouse_job_last_success_timestamp_seconds{pipeline="lake_housekeeping"}') is not None
    groups = requests.get(f"{PROMETHEUS}/api/v1/rules", timeout=30).json()["data"]["groups"]
    rule = next(r for g in groups for r in g["rules"] if r["name"] == "LakeHousekeepingStale")
    assert rule["health"] == "ok"
