"""The lake's daily load (lake.run) lands course 1's pinned handoff; it no longer runs course 1's code.

    pytest lakehouse/tests/test_lake_load.py      # inside the jupyter container (or the CI sandbox)

Course 2 starts from course 1's cleansed output (Priority 3, 2026-11-02). lake.run checks the pinned handoff against
the lock, then lands every table as it is: ref.* (the reference data), bronze.* (what course 1 received, with the
load's own metadata columns) and silver.* (what course 1 made of it). These tests load a sandbox lake, a plain DuckDB
file, never the real one.
"""
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.handoff import parity, receive  # noqa: E402
from test_universe import code_lines  # noqa: E402

pytestmark = pytest.mark.skipif(not receive.pinned_dir().exists(), reason="no published handoff here")
METADATA = ["_run_id", "_ingested_at", "_source_file", "_seed", "_data_version"]
# What running course 1's code looks like: its generator and its sessions 2-4.
COURSE_1_CODE = ["env.generate", "lnl import cleaning", "conversions", "session2", "session3", "session4",
                 "load_raw", "load_naive", "answer_key ( )", "NULL_TOKENS", "REFERENCE_DIR"]


@pytest.fixture(scope="module")
def landed(tmp_path_factory):
    """A sandbox lake loaded once by lake.run: (its metrics, a read-only connection)."""
    mp = pytest.MonkeyPatch()
    mp.setenv("LAKE_CATALOG", str(tmp_path_factory.mktemp("lake") / "lake.duckdb"))
    mp.delenv("LAKE_DATA_PATH", raising=False)
    metrics = lake.run()
    con = lake.lake_connect(read_only=True)
    yield metrics, con
    con.close()
    mp.undo()


def test_every_handoff_table_lands_as_it_is(landed):
    """Each pinned table lands under its own name with the same columns, types and rows as its handoff file
    (bronze also gets the load's metadata columns, at the end)."""
    _, con = landed
    lock = receive.read_lock()
    for table, entry in lock["tables"].items():
        path = str(receive.pinned_dir() / entry["file"])
        want = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        got = [c for c in con.execute(f"DESCRIBE lake.{table}").fetchall() if c[0] not in METADATA]
        assert [(c[0], c[1]) for c in got] == [(c[0], c[1]) for c in want], table
        cols = ", ".join(f'"{c[0]}"' for c in want)
        for a, b in ((f"lake.{table}", f"read_parquet('{path}')"), (f"read_parquet('{path}')", f"lake.{table}")):
            extra = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM {a} EXCEPT ALL SELECT {cols} FROM {b})")
            assert extra.fetchone()[0] == 0, table


def test_bronze_says_where_and_when_it_came_from(landed):
    """bronze.hero_offenders_raw carries the load's run id and the source course 1 named: the raw file, its seed and
    data version. The quality scorecard skips these columns by name."""
    metrics, con = landed
    source = receive.read_lock()["source"]
    cols = [c[0] for c in con.execute("DESCRIBE lake.bronze.hero_offenders_raw").fetchall()]
    assert cols[-5:] == METADATA
    assert con.execute("SELECT DISTINCT _run_id, _source_file, _seed, _data_version FROM lake.bronze.hero_offenders_raw"
                       ).fetchall() == [(metrics["run_id"], source["file"], source["seed"], source["data_version"])]


def test_the_load_reports_its_numbers(landed):
    """The metrics the daily run pushes: 100,000 rows in bronze and silver, every flagged value and the records they
    touch (as silver counts them), and the seven reference tables."""
    metrics, con = landed
    flagged, records = con.execute("SELECT count(*), count(DISTINCT offender_number) "
                                   "FROM lake.silver.hero_offenders_exceptions").fetchone()
    assert (metrics["rows_bronze"], metrics["rows_silver"]) == (100_000, 100_000)
    assert (metrics["exceptions"], metrics["records_with_exceptions"]) == (flagged, records)
    assert metrics["reference_tables"] == 7


def test_the_landed_lake_matches_the_course(landed):
    """The 19 numbers hold on what landed, as they did on the files."""
    _, con = landed
    con.execute("USE lake")
    assert parity.failures(con) == []


def test_a_changed_handoff_is_not_loaded(tmp_path, monkeypatch):
    """Change one byte of a copy of the handoff and point the load at it: lake.run refuses before writing anything,
    naming the table."""
    copy = tmp_path / "handoff" / receive.read_lock()["version"]
    shutil.copytree(receive.pinned_dir(), copy)
    target = copy / receive.read_lock()["tables"]["silver.trust_test"]["file"]
    data = bytearray(target.read_bytes())
    data[100] ^= 0xFF
    target.write_bytes(bytes(data))
    monkeypatch.setattr(receive, "ROOT", tmp_path / "handoff")
    monkeypatch.setenv("LAKE_CATALOG", str(tmp_path / "lake.duckdb"))
    with pytest.raises(RuntimeError, match=r"silver\.trust_test"):
        lake.run()
    assert not (tmp_path / "lake.duckdb").exists()


def test_the_lake_no_longer_runs_course_1s_code():
    """lake.py lands what course 1 published; nothing in it generates the raw file or runs sessions 2-4."""
    found = [f"lake.py:{n}: {needle}" for n, line in code_lines(Path(lake.__file__)).items()
             for needle in COURSE_1_CODE if needle in line]
    assert found == []
