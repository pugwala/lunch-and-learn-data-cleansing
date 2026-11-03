"""Tests for the scale benchmark (dataset expansion, chunk 10): lakehouse/universe/bench.py.

    pytest lakehouse/tests/test_universe_bench.py    # inside the jupyter container (or CI's sandbox); reads the lake

One benchmark runs at scale S into a temporary folder: the world build, a scratch DuckLake (its own catalog file and
data folder, seeded from course 1's pinned handoff with the two tables the world's rule tests read), dbt's world models in the two runs Dagster
makes (world_full, then world_monthly), and the benchmark queries. The tests check that:
  - a scratch folder that sits in, or holds, the live lake's data or the live world is refused (a run clears it);
  - peak memory counts the whole process tree (the build's workers are child processes), and a failed step says so;
  - every step is timed and measured, and the report is written to bench.json;
  - the scratch lake is its own (catalog and Parquet in the folder) and holds every row of the world;
  - every benchmark query answers, timed over the repeats;
  - the folder sizes are reported;
  - the live lake is left as it was.
"""
import json
import os
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.universe import bench, build, scale  # noqa: E402

STEPS = ["build", "seed", "world_full", "world_monthly"]


def live_state():
    """What would show the benchmark writing to the live lake: every file in the live data path's world folders (dbt
    writes there whether it used the live catalog or only the live data path), or a plain lake file's size and mtime.
    Other jobs' writes to the live lake don't count: only the world's folders are looked at."""
    if lake.is_ducklake():
        data = Path(os.environ.get("LAKE_DATA_PATH", "/lake/data/"))
        return sorted(str(f) for schema in bench.SCHEMAS for f in (data / schema).rglob("*"))
    st = Path(lake.catalog()).stat()
    return (st.st_size, st.st_mtime_ns)


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    """One benchmark at scale S in a temporary folder, with the live lake's state before and after."""
    folder = tmp_path_factory.mktemp("bench") / "S"
    before = live_state()
    report = bench.run("S", folder, workers=4, repeats=2)
    return folder, report, before, live_state()


def test_a_folder_in_or_around_the_live_lake_is_refused(monkeypatch, tmp_path):
    """A run clears its folder's data and world, so the live data path, the live world, anything inside them or
    anything that holds them is refused; a folder of its own is fine."""
    monkeypatch.setenv("LAKE_DATA_PATH", "/lake/data/")
    for bad in ["/lake/data", "/lake/data/world_gold", "/lake", "/", str(build.DEFAULT_ROOT / "M"), str(build.DEFAULT_ROOT)]:
        with pytest.raises(ValueError):
            bench.check_folder(Path(bad))
    bench.check_folder(Path("/lake/scratch/L"))
    bench.check_folder(tmp_path / "S")


def test_peak_memory_counts_the_whole_process_tree():
    """Two grandchildren holding 200 MB each at once show as one peak of 400 MB (the kernel's own peak would only
    see the biggest single process), and a failing command is reported as failed."""
    hold = "x = bytearray(200_000_000); import time; time.sleep(1.5)"
    spawn = (f"import subprocess, sys; ps = [subprocess.Popen([sys.executable, '-c', {hold!r}]) for _ in range(2)]; "
             "[p.wait() for p in ps]")
    tree = bench.measured([sys.executable, "-c", spawn])
    assert tree["ok"] and tree["peak_mb"] >= 350, tree
    failed = bench.measured([sys.executable, "-c", "import sys; print('boom'); sys.exit(3)"])
    assert failed["ok"] is False and "boom" in failed["tail"]


def test_only_the_dbt_steps_are_pointed_at_the_scratch_lake(monkeypatch, tmp_path):
    """dbt writes to the scratch catalog and data path with the DuckLake target; the build and the seed keep the
    usual settings. (A mutant pointing dbt at the live lake is checked here, never run.)"""
    monkeypatch.setenv("LAKE_CATALOG", "ducklake:postgres:dbname=live")
    monkeypatch.setenv("LAKE_DATA_PATH", "/lake/data/")
    for name in ["world_full", "world_monthly"]:
        env = bench.step_env(name, tmp_path)
        assert env["LAKE_CATALOG"] == f"ducklake:{tmp_path}/catalog.ducklake"
        assert env["LAKE_DATA_PATH"] == f"{tmp_path}/data/" and env["DBT_TARGET"] == "lake"
    for name in ["build", "seed"]:
        assert bench.step_env(name, tmp_path)["LAKE_CATALOG"] == "ducklake:postgres:dbname=live"
    assert set(bench.steps("S", tmp_path, 2)) == set(STEPS)


def test_every_step_is_timed_measured_and_written(run):
    """build, seed, world_full and world_monthly each ran, took time and memory; bench.json holds the report."""
    folder, report, _, _ = run
    assert list(report["steps"]) == STEPS
    for name, step in report["steps"].items():
        assert step["ok"], (name, step.get("tail"))
        assert step["seconds"] > 0 and step["peak_mb"] > 0, (name, step)
    assert report["scale"] == "S" and report["world_rows"] == scale.total("S")
    assert json.loads((folder / "bench.json").read_text()) == report


def test_the_scratch_lake_is_its_own_and_holds_the_whole_world(run):
    """The catalog and the Parquet files are in the folder, and gold's facts hold every clean row of the world."""
    folder, report, _, _ = run
    assert (folder / "catalog.ducklake").is_file()
    assert any((folder / "data").rglob("*.parquet"))
    con = bench.connect(folder, read_only=True)
    try:
        for table, clean in [("fct_incident", "incident"), ("fct_participation", "participant"),
                             ("fct_damage", "damage"), ("fct_charge", "charge"), ("fct_power_use", "power_use")]:
            n = con.execute(f"SELECT count(*) FROM world_gold.{table}").fetchone()[0]
            assert n == scale.rows(clean, "S"), table
            assert report["rows"][f"world_gold.{table}"] == n
    finally:
        con.close()


def test_every_benchmark_query_answers(run):
    """Each query in bench.QUERIES returned rows, timed on every repeat."""
    _, report, _, _ = run
    assert set(report["queries"]) == set(bench.QUERIES)
    for name, q in report["queries"].items():
        assert q["rows"] > 0, name
        assert len(q["seconds"]) == 2 and min(q["seconds"]) > 0, name
        assert q["best_s"] == min(q["seconds"])


def test_folder_sizes_are_reported(run):
    """The world's clean tables, messy CSVs, answer keys and the scratch lake's Parquet each take some space."""
    _, report, _, _ = run
    for part in ["universe", "messy", "answer_key", "lake_data"]:
        assert report["sizes_mb"][part] > 0, part


def test_the_live_lake_is_left_as_it_was(run):
    """The benchmark never writes to the live lake: its state is the same after the run."""
    _, _, before, after = run
    assert before == after
