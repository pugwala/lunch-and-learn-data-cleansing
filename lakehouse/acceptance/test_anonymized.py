"""Acceptance test: the lake and the repo carry no tie-back to a real organization.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_anonymized.py

The course data is fictional; its record key is offender_number. This checks every place the lake keeps names (the
DuckLake catalog, current and historical column versions that time travel can reach; every Parquet file; the landing
CSVs) and runs the repo guard. The banned-word pattern is read from tools/tieback_check.sh, so it lives in one place.
"""
import os
import re
import subprocess
from pathlib import Path

import duckdb
import psycopg2
import pytest

COURSE = Path(__file__).resolve().parents[2]
LAKE = Path(os.environ.get("LAKE_ROOT", "/lake"))
GUARD = COURSE / "tools" / "tieback_check.sh"


def banned():
    """The guard's banned-word pattern, as a case-insensitive regex."""
    line = next(l for l in GUARD.read_text().splitlines() if l.startswith("PATTERN="))
    return re.compile(line.split("=", 1)[1].strip("'"), re.IGNORECASE)


@pytest.fixture(scope="module")
def catalog():
    """A connection to the DuckLake catalog database in Postgres, as the role the loader uses."""
    conn = psycopg2.connect(host="catalog-pg", dbname="ducklake", user="lake_rw", password=os.environ["PGPASSWORD"])
    yield conn
    conn.close()


def test_catalog_has_no_banned_names_in_any_version(catalog):
    """No column, current or historical, has a banned name: time travel can't bring one back."""
    with catalog.cursor() as cur:
        cur.execute("SELECT column_name FROM ducklake_column")
        names = [r[0] for r in cur.fetchall()]
    assert names, "the catalog has no columns: has the lake been loaded?"
    assert not [n for n in names if banned().search(n)]


def test_catalog_uses_offender_number(catalog):
    """The record key is offender_number in the current version of the tables."""
    with catalog.cursor() as cur:
        cur.execute("SELECT count(*) FROM ducklake_column WHERE column_name = 'offender_number' AND end_snapshot IS NULL")
        assert cur.fetchone()[0] > 0


def test_parquet_files_have_no_banned_names():
    """Every Parquet file under the lake's data folder is free of banned column names."""
    files = sorted((LAKE / "data").rglob("*.parquet"))
    assert files, "no Parquet files: has the lake been loaded?"
    names = {r[0] for r in duckdb.connect().execute("SELECT DISTINCT name FROM parquet_schema(?)",
                                                    [[str(f) for f in files]]).fetchall()}
    assert not {n for n in names if banned().search(n)}


def test_landing_csvs_have_no_banned_names():
    """The generated landing files carry clean headers."""
    files = sorted((LAKE / "landing").glob("*.csv"))
    assert files, "no landing CSVs: has the generator run?"
    for f in files:
        with f.open() as fh:
            assert not banned().search(fh.readline()), f"{f.name} header has a banned name"


def test_repo_guard_passes():
    """tools/tieback_check.sh finds nothing in the repo (git-tracked files and PDF text)."""
    r = subprocess.run(["git", "-c", "safe.directory=*", "-C", str(COURSE), "status", "--porcelain"],
                       capture_output=True, text=True)
    assert r.returncode == 0, f"git can't read the repo here: {r.stderr}"   # else the guard would check nothing
    g = subprocess.run(["bash", str(GUARD)], cwd=COURSE, capture_output=True, text=True,
                       env={**os.environ, "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "safe.directory",
                            "GIT_CONFIG_VALUE_0": "*"})
    assert g.returncode == 0, g.stdout + g.stderr
