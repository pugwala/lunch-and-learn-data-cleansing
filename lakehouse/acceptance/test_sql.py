"""Acceptance test for the saved %sql connection in Jupyter (Phase 4, slice 3).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_sql.py

In any notebook, one line connects jupysql to the lake:   %load_ext lakehouse.sql
then  %sql SELECT * FROM gold.trust_test  (or a %%sql cell) queries DuckLake directly. The lake is attached
READ_ONLY as the lake_ro role. lake_ro's password comes from LAKE_RO_PASSWORD through a temporary DuckDB secret,
never the ATTACH string, because DuckLake's connection errors print that string in full.
"""
import os
import subprocess
from pathlib import Path

import psycopg2
import pytest
import yaml

from test_daily_run import TRUST_TEST

LAKEHOUSE = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def shell():
    """A real IPython shell with the extension loaded, as a notebook kernel would have it."""
    from IPython.core.interactiveshell import InteractiveShell
    ip = InteractiveShell.instance()
    ip.run_line_magic("load_ext", "lakehouse.sql")
    yield ip
    InteractiveShell.clear_instance()


def sql(shell, query):
    """Rows of a %sql query, as tuples."""
    return [tuple(r) for r in shell.run_line_magic("sql", query)]


# --- configuration ---------------------------------------------------------------------------------------------

def test_jupyter_has_the_read_only_password_and_finds_the_extension():
    """Compose gives Jupyter lake_ro's password, and notebooks in any folder can import lakehouse.sql."""
    env = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]["jupyter"]["environment"]
    assert "LAKE_RO_PASSWORD" in str(env.get("LAKE_RO_PASSWORD", ""))
    r = subprocess.run(["python", "-c", "import lakehouse.sql"], cwd="/", capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


# --- answers ---------------------------------------------------------------------------------------------------

def test_sql_magic_reads_gold(shell):
    """%sql answers from gold through the saved connection: the Trust Test's eight cleansed answers."""
    got = sql(shell, "SELECT question_id, cleansed FROM gold.trust_test ORDER BY question_id")
    assert got == [(q, cleansed) for q, _, _, cleansed in TRUST_TEST]


def test_session_recovers_after_an_error(shell):
    """A query that fails while running (a bad conversion here) doesn't break the ones after it: DuckDB aborts the
    transaction, and the extension rolls it back. (A typo fails before running, so it never aborts anything.)"""
    with pytest.raises(Exception):
        shell.run_line_magic("sql", "SELECT CAST(as_loaded AS INTEGER) FROM gold.trust_test")
    assert sql(shell, "SELECT count(*) FROM gold.trust_test") == [(8,)]


# --- governance ------------------------------------------------------------------------------------------------

def test_writes_leave_the_lake_unchanged(shell):
    """The lake is attached READ_ONLY (and lake_ro has read grants only): a CREATE through %sql fails, no table
    appears, and DuckDB reports the attach as read-only."""
    assert sql(shell, "SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'") == [(True,)]
    try:
        shell.run_line_magic("sql", "CREATE TABLE gold.should_not_exist AS SELECT 1 AS x")
    except Exception:
        pass   # expected; what matters is below
    assert sql(shell, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'should_not_exist'") == [(0,)]


def test_connects_as_lake_ro(shell):
    """The catalog session behind %sql belongs to lake_ro, the role with read grants only."""
    sql(shell, "SELECT 1")
    with psycopg2.connect(host="catalog-pg", dbname="ducklake", user="lake_rw",
                          password=os.environ["PGPASSWORD"]) as pg, pg.cursor() as cur:
        cur.execute("SELECT DISTINCT usename FROM pg_stat_activity WHERE application_name = 'lakehouse-sql'")
        assert cur.fetchall() == [("lake_ro",)]


def test_password_never_in_the_attach_string(shell):
    """lake_ro's password lives only in a DuckDB secret, so no attach error can print it."""
    paths = sql(shell, "SELECT path FROM duckdb_databases() WHERE database_name = 'lake'")
    assert paths and os.environ["LAKE_RO_PASSWORD"] not in str(paths)
