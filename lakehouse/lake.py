"""DuckLake lakehouse helpers: land course 1's handoff in reference, bronze and silver tables.

Course 2 (this lakehouse) starts where course 1 (the cleansing course) ends: course 1 publishes its cleansed output
as pinned files (lakehouse/handoff), and run() lands the pinned version as it is, after checking every file against
the lock. Nothing here runs course 1's code: its generator and sessions 2-4 run on course 1's side, in
lakehouse/handoff/publish.py, which only publishes when the 19 Facilitator Guide numbers match. So the lakehouse and
the class still agree on every standardized value.

Where the lake lives comes from two environment variables:
  LAKE_CATALOG    ducklake:postgres:dbname=ducklake host=catalog-pg user=lake_rw   (the real setup)
                  ducklake:/path/to/catalog.ducklake                               (DuckLake, DuckDB-file catalog)
                  /path/to/lake.duckdb                                             (plain DuckDB file, for testing)
  LAKE_DATA_PATH  where DuckLake writes Parquet, e.g. /lake/data/   (ignored for a plain DuckDB file)
The Postgres password comes from PGPASSWORD, never from the attach string, so it never lands in a log.

Nothing here drops or fixes a record. Exceptions are flagged in silver.hero_offenders_exceptions and the
records stay in silver, the same "flag, don't fix" rule the course teaches.
"""
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from lnl import env

LAKE = "lake"
SCHEMAS = ["ref", "bronze", "silver", "gold", "ops"]
SENSITIVE_COLUMNS = ["race", "race_code", "race_omb", "religion", "religion_group", "religion_family",
                     "stg_affiliation", "stg_group", "stg_status"]

TABLE_COMMENTS = {
    "bronze.hero_offenders_raw": "The raw file course 1 received, exactly as written: every column VARCHAR, nothing "
                                 "converted. Landed from course 1's handoff, with the load's metadata columns.",
    "bronze.conduct_notes_answer_key": "True sentiment of each conduct note, as written. Scoring only.",
    "silver.hero_offenders": "Course 1's cleansed records (its sessions 2-4), landed from its pinned handoff. "
                             "offender_number is the BIGINT record key. Ordered codes are VARCHAR with a matching "
                             "_rank column.",
    "silver.hero_offenders_exceptions": "Every value a rule flagged, one row each. Records are never dropped.",
    "silver.conduct_notes_answer_key": "Answer key with offender_number as BIGINT so it joins to silver.",
    "silver.trust_test": "Course 1's Trust Test scorecard: each question answered from a default load "
                         "and from the cleansed, typed data. Aggregates only.",
    "ops.load_runs": "One row per load: counts, timing and the DuckLake snapshot it produced.",
}


# ---- Connection ---------------------------------------------------------------------------------
def catalog() -> str:
    """Where the lake's catalog is: LAKE_CATALOG (ducklake:postgres:... on the VM), else a DuckDB file in the data dir."""
    return os.environ.get("LAKE_CATALOG", str(env.data_dir() / "lake.duckdb"))


def is_ducklake() -> bool:
    """True when the catalog is DuckLake (on the VM); False for a plain DuckDB file (a laptop or the sandbox)."""
    return catalog().startswith("ducklake:")


def lake_connect(read_only: bool = False):
    """A DuckDB connection with the lake attached as `lake` and selected, so `SELECT * FROM silver.x` works."""
    import duckdb
    con = duckdb.connect()
    target = catalog()
    options = []
    if is_ducklake():
        con.execute("INSTALL ducklake; LOAD ducklake;")
        if target.startswith("ducklake:postgres:"):
            con.execute("INSTALL postgres; LOAD postgres;")
        data_path = os.environ.get("LAKE_DATA_PATH")
        if data_path:
            options.append(f"DATA_PATH '{data_path}'")
    else:
        Path(target).parent.mkdir(parents=True, exist_ok=True)
    if read_only:
        options.append("READ_ONLY")
    opts = f" ({', '.join(options)})" if options else ""
    con.execute(f"ATTACH '{target}' AS {LAKE}{opts}")
    con.execute(f"USE {LAKE}")
    return con


def attach_lake_ro(con) -> None:
    """Attach DuckLake READ_ONLY as the lake_ro role to a DuckDB connection, and select it as `lake`.
    lake_ro's password (LAKE_RO_PASSWORD) goes into a temporary default postgres secret, never the ATTACH string:
    DuckLake's connection errors print that string in full. The catalog session is tagged
    application_name=lakehouse-sql so it can be told apart from Cube's, which is also lake_ro.
    USE only reaches the connection that runs it, so every new connection must call this (see lakehouse/sql_magic.py)."""
    if not catalog().startswith("ducklake:postgres:"):   # sandbox: a plain DuckDB file has no roles
        con.execute(f"ATTACH {_lit(catalog())} AS {LAKE} (READ_ONLY)")
        con.execute(f"USE {LAKE}")
        return
    params = dict(p.split("=", 1) for p in catalog()[len("ducklake:postgres:"):].split())
    params.update(user="lake_ro", application_name="lakehouse-sql")
    con.execute("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;")
    con.execute(f"CREATE TEMPORARY SECRET (TYPE postgres, HOST {_lit(params['host'])}, DATABASE {_lit(params['dbname'])}, "
                f"USER 'lake_ro', PASSWORD {_lit(os.environ['LAKE_RO_PASSWORD'])})")
    options = ["READ_ONLY"]
    if os.environ.get("LAKE_DATA_PATH"):
        options.insert(0, f"DATA_PATH {_lit(os.environ['LAKE_DATA_PATH'])}")
    target = "ducklake:postgres:" + " ".join(f"{k}={v}" for k, v in params.items())
    con.execute(f"ATTACH {_lit(target)} AS {LAKE} ({', '.join(options)})")
    con.execute(f"USE {LAKE}")


def lake_ro_connect():
    """A DuckDB connection with the lake attached read-only as lake_ro (see attach_lake_ro)."""
    import duckdb
    con = duckdb.connect()
    attach_lake_ro(con)
    return con


def ensure_schemas(con) -> None:
    """Create the lake's schemas (ref, bronze, silver, gold, ops) if they're missing."""
    for schema in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE}.{schema}")


def snapshot_id(con):
    """Latest DuckLake snapshot (None for a plain DuckDB file)."""
    if not is_ducklake():
        return None
    try:
        return con.execute(f"SELECT max(snapshot_id) FROM ducklake_snapshots('{LAKE}')").fetchone()[0]
    except Exception:
        return None


# ---- Loading ------------------------------------------------------------------------------------
def _replace(con, table: str, view: str, frame=None) -> None:
    """Replace a lake table with the rows of a view, registering a pandas frame under that view's name first if given."""
    if frame is not None:
        con.register(view, frame)
    con.execute(f"DROP TABLE IF EXISTS {LAKE}.{table}")
    con.execute(f"CREATE TABLE {LAKE}.{table} AS SELECT * FROM {view}")
    if frame is not None:
        con.unregister(view)


def _lit(value) -> str:
    """A SQL string literal (views can't take prepared parameters)."""
    return "'" + str(value).replace("'", "''") + "'"


def _write_comments(con) -> None:
    """All table comments in one transaction (one snapshot). Cosmetic: a failure is rolled back and ignored."""
    con.execute("BEGIN TRANSACTION")
    try:
        for table, text in TABLE_COMMENTS.items():
            if not table.startswith("ops."):
                con.execute(f"COMMENT ON TABLE {LAKE}.{table} IS {_lit(text)}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")


def run() -> dict:
    """Land course 1's pinned handoff (lakehouse/handoff: checked file by file against the lock first) as ref, bronze
    and silver in one transaction = one snapshot. Every table lands as published; bronze.hero_offenders_raw also gets
    the load's metadata (run id, time, and the source course 1 named). Course 1's code isn't run here.
    Comments and the ops.load_runs row follow as two small transactions, so a load is three snapshots."""
    from lakehouse.handoff import receive
    started = time.time()
    run_id = uuid.uuid4().hex[:12]
    handoff = receive.require()
    lock = receive.read_lock()
    source = lock["source"]

    con = lake_connect()
    con.execute("BEGIN TRANSACTION")
    try:
        ensure_schemas(con)
        for table, entry in sorted(lock["tables"].items()):
            files = _lit(handoff / entry["file"])
            stamp = (f", {_lit(run_id)} AS _run_id, now() AS _ingested_at, {_lit(source['file'])} AS _source_file, "
                     f"{int(source['seed'])}::INTEGER AS _seed, {int(source['data_version'])}::INTEGER AS _data_version"
                     if table == "bronze.hero_offenders_raw" else "")
            con.execute(f"CREATE OR REPLACE TEMP VIEW handoff_src AS SELECT *{stamp} FROM read_parquet({files})")
            _replace(con, table, "handoff_src")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    data_snapshot = snapshot_id(con)
    # Comments go after the commit: inside a transaction, one failed statement aborts the whole load,
    # and a cosmetic comment must never cost a run.
    _write_comments(con)

    metrics = {
        "run_id": run_id,
        "finished_at": datetime.now(timezone.utc),
        "rows_bronze": con.execute(f"SELECT count(*) FROM {LAKE}.bronze.hero_offenders_raw").fetchone()[0],
        "rows_silver": con.execute(f"SELECT count(*) FROM {LAKE}.silver.hero_offenders").fetchone()[0],
        "exceptions": con.execute(f"SELECT count(*) FROM {LAKE}.silver.hero_offenders_exceptions").fetchone()[0],
        "records_with_exceptions": con.execute(
            f"SELECT count(DISTINCT offender_number) FROM {LAKE}.silver.hero_offenders_exceptions").fetchone()[0],
        "reference_tables": sum(t.startswith("ref.") for t in lock["tables"]),
        "seconds": round(time.time() - started, 1),
        "snapshot_id": data_snapshot,
    }
    con.execute("BEGIN TRANSACTION")
    con.execute(f"""CREATE TABLE IF NOT EXISTS {LAKE}.ops.load_runs (
        run_id VARCHAR, finished_at TIMESTAMPTZ, rows_bronze BIGINT, rows_silver BIGINT, exceptions BIGINT,
        records_with_exceptions BIGINT, reference_tables INTEGER, seconds DOUBLE, snapshot_id BIGINT)""")
    con.execute(f"INSERT INTO {LAKE}.ops.load_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(metrics.values()))
    con.execute("COMMIT")
    con.close()
    return metrics
