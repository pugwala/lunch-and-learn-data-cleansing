"""DuckLake lakehouse helpers: land the course data in bronze, silver and reference tables.

Not part of the Databricks course, and deliberately kept out of the lnl package: the course only ever reads
from lnl, never the other way round. This is the home-lab lakehouse track (see lakehouse/README.md): the same
generator and the same session 2-4 cleansing, written to a DuckLake catalog instead of a Delta table, so the
lakehouse and the class always agree on every standardized value.

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

import pandas as pd

from lnl import config, env

LAKE = "lake"
SCHEMAS = ["ref", "bronze", "silver", "gold", "ops"]
SENSITIVE_COLUMNS = ["race", "race_code", "race_omb", "religion", "religion_group", "religion_family",
                     "stg_affiliation", "stg_group", "stg_status"]

TABLE_COMMENTS = {
    "bronze.hero_offenders_raw": "Generator output exactly as written: every column VARCHAR, nothing converted.",
    "bronze.conduct_notes_answer_key": "True sentiment of each conduct note, as written. Scoring only.",
    "silver.hero_offenders": "Sessions 2-4 applied (lnl.pipeline). offender_number is the BIGINT record key. "
                             "Ordered codes are VARCHAR with a matching _rank column.",
    "silver.hero_offenders_exceptions": "Every value a rule flagged, one row each. Records are never dropped.",
    "silver.conduct_notes_answer_key": "Answer key with offender_number as BIGINT so it joins to silver.",
    "silver.trust_test": "The course's Trust Test scorecard (lnl.trust): each question answered from a default load "
                         "and from the cleansed, typed data. Aggregates only.",
    "ops.load_runs": "One row per load: counts, timing and the DuckLake snapshot it produced.",
}


# ---- Connection ---------------------------------------------------------------------------------
def catalog() -> str:
    return os.environ.get("LAKE_CATALOG", str(env.data_dir() / "lake.duckdb"))


def is_ducklake() -> bool:
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


def ensure_schemas(con) -> None:
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


# ---- Shaping data for the lake -----------------------------------------------------------------
def to_lake_frame(df: pd.DataFrame) -> pd.DataFrame:
    """pandas -> types DuckDB keeps. Categories become text; ordered ones get a 1-based _rank column so
    'G4 and above' still works in SQL (DuckLake has no ENUM type)."""
    out = df.reset_index() if df.index.name else df.copy()
    for col in list(out.columns):
        dtype = out[col].dtype
        if isinstance(dtype, pd.CategoricalDtype):
            if dtype.ordered:
                codes = out[col].cat.codes.astype("Int16") + 1
                out[f"{col}_rank"] = codes.where(codes > 0)
            out[col] = out[col].astype("string")
        elif dtype == object:
            out[col] = out[col].astype("string")
    return out


def _ids(index_or_series) -> pd.Series:
    """Padded 8-digit ID text (as the course's people-facing lists use) back to BIGINT."""
    return pd.to_numeric(pd.Series(index_or_series, dtype="string"), errors="coerce").astype("Int64")


def exceptions_table(s2: pd.DataFrame, s3: pd.DataFrame, silver: pd.DataFrame) -> pd.DataFrame:
    """One row per flagged value, from the course's own exception lists. s2/s3 = the data after sessions 2/3."""
    from lnl import conversions, domains
    parts = []

    dom = domains.domain_exceptions(silver)
    parts.append(pd.DataFrame({"offender_number": _ids(dom.index), "rule": "domain_unmapped",
                               "column_name": dom["column"].values,
                               "value_as_written": dom["value as written"].astype("string").values,
                               "reason": dom["reason"].values}))

    conv = conversions.exceptions(s3)
    parts.append(pd.DataFrame({"offender_number": _ids(conv.index), "rule": "conversion_failed",
                               "column_name": conv["column"].values,
                               "value_as_written": conv["value as written"].astype("string").values,
                               "reason": "did not convert to its type"}))

    flagged = silver["dob_flag"].dropna()
    parts.append(pd.DataFrame({"offender_number": pd.Series(flagged.index, dtype="Int64"), "rule": "dob_check",
                               "column_name": "date_of_birth",
                               "value_as_written": s2.loc[flagged.index, "date_of_birth"].astype("string").values,
                               "reason": flagged.astype("string").values}))

    ovc = domains.offense_violence_conflicts(silver)
    parts.append(pd.DataFrame({"offender_number": _ids(ovc.index), "rule": "offense_violence_conflict",
                               "column_name": "violence_code_std",
                               "value_as_written": ovc["violence_code_std"].astype("string").values,
                               "reason": "violence code disagrees with offense against " +
                                         ovc["offense_against"].astype("string").values}))

    out = pd.concat(parts, ignore_index=True)
    out["sid_number"] = out["offender_number"].map(silver["sid_number"]).astype("Int64")
    return out[["offender_number", "sid_number", "rule", "column_name", "value_as_written", "reason"]]


def trust_test_table(naive: pd.DataFrame, typed: pd.DataFrame) -> pd.DataFrame:
    """The course's Trust Test scorecard (lnl.trust, as notebook 4 builds it), one row per question.
    naive = a pandas-defaults load (pipeline.load_naive); typed = the data after session 4. Aggregates only."""
    from lnl import trust
    card = trust.scorecard(trust.naive_answers(naive), trust.typed_answers(typed))
    return pd.DataFrame({"question_id": card.index, "question": card["Question"].values,
                         "as_loaded": card["Session 1 (as loaded)"].values,
                         "cleansed": card["Session 4 (cleansed + typed)"].values,
                         "changed": (card["Changed?"] == "yes").values})


# ---- Loading ------------------------------------------------------------------------------------
def _replace(con, table: str, view: str, frame=None) -> None:
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


def load_reference(con) -> int:
    count = 0
    for path in sorted(domains_reference_dir().glob("*.csv")):
        table = f"ref.{path.stem}"
        con.execute(f"DROP TABLE IF EXISTS {LAKE}.{table}")
        con.execute(f"CREATE TABLE {LAKE}.{table} AS SELECT * FROM read_csv(?, all_varchar = true, header = true)",
                    [str(path)])
        count += 1
    # The documented "unknown" codes (lnl.cleaning.NULL_TOKENS), so gold scores missing values the way the course
    # does without copying the list into SQL.
    from lnl.cleaning import NULL_TOKENS
    tokens = pd.DataFrame([(col, token) for col, values in NULL_TOKENS.items() for token in values],
                          columns=["column_name", "token"])
    _replace(con, "ref.null_tokens", "null_tokens_df", tokens)
    return count + 1


def domains_reference_dir() -> Path:
    from lnl import domains
    return domains.REFERENCE_DIR


def run(regenerate: bool = False) -> dict:
    """Generate (if needed) and land reference, bronze and silver in one transaction = one snapshot.
    Comments and the ops.load_runs row follow as two small transactions, so a load is three snapshots."""
    from lnl import cleaning, conversions, dates, pipeline
    started = time.time()
    run_id = uuid.uuid4().hex[:12]
    paths = env.generate(force=regenerate)

    raw = pipeline.load_raw()
    s2 = cleaning.session2(raw)
    s3 = dates.session3(s2)
    silver = conversions.session4(s3)
    exc = exceptions_table(s2, s3, silver)
    trust = trust_test_table(pipeline.load_naive(), silver)
    silver_frame = to_lake_frame(silver)
    key = to_lake_frame(pipeline.answer_key())

    con = lake_connect()
    con.execute("BEGIN TRANSACTION")
    try:
        ensure_schemas(con)
        ref_tables = load_reference(con)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW bronze_src AS
            SELECT *, {_lit(run_id)} AS _run_id, now() AS _ingested_at, {_lit(paths["data"].name)} AS _source_file,
                   {int(config.SEED)}::INTEGER AS _seed, {int(env.DATA_VERSION)}::INTEGER AS _data_version
            FROM read_csv({_lit(paths["data"])}, all_varchar = true, header = true)""")
        _replace(con, "bronze.hero_offenders_raw", "bronze_src")
        con.execute("CREATE OR REPLACE TEMP VIEW key_src AS "
                    f"SELECT * FROM read_csv({_lit(paths['answer_key'])}, all_varchar = true, header = true)")
        _replace(con, "bronze.conduct_notes_answer_key", "key_src")
        _replace(con, "silver.hero_offenders", "silver_df", silver_frame)
        _replace(con, "silver.hero_offenders_exceptions", "exc_df", exc)
        _replace(con, "silver.conduct_notes_answer_key", "key_df", key)
        _replace(con, "silver.trust_test", "trust_df", trust)
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
        "exceptions": len(exc),
        "records_with_exceptions": int(exc["offender_number"].nunique()),
        "reference_tables": ref_tables,
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
