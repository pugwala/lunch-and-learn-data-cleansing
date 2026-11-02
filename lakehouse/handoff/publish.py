"""Course 1's side of the handoff: publish its cleansed output for course 2, but only if it matches the 19 numbers.

    python -m lakehouse.handoff.publish [--version v2] [--out DIR]

Runs course 1's own code (the generator, then sessions 2-4, through lake.course1_output), writes one Parquet file per
table into a scratch folder, and checks the 19 Facilitator Guide numbers on those files (parity.py), read the way
course 2 will read them (receive.connect). Only when every number matches does the folder get its manifest and take
its place as <out>; a miss raises ParityFailed and publishes nothing. Course 2 then adopts it on purpose
(python -m lakehouse.handoff.receive adopt <out>), which checks the contract and pins it.

What's published: what course 1 received (bronze: the raw file and its conduct-notes answer key, every column as
text, as written), what it made of it (silver: the cleansed records, every flagged value, the answer key keyed to
the records, the Trust Test), and the reference data its rules use (each course CSV as text, plus the null tokens).
The contract (contract.yml) describes every table.
"""
import argparse
import importlib.util
import json
import shutil
import sys
from pathlib import Path

import duckdb
import pandas as pd

from . import parity, receive

VERSION = "v2"
SOURCE_ROWS, SOURCE_SEED = 100_000, 2026
GENERATOR = Path(__file__).resolve().parents[2] / "generator" / "gen_hero_offenders.py"
sha256 = receive.sha256


class ParityFailed(RuntimeError):
    """Course 1's output doesn't match the 19 numbers, so it wasn't published."""


# ---- Course 1's own code: sessions 2-4 and the tables they make (moved here from lake.py, 2026-11-02) ----------

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


def course1_output(regenerate: bool = False) -> tuple:
    """Course 1's cleansed output, made by its own code: generate the raw file (if needed), then sessions 2-4.
    Returns (the generator's file paths, {table: frame}) for the four silver tables, which publish() hands over."""
    from lnl import cleaning, conversions, dates, env, pipeline
    paths = env.generate(force=regenerate)
    raw = pipeline.load_raw()
    s2 = cleaning.session2(raw)
    s3 = dates.session3(s2)
    silver = conversions.session4(s3)
    return paths, {
        "silver.hero_offenders": to_lake_frame(silver),
        "silver.hero_offenders_exceptions": exceptions_table(s2, s3, silver),
        "silver.conduct_notes_answer_key": to_lake_frame(pipeline.answer_key()),
        "silver.trust_test": trust_test_table(pipeline.load_naive(), silver),
    }


def data_version() -> int:
    """The version of the generator's data layout (lnl.env.DATA_VERSION, the v6 in the raw file's name)."""
    from lnl import env
    return int(env.DATA_VERSION)


def raw_tables(paths: dict) -> dict:
    """{bronze.<name>: (SQL that reads it, None)}: what course 1 received, every column as text, as written: the
    generator's raw file and its conduct-notes answer key."""
    def as_text(path):
        """SQL reading one CSV with every column as text."""
        literal = str(path).replace("'", "''")
        return f"SELECT * FROM read_csv('{literal}', all_varchar = true, header = true)", None
    return {"bronze.hero_offenders_raw": as_text(paths["data"]),
            "bronze.conduct_notes_answer_key": as_text(paths["answer_key"])}


def reference_tables() -> dict:
    """{ref.<name>: (SQL that reads it, a frame to register or None)}: each course reference CSV, all text, as the
    lake loads it, plus the documented null tokens (lnl.cleaning.NULL_TOKENS)."""
    from lnl import domains
    from lnl.cleaning import NULL_TOKENS
    out = {}
    for path in sorted(domains.REFERENCE_DIR.glob("*.csv")):
        literal = str(path).replace("'", "''")
        out[f"ref.{path.stem}"] = (f"SELECT * FROM read_csv('{literal}', all_varchar = true, header = true)", None)
    tokens = pd.DataFrame([(col, token) for col, values in NULL_TOKENS.items() for token in values],
                          columns=["column_name", "token"])
    out["ref.null_tokens"] = ("SELECT * FROM frame", tokens)
    return out


def write_parquet(con, sql: str, path: Path, frame=None) -> int:
    """Write a query's rows to one Parquet file (zstd, insertion order kept) and return how many rows it holds."""
    if frame is not None:
        con.register("frame", frame)
    literal = str(path).replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{literal}' (FORMAT parquet, COMPRESSION zstd)")
    if frame is not None:
        con.unregister("frame")
    return con.execute(f"SELECT count(*) FROM read_parquet('{literal}')").fetchone()[0]


def publish(out_dir: Path, version: str = VERSION) -> dict:
    """Publish course 1's output as <out_dir>/<table>.parquet plus manifest.json, after the 19 numbers pass on the
    written files; returns the manifest. A miss raises ParityFailed and leaves nothing at out_dir."""
    out_dir = Path(out_dir)
    scratch = out_dir.with_name(out_dir.name + ".partial")
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)
    try:
        paths, frames = course1_output()
        con = duckdb.connect()
        con.execute("SET preserve_insertion_order = true")
        sources = ({t: ("SELECT * FROM frame", f) for t, f in frames.items()} | raw_tables(paths)
                   | reference_tables())
        tables = {}
        for table, (sql, frame) in sorted(sources.items()):
            path = scratch / f"{table}.parquet"
            rows = write_parquet(con, sql, path, frame)
            tables[table] = {"file": path.name, "rows": rows, "sha256": sha256(path)}
        manifest = {"version": version, "tables": tables,
                    "source": {"file": Path(paths["data"]).name, "rows": SOURCE_ROWS, "seed": SOURCE_SEED,
                               "data_version": data_version(), "sha256": sha256(paths["data"])}}
        missed = parity.failures(receive.connect(scratch, manifest))
        if missed:
            raise ParityFailed(f"Course 1's output misses {len(missed)} of the 19 numbers: " + "; ".join(missed))
        manifest["parity"] = {"checks": len(parity.CHECKS), "passed": len(parity.CHECKS)}
        (scratch / receive.MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    except Exception:
        shutil.rmtree(scratch, ignore_errors=True)
        raise
    shutil.rmtree(out_dir, ignore_errors=True)
    scratch.rename(out_dir)
    return manifest


def generate_source(work_dir: Path) -> str:
    """Run course 1's generator as the course does (100,000 rows, seed 2026) into work_dir and return the data
    file's SHA-256: the raw file every published version names as its source."""
    spec = importlib.util.spec_from_file_location("gen_hero_offenders_for_handoff", GENERATOR)
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    out = work_dir / "source.csv"
    gen.generate(rows=SOURCE_ROWS, out=str(out), seed=SOURCE_SEED, answer_key=str(work_dir / "source_answer_key.csv"))
    return sha256(out)


def main(argv=None) -> int:
    """Command line: publish a version (course 2 adopts it separately, with receive.py)."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--version", default=VERSION)
    ap.add_argument("--out", type=Path, help="default: receive.ROOT/<version>")
    args = ap.parse_args(argv)
    out = args.out or receive.ROOT / args.version
    manifest = publish(out, args.version)
    print(f"Published {args.version} to {out}: {len(manifest['tables'])} tables, "
          f"{manifest['parity']['passed']} of {manifest['parity']['checks']} numbers match")
    return 0


if __name__ == "__main__":
    sys.exit(main())
