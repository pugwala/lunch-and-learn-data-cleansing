"""Course 1's side of the handoff: publish its cleansed output for course 2, but only if it matches the 19 numbers.

    python -m lakehouse.handoff.publish [--version v1] [--out DIR] [--lock]

Runs course 1's own code (the generator, then sessions 2-4, through lake.course1_output), writes one Parquet file per
table into a scratch folder, and checks the 19 Facilitator Guide numbers on those files (parity.py), read the way
course 2 will read them (receive.connect). Only when every number matches does the folder get its manifest and take
its place as <out>; a miss raises ParityFailed and publishes nothing. --lock also pins the result for course 2
(course1.lock.json), which is how a new version is adopted on purpose.

What's published: the four silver tables (the cleansed records, every flagged value, the conduct-notes answer key,
the Trust Test) and the reference data the rules use (each course CSV as text, plus the null tokens).
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

VERSION = "v1"
SOURCE_ROWS, SOURCE_SEED = 100_000, 2026
GENERATOR = Path(__file__).resolve().parents[2] / "generator" / "gen_hero_offenders.py"
sha256 = receive.sha256


class ParityFailed(RuntimeError):
    """Course 1's output doesn't match the 19 numbers, so it wasn't published."""


def course1_output() -> tuple:
    """(the generator's file paths, {table: frame}) for the four silver tables, from course 1's own code."""
    from lakehouse import lake
    return lake.course1_output()


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
        sources = {t: ("SELECT * FROM frame", f) for t, f in frames.items()} | reference_tables()
        tables = {}
        for table, (sql, frame) in sorted(sources.items()):
            path = scratch / f"{table}.parquet"
            rows = write_parquet(con, sql, path, frame)
            tables[table] = {"file": path.name, "rows": rows, "sha256": sha256(path)}
        manifest = {"version": version, "tables": tables,
                    "source": {"file": Path(paths["data"]).name, "rows": SOURCE_ROWS, "seed": SOURCE_SEED,
                               "sha256": sha256(paths["data"])}}
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
    """Command line: publish a version (and, with --lock, pin it for course 2)."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--version", default=VERSION)
    ap.add_argument("--out", type=Path, help="default: receive.ROOT/<version>")
    ap.add_argument("--lock", action="store_true", help="pin this version for course 2 (course1.lock.json)")
    args = ap.parse_args(argv)
    out = args.out or receive.ROOT / args.version
    manifest = publish(out, args.version)
    print(f"Published {args.version} to {out}: {len(manifest['tables'])} tables, "
          f"{manifest['parity']['passed']} of {manifest['parity']['checks']} numbers match")
    if args.lock:
        receive.LOCK.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        print(f"Pinned {args.version} in {receive.LOCK}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
