"""Course 2's side of the handoff: which version of course 1's cleansed output it builds on, and proof the files are it.

course1.lock.json (next to this file) is the pin: the version, the raw file course 1 made it from, and every table's
file, row count and SHA-256, copied from that version's manifest when course 2 chose it. A new course 1 release
changes nothing here until someone publishes it and updates the lock on purpose.

The files live in ROOT/<version>/ (default /lake/landing/handoff/course1/v1): course 2 lands them like any upstream
delivery. require() refuses a folder whose files differ from the lock; connect() reads them as DuckDB views named
like the lake's tables (silver.hero_offenders, ref.nibrs_offenses, ...).
"""
import hashlib
import json
import os
from pathlib import Path

import duckdb

ROOT = Path(os.environ.get("HANDOFF_ROOT", "/lake/landing/handoff/course1"))
LOCK = Path(__file__).with_name("course1.lock.json")
MANIFEST = "manifest.json"


def sha256(path: Path) -> str:
    """A file's SHA-256, read in 1 MB blocks."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def read_lock() -> dict:
    """The pinned version: course1.lock.json."""
    return json.loads(LOCK.read_text())


def pinned_dir() -> Path:
    """Where the pinned version's files are: ROOT/<version>."""
    return ROOT / read_lock()["version"]


def verify(handoff_dir: Path, pin: dict = None) -> list:
    """Every way the folder differs from the pin (the lock, unless another manifest is given), as
    "<table> (<file>): missing" or "... changed"; [] when every file is exactly the pinned one."""
    pin = pin or read_lock()
    problems = []
    for table, entry in sorted(pin["tables"].items()):
        path = Path(handoff_dir) / entry["file"]
        if not path.exists():
            problems.append(f"{table} ({entry['file']}): missing")
        elif sha256(path) != entry["sha256"]:
            problems.append(f"{table} ({entry['file']}): changed (fingerprint differs from the lock)")
    return problems


def require(handoff_dir: Path = None) -> Path:
    """The handoff folder (the pinned one by default), after checking it against the lock; raises RuntimeError
    naming every missing or changed table, so nothing is built on the wrong data."""
    handoff_dir = Path(handoff_dir) if handoff_dir else pinned_dir()
    problems = verify(handoff_dir)
    if problems:
        raise RuntimeError(f"The handoff in {handoff_dir} isn't the pinned version "
                           f"{read_lock()['version']}: " + "; ".join(problems))
    return handoff_dir


def connect(handoff_dir: Path, manifest: dict = None):
    """An in-memory DuckDB connection with a view per handoff table, named as in the lake (schema.table), over the
    folder's Parquet files (listed in its manifest.json unless a manifest is given)."""
    handoff_dir = Path(handoff_dir)
    manifest = manifest or json.loads((handoff_dir / MANIFEST).read_text())
    con = duckdb.connect()
    for table, entry in sorted(manifest["tables"].items()):
        schema = table.split(".")[0]
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        path = str(handoff_dir / entry["file"]).replace("'", "''")
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    return con
