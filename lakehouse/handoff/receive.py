"""Course 2's side of the handoff: which version of course 1's cleansed output it builds on, and proof the files are it.

course1.lock.json (next to this file) is the pin: the version, the raw file course 1 made it from, and every table's
file, row count and SHA-256, copied from that version's manifest when course 2 chose it. A new course 1 release
changes nothing here until someone publishes it and updates the lock on purpose.

contract.yml is what course 2 accepts from course 1: every table, its description, its key, and each column's type.
adopt() pins a new version only if its files match its own manifest and it keeps the contract, so a version with a
changed type or a repeated key is refused before anything is built on it:

    python -m lakehouse.handoff.receive adopt /lake/landing/handoff/course1/v3    # check, then pin
    python -m lakehouse.handoff.receive check                                    # is the pinned version intact?

The files live in ROOT/<version>/ (default /lake/landing/handoff/course1/v2): course 2 lands them like any upstream
delivery. require() refuses a folder whose files differ from the lock; connect() reads them as DuckDB views named
like the lake's tables (silver.hero_offenders, ref.nibrs_offenses, ...). lake.run() lands them in the lake.
"""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import duckdb
import yaml

ROOT = Path(os.environ.get("HANDOFF_ROOT", "/lake/landing/handoff/course1"))
LOCK = Path(__file__).with_name("course1.lock.json")
CONTRACT = Path(__file__).with_name("contract.yml")
MANIFEST = "manifest.json"


class ContractBroken(RuntimeError):
    """A version that doesn't match its manifest or breaks the contract, so course 2 won't adopt it."""


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


def read_contract() -> dict:
    """What course 2 accepts from course 1: contract.yml."""
    return yaml.safe_load(CONTRACT.read_text())


def contract_problems(handoff_dir: Path, manifest: dict = None) -> list:
    """Every way a version breaks the contract: a table missing or not in it, a column missing, extra, out of order
    or of another type, a key that repeats or is empty. [] when it keeps the contract."""
    handoff_dir = Path(handoff_dir)
    manifest = manifest or json.loads((handoff_dir / MANIFEST).read_text())
    contract = read_contract()["tables"]
    problems = [f"{t}: in the contract but not in the version" for t in sorted(set(contract) - set(manifest["tables"]))]
    problems += [f"{t}: not in the contract" for t in sorted(set(manifest["tables"]) - set(contract))]
    con = connect(handoff_dir, manifest)
    for table in sorted(set(contract) & set(manifest["tables"])):
        want = contract[table]["columns"]
        got = {row[0]: row[1] for row in con.execute(f"DESCRIBE {table}").fetchall()}
        problems += [f"{table}.{c}: missing" for c in want if c not in got]
        problems += [f"{table}.{c}: not in the contract" for c in got if c not in want]
        problems += [f"{table}.{c}: {got[c]}, the contract says {want[c]}" for c in want if c in got and got[c] != want[c]]
        if set(got) == set(want) and list(got) != list(want):
            problems.append(f"{table}: columns in another order than the contract's")
        key = contract[table].get("key") or []
        if key and all(k in got for k in key):
            cols = ", ".join(key)
            repeats = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM {table} GROUP BY ALL "
                                  "HAVING count(*) > 1)").fetchone()[0]
            empty = con.execute(f"SELECT count(*) FROM {table} WHERE "
                                + " OR ".join(f"{k} IS NULL" for k in key)).fetchone()[0]
            if repeats:
                problems.append(f"{table}: key ({cols}) repeats for {repeats:,} values")
            if empty:
                problems.append(f"{table}: key ({cols}) is empty in {empty:,} rows")
    con.close()
    return problems


def adopt(handoff_dir: Path) -> dict:
    """Pin a published version for course 2: check its files against its own manifest and the version against the
    contract, then write its manifest as the lock and return it. Raises ContractBroken (and pins nothing) on any
    problem."""
    handoff_dir = Path(handoff_dir)
    manifest = json.loads((handoff_dir / MANIFEST).read_text())
    problems = verify(handoff_dir, manifest) + contract_problems(handoff_dir, manifest)
    if problems:
        raise ContractBroken(f"Not adopting {manifest['version']} from {handoff_dir}: " + "; ".join(problems))
    LOCK.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def main(argv=None) -> int:
    """Command line: adopt a published version, or check the pinned one."""
    ap = argparse.ArgumentParser(description="Course 2's side of the handoff from course 1.")
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("adopt", help="check a published version and pin it").add_argument("folder", type=Path)
    sub.add_parser("check", help="check the pinned version's files against the lock")
    args = ap.parse_args(argv)
    if args.command == "adopt":
        m = adopt(args.folder)
        print(f"Adopted {m['version']}: {len(m['tables'])} tables keep the contract; pinned in {LOCK}")
        return 0
    problems = verify(pinned_dir())
    print("\n".join(problems) or f"{read_lock()['version']} in {pinned_dir()} matches the lock")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
