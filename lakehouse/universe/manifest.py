"""The build's manifest: what was built, from which seed, scale and handoff (core), and every file's fingerprint (the clean tables
under "tables"; their messy copies and answer keys, mess.py, under "mess" as messy/<table> and answer_key/<table>).

manifest.json sits at the top of a build's folder. verify() rereads every file and reports any that is missing or
whose bytes changed, so a damaged or hand-edited build can't pass for a real one.
"""
import json
from pathlib import Path

from . import core

GENERATOR_VERSION = 1
NAME = "manifest.json"


def write(out_dir: Path, seed: int, scale_name: str, tables: dict, mess: dict = None) -> Path:
    """Write manifest.json for a build: tables is {table: build_table()'s entry}, mess {messy/<table> or
    answer_key/<table>: the same kind of entry}."""
    data = {"generator_version": GENERATOR_VERSION, "seed": seed, "scale": scale_name,
            "core": core.handoff_info(), "tables": tables, "mess": mess or {}}
    path = Path(out_dir) / NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    return path


def read(out_dir: Path) -> dict:
    """A build's manifest."""
    return json.loads((Path(out_dir) / NAME).read_text())


def verify(out_dir: Path) -> list:
    """Every problem with a build, as "<table>/<file>: missing" or "<table>/<file>: changed"; [] when it's whole."""
    problems = []
    m = read(out_dir)
    for table, entry in {**m["tables"], **m.get("mess", {})}.items():
        for f in entry["files"]:
            path = Path(out_dir) / table / f["name"]
            if not path.exists():
                problems.append(f"{table}/{f['name']}: missing")
            elif core.sha256(path) != f["sha256"]:
                problems.append(f"{table}/{f['name']}: changed (fingerprint differs from the manifest)")
    return problems
