"""The core: the course's 100,000 records, which the expanded world is built around and must never change.

CORE_SHA256 is the fingerprint of the file the course generator makes with its defaults (100,000 rows, seed 2026),
the same file the lake loads (/lake/landing/hero_offender_data_v6_s2026_r100000.csv). course_core_sha256() runs
the course generator itself, unchanged, so a test can prove the core still comes out byte-for-byte the same.
"""
import hashlib
import importlib.util
from pathlib import Path

CORE_SHA256 = "649dc8398937e22f2fddf3203bb14023d91a3d0789839b41c072465b1cc1fde8"
CORE_ROWS, CORE_SEED = 100_000, 2026
GENERATOR = Path(__file__).resolve().parents[2] / "generator" / "gen_hero_offenders.py"


def sha256(path: Path) -> str:
    """A file's SHA-256, read in 1 MB blocks."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def course_core_sha256(work_dir: Path) -> str:
    """Run the course generator (100,000 rows, seed 2026) into work_dir and return the data file's SHA-256."""
    spec = importlib.util.spec_from_file_location("gen_hero_offenders_for_core_check", GENERATOR)
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    out = work_dir / "core.csv"
    gen.generate(rows=CORE_ROWS, out=str(out), seed=CORE_SEED, answer_key=str(work_dir / "core_answer_key.csv"))
    return sha256(out)
