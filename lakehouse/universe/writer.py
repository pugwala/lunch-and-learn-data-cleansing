"""Write one table of the expanded world as numbered Parquet chunks.

A factory makes one chunk at a time from its own stream: factory(stream, start, n) returns a pyarrow Table of n
rows whose first row is the table's row number `start`. Factories work on whole numpy arrays, never row by row,
which is what makes millions of rows quick. Chunks are independent, so they can be built by several processes
(started fresh with spawn: pyarrow runs threads, and fork can deadlock then; so a factory must be a top-level
function in an importable module). Each chunk is written as <out_dir>/<table>/part-<chunk>.parquet with the same settings every time, so the same seed gives
the same bytes.
"""
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow.parquet as pq

from . import core
from .rng import stream

CHUNK_ROWS = 1_000_000


def chunk_plan(rows: int, chunk_rows: int) -> list:
    """[(chunk number, first row, row count)] covering rows in chunks of chunk_rows (the last may be shorter)."""
    return [(i, start, min(chunk_rows, rows - start)) for i, start in enumerate(range(0, rows, chunk_rows))]


def write_chunk(name: str, chunk: int, start: int, n: int, factory, out_dir: Path, seed: int) -> dict:
    """Make one chunk from its stream, write it as Parquet, and return its manifest entry."""
    table = factory(stream(seed, name, chunk), start, n)
    if table.num_rows != n:
        raise ValueError(f"{name} chunk {chunk}: the factory made {table.num_rows} rows, not {n}")
    path = Path(out_dir) / name / f"part-{chunk:05d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd", use_dictionary=True, write_statistics=True)
    return {"name": path.name, "rows": n, "sha256": core.sha256(path)}


def build_table(name: str, rows: int, factory, out_dir: Path, seed: int, chunk_rows: int = CHUNK_ROWS,
                workers: int = 1, only_chunks=None) -> dict:
    """Build a table (or only some of its chunks) and return its manifest entry: {"rows": ..., "files": [...]}."""
    plan = [c for c in chunk_plan(rows, chunk_rows) if only_chunks is None or c[0] in only_chunks]
    args = [(name, i, start, n, factory, out_dir, seed) for i, start, n in plan]
    if workers > 1 and len(args) > 1:
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
            files = list(pool.map(write_chunk, *zip(*args)))
    else:
        files = [write_chunk(*a) for a in args]
    return {"rows": sum(f["rows"] for f in files), "files": files}
