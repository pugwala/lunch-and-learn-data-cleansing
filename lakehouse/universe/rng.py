"""Random streams for the expanded world: one independent numpy stream per (seed, table, chunk).

Nothing here shares the course generator's Python `random` sequence, so the core 100,000 records can't shift.
A stream is keyed by the seed, a stable number for the table's name (CRC-32, never Python's per-process hash) and
the chunk number, so chunk 7 of a table comes out the same whether it's built first, last, alone or in parallel.
"""
import zlib

import numpy as np

# The first three draws of stream(2026, "toy", 0).integers(0, 1_000_000, 3). If numpy ever changed PCG64 or
# SeedSequence, the world would silently change; test_universe.py fails on this instead.
PINNED_FIRST_DRAWS = [359440, 66465, 889136]


def table_key(table: str) -> int:
    """A stable number for a table's name: the same on every machine and every run."""
    return zlib.crc32(table.encode("utf-8"))


def stream(seed: int, table: str, chunk: int) -> np.random.Generator:
    """The random stream for one chunk of one table."""
    return np.random.Generator(np.random.PCG64(np.random.SeedSequence([seed, table_key(table), chunk])))
