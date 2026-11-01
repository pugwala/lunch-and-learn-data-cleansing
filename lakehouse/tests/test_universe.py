"""Unit tests for the expanded-world generator's foundation (lakehouse/universe/, dataset expansion chunk 2).

    pytest lakehouse/tests/test_universe.py      # inside the jupyter container; no lake needed

The foundation makes no world tables yet. It promises four things the later chunks build on:
  - today's 100,000 course records stay byte-for-byte what the course generator makes (the course is untouched);
  - every table chunk comes from its own random stream, so the same seed gives the same bytes, a chunk rebuilt on
    its own matches the full build, and parallel builds match serial ones;
  - the S/M/L knob sizes every table from the approved data model, always keeping today's people;
  - a manifest fingerprints every file, so a changed file is caught.
"""
import hashlib
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse.universe import core, manifest, rng, scale, writer  # noqa: E402

LANDING_CORE = Path("/lake/landing/hero_offender_data_v6_s2026_r100000.csv")


def toy(stream, start, n):
    """A small stand-in table for the tests: ids, an integer and a float column, all from the chunk's stream."""
    return pa.table({"id": np.arange(start, start + n, dtype=np.int64),
                     "x": stream.integers(0, 1_000, n), "f": stream.random(n)})


def build(tmp_path, name="run", seed=2026, workers=1, only_chunks=None, rows=25_000):
    """Build the toy table (2.5 chunks of 10,000 rows) and return its manifest entry."""
    return writer.build_table("toy", rows, toy, tmp_path / name, seed=seed, chunk_rows=10_000,
                              workers=workers, only_chunks=only_chunks)


def digests(entry):
    """{file name: sha256} from a table's manifest entry."""
    return {f["name"]: f["sha256"] for f in entry["files"]}


# --- the core stays the course's ---------------------------------------------------------------------------------

def test_course_generator_still_makes_the_pinned_core(tmp_path):
    """Run the course generator as the course does (100,000 rows, seed 2026): its file matches the pinned
    fingerprint, so nothing in the expansion has changed today's records."""
    assert core.course_core_sha256(tmp_path) == core.CORE_SHA256


@pytest.mark.skipif(not LANDING_CORE.exists(), reason="no landing file outside the lakehouse VM")
def test_the_lakes_landing_file_is_the_pinned_core():
    """The file the lake loads today is the same core."""
    assert hashlib.sha256(LANDING_CORE.read_bytes()).hexdigest() == core.CORE_SHA256


# --- deterministic, chunked, parallel ----------------------------------------------------------------------------

def test_streams_are_pinned():
    """A stream depends only on seed, table and chunk; its first draws are pinned, so a numpy upgrade that changed
    them would fail here rather than silently change the world."""
    assert rng.stream(2026, "toy", 0).integers(0, 1_000_000, 3).tolist() == rng.PINNED_FIRST_DRAWS
    assert rng.stream(2026, "toy", 1).random() != rng.stream(2026, "toy", 0).random()
    assert rng.stream(2026, "other", 0).random() != rng.stream(2026, "toy", 0).random()


def test_same_seed_gives_the_same_bytes(tmp_path):
    """Two builds with the same seed write identical files, chunk by chunk."""
    first, second = build(tmp_path, "a"), build(tmp_path, "b")
    assert digests(first) == digests(second)
    assert [f["rows"] for f in first["files"]] == [10_000, 10_000, 5_000] and first["rows"] == 25_000


def test_another_seed_gives_another_world(tmp_path):
    """A different seed changes the data."""
    assert digests(build(tmp_path, "a", seed=2026)) != digests(build(tmp_path, "b", seed=7))


def test_a_chunk_rebuilt_alone_matches_the_full_build(tmp_path):
    """Rebuilding only chunk 1 gives the same file as chunk 1 of a full build."""
    full, alone = build(tmp_path, "full"), build(tmp_path, "alone", only_chunks=[1])
    assert list(digests(alone)) == ["part-00001.parquet"]
    assert digests(alone)["part-00001.parquet"] == digests(full)["part-00001.parquet"]


def test_each_chunk_draws_its_own_numbers(tmp_path):
    """Chunk 1 doesn't repeat chunk 0's random numbers: each chunk has its own stream, not a copy of the first."""
    import pyarrow.parquet as pq
    build(tmp_path, "run")
    part = lambda i: pq.read_table(tmp_path / "run" / "toy" / f"part-{i:05d}.parquet")  # noqa: E731
    assert part(0).column("x").to_pylist() != part(1).column("x").to_pylist()


def test_parallel_build_matches_serial(tmp_path):
    """Three worker processes write the same bytes as one."""
    assert digests(build(tmp_path, "one", workers=1)) == digests(build(tmp_path, "three", workers=3))


# --- the scale knob ----------------------------------------------------------------------------------------------

def test_scale_m_is_the_approved_model():
    """At M every table has the approved row count, about 16.6M rows in all, with today's 86,879 people inside."""
    assert scale.rows("participant", "M") == 6_000_000 and scale.rows("incident", "M") == 1_200_000
    assert scale.rows("person", "M") == 440_000
    assert 16_000_000 < scale.total("M") < 17_000_000


def test_scale_grows_with_the_knob_but_always_keeps_todays_people():
    """S is about a tenth of M and L four times it, but every scale keeps all 86,879 of today's people, and the
    catalogs (powers, teams) don't grow with it."""
    assert scale.rows("incident", "S") * 10 == scale.rows("incident", "M") == scale.rows("incident", "L") // 4
    for size in ("S", "M", "L"):
        assert scale.rows("person", size) >= scale.CORE_PEOPLE == 86_879
        assert scale.rows("power", size) == scale.rows("power", "M")
    with pytest.raises(ValueError):
        scale.rows("incident", "XL")


# --- the manifest ------------------------------------------------------------------------------------------------

def test_manifest_round_trip_is_clean(tmp_path):
    """A freshly written manifest verifies with no problems, and records the seed, scale and core fingerprint."""
    entry = build(tmp_path, "run")
    manifest.write(tmp_path / "run", seed=2026, scale_name="S", tables={"toy": entry})
    data = manifest.read(tmp_path / "run")
    assert (data["seed"], data["scale"], data["core_sha256"]) == (2026, "S", core.CORE_SHA256)
    assert manifest.verify(tmp_path / "run") == []


def test_manifest_catches_a_changed_or_missing_file(tmp_path):
    """Change one byte of one file and delete another: verify names both."""
    entry = build(tmp_path, "run")
    manifest.write(tmp_path / "run", seed=2026, scale_name="S", tables={"toy": entry})
    part0 = tmp_path / "run" / "toy" / "part-00000.parquet"
    data = bytearray(part0.read_bytes())
    data[100] ^= 0xFF
    part0.write_bytes(bytes(data))
    (tmp_path / "run" / "toy" / "part-00002.parquet").unlink()
    problems = manifest.verify(tmp_path / "run")
    assert any("part-00000.parquet" in p and "changed" in p for p in problems)
    assert any("part-00002.parquet" in p and "missing" in p for p in problems)
