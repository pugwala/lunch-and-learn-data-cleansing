"""Unit tests for the expanded-world generator's foundation (lakehouse/universe/, dataset expansion chunk 2).

    pytest lakehouse/tests/test_universe.py      # inside the jupyter container; no lake needed

The foundation makes no world tables yet. It promises four things the later chunks build on:
  - today's 100,000 records come from course 1's pinned handoff (lakehouse/handoff), checked against its lock, and
    nothing here runs course 1's generator or reads its live tables;
  - every table chunk comes from its own random stream, so the same seed gives the same bytes, a chunk rebuilt on
    its own matches the full build, and parallel builds match serial ones;
  - the S/M/L knob sizes every table from the approved data model, always keeping today's people;
  - a manifest fingerprints every file, so a changed file is caught.
"""
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse.handoff import receive  # noqa: E402
from lakehouse.universe import core, manifest, rng, scale, writer  # noqa: E402

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


# --- the core comes from course 1's pinned handoff, never from course 1 itself ---------------------------------

UNIVERSE = Path(__file__).resolve().parents[1] / "universe"
# What reaching into course 1 looks like in code: running or importing its generator, or reading its live tables.
COURSE_1_REACH = ["importlib", '"generator"', "generator/", "gen_hero_offenders import", "lake.silver", "lake.gold",
                  "lake_connect", "lake_ro_connect", "from lakehouse import lake"]


def code_lines(path: Path) -> dict:
    """A module's code, {line number: its tokens}, with docstrings and comments left out: saying where something
    came from is fine; reaching for it isn't."""
    import ast
    import io
    import tokenize
    source = path.read_text()
    docs = [n.body[0].value for n in ast.walk(ast.parse(source))
            if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef)) and n.body
            and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)]
    skip = {line for d in docs for line in range(d.lineno, d.end_lineno + 1)}
    out = {}
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type != tokenize.COMMENT and tok.start[0] not in skip:
            out.setdefault(tok.start[0], []).append(tok.string)
    return {n: " ".join(parts) for n, parts in out.items()}


def test_universe_never_reaches_into_course_1():
    """No universe module runs or imports course 1's generator or reads its live tables: the core arrives as the
    pinned handoff (lakehouse/handoff), and the word lists are course 2's own copy (course_words.json)."""
    found = [f"{path.name}:{n}: {needle}" for path in sorted(UNIVERSE.glob("*.py"))
             for n, line in code_lines(path).items() for needle in COURSE_1_REACH if needle in line]
    assert found == []


def test_the_word_lists_are_course_2s_own_copy():
    """course_constants() reads universe/course_words.json: the names, cities, powers, STG groups and alias parts
    the world draws from, and the course's "today" as a date."""
    import datetime
    c = core.course_constants()
    assert core.WORDS == UNIVERSE / "course_words.json" and core.WORDS.exists()
    assert {k: len(v) for k, v in c.items() if k != "TODAY"} == {
        "CITIES": 55, "POWERS": 4, "TIER_ORDER": 4, "STG_GROUPS": 12, "LAST_NAMES": 232, "MALE_FIRST": 116,
        "FEMALE_FIRST": 82, "ADJ": 75, "NOUN": 115, "PREFIX": 40, "SUFFIX": 32}
    assert c["TODAY"] == datetime.date(2026, 9, 24)


@pytest.mark.skipif(not receive.pinned_dir().exists(), reason="no published handoff outside the lakehouse VM")
def test_the_core_is_read_from_the_pinned_handoff(tmp_path):
    """extract_core() checks the pinned handoff against the lock, then writes today's people, powers, memberships,
    records and units from it."""
    paths = core.extract_core(tmp_path)
    assert set(paths) == set(core.EXTRACT) and all(p.exists() for p in paths.values())


@pytest.mark.skipif(not receive.pinned_dir().exists(), reason="no published handoff outside the lakehouse VM")
def test_a_changed_handoff_file_is_refused(tmp_path):
    """Change one byte of a copy of the handoff: extract_core() refuses to build on it and names the table."""
    import shutil
    copy = tmp_path / "handoff"
    shutil.copytree(receive.pinned_dir(), copy)
    target = copy / receive.read_lock()["tables"]["silver.hero_offenders"]["file"]
    data = bytearray(target.read_bytes())
    data[200] ^= 0xFF
    target.write_bytes(bytes(data))
    with pytest.raises(RuntimeError, match="silver.hero_offenders"):
        core.extract_core(tmp_path / "out", handoff_dir=copy)


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
    """A freshly written manifest verifies with no problems, and records the seed, scale and the handoff the core
    came from (its version and the raw file course 1 made it from)."""
    entry = build(tmp_path, "run")
    manifest.write(tmp_path / "run", seed=2026, scale_name="S", tables={"toy": entry})
    data = manifest.read(tmp_path / "run")
    lock = receive.read_lock()
    assert (data["seed"], data["scale"]) == (2026, "S")
    assert data["core"] == {"handoff_version": lock["version"], "source_sha256": lock["source"]["sha256"]}
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
