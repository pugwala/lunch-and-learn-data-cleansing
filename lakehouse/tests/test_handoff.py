"""Course 1's side of the handoff (lakehouse/handoff/publish.py): its cleansed output, published as pinned files.

    pytest lakehouse/tests/test_handoff.py      # inside the jupyter container

Course 2 starts where course 1 ends (the boundary document, 2026-11-02). Course 1 runs its own code (the generator,
then sessions 2-4), checks the result against the 19 Facilitator Guide numbers, and only then publishes it: one
Parquet file per table and a manifest with every file's fingerprint. Course 2 pins the version it builds on in
lakehouse/handoff/course1.lock.json (receive.py) and never reads course 1's live tables or imports its code.
"""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse.handoff import parity, publish, receive  # noqa: E402
from lnl import config, domains  # noqa: E402

pytestmark = pytest.mark.skipif((config.ROWS, config.SEED) != (100_000, 2026),
                                reason="the 19 numbers and the lock are for the default 100,000 rows, seed 2026")
SILVER = {"silver.hero_offenders", "silver.hero_offenders_exceptions", "silver.conduct_notes_answer_key",
          "silver.trust_test"}


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    """One fresh publish into a temporary folder: (folder, its manifest)."""
    out = tmp_path_factory.mktemp("handoff") / "v-test"
    return out, publish.publish(out, version="v-test")


def test_publish_writes_every_table_with_its_fingerprint(published):
    """The four silver tables and every reference table (each course CSV, plus the null tokens), one Parquet file
    each; the manifest's row count and SHA-256 are the file's own."""
    out, m = published
    refs = {f"ref.{p.stem}" for p in domains.REFERENCE_DIR.glob("*.csv")} | {"ref.null_tokens"}
    assert set(m["tables"]) == SILVER | refs
    con = duckdb.connect()
    for table, entry in m["tables"].items():
        path = out / entry["file"]
        assert entry["sha256"] == publish.sha256(path), table
        assert entry["rows"] == con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0], table
    assert m["tables"]["silver.hero_offenders"]["rows"] == 100_000
    assert (m["version"], m["source"]["rows"], m["source"]["seed"]) == ("v-test", 100_000, 2026)


def test_the_published_output_passes_the_19_course_numbers(published):
    """The same 19 checks the lake's parity gate runs, run on the published files: none fails."""
    out, m = published
    assert len(parity.CHECKS) == 19
    assert parity.failures(receive.connect(out)) == []
    assert m["parity"] == {"checks": 19, "passed": 19}


def test_a_publish_that_misses_a_number_publishes_nothing(tmp_path, monkeypatch):
    """Drop one cleansed record before publishing: the record count is off, so publish refuses and leaves no
    folder behind for course 2 to pick up."""
    real = publish.course1_output

    def one_short():
        """Course 1's output with its first cleansed record missing."""
        source, frames = real()
        frames["silver.hero_offenders"] = frames["silver.hero_offenders"].iloc[1:]
        return source, frames

    monkeypatch.setattr(publish, "course1_output", one_short)
    with pytest.raises(publish.ParityFailed, match="silver_rows"):
        publish.publish(tmp_path / "v-bad", version="v-bad")
    assert not (tmp_path / "v-bad").exists()
    assert list(tmp_path.iterdir()) == []


def test_a_fresh_publish_matches_the_pinned_lock(published):
    """Course 1's code makes the same bytes every time: a fresh publish's files and source are exactly what course 2
    pinned."""
    _, m = published
    lock = receive.read_lock()
    assert m["tables"] == lock["tables"]
    assert m["source"] == lock["source"]


def test_the_course_generator_still_makes_the_pinned_source(tmp_path):
    """Run the generator as the course does (100,000 rows, seed 2026): its file is the one the pinned handoff was
    made from, so the course's raw records haven't changed."""
    assert publish.generate_source(tmp_path) == receive.read_lock()["source"]["sha256"]

