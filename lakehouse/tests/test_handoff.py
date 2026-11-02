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
BRONZE = {"bronze.hero_offenders_raw", "bronze.conduct_notes_answer_key"}


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    """One fresh publish into a temporary folder: (folder, its manifest)."""
    out = tmp_path_factory.mktemp("handoff") / "v-test"
    return out, publish.publish(out, version="v-test")


def test_publish_writes_every_table_with_its_fingerprint(published):
    """What course 1 received (the raw file and its answer key, as text), what it made of it (the four silver
    tables) and every reference table (each course CSV, plus the null tokens), one Parquet file each; the manifest's
    row count and SHA-256 are the file's own."""
    out, m = published
    refs = {f"ref.{p.stem}" for p in domains.REFERENCE_DIR.glob("*.csv")} | {"ref.null_tokens"}
    assert set(m["tables"]) == SILVER | BRONZE | refs
    con = duckdb.connect()
    for table, entry in m["tables"].items():
        path = out / entry["file"]
        assert entry["sha256"] == publish.sha256(path), table
        assert entry["rows"] == con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0], table
    assert m["tables"]["silver.hero_offenders"]["rows"] == 100_000
    assert m["tables"]["bronze.hero_offenders_raw"]["rows"] == 100_000
    assert (m["version"], m["source"]["rows"], m["source"]["seed"], m["source"]["data_version"]) == ("v-test", 100_000,
                                                                                                    2026, 6)


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


def test_course_1_publishes_the_pinned_version_by_default():
    """publish.py's default version is the one course 2 pinned, so a fresh install (bootstrap.sh) that publishes
    with no arguments makes exactly the files the lock names."""
    assert publish.VERSION == receive.read_lock()["version"]


def test_the_course_generator_still_makes_the_pinned_source(tmp_path):
    """Run the generator as the course does (100,000 rows, seed 2026): its file is the one the pinned handoff was
    made from, so the course's raw records haven't changed."""
    assert publish.generate_source(tmp_path) == receive.read_lock()["source"]["sha256"]



# --- the contract: what course 2 accepts --------------------------------------------------------------------------

def copy_with(out, tmp_path, table, sql):
    """A copy of a published folder with one table rewritten by sql (over the original as t) and its manifest
    updated to match, as a careless new version would be: (folder, manifest)."""
    import json
    import shutil
    copy = tmp_path / "copy"
    shutil.copytree(out, copy)
    m = json.loads((copy / "manifest.json").read_text())
    path = copy / m["tables"][table]["file"]
    con = duckdb.connect()
    rows = con.execute(f"SELECT * FROM read_parquet('{out / m['tables'][table]['file']}') t").arrow().read_all()
    con.register("t", rows)
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")
    m["tables"][table].update(sha256=publish.sha256(path),
                              rows=con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0])
    (copy / "manifest.json").write_text(json.dumps(m))
    return copy, m


def test_the_contract_describes_every_published_table(published):
    """contract.yml names every table course 1 hands over, with a description, its key and every column's type, and
    the published files keep it."""
    out, m = published
    contract = receive.read_contract()
    assert set(contract["tables"]) == set(m["tables"])
    assert all(t["description"] and t["columns"] for t in contract["tables"].values())
    assert contract["tables"]["silver.hero_offenders"]["key"] == ["offender_number"]
    assert receive.contract_problems(out, m) == []


def test_a_version_with_a_changed_column_type_is_not_adopted(published, tmp_path, monkeypatch):
    """A new version where silver.trust_test's changed flag became text: adopt() refuses it, naming the table and
    column, and the lock stays as it was."""
    monkeypatch.setattr(receive, "LOCK", tmp_path / "lock.json")
    copy, _ = copy_with(published[0], tmp_path, "silver.trust_test",
                        "SELECT * REPLACE (CAST(changed AS VARCHAR) AS changed) FROM t")
    with pytest.raises(receive.ContractBroken, match=r"silver\.trust_test.*changed"):
        receive.adopt(copy)
    assert not receive.LOCK.exists()


def test_a_version_with_a_duplicate_key_is_not_adopted(published, tmp_path, monkeypatch):
    """A new version with one Trust Test question twice: its key (question_id) isn't unique, so it isn't adopted."""
    monkeypatch.setattr(receive, "LOCK", tmp_path / "lock.json")
    copy, _ = copy_with(published[0], tmp_path, "silver.trust_test",
                        "SELECT * FROM t UNION ALL SELECT * FROM t WHERE question_id = 'Q1'")
    with pytest.raises(receive.ContractBroken, match=r"silver\.trust_test.*question_id"):
        receive.adopt(copy)
    assert not receive.LOCK.exists()


def test_adopt_pins_a_version_that_keeps_the_contract(published, tmp_path, monkeypatch):
    """A version whose files match its manifest and keep the contract is pinned: the lock becomes its manifest."""
    monkeypatch.setattr(receive, "LOCK", tmp_path / "lock.json")
    out, m = published
    assert receive.adopt(out) == m
    assert receive.read_lock() == m
