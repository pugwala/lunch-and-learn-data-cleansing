"""Acceptance tests for the expanded world's built-in mess and its answer keys (dataset expansion, chunk 6).

    pytest lakehouse/tests/test_universe_mess.py     # inside the jupyter container; needs the lake (silver, ref)

One scale-S build is made in 250,000-row chunks. Every clean table also gets a messy CSV copy (what a source
system would hand over) and an answer key (one row per deliberate defect). The tests check that:
  - the manifest fingerprints the messy copies and keys too, and a chunk rebuilt alone has the same bytes;
  - the key is exact: a messy cell is the clean value written the standard way (canonical) unless the key lists
    it, and every listed cell really differs, as written, from the truth the key gives;
  - each kind of mess comes at its rate (mess.RATES) and is the mess it claims to be: a reformatted date still
    reads as the true date, an orphaned id points at nothing, an out-of-order date comes before what it follows,
    a variant code always means one value, and so on;
  - duplicate people are real people again under new ids, and the copies get referred to;
  - the link to today's records (disposition.offender_number) and every table's own ids stay clean.
"""
import math
import re
import sys
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.universe import build, manifest, mess, scale, writer  # noqa: E402

CHUNK_ROWS = 250_000
OUT = {}
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_FORMATS = ["%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d %H:%M:%S", "%Y%m%d", "%d-%b-%Y", "%B %d, %Y", "%b %d %Y"]
TIMESTAMP_FORMATS = ["%m/%d/%Y %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y %H:%M:%S",
                     "%Y%m%d%H%M%S"]
WORDS = {w: i for i, w in enumerate("zero one two three four five six seven eight nine ten".split())}


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A scale-S build of every table with its mess, and the folder it's in."""
    out = tmp_path_factory.mktemp("universe") / "S"
    con = lake.lake_connect(read_only=True)
    build.build(out, scale_name="S", seed=2026, workers=4, con=con, chunk_rows=CHUNK_ROWS)
    OUT["dir"] = out
    return out


@lru_cache(maxsize=None)
def messy(table: str) -> pa.Table:
    """A table's messy CSV copy, every column read as text exactly as written (blank is "", never null)."""
    parts = sorted((OUT["dir"] / "messy" / table).glob("part-*.csv"))
    names = parts[0].open(encoding="utf-8").readline().rstrip("\n").split(",")
    opts = pcsv.ConvertOptions(column_types={n: pa.string() for n in names}, strings_can_be_null=False)
    return pa.concat_tables(pcsv.read_csv(p, convert_options=opts) for p in parts)


@lru_cache(maxsize=None)
def key(table: str) -> pa.Table:
    """A table's answer key."""
    return pq.read_table(OUT["dir"] / "answer_key" / table)


@lru_cache(maxsize=None)
def canon(table: str) -> dict:
    """The clean table written the standard way: {column: numpy array of text}."""
    return mess.canonical(pq.read_table(OUT["dir"] / table))


@lru_cache(maxsize=None)
def truth(table: str) -> tuple:
    """For each messy row, the clean row it stands for (a duplicate person stands for the original), and
    {column: the truth for each messy row, as standard text}."""
    ids = messy(table)[f"{table}_id"].to_numpy(zero_copy_only=False).astype(np.int64)
    n = len(canon(table)[f"{table}_id"])
    row = ids.copy()
    if table == "person":
        k = key("person").filter(pa.compute.equal(key("person")["kind"], "duplicate_person"))
        orig = dict(zip(k["id"].to_pylist(), map(int, k["truth"].to_pylist())))
        row = np.array([i if i < n else orig[i] for i in ids], dtype=np.int64)
    return row, {c: v[row] for c, v in canon(table).items()}


def keyed(table: str, kind: str) -> list:
    """The key's rows of one kind as dicts."""
    k = key(table)
    return k.filter(pa.compute.equal(k["kind"], kind)).to_pylist()


def eligible_cells(kind: str) -> int:
    """How many cells, across every table, could have got this kind of mess (mess.SPEC says which columns)."""
    total = 0
    for t, spec in mess.SPEC.items():
        _, exp = truth(t)
        values = [c for cls in ("dates", "timestamps", "codes", "names", "money", "numbers", "floats") for c in spec.get(cls, [])]
        if kind == "duplicate_person":
            total += len(canon(t)[f"{t}_id"]) if t == "person" else 0
        elif kind == "null_token":
            total += sum(int((exp[c] == "").sum()) for c in values + list(spec.get("fk", {})))
        elif kind == "unknown":
            total += sum(int((exp[c] != "").sum()) for c in values)
        elif kind == "orphan":
            total += sum(int((exp[c] != "").sum()) for c in spec.get("fk", {}))
        elif kind == "duplicate_ref":
            dup_of = mess.duplicates(tuple(sorted(build.context(OUT["dir"], "S", 2026).items())))[1]
            for c, target in spec.get("fk", {}).items():
                if target == "person":
                    v = exp[c][exp[c] != ""].astype(np.int64)
                    total += int((dup_of[v] >= 0).sum())
        elif kind == "out_of_order":
            for c, (via, _, anchor) in spec.get("order", {}).items():
                ok = exp[c] != ""
                total += int((ok & (exp[anchor] != "")).sum()) if via is None else int(ok.sum())
        else:
            cls = {"date_format": ("dates", "timestamps"), "code_variant": ("codes",), "name_variant": ("names",),
                   "money_text": ("money",), "number_text": ("numbers",)}[kind]
            total += sum(int((exp[c] != "").sum()) for k in cls for c in spec.get(k, []))
    return total


# --- shape -------------------------------------------------------------------------------------------------------

def test_every_table_has_a_messy_copy_and_an_answer_key_in_the_manifest(world):
    """manifest.json fingerprints a messy CSV and an answer key per table, verify() rereads them, and the messy copy
    has the clean table's columns and rows (person also has its duplicates)."""
    m = manifest.read(world)
    assert set(m["mess"]) == {f"{d}/{t}" for d in ("messy", "answer_key") for t in build.ORDER}
    assert set(m["tables"]) == set(build.ORDER)
    assert manifest.verify(world) == []
    for t in build.ORDER:
        clean = pq.read_table(world / t)
        assert messy(t).column_names == clean.column_names, t
        extra = len(keyed(t, "duplicate_person"))
        assert messy(t).num_rows == clean.num_rows + extra == m["mess"][f"messy/{t}"]["rows"], t
        assert key(t).num_rows == m["mess"][f"answer_key/{t}"]["rows"], t
        assert key(t).schema.names == ["id", "column_name", "kind", "as_written", "truth"], t


@pytest.mark.parametrize("table,which", [("person", 0), ("participant", -1)])
def test_a_messy_chunk_rebuilt_alone_matches_the_full_build(world, tmp_path, table, which):
    """One chunk's messy CSV and answer key, rebuilt on their own in a fresh folder, have the same bytes."""
    part = writer.chunk_plan(scale.rows(table, "S"), CHUNK_ROWS)[which][0]
    entries = mess.write_chunk(table, part, build.context(world, "S", 2026), tmp_path)
    full = manifest.read(world)["mess"]
    for d in ("messy", "answer_key"):
        want = {f["name"]: f["sha256"] for f in full[f"{d}/{table}"]["files"]}
        assert entries[d]["sha256"] == want[entries[d]["name"]], d


# --- the key is exact --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("table", build.ORDER)
def test_the_answer_key_lists_exactly_the_cells_that_differ_from_the_truth(world, table):
    """Every messy cell equals the truth written the standard way, except the cells the key lists; for those, the
    key's as_written is what the CSV says and its truth is the clean value (null where the truth is null)."""
    m = messy(table)
    ids = m[f"{table}_id"].to_numpy(zero_copy_only=False).astype(np.int64)
    _, exp = truth(table)
    k = key(table)
    at = {i: n for n, i in enumerate(ids)}
    for c in m.column_names:
        got = m[c].to_numpy(zero_copy_only=False)
        differs = set(ids[got != exp[c]].tolist())
        kc = k.filter(pa.compute.equal(k["column_name"], c))
        assert set(kc["id"].to_pylist()) == differs, c
        for r in kc.to_pylist():
            n = at[r["id"]]
            assert r["as_written"] == got[n], (c, r)
            assert (r["truth"] or "") == exp[c][n], (c, r)
    assert k.num_rows == len(set(zip(k["id"].to_pylist(), k["column_name"].to_pylist())))


# --- each kind of mess -------------------------------------------------------------------------------------------

def test_each_kind_of_mess_comes_at_its_rate(world):
    """Across the world, each kind's share of the cells it could hit is its rate in mess.RATES."""
    for kind in mess.KINDS:
        n = eligible_cells(kind)
        hits = sum(len(keyed(t, kind)) for t in build.ORDER)
        rate = mess.RATES[kind]
        assert n > 0, kind
        assert abs(hits / n - rate) <= 0.15 * rate + 3 * math.sqrt(rate * (1 - rate) / n), (kind, hits, n, rate)


def read_date(text: str):
    """A date written in any of the course's formats, or None. Two-digit years come back as (month, day, yy)."""
    for f in DATE_FORMATS:
        try:
            return datetime.strptime(text, f).date()
        except ValueError:
            pass
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2})", text)
    return (int(m[1]), int(m[2]), int(m[3])) if m else None


def same_day(text: str, iso: str) -> bool:
    """A reformatted date names the same day as the ISO truth (a two-digit year matches its last two digits)."""
    got, want = read_date(text), date.fromisoformat(iso)
    if isinstance(got, tuple):
        return got == (want.month, want.day, want.year % 100)
    return got == want


def test_reformatted_dates_and_times_still_read_as_the_truth(world):
    """A date_format cell isn't in the standard form, yet it reads back to the true date or time."""
    for t in build.ORDER:
        stamps = set(mess.SPEC[t].get("timestamps", []))
        for r in keyed(t, "date_format"):
            assert r["as_written"] != r["truth"], r
            if r["column_name"] in stamps:
                assert any(_parses(r["as_written"], f, r["truth"]) for f in TIMESTAMP_FORMATS), r
            else:
                assert not ISO_DATE.match(r["as_written"]) and same_day(r["as_written"], r["truth"]), r


def _parses(text, fmt, iso):
    """text, read with fmt, is the timestamp iso (YYYY-MM-DD HH:MM:SS)."""
    try:
        return datetime.strptime(text, fmt) == datetime.fromisoformat(iso)
    except ValueError:
        return False


def test_money_and_numbers_written_as_text_still_read_as_the_truth(world):
    """$1,234.00, USD 1,234.00, - for zero, 12.0, 012, twelve: each reads back to the true whole number."""
    def money(s):
        """Money text as a number: currency marks and thousands commas dropped, - for zero."""
        s = s.replace("USD", "").replace("$", "").replace(",", "").strip()
        return 0.0 if s == "-" else float(s)
    for t in build.ORDER:
        for r in keyed(t, "money_text"):
            assert r["as_written"] != r["truth"] and money(r["as_written"]) == int(r["truth"]), r
        for r in keyed(t, "number_text"):
            s = r["as_written"].strip()
            assert r["as_written"] != r["truth"] and (WORDS[s] if s in WORDS else float(s)) == int(r["truth"]), r


def test_variant_codes_and_names_always_mean_one_value(world):
    """A code_variant is never the standard spelling, and within a column each spelling means exactly one value
    (so a crosswalk can undo it); a name_variant is the same name in another case."""
    seen = {}
    for t in build.ORDER:
        for r in keyed(t, "code_variant"):
            assert r["as_written"] != r["truth"], r
            assert seen.setdefault((t, r["column_name"], r["as_written"]), r["truth"]) == r["truth"], r
        for r in keyed(t, "name_variant"):
            assert r["as_written"] != r["truth"] and r["as_written"].casefold() == r["truth"].casefold(), r
    assert len(seen) > 0


def test_unknown_and_null_tokens_stand_in_for_values(world):
    """unknown: a token hides a real value; null_token: a token where the truth is null. Both clean up to null."""
    for t in build.ORDER:
        for r in keyed(t, "unknown"):
            assert r["as_written"] in mess.UNKNOWN_TOKENS and r["truth"] is not None, r
        for r in keyed(t, "null_token"):
            assert r["as_written"] in mess.NULL_TOKENS and r["truth"] is None, r


def test_orphaned_references_point_at_nothing(world):
    """An orphan is a link to an id the target table doesn't have (not even as a duplicate person)."""
    ids = lru_cache(maxsize=None)(lambda target: set(messy(target)[f"{target}_id"].to_pylist()))
    for t, spec in mess.SPEC.items():
        for r in keyed(t, "orphan"):
            target = spec["fk"][r["column_name"]]
            assert r["as_written"] not in ids(target) and r["truth"] in ids(target), r


def test_out_of_order_dates_come_before_what_they_follow(world):
    """An out_of_order date is written (in the standard form) before the date it must follow: in its own row, or
    in the row it links to (the fight before its charge or repair, the charge before its decision)."""
    for t, spec in mess.SPEC.items():
        rows = keyed(t, "out_of_order")
        if not rows:
            continue
        row, exp = truth(t)
        at = {i: n for n, i in enumerate(messy(t)[f"{t}_id"].to_numpy(zero_copy_only=False).astype(np.int64))}
        for r in rows:
            via, target, anchor = spec["order"][r["column_name"]]
            n = at[r["id"]]
            if via is None:
                before = exp[anchor][n]
            else:
                before = canon(target)[anchor][int(exp[via][n])]
            assert ISO_DATE.match(r["as_written"]) and r["as_written"] < before[:10] <= r["truth"], (r, before)


# --- duplicate people --------------------------------------------------------------------------------------------

def test_duplicate_people_are_real_people_under_new_ids(world):
    """Duplicates take ids after the last person, copy a real person (same SID), each person is copied at most
    once, and some links elsewhere point at the copy (duplicate_ref)."""
    n = len(canon("person")["person_id"])
    dups = keyed("person", "duplicate_person")
    ids = sorted(r["id"] for r in dups)
    assert ids == list(range(n, n + len(dups)))
    origs = [int(r["truth"]) for r in dups]
    assert len(set(origs)) == len(origs) and all(0 <= o < n for o in origs)
    m = messy("person")
    sid = dict(zip(m["person_id"].to_numpy(zero_copy_only=False).astype(np.int64).tolist(), m["sid_number"].to_pylist()))
    assert all(sid[r["id"]] == sid[int(r["truth"])] for r in dups)
    copy_of = {int(r["truth"]): str(r["id"]) for r in dups}
    refs = [r for t in build.ORDER for r in keyed(t, "duplicate_ref")]
    assert refs and all(copy_of[int(r["truth"])] == r["as_written"] for r in refs)


# --- what stays clean --------------------------------------------------------------------------------------------

def test_the_link_to_todays_records_and_every_id_stay_clean(world):
    """disposition.offender_number (the link back to the course's records) is never messed, and a table's own id is
    only ever keyed for a duplicate person."""
    assert key("disposition").filter(pa.compute.equal(key("disposition")["column_name"], "offender_number")).num_rows == 0
    for t in build.ORDER:
        own = key(t).filter(pa.compute.equal(key(t)["column_name"], f"{t}_id"))
        assert set(own["kind"].to_pylist()) <= {"duplicate_person"}, t
