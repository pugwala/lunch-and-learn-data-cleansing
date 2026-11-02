"""The expanded world's built-in mess: a messy CSV copy of every clean table, and an answer key for it.

The clean Parquet tables are the truth. Each one also gets what a source system would really hand over:

    messy/<table>/part-NNNNN.csv           every value as text, with deliberate mess at the rates in RATES
    answer_key/<table>/part-NNNNN.parquet  one row per deliberate defect: id, column_name, kind, as_written, truth

The key is exact. A messy cell that isn't in the key is the clean value written the standard way (canonical():
ISO dates, YYYY-MM-DD HH:MM:SS times, true/false, plain numbers, blank for null), and every cell in the key differs
from it. The kinds, the course's own (gen_hero_offenders.py) carried over to the new tables:

  unknown           a token (UNK, N/A, ?) hides a real value: cleans to null, the truth is lost
  null_token        a token (NULL, NONE, blanks) where the truth is null: cleans to null
  date_format       a date or time in another format (03/22/2001, 22-MAR-2001, 20010322, 3/22/01, ...)
  code_variant      a code spelled another way (CAPTURED, Captured, CAPT, " captured ", 13-A, 290.0, Y for true);
                    within a column each spelling means one value, so a crosswalk can undo it
  name_variant      a name in another case (Adams, adams)
  money_text        money as text ($1,234.00, USD 1,234.00, - for zero)
  number_text       a count as text (12.0, 012, twelve)
  orphan            a link to an id the target table doesn't have
  out_of_order      a date before the date it must follow: in its row (left before joining), or in the row it links
                    to (repaired, or charged, before the fight; decided before the charge was filed)
  duplicate_person  a real person entered again under a new id (after the last person), same SID
  duplicate_ref     a link to a person that points at their duplicate instead

Each cell gets at most one kind. One draw per cell picks it from fixed slices of [0, 1) (unknown first, then
out_of_order, then the column's format kind), so each kind's share of the cells it can hit is exactly its rate.
Mess comes from its own streams, (seed, "mess/<table>", chunk) and (seed, "mess_plan", 0), so the clean tables never
change; a messy chunk is made from its clean chunk alone, so it rebuilds the same on its own.
"""
import calendar
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import core, scale
from .rng import stream

# Which columns get which mess. fk: {column: the table it links to}. order: {later date: (the link to follow, or
# None for the same row; the table it leads to; the date it must not come before)}.
SPEC = {
    "location": {"codes": ["kind", "site_type"], "names": ["name"],
                 "fk": {"parent_id": "location", "city_id": "location"}},
    "power": {"codes": ["tier", "permanence", "category"], "names": ["name"]},
    "team": {"codes": ["side", "stg_group"], "names": ["name"], "dates": ["founded_on"],
             "fk": {"base_location_id": "location"}},
    "person": {"codes": ["role", "gender"], "names": ["last_name", "first_name", "alias"], "dates": ["birth_date"],
               "fk": {"home_location_id": "location"}},
    "person_power": {"codes": ["source"], "dates": ["acquired_on", "lost_on"],
                     "fk": {"person_id": "person", "power_id": "power"},
                     "order": {"lost_on": (None, None, "acquired_on")}},
    "team_member": {"codes": ["status"], "dates": ["joined_on", "left_on"],
                    "fk": {"team_id": "team", "person_id": "person"},
                    "order": {"left_on": (None, None, "joined_on")}},
    "incident": {"timestamps": ["started_at"], "numbers": ["duration_min", "bystanders_injured", "bystanders_killed"],
                 "codes": ["incident_type", "outcome"], "fk": {"location_id": "location"}},
    "participant": {"codes": ["side", "role", "status"], "fk": {"incident_id": "incident", "person_id": "person"}},
    "power_use": {"floats": ["effectiveness"],
                  "fk": {"incident_id": "incident", "participant_id": "participant", "power_id": "power"}},
    "damage": {"codes": ["asset_type", "insured", "repair_status"], "money": ["cost_usd", "insurance_paid_usd"],
               "dates": ["repaired_on"], "fk": {"incident_id": "incident"},
               "order": {"repaired_on": ("incident_id", "incident", "started_at")}},
    "charge": {"codes": ["offense_code"], "dates": ["filed_on"],
               "fk": {"incident_id": "incident", "participant_id": "participant"},
               "order": {"filed_on": ("incident_id", "incident", "started_at")}},
    "disposition": {"codes": ["outcome"], "dates": ["decided_on"], "fk": {"charge_id": "charge"},
                    "order": {"decided_on": ("charge_id", "charge", "filed_on")}},
    "mission": {"codes": ["mission_type", "status"], "names": ["codename"], "dates": ["started_on", "ended_on"],
                "fk": {"team_id": "team", "target_team_id": "team", "location_id": "location"},
                "order": {"ended_on": (None, None, "started_on")}},
    "mission_member": {"codes": ["role"], "fk": {"mission_id": "mission", "person_id": "person"}},
    "mission_incident": {"fk": {"mission_id": "mission", "incident_id": "incident"}},
}

# Each kind's share of the cells it can hit.
RATES = {"unknown": 0.01, "null_token": 0.25, "date_format": 0.3, "code_variant": 0.12, "name_variant": 0.15,
         "money_text": 0.6, "number_text": 0.05, "orphan": 0.002, "out_of_order": 0.004, "duplicate_person": 0.02,
         "duplicate_ref": 0.3}
KINDS = list(RATES)
FORMAT_KIND = {"dates": "date_format", "timestamps": "date_format", "codes": "code_variant",
               "names": "name_variant", "money": "money_text", "numbers": "number_text"}
VALUE_CLASSES = ["dates", "timestamps", "codes", "names", "money", "numbers", "floats"]

UNKNOWN_TOKENS = ["UNK", "UNKNOWN", "N/A", "?", "00/00/0000"]
NULL_TOKENS = ["NULL", "NONE", "N/A", "  ", "\t"]
DATE_UNKNOWN = ["UNKNOWN", "UNK", "?", "00/00/0000"]
ORPHAN_GAP = 1_000_000   # orphaned ids start this far past a table's last id, well clear of duplicate people

# Date formats as the course writes them (weights from gen_hero_offenders.fmt_date), the standard ISO one left out.
DATE_STYLES = [("{m:02d}/{d:02d}/{Y}", 22), ("{m}/{d}/{y:02d}", 10), ("{m:02d}-{d:02d}-{Y}", 8),
               ("{Y}-{m:02d}-{d:02d} 00:00:00", 10), ("{Y}{m:02d}{d:02d}", 12), ("{d:02d}-{MON}-{Y}", 10),
               ("{Month} {d}, {Y}", 5), ("{Mon} {d} {Y}", 5)]
TIME_STYLES = ["{mo}/{dd}/{Y} {H}:{M}:{S}", "{mo}/{dd}/{Y} {I}:{M}:{S} {p}", "{Y}-{mo}-{dd}T{H}:{M}:{S}",
               "{dd}-{MON}-{Y} {H}:{M}:{S}", "{Y}{mo}{dd}{H}{M}{S}"]
MONEY_STYLES = [("${v:,.2f}", 35), ("{v:.2f}", 20), ("{v:,.2f}", 10), ("$ {v:,.2f}", 5), ("USD {v:,.2f}", 5),
                ("{v}.0", 25)]
MONEY_ZERO = ["-", "$0.00", "0.00"]
NUMBER_WORDS = "zero one two three four five six seven eight nine ten".split()
BOOL_STYLES = [("Y", "N"), ("Yes", "No"), ("1", "0"), ("T", "F"), ("TRUE", "FALSE"), ("y", "n")]
EXTRA_SPELLINGS = {"gender": {"M": ["Male", "MALE"], "F": ["Female", "FEMALE"]}}
KEY_SCHEMA = pa.schema([("id", pa.int64()), ("column_name", pa.string()), ("kind", pa.string()),
                        ("as_written", pa.string()), ("truth", pa.string())])
NEEDS_QUOTES = r'[",\r\n]|^\s|\s$'


# --- the standard way to write a value ---------------------------------------------------------------------------

def canonical(table: pa.Table) -> dict:
    """Every column as standard text, {column: numpy object array}: ISO dates, YYYY-MM-DD HH:MM:SS times (whole
    seconds), true/false, plain numbers; "" for null."""
    out = {}
    for name in table.column_names:
        col = table[name].combine_chunks()
        if pa.types.is_timestamp(col.type):
            col = col.cast(pa.timestamp("s"))
        out[name] = pc.fill_null(col.cast(pa.string()), "").to_numpy(zero_copy_only=False)
    return out


def iso(days: np.ndarray) -> np.ndarray:
    """Days since 1970 as ISO dates."""
    return pa.array(np.asarray(days, dtype=np.int32)).cast(pa.date32()).cast(pa.string()).to_numpy(zero_copy_only=False)


def ymd(days: np.ndarray) -> tuple:
    """Days since 1970 (before 1970 too) as year, month and day arrays."""
    d = np.asarray(days, dtype=np.int64).astype("datetime64[D]")
    month = d.astype("datetime64[M]")
    return (d.astype("datetime64[Y]").astype(np.int64) + 1970, month.astype(np.int64) % 12 + 1,
            (d - month).astype(np.int64) + 1)


# --- shared plans and lookups (once per process) -----------------------------------------------------------------

@lru_cache(maxsize=None)
def duplicates(ctx_items: tuple) -> tuple:
    """Who gets entered twice: (original person ids, sorted; dup_of, each person's duplicate id or -1). Duplicates
    take ids after the last person, in the originals' order."""
    ctx = dict(ctx_items)
    n = scale.rows("person", ctx["scale"])
    orig = np.flatnonzero(stream(ctx["seed"], "mess_plan", 0).random(n) < RATES["duplicate_person"])
    dup_of = np.full(n, -1, dtype=np.int64)
    dup_of[orig] = n + np.arange(len(orig))
    return orig, dup_of


@lru_cache(maxsize=None)
def anchor_days(table_dir: str, column: str) -> np.ndarray:
    """A clean table's date (or the day of its time) for every id, as days since 1970."""
    col = pq.read_table(table_dir, columns=[column])[column].combine_chunks()
    if pa.types.is_timestamp(col.type):
        col = col.cast(pa.date32())
    return col.cast(pa.int32()).to_numpy(zero_copy_only=False).astype(np.int64)


@lru_cache(maxsize=None)
def crosswalk(table_dir: str, column: str) -> dict:
    """{value: its other spellings} for a code column, from every value the clean column holds. A spelling is kept
    only if it can mean one value: it's not a value itself, and no other value is spelled that way."""
    con = duckdb.connect()
    values = [r[0] for r in con.execute(
        f"SELECT DISTINCT {column} FROM read_parquet('{table_dir}/*.parquet') WHERE {column} IS NOT NULL ORDER BY 1"
    ).fetchall()]
    con.close()
    spellings = {v: spellings_of(v, column) for v in values}
    taken = {}
    for v, options in spellings.items():
        for s in options:
            taken.setdefault(s, set()).add(v)
    reserved = {v for v in values}
    return {v: [s for s in options if s != v and s not in reserved and taken[s] == {v}]
            for v, options in spellings.items()}


def spellings_of(value: str, column: str) -> list:
    """Ways a source system might spell a code: case, word separators, an abbreviation, padding, plus any
    known alternatives (Male for M)."""
    words = value.replace("_", " ").split()
    out = [value.upper(), value.lower(), value.title(), " ".join(words), "_".join(words), f" {value} "]
    if len(words) > 1:
        out.append("".join(w[0] for w in words).upper())
    elif len(value) > 4:
        out.append(value[:4].upper())
    out += EXTRA_SPELLINGS.get(column, {}).get(value, [])
    return list(dict.fromkeys(out))


def offense_spellings(code: str) -> list:
    """How a NIBRS code gets damaged (gen_hero_offenders.offense_code_as_written): Excel's .0, a lowercase letter,
    a separator, the leading zero dropped."""
    out = []
    if code.isdigit():
        out.append(code + ".0")
    if code[-1].isalpha():
        out += [code.lower(), code[:-1] + "-" + code[-1], code[:-1] + " " + code[-1]]
    if code.startswith("0"):
        out.append(code[1:])
    return out


# --- making the mess -----------------------------------------------------------------------------------------------

def class_of(spec: dict, column: str):
    """Which kind of column this is in SPEC ("dates", "codes", ..., "fk"), or None if it stays clean."""
    if column in spec.get("fk", {}):
        return "fk"
    return next((c for c in VALUE_CLASSES if column in spec.get(c, [])), None)


def date_variants(rs, days: np.ndarray) -> np.ndarray:
    """Dates in the course's other formats, picked by its weights."""
    y, m, d = ymd(days)
    weights = np.array([w for _, w in DATE_STYLES], dtype=float)
    style = rs.choice(len(DATE_STYLES), len(days), p=weights / weights.sum())
    fmts = [f for f, _ in DATE_STYLES]
    return np.array([fmts[s].format(Y=f"{yy:04d}", y=yy % 100, m=mm, d=dd, MON=calendar.month_abbr[mm].upper(),
                                    Month=calendar.month_name[mm], Mon=calendar.month_abbr[mm])
                     for s, yy, mm, dd in zip(style.tolist(), y.tolist(), m.tolist(), d.tolist())], dtype=object)


def time_variants(rs, texts: np.ndarray) -> np.ndarray:
    """YYYY-MM-DD HH:MM:SS times in other formats (seconds kept, so nothing is lost)."""
    style = rs.integers(0, len(TIME_STYLES), len(texts))
    out = []
    for s, t in zip(style.tolist(), texts.tolist()):
        h = int(t[11:13])
        out.append(TIME_STYLES[s].format(Y=t[:4], mo=t[5:7], dd=t[8:10], H=t[11:13], M=t[14:16], S=t[17:19],
                                         I=f"{h % 12 or 12:02d}", p="AM" if h < 12 else "PM",
                                         MON=calendar.month_abbr[int(t[5:7])].upper()))
    return np.array(out, dtype=object)


def code_variants(rs, texts: np.ndarray, column: str, col_type, table_dir: str) -> np.ndarray:
    """Codes spelled another way. Spellings that can't be undone (or don't differ) are never used: a code with
    none left keeps its standard spelling (keep the "" entries)."""
    out = np.full(len(texts), "", dtype=object)
    if pa.types.is_boolean(col_type):
        style = rs.integers(0, len(BOOL_STYLES), len(texts))
        pairs = np.array(BOOL_STYLES, dtype=object)
        return np.where(texts == "true", pairs[style, 0], pairs[style, 1])
    options = ({v: offense_spellings(v) for v in np.unique(texts)} if column == "offense_code"
               else crosswalk(table_dir, column))
    for value in sorted(set(texts.tolist())):
        at = np.flatnonzero(texts == value)
        choices = options.get(value, [])
        if choices:
            out[at] = np.asarray(choices, dtype=object)[rs.integers(0, len(choices), len(at))]
    return out


def name_variants(rs, texts: np.ndarray) -> np.ndarray:
    """Names in another case; "" where no other case differs (a name with no letters)."""
    style = rs.integers(0, 3, len(texts))
    out = np.array([(t.title(), t.lower(), t.upper())[s] for s, t in zip(style.tolist(), texts.tolist())], dtype=object)
    other = np.array([t.lower() if t != t.lower() else t.upper() for t in texts.tolist()], dtype=object)
    out = np.where(out == texts, other, out)
    return np.where(out == texts, "", out)


def money_variants(rs, texts: np.ndarray) -> np.ndarray:
    """Whole dollars as the course writes money ($1,234.00, USD 1,234.00, - for zero)."""
    weights = np.array([w for _, w in MONEY_STYLES], dtype=float)
    style = rs.choice(len(MONEY_STYLES), len(texts), p=weights / weights.sum())
    zero = rs.integers(0, len(MONEY_ZERO), len(texts))
    fmts = [f for f, _ in MONEY_STYLES]
    return np.array([MONEY_ZERO[z] if v == 0 else fmts[s].format(v=v)
                     for s, z, v in zip(style.tolist(), zero.tolist(), texts.astype(np.int64).tolist())], dtype=object)


def number_variants(rs, texts: np.ndarray) -> np.ndarray:
    """Counts written as 12.0, 012, " 12" or (up to ten) a word."""
    style = rs.integers(0, 4, len(texts))
    values = texts.astype(np.int64).tolist()
    return np.array([(f"{v}.0", f"0{v}", f" {v}", NUMBER_WORDS[v] if 0 <= v <= 10 else f"{v}.0")[s]
                     for s, v in zip(style.tolist(), values)], dtype=object)


def dirty(table_name: str, clean: pa.Table, rs, ctx: dict) -> tuple:
    """A clean chunk's messy copy (a pyarrow Table of text, duplicate people appended) and its answer key."""
    spec = SPEC[table_name]
    id_col = f"{table_name}_id"
    truth = canonical(clean)
    ids = truth[id_col].astype(np.int64)
    rows = np.arange(len(ids))
    out_ids = ids
    if table_name == "person":
        orig, dup_of = duplicates(tuple(sorted(ctx.items())))
        here = orig[(orig >= ids[0]) & (orig <= ids[-1])]
        rows = np.concatenate([rows, here - ids[0]])
        out_ids = np.concatenate([ids, dup_of[here]])
    truth = {c: v[rows] for c, v in truth.items()}
    written = {c: v.copy() for c, v in truth.items()}
    found = []   # (row positions, column, kind, as written)

    def put(column, at, kind, values):
        """Write mess into some cells of a column and remember it for the key."""
        at = np.asarray(at)
        values = np.asarray(values, dtype=object)
        keep = values != truth[column][at]
        at, values = at[keep], values[keep]
        if len(at):
            written[column][at] = values
            found.append((at, column, kind, values))

    if table_name == "person" and len(rows) > len(ids):
        at = np.arange(len(ids), len(rows))
        put(id_col, at, "duplicate_person", out_ids[at].astype(str).astype(object))
    for column in clean.column_names:
        cls = class_of(spec, column)
        if cls is None:
            continue
        t = truth[column]
        u = rs.random(len(t))
        null = t == ""
        at = np.flatnonzero(null & (u < RATES["null_token"]))
        put(column, at, "null_token", np.asarray(NULL_TOKENS, dtype=object)[rs.integers(0, len(NULL_TOKENS), len(at))])
        if cls == "fk":
            target = spec["fk"][column]
            n_target = scale.rows(target, ctx["scale"])
            at = np.flatnonzero(~null & (u < RATES["orphan"]))
            put(column, at, "orphan", (n_target + ORPHAN_GAP + rs.integers(0, n_target, len(at))).astype(str).astype(object))
            if target == "person":
                dup_of = duplicates(tuple(sorted(ctx.items())))[1]
                cand = np.flatnonzero(~null & (u >= RATES["orphan"]) & (u < RATES["orphan"] + RATES["duplicate_ref"]))
                copy = dup_of[t[cand].astype(np.int64)]
                put(column, cand[copy >= 0], "duplicate_ref", copy[copy >= 0].astype(str).astype(object))
            continue
        tokens = DATE_UNKNOWN if cls in ("dates", "timestamps") else UNKNOWN_TOKENS[:4]
        at = np.flatnonzero(~null & (u < RATES["unknown"]))
        put(column, at, "unknown", np.asarray(tokens, dtype=object)[rs.integers(0, len(tokens), len(at))])
        lo = RATES["unknown"]
        if column in spec.get("order", {}):
            via, target, anchor = spec["order"][column]
            cand = np.flatnonzero(~null & (u >= lo) & (u < lo + RATES["out_of_order"]))
            if via is None:
                cand = cand[truth[anchor][cand] != ""]
                before = np.array([s[:10] for s in truth[anchor][cand]], dtype=object)
                before = pa.array(before.astype(str)).cast(pa.date32()).cast(pa.int32()).to_numpy().astype(np.int64)
            else:
                before = anchor_days(ctx[f"{target}_dir"], anchor)[truth[via][cand].astype(np.int64)]
            put(column, cand, "out_of_order", iso(before - rs.integers(1, 366, len(cand))))
        lo += RATES["out_of_order"]
        kind = FORMAT_KIND.get(cls)
        if kind is None:
            continue
        at = np.flatnonzero(~null & (u >= lo) & (u < lo + RATES[kind]))
        values = t[at]
        if cls == "dates":
            made = date_variants(rs, pa.array(values.astype(str)).cast(pa.date32()).cast(pa.int32()).to_numpy())
        elif cls == "timestamps":
            made = time_variants(rs, values)
        elif cls == "codes":
            made = code_variants(rs, values, column, clean.schema.field(column).type, ctx[f"{table_name}_dir"])
        elif cls == "names":
            made = name_variants(rs, values)
        elif cls == "money":
            made = money_variants(rs, values)
        else:
            made = number_variants(rs, values)
        usable = made != ""
        put(column, at[usable], kind, made[usable])

    messy = pa.table({c: pa.array(written[c], pa.string()) for c in clean.column_names})
    return messy, answer_key(found, clean.column_names, out_ids, truth)


def answer_key(found: list, columns: list, out_ids: np.ndarray, truth: dict) -> pa.Table:
    """The key's rows, in row then column order: the row's id, column_name, kind, as written, truth (null if none)."""
    if not found:
        return KEY_SCHEMA.empty_table()
    pos = np.concatenate([f[0] for f in found])
    col = np.concatenate([np.full(len(f[0]), columns.index(f[1])) for f in found])
    order = np.lexsort((col, pos))
    names = np.asarray(columns, dtype=object)
    kinds = np.concatenate([np.full(len(f[0]), f[2], dtype=object) for f in found])
    written = np.concatenate([f[3] for f in found])
    true = np.concatenate([truth[f[1]][f[0]] for f in found])
    return pa.table({"id": pa.array(out_ids[pos[order]], pa.int64()), "column_name": pa.array(names[col[order]], pa.string()),
                     "kind": pa.array(kinds[order], pa.string()), "as_written": pa.array(written[order], pa.string()),
                     "truth": pa.array(true[order], pa.string(), mask=true[order] == "")}, schema=KEY_SCHEMA)


# --- writing it ------------------------------------------------------------------------------------------------------

def csv_bytes(table: pa.Table) -> bytes:
    """A text table as CSV: a header, then a line per row, quoting only cells that need it (a comma, a quote, a line
    break, or a space at either end), so blanks and padding survive a round trip."""
    cols = []
    for name in table.column_names:
        col = table[name].combine_chunks()
        quoted = pc.binary_join_element_wise('"', pc.replace_substring(col, '"', '""'), '"', "")
        cols.append(pc.if_else(pc.match_substring_regex(col, NEEDS_QUOTES), quoted, col))
    lines = pc.binary_join_element_wise(*cols, ",") if len(cols) > 1 else cols[0]
    return ("\n".join([",".join(table.column_names)] + lines.to_pylist()) + "\n").encode("utf-8")


def write_chunk(table_name: str, chunk: int, ctx: dict, out_dir: Path) -> dict:
    """Make one clean chunk's messy CSV and answer key, write them under out_dir, and return their manifest entries:
    {"messy": {...}, "answer_key": {...}}."""
    part = f"part-{chunk:05d}"
    clean = pq.read_table(Path(ctx[f"{table_name}_dir"]) / f"{part}.parquet")
    messy, key = dirty(table_name, clean, stream(ctx["seed"], f"mess/{table_name}", chunk), ctx)
    out = {}
    for folder, name in (("messy", f"{part}.csv"), ("answer_key", f"{part}.parquet")):
        path = Path(out_dir) / folder / table_name / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if folder == "messy":
            path.write_bytes(csv_bytes(messy))
            rows = messy.num_rows
        else:
            pq.write_table(key, path, compression="zstd", use_dictionary=True, write_statistics=True)
            rows = key.num_rows
        out[folder] = {"name": name, "rows": rows, "sha256": core.sha256(path)}
    return out


def build_mess(out_dir: Path, ctx: dict, built: dict, workers: int = 1) -> dict:
    """Every built table's messy copy and answer key, chunk for chunk; manifest entries keyed messy/<table> and
    answer_key/<table>."""
    jobs = [(t, int(f["name"][5:10])) for t in SPEC if t in built for f in built[t]["files"]]
    args = [(t, c, ctx, out_dir) for t, c in jobs]
    if workers > 1 and len(args) > 1:
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
            done = list(pool.map(write_chunk, *zip(*args)))
    else:
        done = [write_chunk(*a) for a in args]
    entries = {}
    for (t, _), files in zip(jobs, done):
        for folder in ("messy", "answer_key"):
            e = entries.setdefault(f"{folder}/{t}", {"rows": 0, "files": []})
            e["rows"] += files[folder]["rows"]
            e["files"].append(files[folder])
    return entries
