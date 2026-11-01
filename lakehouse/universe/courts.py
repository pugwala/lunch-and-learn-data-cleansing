"""The expanded world's charges and court outcomes: what captured villains were charged with, and how it ended.

  charge       charges 0..94,119 are today's offender records with a SID and a sentence date, in offender-number
               order: charge i is on fight i's villain (participant slot 0), with the record's offense code (null
               where the lake couldn't standardize it), filed between the fight and the sentence. The rest are
               shared out at random over the other captured villains (some get two or three, many none), with NIBRS
               codes that fit the fight, filed within 60 days of it.
  disposition  one per charge (disposition i is charge i's). A record's charge is a conviction decided on the
               sentence date and carries the record's offender number: the link back to the course's data. Today's
               people are only ever convicted through their records, so their other charges end acquitted or
               dismissed. A charge whose decision would fall after today is pending.
"""
from functools import lru_cache

import duckdb
import numpy as np
import pyarrow as pa

from . import fights, scale
from .rng import stream
from .world import TODAY, as_date, column, read

CORE_RECORDS = scale.CORE_RECORDS
# NIBRS codes (lake.ref.nibrs_offenses) that fit each kind of fight.
CODES_BY_TYPE = {"street fight": ["13A", "13B", "13C", "90C", "290"], "robbery": ["120", "13A", "23H", "280"],
                 "heist": ["220", "23H", "26G", "280", "250"], "rampage": ["290", "13A", "09B", "200", "90C"],
                 "standoff": ["13C", "520", "90J", "90Z"], "sabotage": ["290", "200", "26G", "90J"],
                 "abduction": ["100", "13A", "210"], "breakout": ["90Z", "13A", "520", "90J"]}
NEW_PEOPLE_OUTCOMES = {"convicted": 0.35, "pleaded guilty": 0.3, "acquitted": 0.1, "dismissed": 0.25}
TODAYS_PEOPLE_OUTCOMES = {"acquitted": 0.3, "dismissed": 0.7}

CAPTURED = """
    SELECT pa.participant_id, pa.incident_id, i.incident_type, pe.is_core
    FROM read_parquet('{participant_dir}/*.parquet') pa
    JOIN read_parquet('{incident_dir}/*.parquet') i USING (incident_id)
    JOIN read_parquet('{person_dir}/*.parquet') pe USING (person_id)
    WHERE pa.status = 'captured' ORDER BY pa.participant_id"""


def draw(stream, weights: dict, n: int) -> np.ndarray:
    """n picks from {value: weight}, as an object array."""
    return np.asarray(list(weights), dtype=object)[stream.choice(len(weights), n, p=list(weights.values()))]


@lru_cache(maxsize=None)
def charge_plan(ctx_items: tuple) -> dict:
    """Who the charges after today's records are on: every captured villain except the records' own (fight i's
    slot 0), and how many charges each gets, shared out at random from the stream (seed, "charge_plan", 0)."""
    ctx = dict(ctx_items)
    con = duckdb.connect()
    caught = con.execute(CAPTURED.format(**ctx)).arrow().read_all()
    con.close()
    pid = column(caught, "participant_id")
    records_own = fights.plan_for(ctx)["part_off"][:CORE_RECORDS]
    keep = ~np.isin(pid, records_own)
    rest = scale.rows("charge", ctx["scale"]) - CORE_RECORDS
    rs = stream(ctx["seed"], "charge_plan", 0)
    counts = rs.multinomial(rest, np.full(int(keep.sum()), 1 / keep.sum()))
    take = lambda name: np.repeat(column(caught, name)[keep], counts)  # noqa: E731
    return {"participant": take("participant_id"), "incident": take("incident_id"),
            "incident_type": take("incident_type"), "is_core": take("is_core")}


def charge(stream, start, n, ctx):
    """Charges start..start+n-1: on which participant in which fight, the NIBRS offense code, and when filed."""
    p = fights.plan_for(ctx)
    records = read(ctx["records"])
    ids = np.arange(start, start + n, dtype=np.int64)
    k = int((ids < CORE_RECORDS).sum())
    incident, participant = np.zeros(n, np.int64), np.zeros(n, np.int64)
    code, filed = np.empty(n, dtype=object), np.zeros(n, np.int64)
    if k:
        incident[:k] = ids[:k]
        participant[:k] = p["part_off"][ids[:k]]
        code[:k] = column(records, "offense_code")[ids[:k]]
        day, sentence = p["day"][ids[:k]], column(records, "sentence_date").astype(np.int64)[ids[:k]]
        filed[:k] = day + (stream.random(k) * (sentence - day)).astype(np.int64)
    m = n - k
    if m:
        c = charge_plan(tuple(sorted(ctx.items())))
        rows = ids[k:] - CORE_RECORDS
        incident[k:], participant[k:] = c["incident"][rows], c["participant"][rows]
        kinds = c["incident_type"][rows]
        for kind, codes in CODES_BY_TYPE.items():
            at = np.flatnonzero(kinds == kind)
            code[k + at] = np.asarray(codes, dtype=object)[stream.integers(0, len(codes), len(at))]
        filed[k:] = np.minimum(p["day"][incident[k:]] + stream.integers(0, 60, m), TODAY)
    return pa.table({"charge_id": ids, "incident_id": incident, "participant_id": participant,
                     "offense_code": pa.array(code, pa.string()), "filed_on": as_date(filed)})


def disposition(stream, start, n, ctx):
    """Court outcomes start..start+n-1 (one per charge): convicted, pleaded guilty, acquitted, dismissed or
    pending; the day it was decided; and, for today's records, the offender number."""
    records = read(ctx["records"])
    ids = np.arange(start, start + n, dtype=np.int64)
    filed = column(read(ctx["charge_dir"]), "filed_on").astype(np.int64)[ids]
    k = int((ids < CORE_RECORDS).sum())
    outcome, decided = np.empty(n, dtype=object), np.zeros(n, np.int64)
    if k:
        outcome[:k] = np.where(stream.random(k) < 0.6, "convicted", "pleaded guilty")
        decided[:k] = column(records, "sentence_date").astype(np.int64)[ids[:k]]
    m = n - k
    if m:
        is_core = charge_plan(tuple(sorted(ctx.items())))["is_core"][ids[k:] - CORE_RECORDS]
        decided[k:] = filed[k:] + stream.integers(30, 900, m)
        ruled = np.where(is_core, draw(stream, TODAYS_PEOPLE_OUTCOMES, m), draw(stream, NEW_PEOPLE_OUTCOMES, m))
        outcome[k:] = np.where(decided[k:] > TODAY, "pending", ruled)
    numbers = records["offender_number"].slice(start, k).combine_chunks() if k else pa.array([], pa.string())
    numbers = pa.concat_arrays([numbers.cast(pa.string()), pa.nulls(m, pa.string())])
    return pa.table({"disposition_id": ids, "charge_id": ids, "outcome": outcome,
                     "decided_on": as_date(decided, outcome != "pending"), "offender_number": numbers})
