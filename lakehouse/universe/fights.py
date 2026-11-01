"""The expanded world's fights: incidents, who fought in them, which powers they used, and what got damaged.

A fight's rows are spread over several tables and, in the bigger ones, over several chunks (one fight's
participants can straddle two files). So that any chunk builds alone, every process first works out the same
**plan** from its own stream, (seed, "incident_plan", 0): each fight's day, size (a severity weight), how many
villains, heroes and civilians it has, and its outcome. A fight's participants and damages then have consecutive
ids, so a chunk finds the fight behind each of its rows with a binary search over the plan's offsets.

  incident     incidents 0..94,119 are the fights behind today's offender records (by offender number, those with a
               SID and a sentence date): dated before the sentence, the record's villain captured. The rest fall
               between 1996 and today, more of them in recent years.
  participant  per fight: villains first (at least one), then heroes, then civilians. Each random villain slot goes
               to a new villain with chance NEW_VILLAIN_SHARE (while enough are about), else to one of today's, so
               new villains fight far more often each and fewer charges fall on today's people. A person is picked only once
               they could be there on the day: villains and heroes from when they gained their first power,
               civilians from 16. Within a fight, slot j of a side takes position (b + j * stride) mod c of the
               people active that day, with stride a prime bigger than any pool, so nobody is picked twice.
  power_use    only powers the person held on the day (found with one DuckDB join); uses are shared out over the
               villains and heroes who held one.
  damage       0 or more per fight, more in bigger fights; cost by asset type, insurance, repair.
"""
from datetime import date
from functools import lru_cache

import duckdb
import numpy as np
import pyarrow as pa

from . import scale
from .rng import stream
from .world import TODAY, as_date, column, days, pick, read

CORE_RECORDS = scale.CORE_RECORDS
FIRST_DAY = days(date(1996, 1, 1))
NEVER = np.iinfo(np.int32).max            # "no first power day" in the pools
SIDES = ("villain", "hero", "civilian")
NEW_VILLAIN_SHARE = 0.6   # chance a fight's random villain slot goes to a new villain (chunk 5b, Ian's option B)
OUTCOMES = np.asarray(["captured", "partial", "escaped"], dtype=object)
INCIDENT_TYPES = {"street fight": 0.28, "robbery": 0.2, "heist": 0.12, "rampage": 0.1, "standoff": 0.1,
                  "sabotage": 0.08, "abduction": 0.07, "breakout": 0.05}
# asset type: (typical cost in USD, chance it's insured)
ASSETS = {"vehicle": (18_000, 0.8), "building": (120_000, 0.85), "infrastructure": (400_000, 0.4),
          "public property": (25_000, 0.3), "personal property": (4_000, 0.5), "utility": (80_000, 0.7)}
ASSET_SHARE = [0.3, 0.25, 0.12, 0.15, 0.13, 0.05]
TIER_EFFECT = {"DELTA": 0.35, "BETA": 0.5, "ALPHA": 0.65, "OMEGA": 0.8}


def primes_above(n: int, count: int) -> np.ndarray:
    """The first count primes above n (trial division; n is small enough)."""
    out, k = [], n + 1
    while len(out) < count:
        if all(k % d for d in range(2, int(k ** 0.5) + 1)):
            out.append(k)
        k += 1
    return np.asarray(out, dtype=np.int64)


STRIDES = primes_above(10_000_000, 8)     # bigger than any pool at any scale, so coprime with every pool size


# ---- who could fight when ---------------------------------------------------------------------------------------

@lru_cache(maxsize=None)
def first_power_day(person_dir: str, person_power_dir: str) -> np.ndarray:
    """For every person, the earliest day they held a power (NEVER for no power, or none with a known day)."""
    pp = read(person_power_dir)
    start = np.full(read(person_dir).num_rows, NEVER, dtype=np.int64)
    np.minimum.at(start, column(pp, "person_id"), column(pp, "acquired_on", fill=NEVER).astype(np.int64))
    return start


@lru_cache(maxsize=None)
def pools(person_dir: str, person_power_dir: str) -> dict:
    """{pool: (person ids, the day each could first fight)}, sorted by that day, so the people who could be in a
    fight on day t are the first searchsorted(days, t, "right") of them. Villains come in two pools, today's and
    new ones, so a fight can take some from each without picking anyone twice."""
    people = read(person_dir)
    role, birth = column(people, "role"), column(people, "birth_date", fill=0).astype(np.int64)
    start = first_power_day(person_dir, person_power_dir)
    todays = np.arange(len(role)) < scale.CORE_PEOPLE
    ready = {"core_villain": (role == "villain") & todays & (start < NEVER),
             "new_villain": (role == "villain") & ~todays & (start < NEVER),
             "hero": np.isin(role, ["hero", "secondary_hero"]) & (start < NEVER),
             "civilian": role == "civilian"}
    out = {}
    for side, ok in ready.items():
        ids = np.flatnonzero(ok)
        when = birth[ids] + 16 * 365 if side == "civilian" else start[ids]
        order = np.lexsort((ids, when))
        out[side] = (ids[order], when[order])
    return out


# ---- the plan ---------------------------------------------------------------------------------------------------

def plan_for(ctx: dict) -> dict:
    """The fight plan for a build's context."""
    return plan(ctx["seed"], ctx["scale"], ctx["records"], ctx["people"], ctx["person_dir"], ctx["person_power_dir"])


@lru_cache(maxsize=None)
def plan(seed: int, scale_name: str, records_path: str, people_path: str, person_dir: str,
         person_power_dir: str) -> dict:
    """Every fight's day, severity, villain/hero/civilian counts, outcome and pick parameters, plus the offsets of
    its participants and damages. The same in every process: it comes from its own stream, not a chunk's."""
    rs = stream(seed, "incident_plan", 0)
    n_inc = scale.rows("incident", scale_name)
    n_part, n_dmg = scale.rows("participant", scale_name), scale.rows("damage", scale_name)
    if n_inc < CORE_RECORDS or n_part < 2 * n_inc:
        raise ValueError(f"scale {scale_name}: too few incidents or participants for today's records")

    # Today's records: the villain (person ids follow SID order) and a day before the sentence, on or after the
    # day they first held a power, at most two years before.
    records = read(records_path)
    sids = column(read(people_path), "sid_number")
    record_person = np.searchsorted(sids, column(records, "sid_number")).astype(np.int64)
    sentence = column(records, "sentence_date").astype(np.int64)
    known = first_power_day(person_dir, person_power_dir)[record_person]
    lo = np.maximum.reduce([np.full(CORE_RECORDS, FIRST_DAY), sentence - 730, np.where(known < NEVER, known, 0)])
    day = np.empty(n_inc, dtype=np.int64)
    day[:CORE_RECORDS] = lo + (rs.random(CORE_RECORDS) * (sentence - lo)).astype(np.int64)
    # The rest: 1996 to today, growing (density e^(2x) over the span), so recent years have more fights.
    x = np.log1p(rs.random(n_inc - CORE_RECORDS) * np.expm1(2.0)) / 2.0
    day[CORE_RECORDS:] = np.minimum(FIRST_DAY + (x * (TODAY - FIRST_DAY + 1)).astype(np.int64), TODAY)

    # Sizes: one villain and one more person each, the rest shared out by severity, then split by side.
    weight = rs.gamma(1.5, 1.0, n_inc)
    second = rs.choice(3, n_inc, p=[0.3, 0.45, 0.25])
    extra = rs.multinomial(n_part - 2 * n_inc, weight / weight.sum())
    by_side = rs.multinomial(extra, [0.35, 0.4, 0.25])
    by_side[:, 0] += 1
    by_side[np.arange(n_inc), second] += 1
    nv, nh, nc = (by_side[:, k].astype(np.int64) for k in range(3))

    # Outcome: one villain is caught or gets away; with more, some may be caught. Today's records: caught.
    outcome = np.where(nv == 1, np.where(rs.random(n_inc) < 0.55, 0, 2), rs.choice(3, n_inc, p=[0.35, 0.35, 0.3]))
    outcome[:CORE_RECORDS] = np.where(nv[:CORE_RECORDS] == 1, 0, np.where(rs.random(CORE_RECORDS) < 0.5, 0, 1))

    out = {"day": day, "weight": weight, "nv": nv, "nh": nh, "nc": nc, "outcome": outcome,
           "b": rs.integers(0, 2 ** 40, (n_inc, 3)), "s": STRIDES[rs.integers(0, len(STRIDES), (n_inc, 3))],
           "record_person": record_person,
           "part_off": np.concatenate([[0], np.cumsum(nv + nh + nc)]),
           "dmg_off": np.concatenate([[0], np.cumsum(rs.multinomial(n_dmg, weight / weight.sum()))])}
    # Drawn last, so every draw above (and so every fight's day, size and outcome) stays as it was before chunk 5b:
    # how many of each fight's random villain slots go to new villains, at most as many as are about that day.
    records = (np.arange(n_inc) < CORE_RECORDS).astype(np.int64)
    new_about = np.searchsorted(pools(person_dir, person_power_dir)["new_villain"][1], day, side="right")
    out["n_new"] = np.minimum(rs.binomial(nv - records, NEW_VILLAIN_SHARE), new_about)
    return out


def owner(offsets: np.ndarray, ids: np.ndarray) -> np.ndarray:
    """The incident each row belongs to, from the plan's offsets (rows of incident i are offsets[i]..offsets[i+1]-1)."""
    return np.searchsorted(offsets, ids, side="right") - 1


# ---- incidents --------------------------------------------------------------------------------------------------

def incident(stream, start, n, ctx):
    """Incidents start..start+n-1: when (mostly evenings), how long, where, what kind, how it ended, and how many
    bystanders (people not named as participants) were hurt or killed."""
    p = plan_for(ctx)
    ids = np.arange(start, start + n, dtype=np.int64)
    weight = p["weight"][ids]
    places = read(ctx["location_dir"])
    sites = np.flatnonzero((column(places, "kind") == "site") & np.asarray(places["unit_code"].is_null()))
    evening = stream.random(n) < 0.45
    secs = np.where(evening, stream.normal(22 * 3600, 2.5 * 3600, n), stream.uniform(0, 86_400, n)).astype(np.int64) % 86_400
    started = (p["day"][ids] * 86_400 + secs) * 1_000_000
    duration = 5 + np.round(weight * 25 * stream.lognormal(0, 0.5, n)).astype(np.int32)
    kinds = list(INCIDENT_TYPES)
    return pa.table({"incident_id": ids,
                     "started_at": pa.array(started, pa.int64()).cast(pa.timestamp("us")),
                     "duration_min": pa.array(duration, pa.int32()),
                     "location_id": pa.array(sites[stream.integers(0, len(sites), n)], pa.int32()),
                     "incident_type": np.asarray(kinds, dtype=object)[stream.choice(len(kinds), n, p=list(INCIDENT_TYPES.values()))],
                     "outcome": OUTCOMES[p["outcome"][ids]],
                     "bystanders_injured": pa.array(stream.poisson(weight * 1.2), pa.int32()),
                     "bystanders_killed": pa.array(stream.poisson(weight * 0.05), pa.int32())})


# ---- participants -----------------------------------------------------------------------------------------------

def participant(stream, start, n, ctx):
    """Participants start..start+n-1: who fought in which incident, on which side, in what role, and what became of
    them (villains captured or escaped, heroes and civilians unharmed, injured or, for civilians, rescued)."""
    p = plan_for(ctx)
    ids = np.arange(start, start + n, dtype=np.int64)
    inc = owner(p["part_off"], ids)
    slot = ids - p["part_off"][inc]
    nv, nh = p["nv"][inc], p["nh"][inc]
    side = np.where(slot < nv, 0, np.where(slot < nv + nh, 1, 2))
    j = slot - np.where(side == 0, 0, np.where(side == 1, nv, nv + nh))
    size = np.choose(side, [nv, nh, p["nc"][inc]])
    day = p["day"][inc]
    people = pools(ctx["person_dir"], ctx["person_power_dir"])

    def drawn(rows, k, pool, need):
        """Person at stride position k of a pool, for the given rows (all of one side); need is how many each
        row's fight takes from that pool."""
        sc = side[rows][0]
        who, when = people[pool]
        active = np.searchsorted(when, day[rows], side="right")
        if (active < need).any():
            raise ValueError(f"too few people in the {pool} pool about on some day for the fights planned")
        return who[(p["b"][inc[rows], sc] + k * p["s"][inc[rows], sc]) % active]

    person = np.empty(n, dtype=np.int64)
    for sc, pool in ((1, "hero"), (2, "civilian")):
        rows = np.flatnonzero(side == sc)
        if len(rows):
            person[rows] = drawn(rows, j[rows], pool, size[rows])
    # Villains: in a record's fight, slot 0 is the record's villain; the next n_new slots are new villains, the
    # rest today's (their stride positions start at 1 in a record's fight, keeping position 0 free).
    rec = (inc < CORE_RECORDS).astype(np.int64)
    n_new = p["n_new"][inc]
    from_new = (side == 0) & (j >= rec) & (j < rec + n_new)
    from_core = (side == 0) & (j >= rec + n_new)
    for mask, pool, k, need in ((from_new, "new_villain", j - rec, n_new), (from_core, "core_villain", j - n_new, nv - n_new)):
        rows = np.flatnonzero(mask)
        if len(rows):
            person[rows] = drawn(rows, k[rows], pool, need[rows])
    # Today's records: villain 0 is the record's villain. Another of today's villains who drew them takes position
    # 0 instead, which no other slot uses and which holds someone else.
    core = (inc < CORE_RECORDS) & (side == 0)
    record = p["record_person"][np.minimum(inc, CORE_RECORDS - 1)]
    person[core & (j == 0)] = record[core & (j == 0)]
    clash = np.flatnonzero(core & (j > 0) & (person == record))
    if len(clash):
        person[clash] = drawn(clash, 0, "core_villain", (nv - n_new)[clash])

    outcome = p["outcome"][inc]
    caught = np.where(outcome == 0, True, np.where(outcome == 2, False,
                      np.where(j == 0, True, np.where(j == 1, False, stream.random(n) < 0.5))))
    hurt = stream.random(n)
    status = np.where(side == 0, np.where(caught, "captured", "escaped"),
                      np.where(side == 1, np.where(hurt < 0.2, "injured", "unharmed"),
                               np.where((hurt < 0.25) & (nh > 0), "rescued", np.where(hurt > 0.85, "injured", "unharmed"))))
    leads = (j == 0) & (size >= 3) & (side < 2)
    role = np.where(side == 0, np.where(stream.random(n) < 0.7, "fighter", "accomplice"),
                    np.where(side == 1, np.where(stream.random(n) < 0.75, "fighter", "support"),
                             pick(stream, ["bystander"] * 6 + ["victim"] * 3 + ["witness"], n)))
    role = np.where(leads, "leader", role)
    return pa.table({"participant_id": ids, "incident_id": inc, "person_id": person,
                     "side": np.asarray(SIDES, dtype=object)[side], "role": role.astype(object),
                     "status": status.astype(object)})


# ---- power uses -------------------------------------------------------------------------------------------------

HELD_ON_THE_DAY = """
    SELECT pa.participant_id, pa.incident_id, pp.power_id, w.tier
    FROM read_parquet('{participant_dir}/*.parquet') pa
    JOIN read_parquet('{incident_dir}/*.parquet') i USING (incident_id)
    JOIN read_parquet('{person_power_dir}/*.parquet') pp
      ON pp.person_id = pa.person_id
     AND (pp.acquired_on IS NULL OR pp.acquired_on <= i.started_at::DATE)
     AND (pp.lost_on IS NULL OR pp.lost_on > i.started_at::DATE)
    JOIN read_parquet('{power_dir}/*.parquet') w USING (power_id)
    WHERE pa.side <> 'civilian'
    ORDER BY pa.participant_id, pp.power_id"""


@lru_cache(maxsize=None)
def use_plan(seed: int, scale_name: str, participant_dir: str, incident_dir: str, person_power_dir: str,
             power_dir: str) -> dict:
    """Every (participant, power they held on the day) pair, and which participant each power use belongs to:
    the uses are shared out at random over the participants with at least one such power."""
    con = duckdb.connect()
    held = con.execute(HELD_ON_THE_DAY.format(participant_dir=participant_dir, incident_dir=incident_dir,
                                              person_power_dir=person_power_dir, power_dir=power_dir)).arrow().read_all()
    con.close()
    pid = column(held, "participant_id")
    first = np.flatnonzero(np.concatenate([[True], pid[1:] != pid[:-1]]))
    counts = np.diff(np.concatenate([first, [len(pid)]]))
    rs = stream(seed, "power_use_plan", 0)
    uses = rs.multinomial(scale.rows("power_use", scale_name), np.full(len(first), 1 / len(first)))
    return {"participant": pid, "incident": column(held, "incident_id"), "power": column(held, "power_id"),
            "tier": column(held, "tier"), "first": first, "count": counts,
            "user": np.repeat(np.arange(len(first)), uses)}


def power_use(stream, start, n, ctx):
    """Power uses start..start+n-1: which participant used which power (one they held that day) and how well it
    worked (0 to 1; stronger tiers work better)."""
    u = use_plan(ctx["seed"], ctx["scale"], ctx["participant_dir"], ctx["incident_dir"], ctx["person_power_dir"],
                 ctx["power_dir"])
    who = u["user"][start: start + n]
    row = u["first"][who] + (stream.random(n) * u["count"][who]).astype(np.int64)
    base = np.vectorize(TIER_EFFECT.get, otypes=[float])(u["tier"][row])
    effect = np.round(np.clip(base + stream.normal(0, 0.18, n), 0, 1), 2)
    return pa.table({"power_use_id": np.arange(start, start + n, dtype=np.int64),
                     "incident_id": u["incident"][row], "participant_id": u["participant"][row],
                     "power_id": pa.array(u["power"][row], pa.int16()), "effectiveness": pa.array(effect, pa.float32())})


# ---- damage -----------------------------------------------------------------------------------------------------

def damage(stream, start, n, ctx):
    """Damages start..start+n-1: what was damaged in which fight, what it cost (whole USD), whether it was insured
    and what the insurer paid (null while the claim is open), and how the repair stands."""
    p = plan_for(ctx)
    ids = np.arange(start, start + n, dtype=np.int64)
    inc = owner(p["dmg_off"], ids)
    day, weight = p["day"][inc], p["weight"][inc]
    kinds = list(ASSETS)
    asset = stream.choice(len(kinds), n, p=ASSET_SHARE)
    typical = np.asarray([ASSETS[k][0] for k in kinds])[asset]
    cost = np.maximum(50, np.round(typical * stream.lognormal(0, 1.0, n) * (0.5 + weight / 2))).astype(np.int64)
    insured = stream.random(n) < np.asarray([ASSETS[k][1] for k in kinds])[asset]
    settled = insured & (day < TODAY - 60) & (stream.random(n) < 0.9)
    paid = np.round(cost * stream.uniform(0.5, 1.0, n)).astype(np.int64)
    finish = day + stream.integers(7, 720, n)
    written_off = stream.random(n) < 0.08
    repaired = ~written_off & (finish <= TODAY)
    status = np.where(written_off, "written off", np.where(repaired, "repaired",
                      np.where(TODAY - day > 30, "in repair", "assessed")))
    return pa.table({"damage_id": ids, "incident_id": inc,
                     "asset_type": np.asarray(kinds, dtype=object)[asset], "cost_usd": cost, "insured": insured,
                     "insurance_paid_usd": pa.array(paid, pa.int64(), mask=~settled),
                     "repair_status": status.astype(object), "repaired_on": as_date(finish, repaired)})
