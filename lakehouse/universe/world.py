"""The expanded world's first tables: places, powers, teams, people, who holds which power, and team members.

Each table has a factory, factory(stream, start, n, ctx), that makes rows start..start+n-1 as a pyarrow Table from
whole numpy arrays. ctx is a small dict of paths and the scale (build.py passes it with functools.partial), so the
factories are top-level functions that worker processes can import. Later tables read the ones built before them
from disk (person_power reads person), so build.py builds them in order.

The layout is fixed so every table knows the others' ids without reading them:
  location  cities (the course's 55, then every other custody-unit place), 10 districts per city, one site per
            custody unit, then ordinary sites up to 20,000
  power     the 88 powers held today first, then new ones up to 400
  team      the 12 STG groups first (as villain teams), then 88 more villain teams, then 200 hero teams
  person    today's 86,879 people in SID order, then new villains, heroes, secondary heroes and civilians
"""
from datetime import date
from functools import lru_cache

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from . import core, scale

EPOCH = date(1970, 1, 1)
DISTRICTS = ["Downtown", "Old Town", "Harbor", "Uptown", "Eastside", "Westside", "Northgate", "Southport",
             "Riverside", "Midtown"]
SITE_TYPES = ["Bank", "Lab", "Museum", "Harbor Pier", "Park", "Tower", "Power Plant", "Bridge", "Stadium",
              "Warehouse", "Mall", "Rail Yard", "Airport", "Embassy", "Casino", "Reservoir"]
POWER_WORDS = ["BLAST", "SHIELD", "SIGHT", "STEP", "TOUCH", "WAVE", "SENSE", "FORM", "BURST", "GRIP", "CLOAK",
               "PULSE", "STORM", "LINK", "SHIFT", "BIND"]
GEAR_WORDS = ("GADGETS", "ARMOR", "SUIT", "SHIELD", "MACE", "LASSO", "RING", "BANDS", "JETPACK", "CROSSBOW", "ROD",
              "BUDGET")
TEAM_KINDS = {"villain": ["Syndicate", "Cabal", "Gang", "Brotherhood", "Circle", "Cartel"],
              "hero": ["League", "Squad", "Corps", "Guard", "Patrol", "Alliance", "Force"]}
N_LOCATIONS, N_POWERS, N_VILLAIN_TEAMS, N_TEAMS = 20_000, 400, 100, 300
TIER_WEIGHTS = {"DELTA": 0.35, "BETA": 0.35, "ALPHA": 0.22, "OMEGA": 0.08}


# ---- shared helpers ---------------------------------------------------------------------------------------------

def days(d: date) -> int:
    """A date as days since 1970-01-01, the way Arrow stores date32."""
    return (d - EPOCH).days


def as_date(day_numbers, valid=None) -> pa.Array:
    """Days since 1970 as an Arrow date32 array; where valid is False the value is null."""
    mask = None if valid is None else ~np.asarray(valid, dtype=bool)
    return pa.array(np.asarray(day_numbers, dtype=np.int32), type=pa.int32(), mask=mask).cast(pa.date32())


TODAY = days(date(2026, 9, 24))   # the course's "today" (gen_hero_offenders.TODAY)


@lru_cache(maxsize=None)
def read(path: str) -> pa.Table:
    """A Parquet file or a built table's folder, read once per process."""
    return pq.read_table(path)


def column(table: pa.Table, name: str, fill=None) -> np.ndarray:
    """A column as numpy; dates as days since 1970, with fill where null (strings come back as objects)."""
    col = table[name]
    if pa.types.is_date32(col.type):
        col = col.cast(pa.int32())
    if fill is not None:
        col = col.fill_null(fill)
    return col.to_numpy(zero_copy_only=False)


@lru_cache(maxsize=None)
def cities(units_path: str) -> tuple:
    """City names in id order: the course's cities, then every custody-unit place not among them."""
    course = list(core.course_constants()["CITIES"])
    unit_places = sorted(set(column(read(units_path), "unit_name")) - set(course))
    return tuple(course + unit_places)


def pick(stream, options, n):
    """n random picks from a list, as an object array."""
    return np.asarray(options, dtype=object)[stream.integers(0, len(options), n)]


# ---- places -----------------------------------------------------------------------------------------------------

def location(stream, start, n, ctx):
    """Every place: cities, their districts, one site per custody unit, then ordinary sites, with coordinates."""
    units = read(ctx["units"])
    names = cities(ctx["units"])
    n_cities = len(names)
    city_lat, city_lon = stream.uniform(-50, 62, n_cities), stream.uniform(-160, 170, n_cities)
    ids, kind, name, parent, city_id, unit_code, site_type, lat, lon = ([] for _ in range(9))

    def add(k, nm, par, cid, code, stype, la, lo):
        """Append one place."""
        ids.append(len(ids)); kind.append(k); name.append(nm); parent.append(par); city_id.append(cid)
        unit_code.append(code); site_type.append(stype); lat.append(la); lon.append(lo)

    for c, nm in enumerate(names):
        add("city", nm, None, c, None, None, city_lat[c], city_lon[c])
    jitter = stream.normal(0, 0.04, (n_cities * len(DISTRICTS), 2))
    for c, nm in enumerate(names):
        for k, d in enumerate(DISTRICTS):
            j = jitter[c * len(DISTRICTS) + k]
            add("district", f"{nm} {d}", c, c, None, None, city_lat[c] + j[0], city_lon[c] + j[1])
    district_of = lambda c, k=0: n_cities + c * len(DISTRICTS) + k  # noqa: E731
    for code, uname in zip(column(units, "unit_code"), column(units, "unit_name")):
        c = names.index(uname)
        add("site", f"{uname} Unit", district_of(c), c, code, "Custody Unit", city_lat[c] + 0.01, city_lon[c] - 0.01)
    rest = N_LOCATIONS - len(ids)
    rc, rk = stream.integers(0, n_cities, rest), stream.integers(0, len(DISTRICTS), rest)
    rt, rj = stream.integers(0, len(SITE_TYPES), rest), stream.normal(0, 0.01, (rest, 2))
    for i in range(rest):
        c, k = int(rc[i]), int(rk[i])
        add("site", f"{names[c]} {SITE_TYPES[rt[i]]} {i + 1}", district_of(c, k), c, None, SITE_TYPES[rt[i]],
            city_lat[c] + jitter[c * len(DISTRICTS) + k][0] + rj[i][0],
            city_lon[c] + jitter[c * len(DISTRICTS) + k][1] + rj[i][1])
    table = pa.table({"location_id": pa.array(ids, pa.int32()), "kind": kind, "name": name,
                      "parent_id": pa.array(parent, pa.int32()), "city_id": pa.array(city_id, pa.int32()),
                      "unit_code": unit_code, "site_type": site_type,
                      "latitude": np.clip(np.round(lat, 5), -90, 90), "longitude": np.clip(np.round(lon, 5), -180, 180)})
    return table.slice(start, n)


# ---- powers -----------------------------------------------------------------------------------------------------

def power(stream, start, n, ctx):
    """400 powers: today's 88 first, with the course's tiers, then new ones; each with a tier and permanence."""
    c = core.course_constants()
    course_tier = {p: tier for tier, ps in c["POWERS"].items() for p in ps}
    held = sorted(set(column(read(ctx["powers"]), "power")))
    words = [f"{p} {w}" for p in c["PREFIX"] for w in POWER_WORDS]
    new = [w for w in np.asarray(words, dtype=object)[stream.permutation(len(words))] if w not in held]
    names = held + new[: N_POWERS - len(held)]
    tiers = list(TIER_WEIGHTS)
    drawn = np.asarray(tiers, dtype=object)[stream.choice(len(tiers), N_POWERS, p=list(TIER_WEIGHTS.values()))]
    tier = [course_tier.get(nm, drawn[i]) for i, nm in enumerate(names)]
    perm_draw = np.asarray(["permanent", "temporary", "artifact"], dtype=object)[
        stream.choice(3, N_POWERS, p=[0.7, 0.15, 0.15])]
    permanence = ["artifact" if any(g in nm for g in GEAR_WORDS) else ("permanent" if i < len(held) else perm_draw[i])
                  for i, nm in enumerate(names)]
    category = ["gear" if p == "artifact" else ("skill" if t == "DELTA" else "ability") for p, t in zip(permanence, tier)]
    table = pa.table({"power_id": pa.array(range(N_POWERS), pa.int16()), "name": names, "tier": tier,
                      "permanence": permanence, "category": category})
    return table.slice(start, n)


# ---- teams ------------------------------------------------------------------------------------------------------

def team(stream, start, n, ctx):
    """300 teams: the 12 STG groups, 88 more villain teams, then 200 hero teams, each with a founding date and base."""
    c = core.course_constants()
    stg = list(c["STG_GROUPS"])
    n_cities = len(cities(ctx["units"]))
    combos = [(a, b) for a in c["ADJ"] for b in c["NOUN"]]
    order = stream.permutation(len(combos))
    names, side, stg_group = list(stg), ["villain"] * len(stg), list(stg)
    for i in range(N_TEAMS - len(stg)):
        s = "villain" if len(names) < N_VILLAIN_TEAMS else "hero"
        a, b = combos[order[i]]
        kind = TEAM_KINDS[s][i % len(TEAM_KINDS[s])]
        names.append(f"{a.title()} {b.title()} {kind}")
        side.append(s)
        stg_group.append(None)
    founded = stream.integers(days(date(1960, 1, 1)), days(date(2015, 12, 31)), N_TEAMS)
    table = pa.table({"team_id": pa.array(range(N_TEAMS), pa.int16()), "name": names, "side": side,
                      "stg_group": stg_group, "founded_on": as_date(founded),
                      "base_location_id": pa.array(stream.integers(0, n_cities, N_TEAMS), pa.int32())})
    return table.slice(start, n)


# ---- people -----------------------------------------------------------------------------------------------------

def role_bounds(scale_name: str) -> dict:
    """{role: (first person_id, last + 1)} for the new people's blocks, which follow today's people."""
    bounds, at = {}, scale.CORE_PEOPLE
    for role, count in scale.roles(scale_name).items():
        bounds[role] = (at, at + count)
        at += count
    return bounds


def aliases(stream, n):
    """n procedural aliases in the course's two styles: "ADJ NOUN" or PREFIX+SUFFIX run together."""
    c = core.course_constants()
    adj, noun, pre, suf = (np.asarray(c[k], dtype=object) for k in ("ADJ", "NOUN", "PREFIX", "SUFFIX"))
    a, b = adj[stream.integers(0, len(adj), n)], noun[stream.integers(0, len(noun), n)]
    p, s = pre[stream.integers(0, len(pre), n)], suf[stream.integers(0, len(suf), n)]
    style = stream.random(n) < 0.6
    return np.where(style, a + " " + b, p + s)


def person(stream, start, n, ctx):
    """People start..start+n-1: today's people from the lake's latest records, then new ones by role block."""
    c = core.course_constants()
    names = cities(ctx["units"])
    city_ids = {nm: i for i, nm in enumerate(names)}
    ids = np.arange(start, start + n, dtype=np.int64)
    is_core = ids < scale.CORE_PEOPLE
    role = np.full(n, "civilian", dtype=object)
    for r, (lo, hi) in role_bounds(ctx["scale"]).items():
        role[(ids >= lo) & (ids < hi)] = r
    role[is_core] = "villain"

    gender = np.where(stream.random(n) < 0.45, "F", "M").astype(object)
    first = np.where(gender == "F", pick(stream, c["FEMALE_FIRST"], n), pick(stream, c["MALE_FIRST"], n))
    last = pick(stream, c["LAST_NAMES"], n)
    alias = np.where(role == "civilian", None, aliases(stream, n))
    lo_birth = np.where(role == "civilian", days(date(1935, 1, 1)), days(date(1950, 1, 1)))
    birth = lo_birth + (stream.random(n) * (days(date(2006, 12, 31)) - lo_birth)).astype(np.int64)
    birth_valid = np.ones(n, dtype=bool)
    home = stream.integers(0, len(names), n)
    sid = np.zeros(n, dtype=np.int64)

    k = int(is_core.sum())
    if k:
        people = read(ctx["people"]).slice(start, k)
        sid[:k] = column(people, "sid_number")
        last[:k], first[:k], alias[:k] = (column(people, x) for x in ("last_name", "first_name", "alias"))
        gender[:k] = column(people, "gender")
        birth[:k] = column(people, "birth_date", fill=0)
        birth_valid[:k] = ~np.asarray(people["birth_date"].is_null())
        units = column(people, "unit_name")
        home[:k] = [city_ids.get(u, h) for u, h in zip(units, home[:k])]
    return pa.table({"person_id": ids, "sid_number": pa.array(sid, pa.int64(), mask=~is_core), "role": role,
                     "is_core": is_core, "last_name": last, "first_name": first, "alias": alias, "gender": gender,
                     "birth_date": as_date(birth, birth_valid), "home_location_id": pa.array(home, pa.int32())})


@lru_cache(maxsize=None)
def people_index(person_dir: str) -> dict:
    """Built people as numpy, by person_id: role and birth (days since 1970; only used for new people, who all have
    one)."""
    t = read(person_dir)
    return {"role": column(t, "role"), "birth": column(t, "birth_date", fill=0)}


def powered_people(scale_name: str) -> np.ndarray:
    """The new people who hold powers, in order: new villains, heroes, secondary heroes."""
    b = role_bounds(scale_name)
    return np.concatenate([np.arange(*b[r], dtype=np.int64) for r in ("villain", "hero", "secondary_hero")])


# ---- who holds which power --------------------------------------------------------------------------------------

@lru_cache(maxsize=None)
def new_power_rows(scale_name: str) -> tuple:
    """For person_power's rows after today's: (person_id, k) where k is that person's 0-based power number."""
    b = role_bounds(scale_name)
    villains = np.arange(*b["villain"], dtype=np.int64)
    heroes = np.arange(*b["hero"], dtype=np.int64)
    counts = scale.hero_powers(heroes - b["hero"][0])
    hero_rows = np.repeat(heroes, counts)
    hero_k = np.concatenate([np.arange(cnt) for cnt in counts]) if len(counts) else np.array([], dtype=np.int64)
    secondary = np.arange(*b["secondary_hero"], dtype=np.int64)
    person_ids = np.concatenate([villains, hero_rows, secondary])
    k = np.concatenate([np.zeros(len(villains), np.int64), hero_k, np.zeros(len(secondary), np.int64)])
    return person_ids, k


def person_power(stream, start, n, ctx):
    """Who holds which power, from when to when: today's pairs first (held before the first sentence), then the new
    villains', heroes' and secondary heroes'."""
    powers = read(ctx["power_dir"])
    power_id = {nm: i for i, nm in enumerate(column(powers, "name"))}
    tier, permanence = column(powers, "tier"), column(powers, "permanence")
    ids = np.arange(start, start + n, dtype=np.int64)
    person_ids, pw = np.zeros(n, np.int64), np.zeros(n, np.int64)
    acquired, acq_valid = np.zeros(n, np.int64), np.ones(n, bool)
    lost, lost_valid = np.zeros(n, np.int64), np.zeros(n, bool)
    source = np.empty(n, dtype=object)

    core_rows = ids < scale.CORE_PERSON_POWERS
    k = int(core_rows.sum())
    if k:
        pairs = read(ctx["powers"]).slice(start, k)
        people = read(ctx["people"])
        sids = column(people, "sid_number")
        who = np.searchsorted(sids, column(pairs, "sid_number"))
        person_ids[:k] = who
        pw[:k] = [power_id[p] for p in column(pairs, "power")]
        # Held before the first sentence, after birth (from 1900 at the earliest: the course plants absurd birth
        # dates like 1213 on purpose). Unknown (null) when the lake has no birth date or no sentence date.
        known = ~np.asarray(people["birth_date"].is_null())[who] & ~np.asarray(people["first_sentence_date"].is_null())[who]
        base = np.maximum(column(people, "birth_date", fill=0)[who], days(date(1900, 1, 1)))
        first = column(people, "first_sentence_date", fill=0)[who]
        ok = known & (base < first)
        acquired[:k] = np.where(ok, base + (stream.uniform(0.25, 0.999, k) * np.maximum(first - base, 1)).astype(np.int64), 0)
        acq_valid[:k] = ok
        drawn = pick(stream, ["innate", "accident", "serum"], k)
        source[:k] = np.where(permanence[pw[:k]] == "artifact", "artifact",
                              np.where(tier[pw[:k]] == "DELTA", "training", drawn))

    m = n - k
    if m:
        all_people, all_k = new_power_rows(ctx["scale"])
        offset = start + k - scale.CORE_PERSON_POWERS
        who, nth = all_people[offset: offset + m], all_k[offset: offset + m]
        person_ids[k:] = who
        roles = people_index(ctx["person_dir"])["role"][who]
        births = people_index(ctx["person_dir"])["birth"][who]
        spread = (who * 2_654_435_761) % N_POWERS            # a fixed pseudo-random first power per person
        chosen = (spread + nth * 37) % N_POWERS               # 2nd and 3rd powers differ from the 1st
        delta = np.flatnonzero(tier == "DELTA")
        weak = (roles == "secondary_hero") & (stream.random(m) < 0.5)
        chosen = np.where(weak, delta[spread % len(delta)], chosen)
        pw[k:] = chosen
        acquired[k:] = np.minimum(births + stream.integers(8 * 365, 35 * 365, m), TODAY - 1)
        borrowed = pick(stream, ["temporary", "borrowed", "artifact"], m)
        regular = np.where(permanence[chosen] == "artifact", "artifact", pick(stream, ["innate", "accident", "serum"], m))
        source[k:] = np.where(roles == "secondary_hero", np.where(weak, "training", borrowed), regular)
        ends = acquired[k:] + stream.integers(30, 1_500, m)
        lost[k:] = ends
        lost_valid[k:] = np.isin(source[k:], ["temporary", "borrowed"]) & (ends <= TODAY)
    return pa.table({"person_power_id": ids, "person_id": person_ids, "power_id": pa.array(pw, pa.int16()),
                     "acquired_on": as_date(acquired, acq_valid), "lost_on": as_date(lost, lost_valid),
                     "source": source})


# ---- team members -----------------------------------------------------------------------------------------------

def team_member(stream, start, n, ctx):
    """Who belongs to which team: today's STG affiliations first (their status from the lake), then new villains in
    villain teams and heroes in hero teams."""
    stg = list(core.course_constants()["STG_GROUPS"])
    ids = np.arange(start, start + n, dtype=np.int64)
    person_ids, team_ids = np.zeros(n, np.int64), np.zeros(n, np.int64)
    status = np.empty(n, dtype=object)
    joined, left, left_valid = np.zeros(n, np.int64), np.zeros(n, np.int64), np.zeros(n, bool)
    joined_valid = np.ones(n, bool)

    core_rows = ids < scale.CORE_TEAM_MEMBERS
    k = int(core_rows.sum())
    if k:
        rows = read(ctx["memberships"]).slice(start, k)
        people = read(ctx["people"])
        who = np.searchsorted(column(people, "sid_number"), column(rows, "sid_number"))
        person_ids[:k] = who
        team_ids[:k] = [stg.index(g) for g in column(rows, "stg_group")]
        status[:k] = column(rows, "status")
        # Joined before the first sentence; a former member left after the latest. Unknown with no sentence date.
        known = ~np.asarray(people["first_sentence_date"].is_null())[who]
        first = column(people, "first_sentence_date", fill=0)[who]
        latest = column(people, "latest_sentence_date", fill=0)[who]
        joined[:k] = first - stream.integers(30, 1_500, k)
        joined_valid[:k] = known
        ends = latest + stream.integers(30, 900, k)
        left[:k] = ends
        left_valid[:k] = known & (status[:k] == "Former") & (ends <= TODAY)

    m = n - k
    if m:
        powered = powered_people(ctx["scale"])
        offset = start + k - scale.CORE_TEAM_MEMBERS
        if offset + m > len(powered):
            raise ValueError("more team members than new people with powers")
        who = powered[offset: offset + m]
        person_ids[k:] = who
        roles = people_index(ctx["person_dir"])["role"][who]
        births = people_index(ctx["person_dir"])["birth"][who]
        villain = roles == "villain"
        team_ids[k:] = np.where(villain, stream.integers(0, N_VILLAIN_TEAMS, m), stream.integers(N_VILLAIN_TEAMS, N_TEAMS, m))
        status[k:] = np.where(stream.random(m) < 0.05, "leader", "member")
        joined[k:] = np.minimum(births + stream.integers(16 * 365, 40 * 365, m), TODAY - 1)
        ends = joined[k:] + stream.integers(100, 3_000, m)
        left[k:] = ends
        left_valid[k:] = (stream.random(m) < 0.2) & (ends <= TODAY)
    return pa.table({"team_member_id": ids, "team_id": pa.array(team_ids, pa.int16()), "person_id": person_ids,
                     "status": status, "joined_on": as_date(joined, joined_valid),
                     "left_on": as_date(left, left_valid)})
