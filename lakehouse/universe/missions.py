"""The expanded world's missions: operations hero teams plan, who goes on them, and the fights they run into.

Like fights, missions share a plan from their own stream, (seed, "mission_plan", 0): each mission's start and end,
how many members it has and how many fights it links to. Members and links have consecutive ids per mission.

  mission           a hero team's operation, possibly against a villain team, from 1996 to today; ongoing (no end
                    yet) when it would end after today
  mission_member    heroes and secondary heroes who held a power on the start day (the pools of fights.py), one
                    lead each, picked with the same stride trick so nobody is on a mission twice
  mission_incident  fights that happened while the mission ran, each linked at most once to a mission
"""
from functools import lru_cache

import numpy as np
import pyarrow as pa

from . import core, fights, scale
from .rng import stream
from .world import N_LOCATIONS, N_TEAMS, N_VILLAIN_TEAMS, TODAY, as_date, pick

MISSION_TYPES = ["patrol", "pursuit", "raid", "rescue", "escort", "investigation", "stakeout"]
ENDINGS = {"completed": 0.7, "failed": 0.15, "aborted": 0.15}


@lru_cache(maxsize=None)
def mission_plan(ctx_items: tuple) -> dict:
    """Every mission's start, end (TODAY's end for ongoing ones), member and link counts and offsets, pick
    parameters, and the window of fights (in day order) each can link to."""
    ctx = dict(ctx_items)
    rs = stream(ctx["seed"], "mission_plan", 0)
    n = scale.rows("mission", ctx["scale"])
    n_mem, n_link = scale.rows("mission_member", ctx["scale"]), scale.rows("mission_incident", ctx["scale"])
    x = np.log1p(rs.random(n) * np.expm1(2.0)) / 2.0
    start = np.minimum(fights.FIRST_DAY + (x * (TODAY - fights.FIRST_DAY + 1)).astype(np.int64), TODAY)
    end = start + np.clip(np.round(rs.lognormal(2.5, 1.0, n)), 1, 365).astype(np.int64)
    ongoing = end > TODAY
    members = 1 + rs.multinomial(n_mem - n, np.full(n, 1 / n))
    day = fights.plan_for(ctx)["day"]
    by_day = np.argsort(day, kind="stable")
    lo = np.searchsorted(day[by_day], start, side="left")
    hi = np.searchsorted(day[by_day], np.where(ongoing, TODAY, end), side="right")
    # Links in proportion to the fights that happened while each mission ran; a rare overflow (more links than
    # fights) goes to the missions with the most fights to spare, one each, so the total stays exact.
    links = rs.multinomial(n_link, (hi - lo) / (hi - lo).sum())
    over = np.maximum(links - (hi - lo), 0)
    links -= over
    links[np.argsort(-(hi - lo - links), kind="stable")[: over.sum()]] += 1
    if (hi - lo < links).any():
        raise ValueError("a mission has more fights to link than happened while it ran")
    return {"start": start, "end": end, "ongoing": ongoing, "members": members,
            "mem_off": np.concatenate([[0], np.cumsum(members)]), "link_off": np.concatenate([[0], np.cumsum(links)]),
            "b": rs.integers(0, 2 ** 40, (n, 2)), "s": fights.STRIDES[rs.integers(0, len(fights.STRIDES), (n, 2))],
            "by_day": by_day, "lo": lo, "window": hi - lo}


def mission(stream, start, n, ctx):
    """Missions start..start+n-1: codename, kind, the hero team running it and the villain team it's after (if
    any), where, when, and how it ended."""
    m = mission_plan(tuple(sorted(ctx.items())))
    c = core.course_constants()
    ids = np.arange(start, start + n, dtype=np.int64)
    ongoing = m["ongoing"][ids]
    codename = "Operation " + pick(stream, [a.title() for a in c["ADJ"]], n) + " " + pick(stream, [b.title() for b in c["NOUN"]], n)
    status = np.where(ongoing, "ongoing", np.asarray(list(ENDINGS), dtype=object)[
        stream.choice(len(ENDINGS), n, p=list(ENDINGS.values()))])
    target = stream.integers(0, N_VILLAIN_TEAMS, n)
    return pa.table({"mission_id": ids, "codename": codename, "mission_type": pick(stream, MISSION_TYPES, n),
                     "team_id": pa.array(stream.integers(N_VILLAIN_TEAMS, N_TEAMS, n), pa.int16()),
                     "target_team_id": pa.array(target, pa.int16(), mask=stream.random(n) < 0.4),
                     "location_id": pa.array(stream.integers(0, N_LOCATIONS, n), pa.int32()),
                     "started_on": as_date(m["start"][ids]), "ended_on": as_date(m["end"][ids], ~ongoing),
                     "status": status.astype(object)})


def mission_member(stream, start, n, ctx):
    """Mission members start..start+n-1: who went on which mission, as lead (the first), member or support."""
    m = mission_plan(tuple(sorted(ctx.items())))
    ids = np.arange(start, start + n, dtype=np.int64)
    mis = fights.owner(m["mem_off"], ids)
    j = ids - m["mem_off"][mis]
    who, when = fights.pools(ctx["person_dir"], ctx["person_power_dir"])["hero"]
    active = np.searchsorted(when, m["start"][mis], side="right")
    if (active < m["members"][mis]).any():
        raise ValueError("too few heroes about on some day for the missions planned")
    person = who[(m["b"][mis, 0] + j * m["s"][mis, 0]) % active]
    role = np.where(j == 0, "lead", np.where(stream.random(n) < 0.7, "member", "support"))
    return pa.table({"mission_member_id": ids, "mission_id": mis, "person_id": person, "role": role.astype(object)})


def mission_incident(stream, start, n, ctx):
    """Links start..start+n-1 between a mission and a fight that happened while it ran."""
    m = mission_plan(tuple(sorted(ctx.items())))
    ids = np.arange(start, start + n, dtype=np.int64)
    mis = fights.owner(m["link_off"], ids)
    j = ids - m["link_off"][mis]
    at = m["lo"][mis] + (m["b"][mis, 1] + j * m["s"][mis, 1]) % m["window"][mis]
    return pa.table({"mission_incident_id": ids, "mission_id": mis, "incident_id": m["by_day"][at]})
