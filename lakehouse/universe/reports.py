"""The expanded world's incident reports: free-text accounts of fights, for the text-analysis sessions (Priority 5).

A report is what someone wrote about a fight, so it reads like people write: short, uneven, sometimes in capitals,
sometimes with typos, now and then just "SEE ATTACHED". It also names people, and that is the point. Buried in the
prose are the personal details a text session has to find and redact: names, aliases, SIDs, birth dates, heroes'
secret identities, witnesses' phone numbers (fictional 555-01xx numbers only).

  incident_report  one per written-up fight at most; bigger fights are written up more often (weighted sampling
                   from the fight plan, its own stream (seed, "report_plan", 0)). Three kinds:
                     field report       a hero who was there, third person (most reports)
                     witness statement  a civilian who was there, first person, with their own name and phone
                     desk report        no author: the call as the desk took it
                   filed after the fight ended; never after today unless the fight itself ran past today.

The mention key, mention_key/incident_report (made with the mess, see mess.write_chunk): one row per
personal detail in a narrative, with its place (start_pos..end_pos, counted from 0, end excluded), kind
(person_name, alias, sid, birth_date, phone), as_written, the person, and the truth as the person table holds it
(LAST, FIRST; the alias; the SID; the ISO birth date; the phone as NNN-555-01NN). The mess never touches the
narrative, so the places hold in bronze and silver too.

Composing a narrative is a Python loop per report (text doesn't vectorize). Each report draws from its own small
random.Random, seeded from the chunk's stream, so a chunk still rebuilds alone and a report can take as many draws
as its fight needs.
"""
import random
from datetime import date, datetime, timedelta
from functools import lru_cache

import duckdb
import numpy as np
import pyarrow as pa

from . import fights, scale
from .rng import stream
from .world import TODAY

MENTIONS = "mention_key/incident_report"   # beside messy/ and answer_key/, never inside answer_key/ (other columns)
JUNK = [None, "SEE ATTACHED", "see attached", "TBD", "Report to follow.", "N/A - see CAD log"]
JUNK_SHARE, SHOUT_SHARE, TYPO_SHARE, DESK_SHARE, WITNESS_SHARE = 0.025, 0.1, 0.15, 0.05, 0.12
EPOCH = date(1970, 1, 1)
TODAY_END_US = ((TODAY + 1) * 86_400 - 1) * 1_000_000   # 23:59:59 today; times are whole seconds
# Delay from the fight's end to filing, in hours: (log-mean, log-sd) per kind.
DELAY = {"field report": (1.5, 1.0), "witness statement": (3.0, 1.2), "desk report": (-1.5, 0.7)}

MENTION_SCHEMA = pa.schema([("incident_report_id", pa.int64()), ("start_pos", pa.int32()), ("end_pos", pa.int32()),
                            ("kind", pa.string()), ("as_written", pa.string()), ("person_id", pa.int64()),
                            ("truth", pa.string())])
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
          "November", "December"]


class Draws:
    """One report's random draws, from its own seed."""

    def __init__(self, seed: int):
        """A fresh generator for one report."""
        self.r = random.Random(seed)

    def u(self) -> float:
        """The next uniform in [0, 1)."""
        return self.r.random()

    def pick(self, options):
        """One of options, uniformly."""
        return options[int(self.u() * len(options))]


class Narrative:
    """A narrative built piece by piece: plain words (which may get a typo) and mentions (kept exactly, placed)."""

    def __init__(self, draws: Draws, typos: bool):
        """Start empty."""
        self.d, self.typos, self.parts, self.at, self.mentions = draws, typos, [], 0, []

    def say(self, text: str):
        """Plain words; when this report has typos, a quarter of the pieces swap two letters in one long word."""
        if self.typos and self.d.u() < 0.25:
            words = text.split(" ")
            long_ = [i for i, w in enumerate(words) if len(w) >= 5 and w.isalpha()]
            if long_:
                i = self.d.pick(long_)
                w = words[i]
                k = 1 + int(self.d.u() * (len(w) - 2))
                words[i] = w[:k - 1] + w[k] + w[k - 1] + w[k + 1:]
                text = " ".join(words)
        self.parts.append(text)
        self.at += len(text)

    def mention(self, text: str, kind: str, person_id: int, truth: str):
        """A personal detail, written as text, remembered with its place."""
        self.mentions.append((self.at, self.at + len(text), kind, int(person_id), truth))
        self.parts.append(text)
        self.at += len(text)

    def done(self, shout: bool) -> tuple:
        """(the text, [(start, end, kind, as written, person, truth)]); in capitals when shouting (ASCII only, so
        every place stays put)."""
        text = "".join(self.parts)
        if shout and text.isascii():
            text = text.upper()
        return text, [(s, e, k, text[s:e], p, t) for s, e, k, p, t in self.mentions]


# --- how things get written ---------------------------------------------------------------------------------------

def written_date(d: date, draws: Draws) -> str:
    """A date as people write it."""
    return draws.pick([f"{d.month:02d}/{d.day:02d}/{d.year}", f"{d.month}/{d.day}/{d.year % 100:02d}", d.isoformat(),
                       f"{MONTHS[d.month - 1]} {d.day}, {d.year}", f"{d.day:02d}-{MONTHS[d.month - 1][:3].upper()}-{d.year}"])


def written_time(t: datetime, draws: Draws) -> str:
    """A time of day as people write it."""
    h12 = t.hour % 12 or 12
    return draws.pick([f"{t.hour:02d}{t.minute:02d} hrs", f"{h12}:{t.minute:02d} {'PM' if t.hour >= 12 else 'AM'}",
                       f"{t.hour:02d}:{t.minute:02d}"])


def nice(name: str) -> str:
    """An upper-case name or alias as prose writes it: Kyle, Fearless Thermoknight."""
    return " ".join(w.capitalize() for w in name.split())


def phone(person_id: int) -> str:
    """A person's fictional phone number, NNN-555-01NN (the 555-0100..0199 range is reserved for fiction)."""
    return f"{201 + (person_id * 7919) % 790}-555-01{person_id % 100:02d}"


def say_name(n: Narrative, p: dict):
    """A person's real name, one of several ways."""
    first, last = p["first_name"], p["last_name"]
    written = n.d.pick([f"{last}, {first}", f"{nice(first)} {nice(last)}", f"{first[0]}. {nice(last)}",
                        f"{first} {last}"])
    n.mention(written, "person_name", p["person_id"], f"{last}, {first}")


def say_alias(n: Narrative, p: dict):
    """A person's alias, as prose writes it."""
    n.mention(nice(p["alias"]), "alias", p["person_id"], p["alias"])


def has_name(p: dict) -> bool:
    """Whether both parts of a person's name are known (a few of today's villains lack one)."""
    return p["first_name"] is not None and p["last_name"] is not None


def say_who(n: Narrative, p: dict):
    """How prose refers to a villain: their alias, else their name, else as unidentified."""
    if p["alias"] is not None:
        say_alias(n, p)
    elif has_name(p):
        say_name(n, p)
    else:
        n.say("an unidentified suspect")


def say_sid(n: Narrative, p: dict):
    """A person's SID, with one of the labels people use."""
    sid = str(p["sid_number"])
    n.mention(n.d.pick([f"SID {sid}", f"SID# {sid}", f"SID no. {sid}", f"sid:{sid}", f"S.I.D. {sid}"]), "sid",
              p["person_id"], sid)


def say_birth(n: Narrative, p: dict):
    """A person's birth date, as people write dates."""
    n.mention(written_date(p["birth_date"], n.d), "birth_date", p["person_id"], p["birth_date"].isoformat())


def say_phone(n: Narrative, p: dict):
    """A person's phone number, written one of three ways."""
    a, b, c = phone(p["person_id"]).split("-")
    n.mention(n.d.pick([f"{a}-{b}-{c}", f"({a}) {b}-{c}", f"{a}.{b}.{c}"]), "phone", p["person_id"], f"{a}-{b}-{c}")


# --- the three kinds of report ---------------------------------------------------------------------------------------

def field_report(n: Narrative, f: dict, author: dict):
    """A hero's account, third person: the scene, each named villain, the help, the damage, the signature."""
    d = n.d
    n.say(d.pick([f"On {written_date(f['day'], d)} at approx. {written_time(f['at'], d)}, units responded to a reported "
                  f"{f['type']} at {f['place']}. ",
                  f"Responded to {f['place']} on {written_date(f['day'], d)}, {written_time(f['at'], d)}, re: {f['type']} in progress. ",
                  f"{written_date(f['day'], d)} {written_time(f['at'], d)} - {f['type']} at {f['place']}. "]))
    for k, v in enumerate(f["villains"][:3]):
        if has_name(v):
            n.say(f"Suspect {k + 1} identified as ")
            say_name(n, v)
        else:
            n.say(f"Suspect {k + 1}, name unknown")
        if v["alias"] is not None and d.u() < 0.6:
            n.say(' aka "')
            say_alias(n, v)
            n.say('"')
        if v["sid_number"] is not None and d.u() < 0.7:
            n.say(", ")
            say_sid(n, v)
        if v["birth_date"] is not None and d.u() < 0.5:
            n.say(", DOB ")
            say_birth(n, v)
        n.say(". ")
        if v["power"] and d.u() < 0.7:
            say_who(n, v)
            n.say(f" used {v['power'].lower()} against responding units. ")
        say_who(n, v)
        n.say(" was taken into custody. " if v["status"] == "captured" else
              d.pick([" fled the scene before backup arrived. ", " escaped on foot. ", " got away. "]))
    for h in [h for h in f["heroes"] if h["person_id"] != author["person_id"]][:2]:
        n.say("Assisted by ")
        say_alias(n, h)
        if d.u() < 0.2:
            n.say(" (")
            say_name(n, h)
            n.say(")")
        n.say(". ")
    for c in f["civilians"][:2]:
        if d.u() < 0.35:
            n.say(f"{c['role'].capitalize()} ")
            say_name(n, c)
            n.say(" gave a statement, contact ")
            say_phone(n, c)
            n.say(". ")
    say_damage(n, f)
    n.say("Reporting: ")
    say_alias(n, author)
    if d.u() < 0.1:
        n.say(" / ")
        say_name(n, author)


def witness_statement(n: Narrative, f: dict, author: dict):
    """A civilian's account, first person, signed with their own name and phone."""
    d = n.d
    n.say(d.pick([f"I was at {f['place']} on {written_date(f['day'], d)} around {written_time(f['at'], d)} when it started. ",
                  f"This happened {written_date(f['day'], d)} at {f['place']}, about {written_time(f['at'], d)}. ",
                  f"I saw the whole {f['type']} at {f['place']}. "]))
    for v in f["villains"][:2]:
        before, after = d.pick([("Someone calling themselves ", " started it. "), ("I recognized ", " from the news. "),
                                ("There was this villain, ", ". ")])
        n.say(before)
        say_who(n, v)
        n.say(after)
        if v["power"] and d.u() < 0.5:
            n.say(f"They used {v['power'].lower()}, it was terrifying. ")
        n.say(d.pick(["The heroes caught them. ", "They got taken away in cuffs. "]) if v["status"] == "captured" else
              d.pick(["They got away. ", "Nobody caught them. "]))
    for h in f["heroes"][:1]:
        say_alias(n, h)
        n.say(d.pick([" showed up and helped everyone out. ", " arrived after a few minutes. "]))
    say_damage(n, f)
    n.say("My name is ")
    say_name(n, author)
    n.say(", you can reach me at ")
    say_phone(n, author)
    n.say(". ")
    if author["birth_date"] is not None and d.u() < 0.2:
        n.say("I was born ")
        say_birth(n, author)
        n.say(".")


def desk_report(n: Narrative, f: dict):
    """The call as the desk took it: when, where, who, and a records check on the first suspect."""
    d = n.d
    n.say(f"Call received {written_date(f['day'], d)} {written_time(f['at'], d)}. Caller reported a {f['type']} at {f['place']}. ")
    v = f["villains"][0]
    n.say("Suspect described as ")
    say_who(n, v)
    n.say(f". {1 + int(d.u() * 4)} units dispatched. ")
    if v["sid_number"] is not None and has_name(v) and d.u() < 0.5:
        n.say("Records check: ")
        say_name(n, v)
        n.say(", ")
        say_sid(n, v)
        n.say(". ")
    say_damage(n, f)


def say_damage(n: Narrative, f: dict):
    """Damage and bystanders, when there were any."""
    if f["n_damage"]:
        n.say(n.d.pick([f"Property damage est. ${f['cost']:,}. ", f"Damage to {f['n_damage']} properties. ",
                        "Significant property damage. "]))
    if f["injured"]:
        n.say(f"{f['injured']} bystander{'s' if f['injured'] > 1 else ''} injured. ")
    if f["killed"]:
        n.say(f"{f['killed']} fatalit{'ies' if f['killed'] > 1 else 'y'}. ")


# --- the table ------------------------------------------------------------------------------------------------------

@lru_cache(maxsize=None)
def report_plan(ctx_items: tuple) -> np.ndarray:
    """The written-up fights, in incident order: a weighted sample without replacement (each fight's key is
    u ** (1 / severity), the top keys win), so bigger fights are written up more often."""
    ctx = dict(ctx_items)
    weight = fights.plan_for(ctx)["weight"]
    n = scale.rows("incident_report", ctx["scale"])
    key = stream(ctx["seed"], "report_plan", 0).random(len(weight)) ** (1.0 / weight)
    return np.sort(np.argpartition(-key, n - 1)[:n])


def facts(ctx: dict, incident_ids: np.ndarray) -> list:
    """For each fight (in the order given): when, where, what, damage, bystanders, and its villains, heroes and
    civilians in participant order, each with their names, SID, birth date and first power used."""
    con = duckdb.connect()
    con.register("wanted", pa.table({"incident_id": pa.array(incident_ids, pa.int64())}))
    p = {t: f"read_parquet('{ctx[t + '_dir']}/*.parquet')" for t in
         ("incident", "location", "participant", "person", "power_use", "power", "damage")}
    inc = con.execute(f"""
        SELECT i.incident_id, epoch_us(i.started_at) AS us, i.duration_min, i.incident_type, l.name AS place,
               i.bystanders_injured, i.bystanders_killed, coalesce(d.cost, 0)::BIGINT AS cost, coalesce(d.n, 0) AS n_damage
        FROM {p['incident']} i SEMI JOIN wanted USING (incident_id) JOIN {p['location']} l USING (location_id)
        LEFT JOIN (SELECT incident_id, sum(cost_usd) AS cost, count(*) AS n FROM {p['damage']}
                   SEMI JOIN wanted USING (incident_id) GROUP BY 1) d USING (incident_id)
        ORDER BY i.incident_id""").fetchall()
    people = con.execute(f"""
        SELECT pa.incident_id, pa.side, pa.role, pa.status, pe.person_id, pe.first_name, pe.last_name, pe.alias,
               pe.sid_number, pe.birth_date, w.name AS power
        FROM {p['participant']} pa SEMI JOIN wanted USING (incident_id) JOIN {p['person']} pe USING (person_id)
        LEFT JOIN (SELECT participant_id, min(power_id) AS power_id FROM {p['power_use']}
                   SEMI JOIN wanted USING (incident_id) GROUP BY 1) u USING (participant_id)
        LEFT JOIN {p['power']} w USING (power_id)
        ORDER BY pa.participant_id""").fetchall()
    con.close()
    out = {}
    for iid, us, dur, typ, place, injured, killed, cost, n_dmg in inc:
        at = datetime(1970, 1, 1) + timedelta(microseconds=us)
        out[iid] = {"us": us, "end_us": us + dur * 60_000_000, "at": at, "day": at.date(), "type": typ,
                    "place": place, "injured": injured, "killed": killed, "cost": cost, "n_damage": n_dmg,
                    "villains": [], "heroes": [], "civilians": []}
    cols = ("side", "role", "status", "person_id", "first_name", "last_name", "alias", "sid_number", "birth_date", "power")
    for iid, *row in people:
        p_ = dict(zip(cols, row))
        out[iid][{"villain": "villains", "hero": "heroes", "civilian": "civilians"}[p_["side"]]].append(p_)
    return [out[int(i)] for i in incident_ids]


def compose(stream, start: int, n: int, ctx: dict) -> tuple:
    """Reports start..start+n-1 and their mentions: (incident_report table, mention table)."""
    chosen = report_plan(tuple(sorted(ctx.items())))[start:start + n]
    ids = np.arange(start, start + n, dtype=np.int64)
    seeds = stream.integers(0, 2 ** 62, n)
    kind, author, filed, text = [], [], [], []
    m_rows = []
    for k, f in enumerate(facts(ctx, chosen)):
        d = Draws(int(seeds[k]))
        pick = d.u()
        if pick < DESK_SHARE:
            kd, who = "desk report", None
        elif f["heroes"] and (not f["civilians"] or pick >= DESK_SHARE + WITNESS_SHARE):
            kd, who = "field report", f["heroes"][0]
        elif f["civilians"]:
            kd, who = "witness statement", f["civilians"][int(d.u() * len(f["civilians"]))]
        else:
            kd, who = "desk report", None
        mu, sd = DELAY[kd]
        delay = int(d.r.lognormvariate(mu, sd) * 3_600) * 1_000_000   # whole seconds, like every time in the world
        filed.append(filed_time(f["end_us"], delay))
        kind.append(kd)
        author.append(None if who is None else who["person_id"])
        if d.u() < JUNK_SHARE:
            text.append(d.pick(JUNK))
            continue
        shout, typos = d.u() < SHOUT_SHARE, d.u() < TYPO_SHARE
        nar = Narrative(d, typos)
        if kd == "field report":
            field_report(nar, f, who)
        elif kd == "witness statement":
            witness_statement(nar, f, who)
        else:
            desk_report(nar, f)
        body, found = nar.done(shout)
        text.append(body.strip())
        m_rows += [(int(ids[k]), s, e, kd_, w, p, t) for s, e, kd_, w, p, t in found]
    table = pa.table({"incident_report_id": ids, "incident_id": pa.array(chosen, pa.int64()),
                      "author_person_id": pa.array(author, pa.int64()), "report_kind": pa.array(kind, pa.string()),
                      "filed_at": pa.array(filed, pa.int64()).cast(pa.timestamp("us")),
                      "narrative": pa.array(text, pa.string())})
    cols = list(zip(*m_rows)) if m_rows else [[] for _ in MENTION_SCHEMA]
    mentions = pa.table({f.name: pa.array(c, f.type) for f, c in zip(MENTION_SCHEMA, cols)}, schema=MENTION_SCHEMA)
    return table, mentions


def filed_time(end_us: int, delay_us: int) -> int:
    """When a report is filed: delay_us after the fight ends, but never after 23:59:59 today unless the fight
    itself ended later (then right at its end)."""
    return min(end_us + delay_us, max(end_us, TODAY_END_US))


def incident_report(stream, start, n, ctx):
    """Incident reports start..start+n-1 (the factory build.py calls; the mentions are made with the mess)."""
    return compose(stream, start, n, ctx)[0]
