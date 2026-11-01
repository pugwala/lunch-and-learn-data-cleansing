"""The S/M/L knob: how many rows each table of the expanded world has, from the approved data model.

Each table has a fixed part, which doesn't change with scale, and a scaled part, the approved count at M times the
scale's factor. Charges and dispositions are fixed at one per linkable offender record plus scaled ones. People are fixed at today's 86,879 (every SID in the course data) plus scaled heroes, secondary
heroes, civilians and new villains, so every scale keeps today's people. Catalogs (powers, teams, places) are fixed.
"""
CORE_PEOPLE = 86_879          # distinct SIDs in the course's 100,000 records
CORE_PERSON_POWERS = 140_795  # distinct (SID, power) pairs they hold today
CORE_TEAM_MEMBERS = 9_065     # distinct (SID, STG group) affiliations today
CORE_RECORDS = 94_120         # offender records with a SID and a sentence date: each gets its own fight

FACTORS = {"S": 0.1, "M": 1.0, "L": 4.0}

# table: (fixed rows, scaled rows at M). Approved by Ian on 2026-11-01 (chunk 0).
MODEL = {
    "person":           (CORE_PEOPLE, 440_000 - CORE_PEOPLE),
    "power":            (400, 0),
    "person_power":     (CORE_PERSON_POWERS, None),   # scaled part follows from the roles, see powered_rows()
    "team":             (300, 0),
    "team_member":      (CORE_TEAM_MEMBERS, 60_000 - CORE_TEAM_MEMBERS),
    "location":         (20_000, 0),
    "incident":         (0, 1_200_000),
    "participant":      (0, 6_000_000),
    "power_use":        (0, 4_000_000),
    "damage":           (0, 1_800_000),
    "charge":           (CORE_RECORDS, 900_000 - CORE_RECORDS),      # today's records first, at every scale
    "disposition":      (CORE_RECORDS, 900_000 - CORE_RECORDS),
    "mission":          (0, 150_000),
    "mission_member":   (0, 600_000),
    "mission_incident": (0, 200_000),
    "incident_report":  (0, 200_000),
}


# The new people at M, by role, in the order they follow today's villains in the person table.
ROLES_AT_M = {"villain": 13_121, "hero": 30_000, "secondary_hero": 50_000}   # civilians are the rest


def check(scale_name: str) -> float:
    """The scale's factor, or ValueError for an unknown scale."""
    if scale_name not in FACTORS:
        raise ValueError(f"Unknown scale {scale_name!r}: use one of {', '.join(FACTORS)}")
    return FACTORS[scale_name]


def roles(scale_name: str) -> dict:
    """New people per role at a scale, in person-table order: villain, hero, secondary_hero, civilian."""
    f = check(scale_name)
    out = {role: round(n * f) for role, n in ROLES_AT_M.items()}
    out["civilian"] = rows("person", scale_name) - CORE_PEOPLE - sum(out.values())
    return out


def hero_powers(i):
    """How many powers the i-th new hero holds: 1, 2, 3, 1, 2, 3, ... (works on numpy arrays too)."""
    return 1 + i % 3


def powered_rows(scale_name: str) -> int:
    """person_power rows for new people: one per new villain, 1-3 per hero (hero_powers), one per secondary hero."""
    r = roles(scale_name)
    full, rest = divmod(r["hero"], 3)
    return r["villain"] + (full * 6 + (0, 1, 3)[rest]) + r["secondary_hero"]


def rows(table: str, scale_name: str) -> int:
    """How many rows a table has at a scale (S, M or L)."""
    f = check(scale_name)
    fixed, scaled = MODEL[table]
    if table == "person_power":
        return fixed + powered_rows(scale_name)
    return fixed + round(scaled * f)


def total(scale_name: str) -> int:
    """Rows across every table at a scale."""
    return sum(rows(t, scale_name) for t in MODEL)
