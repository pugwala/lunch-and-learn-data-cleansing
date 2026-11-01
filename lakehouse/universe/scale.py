"""The S/M/L knob: how many rows each table of the expanded world has, from the approved data model.

Each table has a fixed part, which doesn't change with scale, and a scaled part, the approved count at M times the
scale's factor. People are fixed at today's 86,879 (every SID in the course data) plus scaled heroes, secondary
heroes, civilians and new villains, so every scale keeps today's people. Catalogs (powers, teams, places) are fixed.
"""
CORE_PEOPLE = 86_879     # distinct SIDs in the course's 100,000 records

FACTORS = {"S": 0.1, "M": 1.0, "L": 4.0}

# table: (fixed rows, scaled rows at M). Approved by Ian on 2026-11-01 (chunk 0).
MODEL = {
    "person":           (CORE_PEOPLE, 440_000 - CORE_PEOPLE),
    "power":            (400, 0),
    "person_power":     (0, 150_000),
    "team":             (300, 0),
    "team_member":      (0, 60_000),
    "location":         (20_000, 0),
    "incident":         (0, 1_200_000),
    "participant":      (0, 6_000_000),
    "power_use":        (0, 4_000_000),
    "damage":           (0, 1_800_000),
    "charge":           (0, 900_000),
    "disposition":      (0, 900_000),
    "mission":          (0, 150_000),
    "mission_member":   (0, 600_000),
    "mission_incident": (0, 200_000),
    "incident_report":  (0, 200_000),
}


def rows(table: str, scale_name: str) -> int:
    """How many rows a table has at a scale (S, M or L)."""
    if scale_name not in FACTORS:
        raise ValueError(f"Unknown scale {scale_name!r}: use one of {', '.join(FACTORS)}")
    fixed, scaled = MODEL[table]
    return fixed + round(scaled * FACTORS[scale_name])


def total(scale_name: str) -> int:
    """Rows across every table at a scale."""
    return sum(rows(t, scale_name) for t in MODEL)
