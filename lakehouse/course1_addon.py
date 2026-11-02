"""Course 1's add-on: the cleansing course's teaching material in the platform, switched on or off as one piece.

Course 2's platform stands on its own (Priority 4, 2026-11-02). Course 1's material rides on it as an add-on, which
one switch in .env decides:

    LAKEHOUSE_COURSE1=on    (the default) the add-on is there
    LAKEHOUSE_COURSE1=off   course 2's platform alone

What the add-on adds (PIECES), and how each tool honours the switch:

    dbt        gold.trust_test and its tests: models/course1/, enabled by dbt_project.yml only while on
    cube       the trust_test cube: cube/course1/, which cube.js loads only while on
    superset   the Trust Test dashboard: dashboards.py builds it only while on
    dagster    the weekly notebook_smoke job and its schedule, and load_lake's blocking parity check (the 19
               Facilitator Guide numbers on the landed lake): declared only while on
    bootstrap  stage_model.py, session 7's embedding model for the notebooks: staged only while on
    notebooks  lakehouse/08_from_lake.ipynb, the bridge from the sessions to the lake, run by notebook_smoke

Course 1's handed-over data (lakehouse/handoff, silver.trust_test among it) lands either way: the add-on is what's
built on top of it. The daily run pushes lakehouse_course1_addon_enabled, so the NotebookSmokeStale alert only
counts while the add-on is on. Turning it off on a running lab stops building the pieces; tables, cubes and
dashboards it built before stay until removed (README, "Course 1's add-on").
"""
import os

SWITCH = "LAKEHOUSE_COURSE1"
PIECES = {
    "dbt": ["gold.trust_test"],
    "cube": ["trust_test"],
    "superset": ["trust-test"],
    "dagster": ["notebook_smoke", "notebook_smoke_schedule", "matches_course_numbers"],
    "bootstrap": ["stage_model.py"],
    "notebooks": ["lakehouse/08_from_lake.ipynb"],
}


def enabled() -> bool:
    """True while course 1's add-on is on: LAKEHOUSE_COURSE1 unset or "on"; False for "off". Any other value raises
    ValueError, so a typo can't quietly switch the add-on off."""
    value = os.environ.get(SWITCH, "on").strip().lower()
    if value not in ("on", "off"):
        raise ValueError(f"{SWITCH} must be on or off, not {value!r}")
    return value == "on"
