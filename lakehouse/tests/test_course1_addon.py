"""Course 1's material in the platform is an optional add-on (Priority 4, 2026-11-02).

    pytest lakehouse/tests/test_course1_addon.py     # inside the jupyter container (or the CI sandbox)

One switch, LAKEHOUSE_COURSE1 (on by default; off), decides whether the platform carries course 1's teaching
material: the Trust Test (gold model, Cube cube, Superset dashboard), the 19-number parity check in the daily run,
and the weekly notebook_smoke job with the session 7 model it needs. With it off, course 2's platform stands on its
own. Course 1's handed-over data (silver.trust_test among it) still lands either way: the add-on is what's built on
top. Each tool honours the switch in its own way; these tests check each one, on copies, never the live stack.
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from lakehouse import course1_addon as addon  # noqa: E402

LAKEHOUSE = REPO / "lakehouse"


def test_the_switch_is_on_unless_set_off(monkeypatch):
    """Unset or "on" means on; "off" (any case or spacing) means off; anything else is a mistake worth stopping for,
    not a silent guess."""
    monkeypatch.delenv(addon.SWITCH, raising=False)
    assert addon.enabled()
    for value, want in (("on", True), ("ON", True), ("off", False), (" Off ", False)):
        monkeypatch.setenv(addon.SWITCH, value)
        assert addon.enabled() is want, value
    monkeypatch.setenv(addon.SWITCH, "maybe")
    with pytest.raises(ValueError, match="LAKEHOUSE_COURSE1"):
        addon.enabled()


def test_the_add_on_names_every_piece():
    """One list of what the add-on adds, for the docs and for these tests."""
    assert addon.PIECES == {
        "dbt": ["gold.trust_test"],
        "cube": ["trust_test"],
        "superset": ["trust-test"],
        "dagster": ["notebook_smoke", "notebook_smoke_schedule", "matches_course_numbers"],
        "bootstrap": ["stage_model.py"],
        "notebooks": ["lakehouse/08_from_lake.ipynb"],
    }


@pytest.fixture(scope="module")
def repo_copy(tmp_path_factory):
    """A copy of the repo's code (no dbt target or logs), so parsing dbt with the switch flipped never touches the
    live manifest Dagster reads."""
    root = tmp_path_factory.mktemp("repo")
    skip = shutil.ignore_patterns("target", "logs", "__pycache__", ".ipynb_checkpoints", "*.duckdb")
    for folder in ("lakehouse", "lnl", "notebooks", "generator"):
        shutil.copytree(REPO / folder, root / folder, ignore=skip)
    return root


def run_in(copy: Path, switch: str, code: str) -> dict:
    """Run python code in the copy with the switch set (and a throwaway local lake for dbt); return its JSON."""
    env = {**os.environ, addon.SWITCH: switch, "DBT_TARGET": "local", "PYTHONPATH": str(copy),
           "LAKE_CATALOG": str(copy / "lake.duckdb")}
    out = subprocess.run([sys.executable, "-c", code], cwd=copy, env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr[-3000:]
    return json.loads(out.stdout.strip().splitlines()[-1])


DAGSTER = """
import json, sys
sys.path.insert(0, "lakehouse/orchestration")
import definitions as d
keys = {"/".join(k.path) for a in d.defs.assets for k in getattr(a, "keys", [])}
print(json.dumps({"jobs": sorted(j.name for j in d.defs.jobs), "schedules": sorted(s.name for s in d.defs.schedules),
                  "checks": sorted(c.name for c in d.load_lake.check_specs), "trust_test": "gold/trust_test" in keys}))
"""


@pytest.mark.parametrize("switch", ["on", "off"])
def test_dagster_declares_the_add_on_only_while_it_is_on(repo_copy, switch):
    """On: the notebook_smoke job and its schedule, the blocking parity check on load_lake, and gold/trust_test from
    dbt. Off: none of them; daily_lake and the rest are unchanged."""
    got = run_in(repo_copy, switch, DAGSTER)
    on = switch == "on"
    assert ("notebook_smoke" in got["jobs"]) is on and ("notebook_smoke_schedule" in got["schedules"]) is on
    assert got["checks"] == (["matches_course_numbers"] if on else [])
    assert got["trust_test"] is on
    assert {"daily_lake", "lake_housekeeping", "world_refresh", "world_monthly_job"} <= set(got["jobs"])


@pytest.mark.parametrize("switch", ["on", "off"])
def test_dbt_builds_the_trust_test_only_while_it_is_on(repo_copy, switch, tmp_path):
    """gold.trust_test (and its tests) are in dbt's project only with the add-on on; every other gold model stays."""
    dbt = repo_copy / "lakehouse" / "dbt"
    env = {**os.environ, addon.SWITCH: switch, "DBT_TARGET": "local", "LAKE_CATALOG": str(tmp_path / "lake.duckdb")}
    out = subprocess.run(["dbt", "ls", "--resource-type", "model", "--output", "name", "--project-dir", str(dbt),
                          "--profiles-dir", str(dbt), "--target", "local", "--target-path", str(tmp_path / "t"),
                          "--log-path", str(tmp_path / "l"), "--quiet"], env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stdout[-2000:] + out.stderr[-2000:]
    models = set(out.stdout.split())
    assert ("trust_test" in models) is (switch == "on")
    assert {"dim_person", "fct_offender_record", "dq_scorecard"} <= models


@pytest.mark.parametrize("switch", ["on", "off"])
def test_superset_builds_the_trust_test_dashboard_only_while_it_is_on(monkeypatch, switch):
    """The dashboards dashboards.py builds: the Trust Test only with the add-on on; the others always."""
    sys.path.insert(0, str(LAKEHOUSE / "superset"))
    import dashboards
    monkeypatch.setenv(addon.SWITCH, switch)
    slugs = [d["slug"] for d in dashboards.dashboards()]
    assert ("trust-test" in slugs) is (switch == "on")
    assert {"custody", "world"} <= set(slugs)


def test_the_cube_add_on_lives_apart_from_the_core_model():
    """The Trust Test cube sits in cube/course1/, which cube.js loads only while the add-on is on (checks.sh asks
    Cube's own image which files it loads, both ways); the core model folder has no course 1 cube."""
    assert (LAKEHOUSE / "cube" / "course1" / "trust_test.yml").exists()
    assert not (LAKEHOUSE / "cube" / "model" / "trust_test.yml").exists()
    assert "LAKEHOUSE_COURSE1" in (LAKEHOUSE / "cube" / "cube.js").read_text()


def test_the_notebook_alert_only_counts_while_the_add_on_is_on():
    """NotebookSmokeStale fires only while the daily run reports the add-on on, so turning it off doesn't leave a
    stale weekly job alerting forever."""
    rules = yaml.safe_load((LAKEHOUSE / "monitoring" / "prometheus" / "rules" / "lakehouse.yml").read_text())
    expr = next(r["expr"] for g in rules["groups"] for r in g["rules"] if r.get("alert") == "NotebookSmokeStale")
    assert "lakehouse_course1_addon_enabled == 1" in expr
