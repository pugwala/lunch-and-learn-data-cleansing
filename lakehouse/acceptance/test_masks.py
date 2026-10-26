"""Acceptance test for the tags-to-masks loop (Phase 5): OpenMetadata says what's restricted; Cube masks it.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_masks.py

dbt's meta.sensitivity seeds the Sensitivity.Restricted tags (om_sync.py), people can add more in OpenMetadata's
UI, and cube/sync_masks.py makes Cube's model mask exactly the restricted columns of gold.fct_offender_record that
Cube serves. om_sync.py only adds tags, so a tag authored in OpenMetadata survives the next sync.
"""
import base64
import os
import subprocess
from pathlib import Path

import pytest
import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
MODEL = LAKEHOUSE / "cube" / "model" / "offender_records.yml"
OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata") + "/api/v1"
FCT = "lakehouse.lake.gold.fct_offender_record"


def model_masks(path=MODEL):
    """(members every group gets masked, members every group gets for real except those) in a model file."""
    cube = next(c for c in yaml.safe_load(Path(path).read_text())["cubes"] if c["name"] == "offender_records")
    policy = next(p for p in cube["access_policy"] if p.get("group") == "*")
    return set(policy["member_masking"]["includes"]), set(policy["member_level"]["excludes"])


def dimensions(path=MODEL):
    """{name: dimension} for offender_records in a model file."""
    cube = next(c for c in yaml.safe_load(Path(path).read_text())["cubes"] if c["name"] == "offender_records")
    return {d["name"]: d for d in cube["dimensions"]}


@pytest.fixture(scope="module")
def admin():
    """A requests session with an OpenMetadata admin token."""
    r = requests.post(f"{OM}/users/login", timeout=30, json={
        "email": "admin@open-metadata.org",
        "password": base64.b64encode(os.environ["OM_ADMIN_PASSWORD"].encode()).decode()})
    r.raise_for_status()
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"
    return s


def restricted_in_openmetadata(admin):
    """Columns of gold.fct_offender_record tagged Sensitivity.Restricted in OpenMetadata, however the tag got there."""
    table = admin.get(f"{OM}/tables/name/{FCT}", params={"fields": "columns,tags"}, timeout=30).json()
    return {c["name"] for c in table["columns"] if any(t["tagFQN"] == "Sensitivity.Restricted" for t in c.get("tags", []))}


def test_cube_masks_exactly_what_openmetadata_restricts(admin):
    """The restricted columns Cube serves are exactly the ones it masks."""
    masked, excluded = model_masks()
    assert masked == excluded == restricted_in_openmetadata(admin) & set(dimensions())


def test_sync_masks_finds_nothing_to_change():
    """cube/sync_masks.py --check agrees: the model already matches OpenMetadata."""
    r = run("cube/sync_masks.py", "--check")
    assert r.returncode == 0, r.stdout[-600:] + r.stderr[-600:]


def test_sync_masks_rewrites_the_policy_for_new_tags(tmp_path):
    """Given a different restricted set, sync_masks masks exactly those columns: it adds a mask where a dimension
    had none, drops the masks no longer needed, leaves everything else as it was, and going back restores the file."""
    copy = tmp_path / "offender_records.yml"
    copy.write_text(MODEL.read_text())
    r = run("cube/sync_masks.py", "--model", str(copy), "--restricted", "race_code,gender")
    assert r.returncode == 0, r.stdout[-600:] + r.stderr[-600:]
    assert model_masks(copy) == ({"race_code", "gender"}, {"race_code", "gender"})
    before, after = dimensions(), dimensions(copy)
    assert {n for n, d in after.items() if "mask" in d} == {"race_code", "gender"}
    unmasked = lambda dims: {n: {k: v for k, v in d.items() if k != "mask"} for n, d in dims.items()}
    assert unmasked(before) == unmasked(after)
    r = run("cube/sync_masks.py", "--model", str(copy), "--restricted", ",".join(model_masks()[0]))
    assert r.returncode == 0 and copy.read_text() == MODEL.read_text(), r.stdout[-600:] + r.stderr[-600:]


def run(script, *args):
    """Run a lakehouse script with python; returns the CompletedProcess."""
    return subprocess.run(["python", str(LAKEHOUSE / script), *args], capture_output=True, text=True, timeout=600)


def test_a_tag_authored_in_openmetadata_reaches_the_masks(admin):
    """Tag a column Restricted in OpenMetadata (as a steward would in the UI): om_sync keeps it and calls that in
    step, and sync_masks --check flags the Cube model as behind until it's re-synced."""
    table = admin.get(f"{OM}/tables/name/{FCT}", params={"fields": "columns,tags"}, timeout=30).json()
    idx = next(i for i, c in enumerate(table["columns"]) if c["name"] == "gender")
    tags = table["columns"][idx].get("tags") or []
    patch = {"Content-Type": "application/json-patch+json"}
    path = f"/columns/{idx}/tags"
    added = tags + [{"tagFQN": "Sensitivity.Restricted", "source": "Classification", "labelType": "Manual",
                     "state": "Confirmed"}]
    admin.patch(f"{OM}/tables/{table['id']}", headers=patch, timeout=30,
                json=[{"op": "add", "path": path, "value": added}]).raise_for_status()
    try:
        r = run("openmetadata/om_sync.py", "--check")
        assert r.returncode == 0, r.stdout[-600:]
        r = run("openmetadata/om_sync.py")
        assert r.returncode == 0, r.stdout[-600:]
        assert "gender" in restricted_in_openmetadata(admin)
        r = run("cube/sync_masks.py", "--check")
        assert r.returncode == 1 and "gender" in r.stdout, r.stdout[-600:] + r.stderr[-600:]
    finally:
        admin.patch(f"{OM}/tables/{table['id']}", headers=patch, timeout=30,
                    json=[{"op": "add", "path": path, "value": tags}]).raise_for_status()
    assert "gender" not in restricted_in_openmetadata(admin)
