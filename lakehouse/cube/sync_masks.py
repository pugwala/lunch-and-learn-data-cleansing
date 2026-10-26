"""Make Cube mask exactly the columns OpenMetadata tags Sensitivity.Restricted: Phase 5's tags-to-masks loop.

    docker compose --profile core exec -T -u "$(id -u):users" jupyter python lakehouse/cube/sync_masks.py [--check]

OpenMetadata is where "restricted" gets decided: om_sync.py seeds the tag from dbt's meta.sensitivity, and a steward
can add it in the UI. This reads the tagged columns of gold.fct_offender_record (as the read-only mcp-reader) and
rewrites the offender_records access policy so every group but restricted_reader gets those columns masked, giving a
newly restricted dimension its mask. It edits lines in place, so the model's comments survive. Cube reads its model
at start (CUBEJS_DEV_MODE is off), so restart cube after a change; bootstrap.sh does. --check only compares.
--restricted a,b uses that list instead of asking OpenMetadata, and --model edits another copy (both for tests).
A dimension that loses its tag loses its mask too, so untagging a column puts the file back as it was.
The -u runs it as the repo's owner, since jupyter's own user can't write the repo's files, in the users group, the
only way into /home/jovyan where the repo is mounted.
"""
import argparse
import base64
import os
import re
import sys
from pathlib import Path

import requests

MODEL = Path(__file__).resolve().parent / "model" / "offender_records.yml"
OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata") + "/api/v1"
FCT = "lakehouse.lake.gold.fct_offender_record"
RESTRICTED = "Sensitivity.Restricted"
CUBE = "offender_records"
MASK = ["        mask:",
        """          sql: "'***'"   # a SQL literal: Cube's YAML compiler reads a bare string as a template and gets null"""]


def restricted_in_openmetadata():
    """Columns of gold.fct_offender_record tagged Sensitivity.Restricted, read as the read-only mcp-reader."""
    r = requests.post(f"{OM}/users/login", timeout=30, json={
        "email": "mcp-reader@open-metadata.org",
        "password": base64.b64encode(os.environ["OM_MCP_PASSWORD"].encode()).decode()})
    r.raise_for_status()
    t = requests.get(f"{OM}/tables/name/{FCT}", params={"fields": "columns,tags"}, timeout=30,
                     headers={"Authorization": f"Bearer {r.json()['accessToken']}"})
    t.raise_for_status()
    return {c["name"] for c in t.json()["columns"] if any(x["tagFQN"] == RESTRICTED for x in c.get("tags") or [])}


def policy_line(lines, start, end, key):
    """Index of the one '<key>: [...]' list line in the "*" policy (restricted_reader's 'includes: "*"' isn't a list)."""
    hits = [i for i in range(start, end) if re.fullmatch(rf" {{10}}{key}: \[.*\]", lines[i])]
    if len(hits) != 1:
        sys.exit(f"sync_masks: expected one '{key}: [...]' line in the {CUBE} access policy, found {len(hits)}")
    return hits[0]


def rewrite(text, restricted):
    """(new model text, columns masked before, columns masked now): the "*" policy lists exactly the restricted
    dimensions, in model order, and exactly those have a mask, so the file depends only on the restricted set."""
    lines = text.split("\n")
    start = lines.index(f"  - name: {CUBE}")
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("  - name: ")), len(lines))
    d0 = next(i for i in range(start, end) if lines[i] == "    dimensions:")
    d1 = next(i for i in range(d0 + 1, end) if re.match(r" {4}\S", lines[i]))
    heads = [i for i in range(d0 + 1, d1) if lines[i].startswith("      - name: ")]
    names = [lines[i].split(": ", 1)[1].strip() for i in heads]
    now = [n for n in names if n in restricted]
    excludes, includes = (policy_line(lines, start, end, k) for k in ("excludes", "includes"))
    was = set(re.findall(r"\w+", lines[includes].split(":", 1)[1]))
    for i in (excludes, includes):
        lines[i] = f"{lines[i].split(':')[0]}: [{', '.join(now)}]"
    for n, (i, name) in reversed(list(enumerate(zip(heads, names)))):   # bottom up, so earlier indexes hold
        nxt = heads[n + 1] if n + 1 < len(heads) else d1
        mask = next((j for j in range(i, nxt) if lines[j].startswith("        mask:")), None)
        if name in restricted and mask is None:
            last = max(j for j in range(i, nxt) if lines[j].strip())
            lines[last + 1:last + 1] = MASK
        elif name not in restricted and mask is not None:   # the mask: line and the lines indented under it
            stop = next((j for j in range(mask + 1, nxt) if not lines[j].startswith(" " * 10)), nxt)
            del lines[mask:stop]
    return "\n".join(lines), was, set(now)


def main(argv=None):
    """Compare (--check) or rewrite the model; print one summary line. Exits 1 when --check finds it behind."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--check", action="store_true", help="compare only; exit 1 if the model is behind OpenMetadata")
    p.add_argument("--model", type=Path, default=MODEL, help="the Cube model file to edit")
    p.add_argument("--restricted", help="comma-separated columns to mask, instead of asking OpenMetadata")
    a = p.parse_args(argv)
    restricted = (set(filter(None, a.restricted.split(","))) if a.restricted is not None
                  else restricted_in_openmetadata())
    text = a.model.read_text()
    new, was, now = rewrite(text, restricted)
    if new == text:
        print(f"Cube masks: in step with OpenMetadata ({len(now)} restricted columns masked)")
        return 0
    change = "; ".join(filter(None, [now - was and "mask " + ", ".join(sorted(now - was)),
                                     was - now and "unmask " + ", ".join(sorted(was - now))])) or "add missing masks"
    if a.check:
        print(f"Cube masks: behind OpenMetadata ({change}); run cube/sync_masks.py, then restart cube")
        return 1
    a.model.write_text(new)
    print(f"Cube masks: updated ({change}); restart cube to apply")
    return 0


if __name__ == "__main__":
    sys.exit(main())
