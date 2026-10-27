"""Acceptance test for the off-machine backup and its restore drill.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_offsite_backup.py

catalog-backup dumps the catalog every night onto /lake, the same disk as the data, and nothing else backed the data
up. offsite-backup (restic 0.19.1, lakehouse/backup/offsite.sh) copies both together, encrypted and deduplicated,
to a backup target: a TrueNAS NFS share once it exists, a local folder until then. The target must hold a marker
file saying which (.lakehouse-backup-target: "local" or "truenas"), so an unmounted share is an error, never a
silent copy onto the VM's own disk. It runs nightly at 02:30 Central (after the 02:00 catalog dump, before
Sunday's 03:30 housekeeping), keeps 7 daily, 4 weekly and 6 monthly snapshots, checks the repository every night,
and once a month runs a restore drill: it reads every byte back, restores the latest snapshot and compares each
restored file with its original. It writes metrics for node-exporter and a status file the tests read here.
"""
import datetime as dt
import json
import os
from pathlib import Path

import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
SERVICES = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]
STATUS = Path(os.environ.get("LAKE_ROOT", "/lake")) / "offsite-cache" / "status.json"
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")


def status():
    """offsite.sh's status file: the last backup, where it went, and the last restore drill."""
    return json.loads(STATUS.read_text())


def query(q):
    """An instant PromQL query's single value, or None."""
    r = requests.get(f"{PROMETHEUS}/api/v1/query", params={"query": q}, timeout=30).json()["data"]["result"]
    return float(r[0]["value"][1]) if r else None


def age(stamp):
    """How long ago an epoch-seconds time was."""
    return dt.datetime.now(dt.timezone.utc) - dt.datetime.fromtimestamp(stamp, dt.timezone.utc)


def test_the_service_reads_the_lake_and_writes_only_its_target():
    """offsite-backup is pinned, reads the data and the catalog dumps read-only, writes its metrics, its cache and the
    target, and gets its repository password as a Compose secret (from .env, never in the repo)."""
    svc = SERVICES["offsite-backup"]
    assert svc["image"] == "restic/restic:${RESTIC_TAG:-0.19.1}"
    volumes = [v.split("   #")[0].strip() for v in svc["volumes"]]
    assert "${LAKE_ROOT:-/lake}/data:/lake/data:ro" in volumes
    assert "${LAKE_ROOT:-/lake}/catalog-backups:/lake/catalog-backups:ro" in volumes
    assert "${LAKE_ROOT:-/lake}/catalog-backups/metrics:/metrics" in volumes
    assert "${BACKUP_TARGET:-/mnt/lakehouse-backups}:/target" in volumes
    assert svc["secrets"] == ["restic_password"]
    assert yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["secrets"]["restic_password"] == \
        {"environment": "RESTIC_PASSWORD"}


def test_a_backup_of_the_catalog_and_the_data_ran_recently():
    """The last backup finished within 26 hours, holds both the catalog dumps and the lake's data, and the repository
    keeps more than one snapshot as days go by (at least the one)."""
    s = status()
    assert age(s["last_backup"]) < dt.timedelta(hours=26)
    assert sorted(s["paths"]) == ["/lake/catalog-backups", "/lake/data"]
    assert s["snapshots"] >= 1 and s["repository_bytes"] > 0


def test_the_backup_says_where_it_is():
    """The status and the metric say whether the target is off the machine: "local" means not yet."""
    s = status()
    assert s["target"] in ("local", "truenas")
    assert query("lakehouse_offsite_backup_off_machine") == (0.0 if s["target"] == "local" else 1.0)


def test_the_restore_drill_got_every_file_back_intact():
    """The last drill (within 35 days) read every byte of the repository back, restored the latest snapshot and found
    every restored file identical to its original."""
    drill = status()["drill"]
    assert age(drill["finished"]) < dt.timedelta(days=35)
    assert drill["compared"] > 0 and drill["mismatched"] == 0 and drill["missing"] == 0


def test_prometheus_watches_the_backup_and_the_drill():
    """The metrics reach Prometheus, and the four alerts load."""
    assert age(query("lakehouse_offsite_backup_last_success_timestamp_seconds")) < dt.timedelta(hours=26)
    assert query("lakehouse_restore_drill_last_success_timestamp_seconds") is not None
    groups = requests.get(f"{PROMETHEUS}/api/v1/rules", timeout=30).json()["data"]["groups"]
    rules = {r["name"]: r for g in groups for r in g["rules"]}
    for name in ("OffsiteBackupStale", "OffsiteBackupFailed", "RestoreDrillStale", "RestoreDrillFailed"):
        assert rules[name]["health"] == "ok", name
