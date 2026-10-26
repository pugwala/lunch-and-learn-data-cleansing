"""Acceptance test for the nightly catalog backup (Phase 6).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_backup.py

catalog-backup runs pg_dump 17 from the same postgres image as catalog-pg, at 02:00 Central (and at start when the
newest backup is a day old), as lake_backup: pg_read_all_data, so it reads everything and writes nothing. It runs as
uid 999, and the dumps are mode 600: they hold every app database, user password hashes included, so the notebook
user (uid 1000) can see that a backup happened (manifest, metrics) but can't read one. Each run
dumps every app database in custom format to /lake/catalog-backups/<UTC stamp>/, checks each with pg_restore --list,
writes a manifest, keeps 14 days, and leaves metrics for node-exporter's textfile collector. Lose the catalog and the
Parquet files are just files.
"""
import json
import os
import time
from pathlib import Path

import psycopg2
import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
BACKUPS = Path(os.environ.get("BACKUP_ROOT", "/lake/catalog-backups"))
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
DATABASES = {"ducklake", "dagster", "superset", "openmetadata_db"}
SERVICE = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]["catalog-backup"]


def newest():
    """The newest complete backup folder (one with a manifest)."""
    done = sorted(p.parent for p in BACKUPS.glob("*/manifest.json"))
    assert done, f"no backup under {BACKUPS}"
    return done[-1]


def query(promql):
    """Run an instant PromQL query; returns the result vector."""
    r = requests.get(f"{PROMETHEUS}/api/v1/query", params={"query": promql}, timeout=30)
    r.raise_for_status()
    return r.json()["data"]["result"]


def test_a_backup_from_the_last_day_covers_every_database():
    """The newest backup is under 26 hours old and has a dump for each app database."""
    folder = newest()
    assert time.time() - folder.stat().st_mtime < 26 * 3600
    assert {p.stem for p in folder.glob("*.dump")} == DATABASES


def test_each_dump_was_read_back_by_pg_restore():
    """pg_restore --list read every dump back (the manifest records what it found): the ducklake one holds the
    catalog's snapshot table."""
    manifest = json.loads((newest() / "manifest.json").read_text())
    assert set(manifest["databases"]) == DATABASES
    assert all(d["entries"] > 0 for d in manifest["databases"].values()), manifest
    assert "ducklake_snapshot" in manifest["databases"]["ducklake"]["tables"]


def test_notebook_user_cannot_read_a_dump():
    """The dumps belong to uid 999 with mode 600: this container's user (1000) gets permission denied."""
    for dump in newest().glob("*.dump"):
        assert dump.stat().st_uid == 999 and dump.stat().st_mode & 0o777 == 0o600, dump.name
        try:
            dump.read_bytes()
        except PermissionError:
            continue
        raise AssertionError(f"{dump.name} is readable by uid {os.getuid()}")


def test_backup_role_reads_but_cannot_write():
    """lake_backup holds pg_read_all_data, isn't a superuser, and can't change the catalog."""
    with psycopg2.connect(host="catalog-pg", dbname="ducklake", user="lake_rw",
                          password=os.environ["PGPASSWORD"]) as pg, pg.cursor() as cur:
        cur.execute("SELECT rolsuper, pg_has_role('lake_backup', 'pg_read_all_data', 'MEMBER'), "
                    "has_table_privilege('lake_backup', 'public.ducklake_snapshot', 'INSERT') "
                    "FROM pg_roles WHERE rolname = 'lake_backup'")
        assert cur.fetchone() == (False, True, False)


def test_prometheus_sees_the_backup():
    """node-exporter's textfile collector reports when the last backup succeeded and each dump's size."""
    age = query("time() - lakehouse_backup_last_success_timestamp_seconds")
    assert age and float(age[0]["value"][1]) < 26 * 3600, age
    sizes = {r["metric"]["database"]: float(r["value"][1]) for r in query("lakehouse_backup_bytes")}
    assert set(sizes) == DATABASES and all(v > 0 for v in sizes.values()), sizes


def test_schedule_and_retention():
    """02:00 Central, 14 days kept, the same image as catalog-pg, run as uid 999, and no host port."""
    env = SERVICE["environment"]
    assert env["TZ"] == "America/Chicago" and str(env["BACKUP_AT"]) == "02:00" and str(env["KEEP_DAYS"]) == "14"
    catalog = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]["catalog-pg"]
    assert SERVICE["image"] == catalog["image"] and SERVICE["user"] == "999:999"
    assert "ports" not in SERVICE
