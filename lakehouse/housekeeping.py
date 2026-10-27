"""DuckLake housekeeping: keep 30 days of time travel, and no more.

    docker compose --profile core exec -T jupyter python lakehouse/housekeeping.py [--dry-run]

Every daily load replaces each table, and DuckLake keeps every replaced file so the lake can be queried as it was.
Left alone, that history grows forever. This runs weekly (the Dagster job lake_housekeeping, Sundays 03:30 Central,
before the 05:00 load) and does DuckLake's own maintenance, in DuckLake's order:

1. expire snapshots older than RETENTION_DAYS (30, Ian's choice); the current snapshot is never expired
2. merge small adjacent files into bigger ones
3. delete files that no snapshot needs any more, once they have waited FILE_GRACE (a day), so a query that started
   before step 1 can still finish reading them
4. delete orphaned files (in the data path, never recorded in the catalog: a write that failed half way) older
   than ORPHAN_AGE (a week)

Deleting is permanent, which is the point: personal data that changed or was deleted is gone from history within
a month too. --dry-run reports what steps 1, 3 and 4 would remove and changes nothing (step 2 has no dry run, so it
is skipped). It needs the read-write role (lake_rw), as the loader does.
"""
import argparse
import datetime as dt
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lakehouse import lake  # noqa: E402

RETENTION_DAYS = 30
FILE_GRACE = dt.timedelta(days=1)
ORPHAN_AGE = dt.timedelta(days=7)


def stats(con, name=lake.LAKE):
    """The lake's history in numbers: snapshots and the oldest one's time, and data files (live, and all of them
    including the ones only old snapshots use), from DuckLake's own catalog."""
    meta = f"__ducklake_metadata_{name}"
    snapshots, oldest = con.execute(f"SELECT count(*), min(snapshot_time) FROM ducklake_snapshots('{name}')").fetchone()
    live_files, total_files, live_bytes, total_bytes = con.execute(f"""
        SELECT count(*) FILTER (WHERE end_snapshot IS NULL), count(*),
               coalesce(sum(file_size_bytes) FILTER (WHERE end_snapshot IS NULL), 0), coalesce(sum(file_size_bytes), 0)
        FROM {meta}.ducklake_data_file""").fetchone()
    return {"snapshots": snapshots, "oldest_snapshot": oldest, "live_files": live_files, "total_files": total_files,
            "live_bytes": int(live_bytes), "total_bytes": int(total_bytes)}


def housekeep(con, keep_since, files_before, orphans_before, dry_run=False, name=lake.LAKE):
    """Run the four steps on an attached DuckLake: expire snapshots older than keep_since, merge small files, delete
    unneeded files scheduled before files_before and orphaned files older than orphans_before. Returns what each
    step removed (or would remove, with dry_run) and the lake's numbers before and after."""
    before = stats(con, name)
    expired = con.execute(f"CALL ducklake_expire_snapshots('{name}', older_than => ?, dry_run => ?)",
                          [keep_since, dry_run]).fetchall()
    merged = 0
    if not dry_run:
        merged = len(con.execute(f"CALL ducklake_merge_adjacent_files('{name}')").fetchall())
    deleted = con.execute(f"CALL ducklake_cleanup_old_files('{name}', older_than => ?, dry_run => ?)",
                          [files_before, dry_run]).fetchall()
    orphans = con.execute(f"CALL ducklake_delete_orphaned_files('{name}', older_than => ?, dry_run => ?)",
                          [orphans_before, dry_run]).fetchall()
    return {"dry_run": dry_run, "snapshots_expired": len(expired), "files_merged": merged,
            "files_deleted": len(deleted), "orphans_deleted": len(orphans), "before": before,
            "after": stats(con, name)}


def run(dry_run=False, now=None):
    """Housekeep the real lake with the standard retention, as lake_rw. Returns housekeep()'s result plus seconds."""
    now = now or dt.datetime.now(dt.timezone.utc)
    started = time.time()
    con = lake.lake_connect()
    try:
        result = housekeep(con, keep_since=now - dt.timedelta(days=RETENTION_DAYS), files_before=now - FILE_GRACE,
                           orphans_before=now - ORPHAN_AGE, dry_run=dry_run)
    finally:
        con.close()
    result["seconds"] = round(time.time() - started, 1)
    return result


def summary(result):
    """One line a person reads: what was removed and how the lake's size changed."""
    b, a = result["before"], result["after"]
    mb = lambda n: f"{n / 1e6:,.0f} MB"   # noqa: E731
    verb = "would remove" if result["dry_run"] else "removed"
    return (f"Lake housekeeping{' (dry run)' if result['dry_run'] else ''}: {verb} {result['snapshots_expired']} "
            f"snapshots, {result['files_deleted']} unneeded and {result['orphans_deleted']} orphaned files"
            f"{'' if result['dry_run'] else ', merged ' + str(result['files_merged'])}; snapshots {b['snapshots']} to "
            f"{a['snapshots']}, lake {mb(b['total_bytes'])} to {mb(a['total_bytes'])} ({mb(a['live_bytes'])} live)")


def main(argv):
    """Run housekeeping on the real lake (or only report, with --dry-run) and print one summary line."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="report what would be removed; change nothing")
    print(summary(run(dry_run=parser.parse_args(argv).dry_run)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
