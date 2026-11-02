"""Land course 1's pinned handoff in the lakehouse: reference, bronze and silver, as one DuckLake snapshot.

    python lakehouse/load_lake.py

Lands the version pinned in lakehouse/handoff/course1.lock.json from HANDOFF_ROOT (default
/lake/landing/handoff/course1), after checking every file. To change the data, course 1 publishes a new version
(python -m lakehouse.handoff.publish) and course 2 adopts it (python -m lakehouse.handoff.receive adopt DIR).
Reads LAKE_CATALOG and LAKE_DATA_PATH from the environment (see lakehouse/.env.example).
Prints the run's metrics as JSON on the last line, for the scheduler and Pushgateway to pick up.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lakehouse import lake  # noqa: E402


def main() -> int:
    """Run one load, print a summary line and then the metrics as JSON (the last line); returns the exit code."""
    metrics = lake.run()
    print(f"Loaded run {metrics['run_id']}: {metrics['rows_silver']:,} silver rows, "
          f"{metrics['exceptions']:,} flagged values on {metrics['records_with_exceptions']:,} records, "
          f"{metrics['seconds']}s, snapshot {metrics['snapshot_id']}")
    print(json.dumps(metrics, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
