"""Land the course data in the lakehouse: reference, bronze and silver, as one DuckLake snapshot.

    python lakehouse/load_lake.py              # reuse the generated CSV if it's already in LNL_DATA_DIR
    python lakehouse/load_lake.py --regenerate # rebuild the CSV from the seed first

Reads LAKE_CATALOG, LAKE_DATA_PATH and LNL_DATA_DIR from the environment (see lakehouse/.env.example).
Prints the run's metrics as JSON on the last line, for the scheduler and Pushgateway to pick up.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lakehouse import lake  # noqa: E402


def main() -> int:
    metrics = lake.run(regenerate="--regenerate" in sys.argv[1:])
    print(f"Loaded run {metrics['run_id']}: {metrics['rows_silver']:,} silver rows, "
          f"{metrics['exceptions']:,} flagged values on {metrics['records_with_exceptions']:,} records, "
          f"{metrics['seconds']}s, snapshot {metrics['snapshot_id']}")
    print(json.dumps(metrics, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
