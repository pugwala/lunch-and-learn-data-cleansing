"""Benchmark a scale of the world end to end in a scratch lake (dataset expansion, chunk 10).

    python -m lakehouse.universe.bench --scale L [--dir /lake/scratch/L] [--workers N] [--repeats 3]

Everything happens in one folder of its own, never the live lake:
  1. build    the world at the scale into <dir>/universe (clean Parquet, messy CSVs, answer keys, mention key)
  2. seed     a scratch DuckLake: its catalog is the file <dir>/catalog.ducklake and its Parquet goes to <dir>/data/,
              seeded with the two tables the world's rule tests read (ref.nibrs_offenses and silver.hero_offenders'
              keys and sentence dates) from course 1's pinned handoff, as the build reads its core: the live lake is
              never opened
  3. world_full     dbt build of the world's bronze, silver dimensions, incidents and gold dimensions, as Dagster's
                    world_refresh runs it
  4. world_monthly  dbt build of the world's big facts, every month in one run, as a Dagster backfill runs it, with
                    the answer-key tests
  5. queries  the benchmark queries (QUERIES, the questions the world's dashboards ask) on the scratch lake's gold,
              each timed over the repeats
Each step runs as its own process and is measured: wall-clock seconds and the peak memory of its whole process tree
(the build's workers are child processes). The report goes to <dir>/bench.json with row counts and folder sizes.
A run clears <dir>'s universe, data, dbt folders and catalog first, so a folder in or around the live lake is refused.
"""
import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from ..handoff import receive

from . import build, manifest, scale

REPO = Path(__file__).resolve().parents[2]
DBT = REPO / "lakehouse" / "dbt"
STEPS = ["build", "seed", "world_full", "world_monthly"]
SCRATCH_PARTS = ["universe", "data", "dbt"]
SCHEMAS = ["world_bronze", "world_silver", "world_gold"]
QUERIES = {
    "fights_by_year": """
        SELECT year(incident_month) AS year, count(*) AS fights, sum(bystanders_killed) AS killed
        FROM world_gold.fct_incident GROUP BY 1 ORDER BY 1""",
    "damage_by_asset": """
        SELECT asset_type, count(*) AS damages, sum(cost_usd) AS cost, sum(insurance_paid_usd) AS paid,
               avg(days_to_repair) AS days
        FROM world_gold.fct_damage GROUP BY 1""",
    "convictions_by_year": """
        SELECT year(decided_on) AS year, count(*) AS charges, avg(is_conviction::INT) AS conviction_rate
        FROM world_gold.fct_charge GROUP BY 1 ORDER BY 1""",
    "busiest_villains": """
        SELECT p.person_key, p.role, count(DISTINCT f.incident_id) AS fights
        FROM world_gold.fct_participation f JOIN world_gold.dim_person p USING (person_key)
        WHERE f.side = 'villain' GROUP BY 1, 2 ORDER BY fights DESC, 1 LIMIT 20""",
    "power_by_tier": """
        SELECT tier, count(*) AS uses, avg(effectiveness) AS effectiveness
        FROM world_gold.fct_power_use GROUP BY 1""",
    "damage_by_city": """
        SELECT l.name AS city, count(DISTINCT i.incident_id) AS fights, sum(d.cost_usd) AS cost
        FROM world_gold.fct_damage d JOIN world_gold.fct_incident i USING (incident_id)
        JOIN world_gold.dim_location l ON l.location_id = i.city_id
        GROUP BY 1 ORDER BY cost DESC, 1 LIMIT 20""",
}


def live_paths() -> list:
    """The live lake's folders a run must never clear or sit inside: its data path and the live worlds' folder."""
    paths = [build.DEFAULT_ROOT]
    if os.environ.get("LAKE_DATA_PATH"):
        paths.append(Path(os.environ["LAKE_DATA_PATH"]))
    return [Path(os.path.abspath(p)) for p in paths]


def check_folder(folder: Path) -> Path:
    """The folder as an absolute path, or ValueError when it is, sits inside, or holds a live lake folder."""
    folder = Path(os.path.abspath(folder))
    for live in live_paths():
        if folder == live or live in folder.parents or folder in live.parents:
            raise ValueError(f"{folder} is in or around the live lake ({live}): pick a scratch folder of its own")
    return folder


def catalog(folder: Path) -> str:
    """The scratch lake's DuckLake catalog: a DuckDB file in the folder."""
    return f"ducklake:{Path(folder) / 'catalog.ducklake'}"


def data_path(folder: Path) -> str:
    """Where the scratch lake's Parquet files go."""
    return f"{Path(folder) / 'data'}/"


def connect(folder: Path, read_only: bool = False):
    """A DuckDB connection with the scratch lake attached as `lake` and selected (created on first write)."""
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake")
    opts = f"DATA_PATH '{data_path(folder)}'" + (", READ_ONLY" if read_only else "")
    con.execute(f"ATTACH '{catalog(folder)}' AS lake ({opts})")
    con.execute("USE lake")
    return con


def seed(folder: Path, handoff_dir: Path = None) -> None:
    """Give the scratch lake the two tables the world's rule tests read, ref.nibrs_offenses and silver.hero_offenders'
    keys and sentence dates, from course 1's pinned handoff (checked against its lock), where the world's core comes
    from too: the live lake is never opened."""
    src = receive.connect(receive.require(handoff_dir), receive.read_lock())
    try:
        nibrs = src.execute("SELECT * FROM ref.nibrs_offenses").arrow().read_all()
        records = src.execute(
            "SELECT offender_number, sid_number, sentence_date FROM silver.hero_offenders").arrow().read_all()
    finally:
        src.close()
    con = connect(folder)
    try:
        con.register("nibrs", nibrs)
        con.register("records", records)
        con.execute("CREATE SCHEMA ref; CREATE SCHEMA silver")
        con.execute("CREATE TABLE ref.nibrs_offenses AS SELECT * FROM nibrs")
        con.execute("CREATE TABLE silver.hero_offenders AS SELECT * FROM records")
    finally:
        con.close()


def tree_rss_mb(root: int) -> float:
    """Resident memory of a process and all its descendants, in MB, from /proc (0 once they are gone)."""
    parents = {}
    for stat in Path("/proc").glob("[0-9]*/stat"):
        try:
            fields = stat.read_text().rsplit(")", 1)[1].split()
        except OSError:
            continue
        parents.setdefault(int(fields[1]), []).append(int(stat.parent.name))
    tree, total = [root], 0
    while tree:
        pid = tree.pop()
        tree.extend(parents.get(pid, []))
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    total += int(line.split()[1])
        except OSError:
            pass
    return total / 1024


def measured(cmd: list, env: dict = None, cwd: Path = REPO, every: float = 0.1) -> dict:
    """Run a command and measure it: ok, return code, wall-clock seconds, the peak memory of its process tree in MB
    (sampled every `every` seconds, and never below the kernel's own peak for the biggest single process), and the
    end of its output."""
    started = time.perf_counter()
    with tempfile.TemporaryFile() as out:
        proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, env=env, cwd=cwd)
        peak = 0.0
        while True:
            peak = max(peak, tree_rss_mb(proc.pid))
            pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
            if pid:
                break
            time.sleep(every)
        proc.returncode = os.waitstatus_to_exitcode(status)
        seconds = time.perf_counter() - started
        out.seek(0)
        tail = out.read()[-4000:].decode(errors="replace")
    peak = max(peak, usage.ru_maxrss / 1024)
    return {"ok": proc.returncode == 0, "returncode": proc.returncode, "seconds": round(seconds, 2),
            "peak_mb": round(peak, 1), "tail": tail}


def dbt_cmd(folder: Path, select: list, world_root: Path) -> list:
    """The dbt build command for the scratch lake (its own target and log folders, the world's root as a var)."""
    return ["dbt", "build", *select, "--project-dir", str(DBT), "--profiles-dir", str(DBT), "--target", "lake",
            "--vars", json.dumps({"world_root": str(world_root)}),
            "--target-path", str(folder / "dbt" / "target"), "--log-path", str(folder / "dbt" / "logs")]


def steps(scale_name: str, folder: Path, workers: int) -> dict:
    """Each step's command, in order, and the environment the dbt steps run with (the scratch lake, never the live)."""
    universe = folder / "universe"
    return {
        "build": [sys.executable, "-m", "lakehouse.universe.build", "--scale", scale_name, "--out", str(universe),
                  "--workers", str(workers)],
        "seed": [sys.executable, "-c", "import sys; from pathlib import Path; from lakehouse.universe import bench; "
                                       "bench.seed(Path(sys.argv[1]))", str(folder)],
        "world_full": dbt_cmd(folder, ["--select", "tag:world", "--exclude", "tag:world_monthly+"], universe),
        "world_monthly": dbt_cmd(folder, ["--select", "tag:world_monthly"], universe),
    }


def step_env(name: str, folder: Path) -> dict:
    """The environment a step runs with: the repo importable; the dbt steps pointed at the scratch lake (catalog,
    data path, DuckLake target). The build and the seed read the handoff, so they keep the usual settings."""
    env = {**os.environ, "PYTHONPATH": str(REPO)}
    if name.startswith("world_"):
        env.update(LAKE_CATALOG=catalog(folder), LAKE_DATA_PATH=data_path(folder), DBT_TARGET="lake")
    return env


def time_queries(folder: Path, repeats: int) -> dict:
    """Each benchmark query run `repeats` times on the scratch lake (read-only): rows, every time, the best time."""
    con = connect(folder, read_only=True)
    try:
        out = {}
        for name, sql in QUERIES.items():
            times = []
            for _ in range(repeats):
                started = time.perf_counter()
                rows = con.execute(sql).fetchall()
                times.append(round(time.perf_counter() - started, 4))
            out[name] = {"rows": len(rows), "seconds": times, "best_s": min(times)}
        return out
    finally:
        con.close()


def row_counts(folder: Path) -> dict:
    """Rows in every world table of the scratch lake, keyed schema.table."""
    con = connect(folder, read_only=True)
    try:
        tables = con.execute("SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake' "
                             f"AND schema_name IN {tuple(SCHEMAS)} ORDER BY 1, 2").fetchall()
        return {f"{s}.{t}": con.execute(f'SELECT count(*) FROM {s}."{t}"').fetchone()[0] for s, t in tables}
    finally:
        con.close()


def size_mb(path: Path) -> float:
    """The bytes of every file under a folder, in MB."""
    return round(sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file()) / 2**20, 1)


def host() -> dict:
    """The machine the benchmark ran on: CPUs and memory."""
    mem = next(int(line.split()[1]) for line in Path("/proc/meminfo").read_text().splitlines()
               if line.startswith("MemTotal:"))
    return {"cpus": os.cpu_count(), "memory_gb": round(mem / 2**20, 1)}


def write(folder: Path, report: dict) -> None:
    """Write the report to <folder>/bench.json."""
    (folder / "bench.json").write_text(json.dumps(report, indent=2) + "\n")


def run(scale_name: str, folder: Path, workers: int = max(1, (os.cpu_count() or 2) - 2), repeats: int = 3,
        log=print) -> dict:
    """Benchmark one scale end to end in a scratch folder (see the module docstring) and return the report, also
    written to <folder>/bench.json. A failed step stops the run with RuntimeError, after writing what ran."""
    scale.check(scale_name)
    folder = check_folder(folder)
    for part in SCRATCH_PARTS:
        shutil.rmtree(folder / part, ignore_errors=True)
    for f in folder.glob("catalog.ducklake*"):
        f.unlink()
    folder.mkdir(parents=True, exist_ok=True)
    report = {"scale": scale_name, "folder": str(folder), "lake": {"catalog": catalog(folder), "data_path": data_path(folder)},
              "started": dt.datetime.now(ZoneInfo("America/Chicago")).isoformat(timespec="seconds"),
              "host": host(), "duckdb": duckdb.__version__, "workers": workers, "steps": {}}
    for name, cmd in steps(scale_name, folder, workers).items():
        step = measured(cmd, env=step_env(name, folder))
        report["steps"][name] = step
        log(f"  {name:14} {step['seconds']:8.1f} s  peak {step['peak_mb']:9,.0f} MB  {'ok' if step['ok'] else 'FAILED'}")
        if not step["ok"]:
            write(folder, report)
            raise RuntimeError(f"step {name} failed:\n{step['tail']}")
        step["tail"] = step["tail"][-600:]
    report["world_rows"] = sum(t["rows"] for t in manifest.read(folder / "universe")["tables"].values())
    report["queries"] = time_queries(folder, repeats)
    report["rows"] = row_counts(folder)
    universe = folder / "universe"
    report["sizes_mb"] = {"universe": size_mb(universe), "messy": size_mb(universe / "messy"),
                          "answer_key": size_mb(universe / "answer_key"), "lake_data": size_mb(folder / "data")}
    write(folder, report)
    return report


def main(argv=None) -> int:
    """Command line: benchmark one scale in a scratch folder and print the summary."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--scale", default="L", choices=list(scale.FACTORS))
    ap.add_argument("--dir", type=Path, help="scratch folder (default /lake/scratch/<scale>)")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 2))
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args(argv)
    folder = args.dir or Path("/lake/scratch") / args.scale
    report = run(args.scale, folder, workers=args.workers, repeats=args.repeats)
    for name, q in report["queries"].items():
        print(f"  {name:20} best {q['best_s']:7.3f} s  ({q['rows']} rows)")
    print(f"  world rows {report['world_rows']:,}; sizes MB {report['sizes_mb']}; report {folder / 'bench.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
