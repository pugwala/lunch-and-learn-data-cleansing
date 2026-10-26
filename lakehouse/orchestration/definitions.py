"""Dagster definitions for the home-lab lakehouse.

    load_lake (Python)  -> ref.*, bronze.*, silver.*, ops.load_runs, then the parity check
    gold (dbt build)    -> gold.dim_*, gold.fct_offender_record, gold.dq_scorecard, plus dbt tests
    openmetadata/lake   -> om_sync.py after gold, so OpenMetadata knows today's tables; a non-blocking check that
                           Cube's masks still match OpenMetadata's Restricted tags
    fuseki/glossary     -> om_to_rdf.py: OpenMetadata's glossary and column tags as one RDF graph in Fuseki
    monitoring/data_quality -> gold.dq_scorecard's pass rates, pushed to the Pushgateway for Prometheus
    push_run_success/failure -> sensors that push every job's last success, duration and last failure
    monitoring_selftest -> a job that fails on request, to exercise the failure path and its alert
    notebook_smoke      -> runs the 7 course notebooks and lakehouse/08_from_lake.ipynb weekly (an op job, not
                           an asset, so daily_lake's all-asset selection never picks it up)

The parity check is blocking: if the lake stops matching the course's numbers, gold is not rebuilt. That's the
home-lab version of a Lakeflow expectation set to stop the pipeline. dbt tests carry the same idea down a level:
error-severity tests fail the run, warn-severity tests (reference lookups, the 95% quality floor) only record.

Runs one at a time (dagster.yaml sets max_concurrent_runs: 1) because DuckLake writers shouldn't race.
"""
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from lakehouse import lake  # noqa: E402
from lakehouse.metrics import push  # noqa: E402

DBT_DIR = REPO / "lakehouse" / "dbt"
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR, target=os.environ.get("DBT_TARGET", "lake"))


def _manifest_is_stale() -> bool:
    """True when any dbt source file is newer than the manifest (or there is no manifest yet)."""
    manifest = dbt_project.manifest_path
    if not manifest.exists():
        return True
    sources = [DBT_DIR / "dbt_project.yml", DBT_DIR / "profiles.yml",
               *(f for folder in ("models", "macros", "tests") for f in (DBT_DIR / folder).rglob("*") if f.is_file())]
    return max(f.stat().st_mtime for f in sources) > manifest.stat().st_mtime


# Re-parse only when the models changed: the webserver, the daemon and every run load this file, and
# parsing each time would cost seconds and race on target/manifest.json.
if _manifest_is_stale():
    dbt_project.preparer.prepare(dbt_project)

# Everything the loader writes, keyed [schema, table]. dbt sources resolve to these same keys, so Dagster wires
# gold downstream of the load without any extra configuration.
LOADED_TABLES = {
    "ref": ["nibrs_offenses", "race_crosswalk", "religion_crosswalk", "stg_groups", "unit_crosswalk",
            "violence_codes", "null_tokens"],
    "bronze": ["hero_offenders_raw", "conduct_notes_answer_key"],
    "silver": ["hero_offenders", "hero_offenders_exceptions", "conduct_notes_answer_key", "trust_test"],
    "ops": ["load_runs"],
}
SILVER_KEY = dg.AssetKey(["silver", "hero_offenders"])
PARITY_CHECK = "matches_course_numbers"


class LoadConfig(dg.Config):
    """Run config for load_lake (and so for daily_lake and regenerate_lake)."""

    regenerate: bool = False
    """Rebuild the CSV from the seed before loading (normally the cached file is reused)."""


@dg.multi_asset(
    name="load_lake",
    specs=[dg.AssetSpec(dg.AssetKey([schema, table]), group_name=schema, kinds={"duckdb", "python"},
                        description=lake.TABLE_COMMENTS.get(f"{schema}.{table}"))
           for schema, tables in LOADED_TABLES.items() for table in tables],
    check_specs=[dg.AssetCheckSpec(PARITY_CHECK, asset=SILVER_KEY, blocking=True,
                                   description="All 19 lakehouse/tests checks match FACILITATOR_GUIDE.md")],
)
def load_lake(context: dg.AssetExecutionContext, config: LoadConfig):
    """Land ref, bronze and silver (lake.run), report each table, push the load's numbers, then run the blocking
    parity check: the 19 lakehouse/tests checks against FACILITATOR_GUIDE.md."""
    metrics = lake.run(regenerate=config.regenerate)
    context.log.info(f"Loaded run {metrics['run_id']}: {metrics['rows_silver']:,} rows, "
                     f"{metrics['exceptions']:,} flagged values, snapshot {metrics['snapshot_id']}")
    shared = {"run_id": metrics["run_id"], "snapshot_id": metrics["snapshot_id"]}
    for schema, tables in LOADED_TABLES.items():
        for table in tables:
            metadata = dict(shared)
            if (schema, table) == ("silver", "hero_offenders"):
                metadata.update(dagster_row_count=metrics["rows_silver"], seconds=metrics["seconds"],
                                records_with_exceptions=metrics["records_with_exceptions"])
            elif (schema, table) == ("silver", "hero_offenders_exceptions"):
                metadata["dagster_row_count"] = metrics["exceptions"]
            elif (schema, table) == ("bronze", "hero_offenders_raw"):
                metadata["dagster_row_count"] = metrics["rows_bronze"]
            yield dg.MaterializeResult(asset_key=dg.AssetKey([schema, table]), metadata=metadata)
    error = push("lakehouse_load", [
        ("lakehouse_load_rows", "Rows the latest load wrote, by layer.", m, {"layer": layer})
        for layer, m in (("bronze", metrics["rows_bronze"]), ("silver", metrics["rows_silver"]))] + [
        ("lakehouse_load_flagged_values", "Values the latest load flagged (never dropped).", metrics["exceptions"], {}),
        ("lakehouse_load_records_with_exceptions", "Records with at least one flagged value.",
         metrics["records_with_exceptions"], {}),
        ("lakehouse_load_seconds", "How long the latest load took.", metrics["seconds"], {}),
        ("lakehouse_load_finished_timestamp_seconds", "When the latest load finished.",
         metrics["finished_at"].timestamp(), {})])
    if error:
        context.log.warning(error)

    result = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                             str(REPO / "lakehouse" / "tests")], capture_output=True, text=True, cwd=REPO)
    summary = (result.stdout.strip().splitlines() or ["no output"])[-1]
    yield dg.AssetCheckResult(asset_key=SILVER_KEY, check_name=PARITY_CHECK, passed=result.returncode == 0,
                              metadata={"summary": summary,
                                        "output": dg.MetadataValue.md(f"```\n{result.stdout[-3000:]}\n```")})


class GoldTranslator(DagsterDbtTranslator):
    """dbt models become gold/<model>; sources keep [schema, table] so they match load_lake."""

    def get_asset_key(self, dbt_resource_props):
        """gold/<model> for a dbt model; dagster-dbt's default ([schema, table]) for a source."""
        if dbt_resource_props["resource_type"] == "model":
            return dg.AssetKey(["gold", dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        """Models go in the gold group; sources keep whatever group load_lake gave them."""
        return "gold" if dbt_resource_props["resource_type"] == "model" else None


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project, dagster_dbt_translator=GoldTranslator())
def gold(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """dbt build: every gold model and its tests, streamed to Dagster as materializations and checks."""
    yield from dbt.cli(["build"], context=context).stream()


OM_KEY = dg.AssetKey(["openmetadata", "lake"])
MASKS_CHECK = "cube_masks_match_openmetadata"


def run_script(*args: str) -> subprocess.CompletedProcess:
    """Run a lakehouse script with this Python, from the repo root; returns the finished process."""
    return subprocess.run([sys.executable, *args], capture_output=True, text=True, cwd=REPO)


@dg.asset(
    key=OM_KEY, group_name="openmetadata", kinds={"openmetadata", "python"}, deps=[*load_lake.keys, *gold.keys],
    description="The glossary, Sensitivity tags and every lake table in OpenMetadata (om_sync.py).",
    check_specs=[dg.AssetCheckSpec(MASKS_CHECK, asset=OM_KEY, blocking=False,
                                   description="Cube masks exactly the columns OpenMetadata tags Restricted")],
)
def openmetadata_lake(context: dg.AssetExecutionContext):
    """Sync OpenMetadata with the lake just built, then check (never rewrite) Cube's masks against its tags.

    The check only warns: rewriting the Cube model and restarting Cube is a reviewed step (bootstrap.sh, then
    commit), not something a 05:00 run should do on its own."""
    sync = run_script("lakehouse/openmetadata/om_sync.py")
    if sync.returncode != 0:
        raise dg.Failure(f"om_sync.py failed: {(sync.stdout + sync.stderr)[-1500:]}")
    summary = sync.stdout.strip().splitlines()[-1]
    context.log.info(summary)
    yield dg.MaterializeResult(asset_key=OM_KEY, metadata={"summary": summary,
                                                           "output": dg.MetadataValue.md(f"```\n{sync.stdout[-3000:]}\n```")})
    masks = run_script("lakehouse/cube/sync_masks.py", "--check")
    said = ((masks.stdout + masks.stderr).strip().splitlines() or ["no output"])[-1]
    yield dg.AssetCheckResult(asset_key=OM_KEY, check_name=MASKS_CHECK, passed=masks.returncode == 0,
                              severity=dg.AssetCheckSeverity.WARN, metadata={"summary": said})


FUSEKI_KEY = dg.AssetKey(["fuseki", "glossary"])


@dg.asset(key=FUSEKI_KEY, group_name="openmetadata", kinds={"sparql", "python"}, deps=[OM_KEY],
          description="OpenMetadata's glossary (SKOS) and column sensitivity as one graph in Fuseki (om_to_rdf.py).")
def fuseki_glossary(context: dg.AssetExecutionContext):
    """Replace the Fuseki graph with what OpenMetadata holds after today's sync."""
    rdf = run_script("lakehouse/openmetadata/om_to_rdf.py")
    if rdf.returncode != 0:
        raise dg.Failure(f"om_to_rdf.py failed: {(rdf.stdout + rdf.stderr)[-1500:]}")
    summary = rdf.stdout.strip().splitlines()[-1]
    context.log.info(summary)
    return dg.MaterializeResult(metadata={"summary": summary})


DQ_FLOOR = 95   # dbt/tests/dq_score_floor.sql warns below this score


@dg.asset(key=dg.AssetKey(["monitoring", "data_quality"]), group_name="monitoring", kinds={"prometheus", "python"},
          deps=[*gold.keys], description="gold.dq_scorecard's pass rates, pushed to the Pushgateway for Prometheus.")
def data_quality(context: dg.AssetExecutionContext):
    """Push each quality dimension's pass rate and the number of rules under the floor; a failed push only warns."""
    con = lake.lake_ro_connect()
    rates = con.sql("SELECT dimension, 100.0 * sum(passed) / sum(checked) FROM gold.dq_scorecard GROUP BY 1").fetchall()
    below = con.sql(f"SELECT count(*) FROM gold.dq_scorecard WHERE score_pct < {DQ_FLOOR}").fetchone()[0]
    con.close()
    error = push("lakehouse_data_quality", [
        ("lakehouse_dq_pass_rate_percent", "Share of checked values that passed, by quality dimension.", pct,
         {"dimension": dim}) for dim, pct in rates] + [
        ("lakehouse_dq_rules_below_floor", f"Quality rules scoring under {DQ_FLOOR}%.", below, {})])
    if error:
        context.log.warning(error)
    return dg.MaterializeResult(metadata={"rules_below_floor": below,
                                          **{f"pass_rate_{d}": round(p, 2) for d, p in rates}})


def push_run(context, outcome: str):
    """Push a finished run's end time under its job's name, plus how long it took when it succeeded."""
    stats = context.instance.get_run_stats(context.dagster_run.run_id)
    ended = stats.end_time or time.time()
    samples = [(f"lakehouse_job_last_{outcome}_timestamp_seconds", f"When the job last ended in {outcome}.", ended, {})]
    if outcome == "success" and stats.start_time:
        samples.append(("lakehouse_job_last_duration_seconds", "How long the job's last successful run took.",
                        ended - stats.start_time, {}))
    error = push("dagster", samples, {"pipeline": context.dagster_run.job_name})
    if error:
        context.log.warning(error)


@dg.run_status_sensor(run_status=dg.DagsterRunStatus.SUCCESS, name="push_run_success", minimum_interval_seconds=15,
                      default_status=dg.DefaultSensorStatus.RUNNING,
                      description="Push every job's last success time and duration to the Pushgateway.")
def push_run_success(context: dg.RunStatusSensorContext):
    """A run succeeded: push it."""
    push_run(context, "success")


@dg.run_failure_sensor(name="push_run_failure", minimum_interval_seconds=15,
                       default_status=dg.DefaultSensorStatus.RUNNING,
                       description="Push every job's last failure time to the Pushgateway.")
def push_run_failure(context: dg.RunFailureSensorContext):
    """A run failed: push it."""
    push_run(context, "failure")


class SelftestConfig(dg.Config):
    """Run config for monitoring_selftest."""

    fail: bool = False
    """True makes the run fail on purpose, to exercise the failure sensor and the alert on it."""


@dg.op(name="selftest")
def selftest_op(context: dg.OpExecutionContext, config: SelftestConfig):
    """Pass, or fail on purpose when asked to."""
    if config.fail:
        raise dg.Failure("monitoring_selftest was asked to fail (fail: true). A test of the alerting, not a problem.")
    context.log.info("monitoring_selftest passed")


@dg.job(description="Fails on request (config fail: true) to test the failure sensor and alert; passes otherwise.")
def monitoring_selftest():
    """One op that passes or fails as configured."""
    selftest_op()


daily_lake = dg.define_asset_job("daily_lake", selection=dg.AssetSelection.all(),
                                 description="Load reference, bronze and silver, check parity, build gold, "
                                             "sync OpenMetadata, refresh Fuseki.")
regenerate_lake = dg.define_asset_job(
    "regenerate_lake", selection=dg.AssetSelection.all(),
    description="Same as daily_lake, but rebuild the CSV from the seed first.",
    config={"ops": {"load_lake": {"config": {"regenerate": True}}}})

daily_schedule = dg.ScheduleDefinition(
    job=daily_lake, cron_schedule="0 5 * * *", execution_timezone="America/Chicago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="05:00 Central, done well before the 07:00 work day.")

# ---- Weekly notebook smoke run --------------------------------------------------------------------------------
# The course notebooks as delivered (exercise blanks unfilled, so their checkers print a prompt, not an error),
# plus the lakehouse's own bridge notebook. Executed copies land in /lake/notebook-runs/<date>_<run>/.
NOTEBOOKS = [*sorted((REPO / "notebooks").glob("0*.ipynb")), REPO / "lakehouse" / "08_from_lake.ipynb"]
NOTEBOOK_RUNS = Path(os.environ.get("NOTEBOOK_RUNS", "/lake/notebook-runs"))


def run_notebook(path: Path, out_dir: Path) -> dict:
    """Execute one notebook with papermill into out_dir, from its own folder; return name, outcome and seconds."""
    import papermill
    started, error = time.time(), None
    try:
        papermill.execute_notebook(str(path), str(out_dir / path.name), cwd=str(path.parent), kernel_name="python3",
                                   progress_bar=False)
    except Exception as exc:   # papermill raises on the first failing cell; keep going with the other notebooks
        lines = [line for line in str(exc).splitlines() if line.strip()]
        error = f"{type(exc).__name__}: {lines[-1][:300] if lines else ''}"
    return {"notebook": path.name, "ok": error is None, "seconds": round(time.time() - started, 1), "error": error}


@dg.op(description="Run every course notebook and lakehouse/08_from_lake.ipynb; fail if any cell errors.")
def run_notebooks(context: dg.OpExecutionContext):
    """Run all notebooks into a dated folder, log each outcome, then fail listing any that broke."""
    out_dir = NOTEBOOK_RUNS / f"{datetime.now(timezone.utc):%Y-%m-%d}_{context.run_id[:8]}"
    out_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for path in NOTEBOOKS:
        result = run_notebook(path, out_dir)
        context.log.info(f"{result['notebook']}: {'ok' if result['ok'] else 'FAILED'} in {result['seconds']}s"
                         + (f" ({result['error']})" if result["error"] else ""))
        results.append(result)
    table = "\n".join(f"| {r['notebook']} | {'ok' if r['ok'] else 'FAILED'} | {r['seconds']} |" for r in results)
    metadata = {"folder": str(out_dir), "notebooks": dg.MetadataValue.md(f"| notebook | result | seconds |\n|---|---|---|\n{table}")}
    failed = [r["notebook"] for r in results if not r["ok"]]
    if failed:
        raise dg.Failure(description=f"{len(failed)} notebook(s) failed: {', '.join(failed)}", metadata=metadata)
    yield dg.Output(len(results), metadata=metadata)


@dg.job(description="Run the 7 course notebooks and the lakehouse bridge notebook; any failing cell fails the run.")
def notebook_smoke():
    """One op: every notebook, in order."""
    run_notebooks()


notebook_smoke_schedule = dg.ScheduleDefinition(
    name="notebook_smoke_schedule", job=notebook_smoke, cron_schedule="0 6 * * 0", execution_timezone="America/Chicago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Sundays 06:00 Central, an hour after daily_lake, so the notebooks see a fresh lake.")

defs = dg.Definitions(
    assets=[load_lake, gold, openmetadata_lake, fuseki_glossary, data_quality],
    jobs=[daily_lake, regenerate_lake, notebook_smoke, monitoring_selftest],
    schedules=[daily_schedule, notebook_smoke_schedule],
    sensors=[push_run_success, push_run_failure],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
