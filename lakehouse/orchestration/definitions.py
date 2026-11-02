"""Dagster definitions for the home-lab lakehouse.

    load_lake (Python)  -> ref.*, bronze.*, silver.*, ops.load_runs from course 1's pinned handoff, then the parity
                           check
    readiness_inputs    -> ops.business_inputs, ops.product_inputs, ops.input_closes (readiness/closes.py): the
                           declared inputs, the fictional units' closes, and each input's days late, pushed
    gold (dbt build)    -> gold.dim_*, gold.fct_offender_record, gold.dq_scorecard, the marts (custody, and the
                           readiness view), plus dbt tests
    openmetadata/lake   -> om_sync.py after gold, so OpenMetadata knows today's tables; a non-blocking check that
                           Cube's masks still match OpenMetadata's Restricted tags
    fuseki/glossary     -> om_to_rdf.py: OpenMetadata's glossary and column tags as RDF in Fuseki, with the rulebook
                           and deduced facts; check business_rules_hold (ontology/shapes.ttl), pushed to Prometheus
    monitoring/data_quality -> gold.dq_scorecard's pass rates, pushed to the Pushgateway for Prometheus
    monitoring/data_products -> each data product's freshness, coverage, readiness and headline numbers
    push_run_success/failure -> sensors that push every job's last success, duration and last failure
    monitoring_selftest -> a job that fails on request, to exercise the failure path and its alert
    lake_housekeeping   -> weekly (Sundays 03:30 Central): housekeeping.py keeps 30 days of DuckLake history and
                           deletes the rest, and pushes the lake's snapshots and size
    notebook_smoke      -> runs the 7 course notebooks and lakehouse/08_from_lake.ipynb weekly (an op job, not
                           an asset, so daily_lake's all-asset selection never picks it up); course 1's add-on
                           (lakehouse/course1_addon.py), so only while LAKEHOUSE_COURSE1 is on
    landing/world       -> the expanded world (lakehouse/universe): clean Parquet, messy CSVs, answer keys
    world_full (dbt)    -> world_bronze.* (the messy CSVs as written), world_silver's dimensions and incidents,
                           world_gold's dimensions, fct_incident and fct_mission: job world_refresh, on demand
    world_monthly (dbt) -> the big facts (participants, power uses, damages, charges, court outcomes, mission
                           fights) in silver and gold, one month per partition from 1996-01; the answer-key tests
                           run here. A backfill is one dbt run (single_run); world_monthly_schedule builds each
                           month once it closes. daily_lake leaves the world out

The parity check is blocking: if the lake stops matching the course's numbers, gold is not rebuilt. It is part of
course 1's add-on, so with LAKEHOUSE_COURSE1 off it isn't declared (the load still refuses a handoff that isn't the
pinned one). That's the
home-lab version of a Lakeflow expectation set to stop the pipeline. dbt tests carry the same idea down a level:
error-severity tests fail the run, warn-severity tests (reference lookups, the 95% quality floor) only record.

Runs one at a time (dagster.yaml sets max_concurrent_runs: 1) because DuckLake writers shouldn't race.
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import yaml
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from lakehouse import course1_addon, lake  # noqa: E402
from lakehouse.metrics import push  # noqa: E402

DBT_DIR = REPO / "lakehouse" / "dbt"
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR, target=os.environ.get("DBT_TARGET", "lake"))


COURSE1 = course1_addon.enabled()   # course 1's add-on (lakehouse/course1_addon.py): LAKEHOUSE_COURSE1 on or off


def _manifest_is_stale() -> bool:
    """True when any dbt source file is newer than the manifest, when the manifest was parsed with course 1's add-on
    the other way (flipping LAKEHOUSE_COURSE1 changes no file), or when there is no manifest yet."""
    manifest = dbt_project.manifest_path
    if not manifest.exists():
        return True
    sources = [DBT_DIR / "dbt_project.yml", DBT_DIR / "profiles.yml",
               *(f for folder in ("models", "macros", "tests") for f in (DBT_DIR / folder).rglob("*") if f.is_file())]
    if max(f.stat().st_mtime for f in sources) > manifest.stat().st_mtime:
        return True
    return ("model.lakehouse_gold.trust_test" in json.loads(manifest.read_text())["nodes"]) != COURSE1


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
# Course 1's add-on: the blocking parity check (the 19 Facilitator Guide numbers), declared only while it's on.
PARITY_SPECS = [dg.AssetCheckSpec(PARITY_CHECK, asset=SILVER_KEY, blocking=True,
                                  description="All 19 lakehouse/tests checks match FACILITATOR_GUIDE.md")] if COURSE1 else []


@dg.multi_asset(
    name="load_lake",
    specs=[dg.AssetSpec(dg.AssetKey([schema, table]), group_name=schema, kinds={"duckdb", "python"},
                        description=lake.TABLE_COMMENTS.get(f"{schema}.{table}"))
           for schema, tables in LOADED_TABLES.items() for table in tables],
    check_specs=PARITY_SPECS,
)
def load_lake(context: dg.AssetExecutionContext):
    """Land course 1's pinned handoff as ref, bronze and silver (lake.run), report each table, push the load's
    numbers, then run the blocking parity check: the 19 lakehouse/tests checks against FACILITATOR_GUIDE.md."""
    metrics = lake.run()
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
         metrics["finished_at"].timestamp(), {}),
        ("lakehouse_course1_addon_enabled", "1 while course 1's add-on (LAKEHOUSE_COURSE1) is on, else 0.",
         int(COURSE1), {})])
    if error:
        context.log.warning(error)

    if not COURSE1:
        return
    result = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                             str(REPO / "lakehouse" / "tests")], capture_output=True, text=True, cwd=REPO)
    summary = (result.stdout.strip().splitlines() or ["no output"])[-1]
    yield dg.AssetCheckResult(asset_key=SILVER_KEY, check_name=PARITY_CHECK, passed=result.returncode == 0,
                              metadata={"summary": summary,
                                        "output": dg.MetadataValue.md(f"```\n{result.stdout[-3000:]}\n```")})


class GoldTranslator(DagsterDbtTranslator):
    """dbt models become <schema>/<model> (gold/..., mart_custody/...); sources keep [schema, table] so they match
    load_lake."""

    def get_asset_key(self, dbt_resource_props):
        """<schema>/<model> for a dbt model; dagster-dbt's default ([schema, table]) for a source."""
        if dbt_resource_props["resource_type"] == "model":
            return dg.AssetKey([dbt_resource_props["config"]["schema"], dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        """Models go in their schema's group (gold, mart_custody); sources keep load_lake's group."""
        return dbt_resource_props["config"]["schema"] if dbt_resource_props["resource_type"] == "model" else None


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project, dagster_dbt_translator=GoldTranslator(),
            exclude="tag:world")
def gold(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """dbt build: every gold model and its tests (not the expanded world's), streamed to Dagster as
    materializations and checks."""
    yield from dbt.cli(["build"], context=context).stream()


# ---- The expanded world (dataset expansion, chunk 7) --------------------------------------------------------
WORLD_SCALE = os.environ.get("WORLD_SCALE", "M")
WORLD_ROOT = Path(os.environ.get("WORLD_ROOT", f"/lake/landing/universe/{WORLD_SCALE}"))
WORLD_VARS = {"world_root": str(WORLD_ROOT)}
world_months = dg.MonthlyPartitionsDefinition(start_date="1996-01-01", timezone="America/Chicago")


@dg.asset(key=["landing", "world"], group_name="world", kinds={"python", "parquet"},
          description="The expanded world (lakehouse/universe): clean Parquet, messy CSVs and answer keys.")
def world_landing(context: dg.AssetExecutionContext):
    """Build the expanded world at WORLD_SCALE into WORLD_ROOT (the same seed gives the same bytes), then verify
    its manifest."""
    result = subprocess.run([sys.executable, "-m", "lakehouse.universe.build", "--scale", WORLD_SCALE,
                             "--out", str(WORLD_ROOT)], capture_output=True, text=True, cwd=REPO)
    context.log.info(result.stdout[-3000:])
    if result.returncode != 0:
        raise dg.Failure(description="The world build failed", metadata={"output": (result.stdout + result.stderr)[-3000:]})
    from lakehouse.universe import manifest
    built = manifest.read(WORLD_ROOT)
    return dg.MaterializeResult(metadata={
        "folder": str(WORLD_ROOT), "scale": WORLD_SCALE,
        "rows": sum(t["rows"] for t in built["tables"].values()),
        "defects": sum(e["rows"] for k, e in built["mess"].items() if k.startswith("answer_key/"))})


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project, dagster_dbt_translator=GoldTranslator(),
            select="tag:world", exclude="tag:world_monthly+", name="world_full")
def world_full(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """dbt build of the world's full-refresh models: bronze, silver's dimensions and incidents, gold's dimensions,
    fct_incident and fct_mission. Anything downstream of a monthly model (its tests, the answer-key tests) is left
    to world_monthly, which runs once these exist."""
    yield from dbt.cli(["build", "--vars", json.dumps(WORLD_VARS)], context=context).stream()


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project, dagster_dbt_translator=GoldTranslator(),
            select="tag:world_monthly", name="world_monthly", partitions_def=world_months,
            backfill_policy=dg.BackfillPolicy.single_run())
def world_monthly(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """dbt build of the world's big facts for the partition's months (vars world_from, world_to: a backfill is one
    run over its whole range), with the answer-key tests."""
    window = context.partition_time_window
    month_vars = {**WORLD_VARS, "world_from": f"{window.start:%Y-%m-%d}", "world_to": f"{window.end:%Y-%m-%d}"}
    yield from dbt.cli(["build", "--vars", json.dumps(month_vars)], context=context).stream()


WORLD_ASSETS = dg.AssetSelection.assets(world_landing, world_full, world_monthly)
world_refresh = dg.define_asset_job(
    "world_refresh", selection=dg.AssetSelection.assets(world_landing, world_full),
    description="Build the expanded world, then its bronze, dimensions and incidents. Then backfill world_monthly.")
world_monthly_job = dg.define_asset_job(
    "world_monthly_job", selection=dg.AssetSelection.assets(world_monthly), partitions_def=world_months,
    description="The world's big facts for one month (or a backfill's range, in one run), and the answer-key tests.")
world_refresh_schedule = dg.ScheduleDefinition(
    name="world_refresh_schedule", job=world_refresh, cron_schedule="30 4 1 * *", execution_timezone="America/Chicago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="04:30 Central on the 1st: the world, its bronze, dimensions and incidents, before world_monthly_job "
                "(05:30) builds the month that just closed. The world's data products promise 32 days.")
world_monthly_schedule = dg.build_schedule_from_partitioned_job(
    world_monthly_job, hour_of_day=5, minute_of_hour=30, default_status=dg.DefaultScheduleStatus.RUNNING,
    description="05:30 Central on the 1st: the month that just closed.")


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


RULES_CHECK = "business_rules_hold"


@dg.asset(key=FUSEKI_KEY, group_name="openmetadata", kinds={"sparql", "python"}, deps=[OM_KEY],
          description="OpenMetadata's glossary (SKOS) and column sensitivity in Fuseki, with the rulebook and the "
                      "deduced facts beside it (om_to_rdf.py).",
          check_specs=[dg.AssetCheckSpec(RULES_CHECK, asset=FUSEKI_KEY, blocking=False,
                                         description="Every business rule in ontology/shapes.ttl holds")])
def fuseki_glossary(context: dg.AssetExecutionContext):
    """Replace the Fuseki graphs with what OpenMetadata and the rules say after today's sync, then check the business
    rules and push the violations per shape to Prometheus (BusinessRuleViolated alerts on any)."""
    from lakehouse.ontology import check_rules
    rdf = run_script("lakehouse/openmetadata/om_to_rdf.py")
    if rdf.returncode != 0:
        raise dg.Failure(f"om_to_rdf.py failed: {(rdf.stdout + rdf.stderr)[-1500:]}")
    summary = rdf.stdout.strip().splitlines()[-1]
    context.log.info(summary)
    yield dg.MaterializeResult(asset_key=FUSEKI_KEY, metadata={"summary": summary})
    found = check_rules.violations()
    for v in found:
        context.log.warning(f"business rule broken: {v['shape']}: {v['focus']}: {v['message']}")
    error = push("lakehouse_business_rules", [
        ("lakehouse_business_rule_violations", "Things breaking each business rule (SHACL shape) right now.",
         sum(v["shape"] == name for v in found), {"shape": name}) for name in check_rules.shapes()])
    if error:
        context.log.warning(error)
    yield dg.AssetCheckResult(asset_key=FUSEKI_KEY, check_name=RULES_CHECK, passed=not found,
                              severity=dg.AssetCheckSeverity.WARN,
                              metadata={"violations": len(found), "shapes": len(check_rules.shapes())})


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


PRODUCTS_DIR = REPO / "lakehouse" / "products"
READINESS_TABLES = {
    "business_inputs": "Business inputs data products need (lakehouse/products/inputs/), synced by closes.py.",
    "product_inputs": "Which inputs each data product depends on (depends_on:), synced by closes.py.",
    "input_closes": "Every close a business unit reported: complete through a month-end. Append-only.",
}


@dg.multi_asset(
    name="readiness_inputs",
    specs=[dg.AssetSpec(dg.AssetKey(["ops", table]), group_name="readiness", kinds={"duckdb", "python"},
                        description=text, deps=list(load_lake.keys))   # after the load: never two lake writers
           for table, text in READINESS_TABLES.items()],
)
def readiness_inputs(context: dg.AssetExecutionContext):
    """Sync the declared inputs and dependencies (products/inputs/, depends_on:), play the fictional business
    units' scheduled closes, and push each input's days late (BusinessInputLate alerts on any)."""
    from lakehouse.readiness import closes
    drift = closes.sync()
    for line in closes.simulate(closes.today_central()):
        context.log.info(line)
    status = closes.status(closes.today_central())
    error = push("lakehouse_readiness", [
        ("lakehouse_input_days_late", "Days past its deadline that a business input hasn't closed a month (0 = on time).",
         s["days_late"], {"input": s["input"], "unit": s["unit"]}) for s in status] + [
        ("lakehouse_input_complete_through_timestamp_seconds", "The month-end a business input is complete through.",
         time.mktime(s["complete_through"].timetuple()) if s["complete_through"] else 0,
         {"input": s["input"], "unit": s["unit"]}) for s in status])
    if error:
        context.log.warning(error)
    for table in READINESS_TABLES:
        yield dg.MaterializeResult(asset_key=dg.AssetKey(["ops", table]),
                                   metadata={"synced": ", ".join(drift) or "unchanged",
                                             **{f"days_late_{s['input']}": s["days_late"] for s in status}})


def table_asset_keys() -> dict:
    """{"schema.table": asset key} for every dbt model, from dbt's manifest: the table a model builds (its schema,
    its alias: wg_fct_incident builds world_gold.fct_incident) and its asset key (<schema>/<model>, as
    GoldTranslator names it)."""
    nodes = json.loads(Path(dbt_project.manifest_path).read_text())["nodes"].values()
    return {f"{n['config']['schema']}.{n['alias']}": dg.AssetKey([n["config"]["schema"], n["name"]])
            for n in nodes if n["resource_type"] == "model"}


def refreshed_at(instance: dg.DagsterInstance, tables: list, keys: dict) -> float:
    """When a product was last refreshed, in epoch seconds: the oldest of its tables' latest materializations
    (any partition), since a product is only as fresh as its stalest table; 0 when a table was never built."""
    events = [instance.get_latest_materialization_event(keys[t]) if t in keys else None for t in tables]
    return min((e.timestamp if e else 0.0) for e in events) if events else 0.0


@dg.asset(key=dg.AssetKey(["monitoring", "data_products"]), group_name="monitoring", kinds={"prometheus", "python"},
          deps=[k for k in gold.keys if k.path[0] in ("mart_custody", "mart_readiness")],
          description="Each data product's freshness, coverage, readiness and headline numbers, pushed for Prometheus.")
def data_products(context: dg.AssetExecutionContext):
    """Push, per product in lakehouse/products/: when it was refreshed (the oldest of its tables' latest Dagster
    materializations: today's for the custody marts, the 1st of the month for the world's), its max age from the
    descriptor, the snapshot's coverage
    when the product delivers it, the latest month-end headline when it is served by custody_population, and
    whether its latest month-end is final (every business input it depends on has closed it)."""
    con = lake.lake_ro_connect()
    total, undated = con.sql("SELECT records_total, records_without_dates FROM mart_custody.snapshot_coverage").fetchone()
    records, people = con.sql("SELECT count(*), count(DISTINCT sid_number) FROM mart_custody.custody_snapshot_monthly "
                              "WHERE month_end = (SELECT max(month_end) FROM mart_custody.custody_snapshot_monthly)").fetchone()
    final = dict(con.sql("SELECT product, is_final FROM mart_readiness.period_readiness "
                         "WHERE month_end = (SELECT max(month_end) FROM mart_readiness.period_readiness)").fetchall())
    con.close()
    keys, pushed = table_asset_keys(), []
    for f in sorted(PRODUCTS_DIR.glob("*.yml")):
        p = yaml.safe_load(f.read_text())
        samples = [("lakehouse_product_refreshed_timestamp_seconds", "When the data product was last refreshed.",
                    refreshed_at(context.instance, p["tables"], keys), {}),
                   ("lakehouse_product_max_age_seconds", "The oldest the product promises to be (its descriptor).",
                    p["max_age_hours"] * 3600, {})]
        if "mart_custody.snapshot_coverage" in p["tables"]:
            samples += [("lakehouse_product_records_total", "Records the product's snapshot draws on.", total, {}),
                        ("lakehouse_product_records_without_dates", "Records it can't place in time.", undated, {})]
        if "custody_population" in p["cube_views"]:
            samples += [("lakehouse_product_people_in_custody", "People held at the latest month-end.", people, {}),
                        ("lakehouse_product_records_in_custody", "Records open at the latest month-end.", records, {})]
        if p["name"] in final:
            samples.append(("lakehouse_product_latest_period_final",
                            "1 when the product's latest month-end is final (every input closed it), else 0.",
                            float(final[p["name"]]), {}))
        error = push("lakehouse_data_products", samples, {"product": p["name"]})
        if error:
            context.log.warning(error)
        pushed.append(p["name"])
    return dg.MaterializeResult(metadata={"products": ", ".join(pushed), "people_in_custody": people})


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


daily_lake = dg.define_asset_job("daily_lake", selection=dg.AssetSelection.all() - WORLD_ASSETS,
                                 description="Load reference, bronze and silver, check parity, build gold, "
                                             "sync OpenMetadata, refresh Fuseki.")

daily_schedule = dg.ScheduleDefinition(
    job=daily_lake, cron_schedule="0 5 * * *", execution_timezone="America/Chicago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="05:00 Central, done well before the 07:00 work day.")

# ---- Weekly lake housekeeping --------------------------------------------------------------------------------
@dg.op(description="Expire DuckLake snapshots older than 30 days, merge small files, delete files nothing needs.")
def housekeep_lake(context: dg.OpExecutionContext):
    """Run housekeeping.py on the lake, log what it removed, and push the lake's snapshots and size."""
    from lakehouse import housekeeping
    result = housekeeping.run()
    context.log.info(housekeeping.summary(result))
    after = result["after"]
    error = push("lakehouse_housekeeping", [
        ("lakehouse_lake_snapshots", "DuckLake snapshots kept (time travel), after housekeeping.", after["snapshots"], {}),
        ("lakehouse_lake_oldest_snapshot_timestamp_seconds", "When the oldest kept snapshot was taken.",
         after["oldest_snapshot"].timestamp(), {}),
        ("lakehouse_lake_live_bytes", "Bytes of data files the current snapshot uses.", after["live_bytes"], {}),
        ("lakehouse_lake_total_bytes", "Bytes of all data files, history included.", after["total_bytes"], {}),
        ("lakehouse_housekeeping_removed", "What the last housekeeping run removed, by kind.",
         result["snapshots_expired"], {"kind": "snapshots"}),
        ("lakehouse_housekeeping_removed", "What the last housekeeping run removed, by kind.",
         result["files_deleted"] + result["orphans_deleted"], {"kind": "files"})])
    if error:
        context.log.warning(error)
    return dg.Output(after["snapshots"], metadata={"summary": housekeeping.summary(result),
                                                   "snapshots_expired": result["snapshots_expired"],
                                                   "files_deleted": result["files_deleted"],
                                                   "orphans_deleted": result["orphans_deleted"]})


@dg.job(description="Keep 30 days of DuckLake history: expire older snapshots, merge small files, delete unneeded files.")
def lake_housekeeping():
    """One op: housekeeping.py on the lake."""
    housekeep_lake()


lake_housekeeping_schedule = dg.ScheduleDefinition(
    name="lake_housekeeping_schedule", job=lake_housekeeping, cron_schedule="30 3 * * 0",
    execution_timezone="America/Chicago", default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Sundays 03:30 Central, done before the 05:00 daily load (runs never overlap: one at a time).")


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

# Course 1's add-on (lakehouse/course1_addon.py): the weekly notebook run, only while it's on.
COURSE1_JOBS, COURSE1_SCHEDULES = ([notebook_smoke], [notebook_smoke_schedule]) if COURSE1 else ([], [])

defs = dg.Definitions(
    assets=[load_lake, readiness_inputs, gold, openmetadata_lake, fuseki_glossary, data_quality, data_products,
            world_landing, world_full, world_monthly],
    jobs=[daily_lake, *COURSE1_JOBS, monitoring_selftest, lake_housekeeping, world_refresh, world_monthly_job],
    schedules=[daily_schedule, *COURSE1_SCHEDULES, lake_housekeeping_schedule, world_refresh_schedule,
               world_monthly_schedule],
    sensors=[push_run_success, push_run_failure],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
