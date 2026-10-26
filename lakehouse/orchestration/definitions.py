"""Dagster definitions for the home-lab lakehouse.

    load_lake (Python)  -> ref.*, bronze.*, silver.*, ops.load_runs, then the parity check
    gold (dbt build)    -> gold.dim_*, gold.fct_offender_record, gold.dq_scorecard, plus dbt tests

The parity check is blocking: if the lake stops matching the course's numbers, gold is not rebuilt. That's the
home-lab version of a Lakeflow expectation set to stop the pipeline. dbt tests carry the same idea down a level:
error-severity tests fail the run, warn-severity tests (reference lookups, the 95% quality floor) only record.

Runs one at a time (dagster.yaml sets max_concurrent_runs: 1) because DuckLake writers shouldn't race.
"""
import os
import subprocess
import sys
from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from lakehouse import lake  # noqa: E402

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

    result = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                             str(REPO / "lakehouse" / "tests")], capture_output=True, text=True, cwd=REPO)
    summary = (result.stdout.strip().splitlines() or ["no output"])[-1]
    yield dg.AssetCheckResult(asset_key=SILVER_KEY, check_name=PARITY_CHECK, passed=result.returncode == 0,
                              metadata={"summary": summary,
                                        "output": dg.MetadataValue.md(f"```\n{result.stdout[-3000:]}\n```")})


class GoldTranslator(DagsterDbtTranslator):
    """dbt models become gold/<model>; sources keep [schema, table] so they match load_lake."""

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "model":
            return dg.AssetKey(["gold", dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        return "gold" if dbt_resource_props["resource_type"] == "model" else None


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project, dagster_dbt_translator=GoldTranslator())
def gold(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


daily_lake = dg.define_asset_job("daily_lake", selection=dg.AssetSelection.all(),
                                 description="Load reference, bronze and silver, check parity, build gold.")
regenerate_lake = dg.define_asset_job(
    "regenerate_lake", selection=dg.AssetSelection.all(),
    description="Same as daily_lake, but rebuild the CSV from the seed first.",
    config={"ops": {"load_lake": {"config": {"regenerate": True}}}})

daily_schedule = dg.ScheduleDefinition(
    job=daily_lake, cron_schedule="0 5 * * *", execution_timezone="America/Chicago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="05:00 Central, done well before the 07:00 work day.")

defs = dg.Definitions(
    assets=[load_lake, gold],
    jobs=[daily_lake, regenerate_lake],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
