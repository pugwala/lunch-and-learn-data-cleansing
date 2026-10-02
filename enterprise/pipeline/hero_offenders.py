"""Lakeflow pipeline: the course's sessions 2-4 as bronze -> silver -> gold, with expectations.

Created and run by enterprise/02_pipeline (or deployed from enterprise/databricks.yml). Settings arrive as pipeline
configuration, so this file never hard-codes a catalog, schema or path:
  lnl.course_root        the course's Git folder, so the cleansing rules in lnl/ can be imported
  lnl.landing_path       the volume folder the source files land in
  lnl.reference_schema   catalog.schema holding the governed crosswalk tables (ref_*)

Every rule here is the same rule taught in sessions 2-4 (see lnl/spark_pipeline.py). What the pipeline adds is
how each rule is enforced, with expectations:
  warn (@dp.expect_all)            keep the record, count the failures in the event log
  drop (@dp.expect_all_or_drop)    keep the record out of silver; the quarantine table holds it for the data owner
  fail (@dp.expect_or_fail)        stop the update: a delivery with a column the agreed layout doesn't have can't be
                                   trusted. (A missing column arrives as blanks; the warn rules catch its effect.)
"""
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

sys.path.insert(0, spark.conf.get("lnl.course_root"))  # noqa: F821 (spark is provided by the pipeline)
from lnl import domains, spark_pipeline as sp  # noqa: E402

LANDING = spark.conf.get("lnl.landing_path")  # noqa: F821
REFERENCE_SCHEMA = spark.conf.get("lnl.reference_schema")  # noqa: F821


def reference(name):
    """The governed copy of a crosswalk, loaded by 01_unity_catalog from lnl/reference/."""
    return spark.read.table(f"{REFERENCE_SCHEMA}.ref_{name}")  # noqa: F821


# ---- Bronze: as received ----------------------------------------------------------------------------
@dp.table(
    name="bronze_offenders",
    comment="Mock source deliveries exactly as received: every column text, nothing fixed, nothing dropped.",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_fail("delivery matches the agreed layout", "_rescued_data IS NULL")
def bronze_offenders():
    return (spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("escape", '"')
            .option("cloudFiles.inferColumnTypes", "false")      # every column arrives as text (session 2's rule)
            .option("cloudFiles.schemaEvolutionMode", "rescue")  # an unexpected column goes to _rescued_data,
                                                                 # never quietly into the table
            .load(LANDING)
            .select("*", F.col("_metadata.file_name").alias("_source_file"),
                    F.current_timestamp().alias("_ingested_at")))


# ---- Silver: cleansed and typed ---------------------------------------------------------------------
@dp.table(
    name="silver_offenders",
    comment="Sessions 2-4 applied: one row per TDCJ number, real types, coded values mapped through the crosswalks.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(sp.DROP_EXPECTATIONS)
@dp.expect_all(sp.WARN_EXPECTATIONS)
def silver_offenders():
    return sp.silver(spark.readStream.table("bronze_offenders"), reference=reference)  # noqa: F821


@dp.table(
    name="silver_quarantine",
    comment="Records kept out of silver because an ID isn't a valid 8-digit number, as received, for the data owner.",
    table_properties={"quality": "silver"},
)
def silver_quarantine():
    flagged = sp.flag_id_problems(sp.strip_and_null(spark.readStream.table("bronze_offenders")))  # noqa: F821
    return flagged.where("_id_problem IS NOT NULL")


# ---- Gold: lists for the data owner, and numbers for reports ---------------------------------------
@dp.materialized_view(name="gold_domain_exceptions",
                      comment="Coded values no crosswalk recognizes, one row per value, for the data owner.")
def gold_domain_exceptions():
    return sp.domain_exceptions(spark.read.table("silver_offenders"))  # noqa: F821


@dp.materialized_view(name="gold_conversion_exceptions",
                      comment="Values that didn't convert to their type in session 4, as written, for the data owner.")
def gold_conversion_exceptions():
    return sp.conversion_exceptions(spark.read.table("silver_offenders"))  # noqa: F821


@dp.materialized_view(name="gold_offense_violence_conflicts",
                      comment="Records whose offense code and violence code disagree. Flagged, not fixed.")
def gold_offense_violence_conflicts():
    return sp.offense_violence_conflicts(spark.read.table("silver_offenders"))  # noqa: F821


@dp.materialized_view(name="gold_race_conflicts",
                      comment="People (SIDs) with more than one race code on file. Flagged, not fixed.")
def gold_race_conflicts():
    return sp.race_conflicts(spark.read.table("silver_offenders"))  # noqa: F821


@dp.materialized_view(name="gold_people_by_unit",
                      comment="Records and people per unit: rows are not people, so both are counted.")
@dp.expect_or_fail("people never outnumber records", "people <= records")
def gold_people_by_unit():
    s = spark.read.table("silver_offenders")  # noqa: F821
    return (s.groupBy("unit_code", "unit_name")
             .agg(F.count("*").alias("records"),
                  F.countDistinct("sid_number").alias("people"),
                  F.sum((F.col("stg_status") == "Confirmed").cast("int")).alias("confirmed_stg_records"),
                  F.sum(F.col("violence_code_std").isin("V2", "V3").cast("int")).alias("violent_records")))


@dp.materialized_view(name="gold_quality_summary",
                      comment="One row per data-quality check: how many records fail it. Feeds monitoring.")
def gold_quality_summary():
    s = spark.read.table("silver_offenders")  # noqa: F821
    checks = {**sp.WARN_EXPECTATIONS}
    total = F.count("*").alias("records")
    failing = [F.sum((~F.expr(rule)).cast("int")).alias(name) for name, rule in checks.items()]
    wide = s.agg(total, *failing)
    stack = ", ".join(f"'{name}', `{name}`" for name in checks)
    return (wide.selectExpr("records", f"stack({len(checks)}, {stack}) AS (check_name, failing_records)")
                .withColumn("failing_share", F.round(F.col("failing_records") / F.col("records"), 5))
                .withColumn("computed_at", F.current_timestamp()))


# Coded columns the gold layer lists, for anyone reading this file: {raw column: standardized columns}
CODED_COLUMNS = domains.DOMAIN_COLUMNS
