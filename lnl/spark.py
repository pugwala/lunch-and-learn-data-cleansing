"""Session 5 helpers: start Spark anywhere, read the raw file, and show results."""
import os

from . import config, env


def get_spark():
    """The notebook's Spark on Databricks; a small local Spark when testing outside Databricks."""
    import warnings
    warnings.filterwarnings("ignore", message=".*PySpark does not yet fully support pandas.*")
    from pyspark.sql import SparkSession
    if env.IS_DATABRICKS:
        try:  # Databricks notebooks (serverless too) already have a session called `spark`
            from IPython import get_ipython
            existing = get_ipython().user_ns.get("spark")
            if existing is not None:
                return existing
        except Exception:
            pass
        return SparkSession.builder.getOrCreate()
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")   # quiets hostname warnings when testing locally
    spark = (SparkSession.builder.master("local[*]").appName("lunch-and-learn")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.driver.memory", "2g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_raw(spark):
    """Read the raw CSV with every column as text, the Spark equivalent of session 2's careful load."""
    path = str(env.paths()["data"])
    options = {"header": "true", "inferSchema": "false", "escape": '"', "multiLine": "false"}
    if path.startswith("/Volumes/"):
        return spark.read.options(**options).csv(path)
    if not env.IS_DATABRICKS:
        return spark.read.options(**options).csv("file://" + path)
    # Databricks without a volume configured: executors can't see files on the driver's disk,
    # so hand Spark the data through pandas instead (same content, different route).
    print("No Unity Catalog volume configured (lnl/config.py); loading through pandas instead.")
    from . import pipeline
    raw = pipeline.load_raw()
    raw = raw.mask(raw == "")  # match Spark's CSV reader, which reads empty fields as null
    return spark.createDataFrame(raw.astype(object).where(raw.notna(), None))


def show(df, n: int = 10):
    """Databricks' interactive table when available, otherwise a regular table."""
    if env.IS_DATABRICKS:
        try:  # Databricks puts `display` in the notebook, not in imported modules, so fetch it from there
            from IPython import get_ipython
            databricks_display = get_ipython().user_ns.get("display")
            if databricks_display is not None:
                return databricks_display(df.limit(n))
        except Exception:
            pass
    from IPython.display import display as ipy_display
    ipy_display(df.limit(n).toPandas())


def ids_to_bigint(df):
    """Session 2's ID rule in Spark: check that every TDCJ and SID number is exactly 8 digits of text,
    then cast both to BIGINT on purpose. Stops (raises) if any non-blank ID fails, instead of letting
    the cast quietly turn it into a null."""
    from pyspark.sql import functions as F
    from .cleaning import ID_COLUMNS
    counts = df.agg(
        F.sum(F.col("tdcj_number").isNull().cast("int")).alias("tdcj_number missing"),
        *[F.sum((F.col(c).isNotNull() & ~F.col(c).rlike(r"^[0-9]{8}$")).cast("int")).alias(f"{c} not 8 digits")
          for c in ID_COLUMNS],
    ).first().asDict()
    failed = {name: int(n) for name, n in counts.items() if n}
    if failed:
        raise ValueError(f"IDs can't be converted yet: {failed}. Fix these with the data owner first.")
    for c in ID_COLUMNS:
        df = df.withColumn(c, F.col(c).cast("bigint"))
    print("TDCJ and SID numbers: every value checked (8 digits), now BIGINT")
    return df


def repair_text(df):
    """Session 2's text repair in Spark: the same replacement table as pandas (domains.TEXT_REPAIRS), applied in
    the same order, each entry matched literally, then runs of whitespace -> one space.
    aggregate() walks the table one entry at a time, like the pandas loop, as a single compact expression."""
    from pyspark.sql import functions as F
    from .domains import TEXT_COLUMNS, TEXT_REPAIRS
    table = F.array(*[F.struct(F.lit(bad).alias("bad"), F.lit(good).alias("good")) for bad, good in TEXT_REPAIRS.items()])
    for c in TEXT_COLUMNS:
        repaired = F.aggregate(table, F.col(c), lambda text, fix: F.replace(text, fix["bad"], fix["good"]))
        df = df.withColumn(c, F.trim(F.regexp_replace(repaired, r"\s+", " ")))
    return df


def domain_key(col):
    """Same key as domains.domain_key: upper case, no periods, straight apostrophes, single spaces, no spaces
    around - / and :."""
    from pyspark.sql import functions as F
    key = F.regexp_replace(F.regexp_replace(F.upper(col), r"\.", ""), "[’‘]", "'")
    key = F.trim(F.regexp_replace(key, r"\s+", " "))
    return F.regexp_replace(key, r"\s*([-/:])\s*", "$1")


def apply_domains(df, reference=None):
    """Session 2's coded domains in Spark: the same reference tables, joined instead of mapped.
    Adds race_code, race_omb, religion_group, religion_family, stg_group, stg_status, offense_code_std,
    offense_against, violence_code_std, unit_name and unit_code. Raw columns stay as written.
    `reference(name)` returns a crosswalk as a Spark DataFrame; by default the CSV files in lnl/reference/ are
    used, and the enterprise track passes a function that reads the governed copies in Unity Catalog."""
    from pyspark.sql import functions as F
    from . import domains
    spark = df.sparkSession
    load = reference or (lambda name: spark.createDataFrame(domains.reference(name)))

    def ref(name, key_col, key_name):
        return F.broadcast(load(name).withColumnRenamed(key_col, key_name))

    out = df.withColumn("_race", domain_key(F.col("race"))).join(
        ref("race_crosswalk", "source_value", "_race"), "_race", "left")
    out = out.withColumn("_religion", domain_key(F.col("religion"))).join(
        ref("religion_crosswalk", "source_value", "_religion"), "_religion", "left")

    key = domain_key(F.col("stg_affiliation"))
    status = None
    for label, pattern in domains.STG_STATUS:
        status = F.when(key.rlike(pattern), label) if status is None else status.when(key.rlike(pattern), label)
    out = (out.withColumn("_stg_marked", status)
              .withColumn("_stg", F.trim(F.regexp_replace(key, domains.STG_ANY_STATUS, "")))
              .join(ref("stg_groups", "alias", "_stg"), "_stg", "left"))
    out = out.withColumn("stg_status", F.when(F.col("stg_group") == "None", "None")
                                        .when(F.col("stg_group").isNotNull(),
                                              F.coalesce(F.col("_stg_marked"), F.lit("Unverified"))))

    code = F.regexp_replace(F.regexp_replace(F.upper(F.col("offense_code")), r"[\s-]", ""), r"\.0$", "")
    code = F.when(code.rlike(r"^\d[A-Z]$"), F.concat(F.lit("0"), code)).otherwise(code)
    nibrs = ref("nibrs_offenses", "code", "offense_code_std").select(
        "offense_code_std", F.col("crime_against").alias("offense_against"))
    out = out.withColumn("offense_code_std", code).join(nibrs, "offense_code_std", "left")
    out = out.withColumn("offense_code_std", F.when(F.col("offense_against").isNotNull(), F.col("offense_code_std")))

    out = out.withColumn("_violence", domain_key(F.col("violence_code"))).join(
        ref("violence_codes", "source_value", "_violence").withColumnRenamed("violence_code", "violence_code_std"),
        "_violence", "left")
    out = out.withColumn("_unit", domain_key(F.col("unit_location"))).join(
        ref("unit_crosswalk", "source_value", "_unit"), "_unit", "left")
    added = [c for cols in domains.DOMAIN_COLUMNS.values() for c in cols]
    return out.select(*df.columns, *added)                 # original columns first, in their original order


def unrecognized_domain_values():
    """Spark expression: per record, how many coded values no reference table recognizes."""
    from pyspark.sql import functions as F
    from .domains import DOMAIN_COLUMNS
    return sum((F.col(raw).isNotNull() & F.col(std[0]).isNull()).cast("int") for raw, std in DOMAIN_COLUMNS.items())


def offense_violence_conflict():
    """Spark expression: True when the offense code and violence code disagree (same rule as pandas)."""
    from pyspark.sql import functions as F
    violent = F.col("violence_code_std").isin("V2", "V3")
    person = F.col("offense_against") == "Person"
    return F.col("violence_code_std").isNotNull() & F.col("offense_against").isNotNull() & (violent != person)


def check_ids(df):
    """Spark has no index, so prove tdcj_number is still a real ID before saving: BIGINT, one per row,
    none missing, all within 8 digits. Returns a small pandas table."""
    import pandas as pd
    from pyspark.sql import functions as F
    from .cleaning import ID_MAX
    tdcj = F.col("tdcj_number")
    row = df.agg(
        F.count("*").alias("records"),
        F.countDistinct("tdcj_number").alias("distinct TDCJ numbers"),
        F.sum(tdcj.isNull().cast("int")).alias("TDCJ number missing"),
        F.sum(((tdcj < 1) | (tdcj > ID_MAX)).cast("int")).alias("TDCJ number outside 8 digits"),
    ).first().asDict()
    is_bigint = dict(df.dtypes).get("tdcj_number") == "bigint"
    ok = (is_bigint and row["records"] == row["distinct TDCJ numbers"] and not row["TDCJ number missing"]
          and not row["TDCJ number outside 8 digits"])
    print("tdcj_number is a valid record ID (BIGINT)" if ok else "tdcj_number is NOT a valid record ID: fix before saving")
    return pd.DataFrame({"count": {k: int(v or 0) for k, v in row.items()}})


def _apply_id_constraints(spark, table: str) -> list:
    """Make the table enforce the IDs: tdcj_number NOT NULL and within 8 digits, SID within 8 digits when
    present (enforced), plus an informational primary key."""
    statements = [
        f"ALTER TABLE {table} ALTER COLUMN tdcj_number SET NOT NULL",
        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS tdcj_number_format",
        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS tdcj_number_range",
        f"ALTER TABLE {table} ADD CONSTRAINT tdcj_number_range CHECK (tdcj_number BETWEEN 1 AND 99999999)",
        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS sid_number_range",
        f"ALTER TABLE {table} ADD CONSTRAINT sid_number_range CHECK (sid_number IS NULL OR sid_number BETWEEN 1 AND 99999999)",
        f"ALTER TABLE {table} DROP PRIMARY KEY IF EXISTS",
        f"ALTER TABLE {table} ADD CONSTRAINT {table.split('.')[-1]}_pk PRIMARY KEY (tdcj_number)",
    ]
    results = []
    for sql in statements:
        try:
            spark.sql(sql)
            results.append("ok   " + sql.split(table, 1)[1].strip())
        except Exception as exc:  # e.g. primary keys need Unity Catalog; report and carry on
            results.append(f"skip {sql.split(table, 1)[1].strip()} ({type(exc).__name__})")
    return results


def save_table(df, name: str = "hero_offenders_clean") -> str:
    """Delta table in Unity Catalog on Databricks (if configured), otherwise local Parquet."""
    if env.IS_DATABRICKS and config.DATABRICKS_CATALOG and config.DATABRICKS_SCHEMA:
        user = df.sparkSession.sql("SELECT current_user()").first()[0].split("@")[0]
        user = "".join(c if c.isalnum() else "_" for c in user).lower()
        table = f"{config.DATABRICKS_CATALOG}.{config.DATABRICKS_SCHEMA}.{name}_{user}"
        spark = df.sparkSession
        if spark.catalog.tableExists(table):
            # A table saved by an earlier version of the course holds the IDs as text, with a text-format CHECK.
            # Drop the old constraints first so the BIGINT write isn't rejected; they're re-added below.
            for sql in [f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS tdcj_number_format",
                        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS tdcj_number_range",
                        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS sid_number_range",
                        f"ALTER TABLE {table} DROP PRIMARY KEY IF EXISTS"]:
                try:
                    spark.sql(sql)
                except Exception:
                    pass
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        applied = _apply_id_constraints(df.sparkSession, table)
        return f"Delta table {table}\n  " + "\n  ".join(applied)
    if env.IS_DATABRICKS:
        return "Skipped: set DATABRICKS_CATALOG and DATABRICKS_SCHEMA in lnl/config.py to save a table."
    path = os.path.join(str(env.data_dir()), f"{name}_spark")
    df.write.mode("overwrite").parquet("file://" + path)
    return f"Parquet folder {path}"
