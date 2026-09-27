"""Session 5 helpers: start Spark anywhere, read the raw file, and show results."""
import os

from . import config, env


def get_spark():
    """The cluster's Spark on Databricks; a small local Spark in Colab or Jupyter."""
    import warnings
    warnings.filterwarnings("ignore", message=".*PySpark does not yet fully support pandas.*")
    from pyspark.sql import SparkSession
    if env.IS_DATABRICKS:
        return SparkSession.builder.getOrCreate()
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")   # quiets hostname warnings in Colab
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
        display(df.limit(n))  # noqa: F821  (display is built into Databricks notebooks)
    else:
        from IPython.display import display as ipy_display
        ipy_display(df.limit(n).toPandas())


def check_ids(df):
    """Spark has no index, so prove tdcj_number is still a real ID before saving: one per row,
    none missing, all 8 digits. Returns a small pandas table."""
    import pandas as pd
    from pyspark.sql import functions as F
    tdcj = F.col("tdcj_number")
    row = df.agg(
        F.count("*").alias("records"),
        F.countDistinct("tdcj_number").alias("distinct TDCJ numbers"),
        F.sum(tdcj.isNull().cast("int")).alias("TDCJ number missing"),
        F.sum((~tdcj.rlike(r"^[0-9]{8}$")).cast("int")).alias("TDCJ number not 8 digits"),
    ).first().asDict()
    ok = (row["records"] == row["distinct TDCJ numbers"] and not row["TDCJ number missing"]
          and not row["TDCJ number not 8 digits"])
    print("tdcj_number is a valid record ID" if ok else "tdcj_number is NOT a valid record ID: fix before saving")
    return pd.DataFrame({"count": {k: int(v or 0) for k, v in row.items()}})


def _apply_id_constraints(spark, table: str) -> list:
    """Make the table enforce the ID: NOT NULL and 8 digits (enforced), plus an informational primary key."""
    statements = [
        f"ALTER TABLE {table} ALTER COLUMN tdcj_number SET NOT NULL",
        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS tdcj_number_format",
        f"ALTER TABLE {table} ADD CONSTRAINT tdcj_number_format CHECK (tdcj_number RLIKE '^[0-9]{{8}}$')",
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
        df.write.mode("overwrite").saveAsTable(table)
        applied = _apply_id_constraints(df.sparkSession, table)
        return f"Delta table {table}\n  " + "\n  ".join(applied)
    if env.IS_DATABRICKS:
        return "Skipped: set DATABRICKS_CATALOG and DATABRICKS_SCHEMA in lnl/config.py to save a table."
    path = os.path.join(str(env.data_dir()), f"{name}_spark")
    df.write.mode("overwrite").parquet("file://" + path)
    return f"Parquet folder {path}"
