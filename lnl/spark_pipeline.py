"""Sessions 2-4 as reusable Spark functions.

Session 5 builds this pipeline one step at a time, in the notebook, so every rule is visible. This module packages
the same rules so other code can reuse them: the enterprise track's Lakeflow pipeline calls `silver()`, and the
exception functions build its gold tables. Every function works on batch and streaming DataFrames.

The one deliberate difference from session 5: session 5 stops (raises) on a bad ID before anything else runs. A
pipeline can't stop half-way through a stream that way, so `silver()` marks the problem in `_id_problem`; the
pipeline's drop expectation keeps that record out of silver, and its quarantine table holds it for the data owner.
Same rule, enforced where the pipeline can enforce it.
"""
from . import cleaning, conversions, domains
from . import spark as lspark

# ---- Session 3: dates -----------------------------------------------------------------------------
DATE_PATTERNS = [   # (shape, Spark pattern): the same nine formats as session 3
    (r"^\d{2}/\d{2}/\d{4}$", "MM/dd/yyyy"), (r"^\d{1,2}/\d{1,2}/\d{2}$", "M/d/yy"),
    (r"^\d{2}-\d{2}-\d{4}$", "MM-dd-yyyy"), (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
    (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", "yyyy-MM-dd HH:mm:ss"),
    (r"^\d{8}$", "yyyyMMdd"), (r"^\d{2}-[A-Za-z]{3}-\d{4}$", "dd-MMM-yyyy"),
    (r"^[A-Za-z]+ \d{1,2}, \d{4}$", "MMMM d, yyyy"), (r"^[A-Za-z]{3} \d{1,2} \d{4}$", "MMM d yyyy"),
]
TWO_DIGIT = r"^\d{1,2}/\d{1,2}/\d{2}$"
DATE_COLUMNS = ["date_of_birth", "sentence_date", "projected_release_date"]

# ---- Session 4: time of day -----------------------------------------------------------------------
TIME_PATTERNS = [(r"^\d{2}:\d{2}$", "HH:mm"), (r"^\d{2}:\d{2}:\d{2}$", "HH:mm:ss"),
                 (r"^\d{1,2}:\d{2} ?[AaPp][Mm]$", "h:mma"), (r"^\d{4}$", "HHmm"), (r"^\d{2}\.\d{2}$", "HH.mm")]
CONVERTED_COLUMNS = ["disciplinary_points", "restitution_owed", "intake_time", *conversions.FLAG_COLUMNS]


def _course_columns(df):
    """The data's own columns. Columns starting with _ are added by the platform (file name, load time)."""
    return [c for c in df.columns if not c.startswith("_")]


def strip_and_null(raw):
    """Session 1's fix: trim spaces and tabs from both ends of every value, then empty text becomes null."""
    from pyspark.sql import functions as F
    out = raw
    for col in _course_columns(raw):
        stripped = F.regexp_replace(F.col(col), r"^\s+|\s+$", "")
        out = out.withColumn(col, F.when(stripped == "", None).otherwise(stripped))
    return out


def flag_id_problems(df):
    """Adds `_id_problem`: null when both IDs are fine, otherwise what's wrong. Run after strip_and_null."""
    from pyspark.sql import functions as F

    def bad(c):
        return F.col(c).isNotNull() & ~F.col(c).rlike(r"^[0-9]{8}$")
    return df.withColumn("_id_problem", F.when(F.col("tdcj_number").isNull(), "tdcj_number missing")
                                         .when(bad("tdcj_number"), "tdcj_number not 8 digits")
                                         .when(bad("sid_number"), "sid_number not 8 digits"))


def clean_session2(raw, reference=None, strict_ids: bool = True):
    """Whitespace, IDs, the data dictionary, text repair, standardized names and the coded domains.
    strict_ids=True stops on a bad ID (session 5). strict_ids=False writes the problem to `_id_problem`
    instead and leaves the ID null, for a pipeline expectation to act on."""
    from pyspark.sql import functions as F
    s2 = strip_and_null(raw)
    if strict_ids:
        s2 = lspark.ids_to_bigint(s2)
    else:
        s2 = flag_id_problems(s2)
        for c in cleaning.ID_COLUMNS:
            s2 = s2.withColumn(c, F.when(F.col(c).rlike(r"^[0-9]{8}$"), F.col(c).cast("bigint")))
    for col, codes in cleaning.NULL_TOKENS.items():
        s2 = s2.withColumn(col, F.when(F.upper(col).isin([c.upper() for c in codes]), None).otherwise(F.col(col)))
    s2 = lspark.repair_text(s2)
    for col in ["last_name", "first_name", "alias"]:
        folded = F.translate(F.upper(col), "ÁÉÍÓÚÑÜ", "AEIOUNU")
        s2 = s2.withColumn(f"{col}_std", F.regexp_replace(folded, r"\s+", " "))
    return lspark.apply_domains(s2, reference)


def parse_date(col: str):
    from pyspark.sql import functions as F
    value = F.col(col)
    result = None
    for shape, pattern in DATE_PATTERNS:
        parsed = F.to_date(F.try_to_timestamp(value, F.lit(pattern)))
        result = F.when(value.rlike(shape), parsed) if result is None else result.when(value.rlike(shape), parsed)
    return result


def fix_century_past(raw_col: str, parsed):
    """Birth and sentence dates can't be in the future."""
    from pyspark.sql import functions as F
    return F.when(F.col(raw_col).rlike(TWO_DIGIT) & (parsed > F.current_date()),
                  F.add_months(parsed, -1200)).otherwise(parsed)


def fix_century_after(raw_col: str, parsed, reference):
    """A release can't come before its sentence; with no sentence date, Python's 1969-2068 window."""
    from pyspark.sql import functions as F
    earlier = F.add_months(parsed, -1200)
    with_reference = F.when(earlier >= reference, earlier).otherwise(parsed)
    without_reference = F.when(F.year(parsed) >= 2069, earlier).otherwise(parsed)
    fixed = F.when(reference.isNull(), without_reference).otherwise(with_reference)
    return F.when(F.col(raw_col).rlike(TWO_DIGIT), fixed).otherwise(parsed)


def clean_session3(s2):
    """Nine date formats, century rules by meaning, age at sentencing, impossible birth dates flagged."""
    from pyspark.sql import functions as F
    s3 = (s2.withColumn("_dob", fix_century_past("date_of_birth", parse_date("date_of_birth")))
            .withColumn("_sent", fix_century_past("sentence_date", parse_date("sentence_date"))))
    s3 = s3.withColumn("_rel", fix_century_after("projected_release_date", parse_date("projected_release_date"),
                                                 F.col("_sent")))
    s3 = (s3.drop(*DATE_COLUMNS).withColumnRenamed("_dob", "date_of_birth")
            .withColumnRenamed("_sent", "sentence_date").withColumnRenamed("_rel", "projected_release_date"))
    dob, sent = F.col("date_of_birth"), F.col("sentence_date")
    before_birthday = (F.month(sent) < F.month(dob)) | ((F.month(sent) == F.month(dob))
                                                       & (F.dayofmonth(sent) < F.dayofmonth(dob)))
    s3 = s3.withColumn("age_at_sentencing", F.year(sent) - F.year(dob) - F.when(before_birthday, 1).otherwise(0))
    return s3.withColumn("dob_flag",
        F.when(F.year(dob) < 1900, "before 1900").when(dob > F.current_date(), "in the future")
         .when(F.col("age_at_sentencing") < 17, "under 17 at sentencing")
         .when(F.col("age_at_sentencing") > 100, "over 100 at sentencing"))


def clean_session4(s3, keep_as_written: bool = False):
    """Whole numbers, exact money, validated time of day, yes/no flags. keep_as_written=True keeps the text
    of every converted column in an `as_written` struct, so values that didn't convert can be listed."""
    from pyspark.sql import functions as F
    s4 = s3
    if keep_as_written:
        s4 = s4.withColumn("as_written", F.struct(*[F.col(c).alias(c) for c in CONVERTED_COLUMNS]))
    words = F.create_map([F.lit(x) for pair in conversions.NUMBER_WORDS.items() for x in pair])
    points_text = F.trim(F.regexp_replace(F.lower("disciplinary_points"), "pts", ""))
    s4 = s4.withColumn("_points_text", F.coalesce(words[points_text], points_text))
    s4 = s4.withColumn("_points", F.expr("try_cast(_points_text AS DOUBLE)"))
    s4 = s4.withColumn("disciplinary_points", F.when(F.col("_points") == F.floor("_points"), F.col("_points").cast("int")))

    money = F.regexp_replace(F.upper("restitution_owed"), r"USD|\$|,|\s", "")
    negative = money.startswith("(") & money.endswith(")")
    money = F.regexp_replace(money, r"[()]", "")
    s4 = s4.withColumn("_money", F.when(money == "-", F.lit("0")).otherwise(money))
    s4 = s4.withColumn("_amount", F.expr("try_cast(_money AS DECIMAL(12,2))"))
    s4 = s4.withColumn("restitution_owed", F.when(negative, -F.col("_amount")).otherwise(F.col("_amount")))

    t = F.col("intake_time")
    compact = F.upper(F.regexp_replace(t, " ", ""))
    intake = None
    for shape, pattern in TIME_PATTERNS:
        parsed = F.date_format(F.try_to_timestamp(compact, F.lit(pattern)), "HH:mm:ss")
        intake = F.when(t.rlike(shape), parsed) if intake is None else intake.when(t.rlike(shape), parsed)
    s4 = s4.withColumn("intake_time", intake)

    for col in conversions.FLAG_COLUMNS:
        value = F.upper(col)
        s4 = s4.withColumn(col, F.when(value.isin(list(conversions.TRUE_VALUES)), True)
                                 .when(value.isin(list(conversions.FALSE_VALUES)), False))
    return s4.drop("_points_text", "_points", "_money", "_amount")


def silver(raw, reference=None):
    """The whole of sessions 2-4 for a pipeline: IDs checked without stopping, originals of converted values kept."""
    return clean_session4(clean_session3(clean_session2(raw, reference, strict_ids=False)), keep_as_written=True)


# ---- Pipeline expectations ------------------------------------------------------------------------
# Warn-only rules: the record is kept and the count of failures is logged with every pipeline update.
_LABELS = {"race": "race is in the crosswalk", "religion": "religion is in the crosswalk",
           "stg_affiliation": "STG affiliation is in the crosswalk", "offense_code": "offense code is on the NIBRS list",
           "violence_code": "violence code is valid", "unit_location": "unit is in the crosswalk"}
WARN_EXPECTATIONS = {
    **{_LABELS[raw]: f"{raw} IS NULL OR {std[0]} IS NOT NULL" for raw, std in domains.DOMAIN_COLUMNS.items()},
    **{f"{col} converted": f"as_written.{col} IS NULL OR {col} IS NOT NULL" for col in CONVERTED_COLUMNS},
    "offense and violence codes agree": ("violence_code_std IS NULL OR offense_against IS NULL OR "
                                         "(violence_code_std IN ('V2', 'V3')) = (offense_against = 'Person')"),
    "birth date is plausible": "dob_flag IS NULL",
}
DROP_EXPECTATIONS = {"IDs are valid 8-digit numbers": "_id_problem IS NULL"}


# ---- Lists for the data owner (the gold layer) ----------------------------------------------------
def domain_exceptions(silver_df):
    """Every coded value no crosswalk recognizes: tdcj_number, column, value_as_written, reason."""
    from functools import reduce
    from pyspark.sql import functions as F
    parts = []
    for raw, std_cols in domains.DOMAIN_COLUMNS.items():
        rows = silver_df.where(F.col(raw).isNotNull() & F.col(std_cols[0]).isNull())
        reason = F.lit(f"not in the {raw.replace('_', ' ')} reference data")
        if raw == "stg_affiliation":
            reason = F.when(F.col(raw).contains("/") & (lspark.domain_key(F.col(raw)) != "N/A"),
                            F.lit("two groups in one cell")).otherwise(reason)
        parts.append(rows.select("tdcj_number", F.lit(raw).alias("column"),
                                 F.col(raw).alias("value_as_written"), reason.alias("reason")))
    return reduce(lambda a, b: a.unionByName(b), parts)


def conversion_exceptions(silver_df):
    """Every value that didn't convert in session 4, from the `as_written` struct `silver()` keeps."""
    from functools import reduce
    from pyspark.sql import functions as F
    parts = []
    for col in CONVERTED_COLUMNS:
        written = F.col(f"as_written.{col}")
        parts.append(silver_df.where(written.isNotNull() & F.col(col).isNull())
                     .select("tdcj_number", F.lit(col).alias("column"), written.alias("value_as_written")))
    return reduce(lambda a, b: a.unionByName(b), parts)


def race_conflicts(silver_df):
    """People (SIDs) whose records carry more than one race code (unknown and declined left out)."""
    from pyspark.sql import functions as F
    known = silver_df.where(F.col("sid_number").isNotNull() & F.col("race_code").isNotNull()
                            & ~F.col("race_code").isin("U", "D"))
    per_person = known.groupBy("sid_number").agg(F.array_sort(F.collect_set("race_code")).alias("codes"))
    return (per_person.where(F.size("codes") > 1)
            .select("sid_number", F.array_join("codes", "/").alias("race_codes_on_file")))


def offense_violence_conflicts(silver_df):
    """Records whose offense code and violence code disagree (the data owner's rule; flag, don't fix)."""
    return silver_df.where(lspark.offense_violence_conflict()).select(
        "tdcj_number", "primary_offense", "offense_code", "offense_code_std", "offense_against", "violence_code_std")
