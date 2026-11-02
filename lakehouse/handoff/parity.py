"""Course 1's 19 numbers (FACILITATOR_GUIDE.md), as checks on its cleansed output.

Two places run them: the lake's blocking parity gate (tests/test_parity.py, on the lake's silver tables) and the
handoff's publish step (publish.py, on the files about to be published). Each check is (name, how to measure it on a
DuckDB connection whose silver.* tables or views are course 1's output, the number the class sees).
"""

UNMAPPED = {"offense_code": 1379, "unit_location": 266, "violence_code": 104, "religion": 86, "stg_affiliation": 77,
            "race": 54}
FAILED_CONVERSIONS = {"disciplinary_points": 206, "restitution_owed": 183, "intake_time": 185, "escape_risk": 220,
                      "protective_custody": 0, "dampener_required": 292}


def scalar(con, sql: str):
    """First column of the first row of a query."""
    return con.execute(sql).fetchone()[0]


def count(sql: str):
    """A check that measures one number with one query."""
    return lambda con: scalar(con, sql)


def record_key(con) -> tuple:
    """How many distinct offender_numbers silver has, and their type."""
    return (scalar(con, "SELECT count(DISTINCT offender_number) FROM silver.hero_offenders"),
            scalar(con, "SELECT data_type FROM information_schema.columns WHERE table_schema = 'silver' "
                        "AND table_name = 'hero_offenders' AND column_name = 'offender_number'"))


def trust_test(con) -> tuple:
    """Session 4's contract: the questions, whether every answer changed, and Q1's and Q2's before and after."""
    rows = con.execute("SELECT question_id, as_loaded, cleansed, changed FROM silver.trust_test "
                       "ORDER BY question_id").fetchall()
    return [r[0] for r in rows], all(r[3] for r in rows), tuple(rows[0][1:3]), tuple(rows[1][1:3])


def exceptions(rule: str, column: str = None) -> str:
    """The query counting silver's flagged values for one rule (and column)."""
    where = f"rule = '{rule}'" + (f" AND column_name = '{column}'" if column else "")
    return f"SELECT count(*) FROM silver.hero_offenders_exceptions WHERE {where}"


CHECKS = [
    ("silver_rows", count("SELECT count(*) FROM silver.hero_offenders"), 100_000),
    ("record_key_unique_bigint", record_key, (100_000, "BIGINT")),
    *[(f"unmapped_{c}", count(exceptions("domain_unmapped", c)), n) for c, n in UNMAPPED.items()],
    *[(f"failed_conversion_{c}", count(exceptions("conversion_failed", c)), n) for c, n in FAILED_CONVERSIONS.items()],
    ("offense_violence_conflicts", count(exceptions("offense_violence_conflict")), 228),
    ("birth_dates_flagged", count(exceptions("dob_check")), 8),
    ("restitution_total_to_the_cent",
     lambda con: str(scalar(con, "SELECT sum(restitution_owed) FROM silver.hero_offenders")), "1333587950.77"),
    ("confirmed_stg_records", count("SELECT count(*) FROM silver.hero_offenders WHERE stg_status = 'Confirmed'"), 3612),
    ("trust_test_all_eight_change", trust_test, ([f"Q{i}" for i in range(1, 9)], True, ("1536", "2991"), ("19", "3"))),
]


def failures(con) -> list:
    """Every check that doesn't match, as "<name>: got <value>, want <value>"; [] when all 19 do."""
    out = []
    for name, measure, want in CHECKS:
        got = measure(con)
        if got != want:
            out.append(f"{name}: got {got!r}, want {want!r}")
    return out
