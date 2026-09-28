"""Exercise checkers. Each prints a friendly verdict; nothing here ever stops a notebook from running."""
from datetime import date
from decimal import Decimal
from functools import lru_cache

import pandas as pd


class _Blank:
    """The ___ placeholder in exercises. Replace it with your answer."""
    def __repr__(self):
        return "___"


___ = _Blank()


def _verdict(answer, expected, hint: str, label: str = "") -> None:
    if isinstance(answer, _Blank):
        print("✏️  Replace ___ with your answer, then run the cell again.")
        return
    ok = answer == expected
    if isinstance(expected, (float, Decimal)) and not isinstance(answer, (str, bool)):
        try:
            ok = abs(float(answer) - float(expected)) < 0.01
        except (TypeError, ValueError):
            ok = False
    if ok:
        print(f"✅ Correct{': ' + label if label else ''} ({expected})")
    else:
        print(f"❌ Not quite. You have {answer!r}. Hint: {hint}")


# Cached data at each stage so checks are instant after the first call
@lru_cache(maxsize=None)
def _naive():
    from . import pipeline
    return pipeline.load_naive()


@lru_cache(maxsize=None)
def _stage(n: int):
    from . import pipeline
    return pipeline.clean_through(n)


# ---- Session 1 ------------------------------------------------------------------------------
def s1_ex1(answer):
    expected = len(_naive()["custody_level"].value_counts(dropna=False))
    _verdict(answer, expected, "count the rows in the value_counts output: len(custody_counts)")


def s1_ex2(answer):
    from .cleaning import strip_and_null
    expected = int(strip_and_null(_naive())["gender"].isna().sum())
    _verdict(answer, expected, 'use clean["gender"].isna().sum()')


def s1_ex3(answer):
    from .cleaning import strip_and_null
    expected = strip_and_null(_naive())["unit_location"].value_counts().index[0]
    _verdict(answer, expected, 'clean["unit_location"].value_counts().index[0] gives the top value')


# ---- Session 2 ------------------------------------------------------------------------------
def s2_ex1(answer):
    expected = int((_stage(2)["housing_restriction"] == "NONE").sum())
    _verdict(answer, expected, 'compare the column to the text "NONE", then .sum()')


def s2_ex2(answer):
    df = _stage(2)
    expected = int(df.loc[df["last_name_std"] == "MCDONALD", "last_name"].nunique())
    _verdict(answer, expected, 'filter last_name_std == "MCDONALD", then .nunique() on the original last_name')


def s2_ex3(answer):
    expected = int((_stage(2).groupby("sid_number").size() > 1).sum())
    _verdict(answer, expected, 'df.groupby("sid_number").size() counts records per person; how many are > 1?')


def s2_ex4(answer):
    df = _stage(2)
    expected = int((df["stg_group"].eq("HYDRA") & df["stg_status"].eq("Confirmed")).sum())
    _verdict(answer, expected, 'two conditions joined with &: df["stg_group"] == "HYDRA" and '
                               'df["stg_status"] == "Confirmed". A bare "HYDRA" is Unverified, not Confirmed.')


# ---- Session 3 ------------------------------------------------------------------------------
def s3_ex1(answer):
    from .dates import TWO_DIGIT, parse_dates
    _, labels = parse_dates(_stage(2)["sentence_date"])
    _verdict(answer, int((labels == TWO_DIGIT).sum()), 'count the format labels equal to "M/D/YY"')


def s3_ex2(answer):
    from .dates import parse_dates
    parsed, _ = parse_dates(_stage(2)["date_of_birth"])
    expected = int((parsed > date.today()).fillna(False).sum())
    _verdict(answer, expected, "compare the parsed dates to date.today() BEFORE fixing centuries")


def s3_ex3(answer):
    expected = int((_stage(3)["projected_release_date"].dt.year == 2027).sum())
    _verdict(answer, expected, 'use df3["projected_release_date"].dt.year == 2027, then .sum()')


# ---- Session 4 ------------------------------------------------------------------------------
def s4_ex1(answer):
    expected = round(float(_stage(4)["disciplinary_points"].mean()), 2)
    _verdict(answer, expected, "points.mean(), rounded to 2 places")


def s4_ex2(answer):
    expected = _stage(4)["restitution_owed"].sum()
    _verdict(answer, expected, "money.sum() on the converted column")


def s4_ex3(true_values, false_values):
    from .conversions import to_boolean
    if any(isinstance(v, _Blank) for v in set(true_values) | set(false_values)):
        remaining = [v for v in set(true_values) | set(false_values) if not isinstance(v, _Blank)]
        print(f"✏️  Replace the ___ entries with more spellings. So far you have {sorted(remaining)}.")
    raw = _stage(3)["escape_risk"]
    clean_true = {v for v in true_values if isinstance(v, str)}
    clean_false = {v for v in false_values if isinstance(v, str)}
    _, unmapped = to_boolean(raw, clean_true, clean_false)
    counts = unmapped.str.upper().value_counts()
    print(f"Still unmapped: {len(unmapped):,} values")
    if len(counts):
        print(counts.head(10).to_string())
    genuinely_unknown = {"MAYBE", "?", "PENDING"}
    if set(counts.index) <= genuinely_unknown:
        print("✅ Every real yes/no spelling is mapped. What's left is genuinely unknown and should stay null.")


def s4_ex4(answer):
    expected = int(_stage(4)["dampener_required"].sum())
    _verdict(answer, expected, 'flags["dampener_required"].sum() counts the True values')


# ---- Session 5 ------------------------------------------------------------------------------
def s5_ex1(answer):
    expected = int(_stage(2)["gender"].isna().sum())
    _verdict(answer, expected, 'filter F.col("gender").isNull(), then .count(). It should match pandas.')


def s5_ex2(answer):
    expected = _stage(4)["restitution_owed"].sum()
    _verdict(answer, expected, 'use s4.agg(F.sum("restitution_owed")).first()[0]. It should match pandas to the cent.')
