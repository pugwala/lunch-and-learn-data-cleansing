"""Session 3: dates. Profile the formats, parse explicitly, fix two-digit years, flag the impossible."""
from datetime import date, datetime

import pandas as pd
import pyarrow as pa

DATE_TYPE = pd.ArrowDtype(pa.date32())  # a true calendar date; handles year 1213 on any pandas version

# (label, what the text looks like, how to read it)
DATE_FORMATS = [
    ("MM/DD/YYYY", r"\d{2}/\d{2}/\d{4}", "%m/%d/%Y"),
    ("M/D/YY", r"\d{1,2}/\d{1,2}/\d{2}", "%m/%d/%y"),
    ("MM-DD-YYYY", r"\d{2}-\d{2}-\d{4}", "%m-%d-%Y"),
    ("YYYY-MM-DD", r"\d{4}-\d{2}-\d{2}", "%Y-%m-%d"),
    ("YYYY-MM-DD HH:MM:SS", r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "%Y-%m-%d %H:%M:%S"),
    ("YYYYMMDD", r"\d{8}", "%Y%m%d"),
    ("DD-MON-YYYY", r"\d{2}-[A-Za-z]{3}-\d{4}", "%d-%b-%Y"),
    ("Month D, YYYY", r"[A-Za-z]+ \d{1,2}, \d{4}", "%B %d, %Y"),
    ("Mon D YYYY", r"[A-Za-z]{3} \d{1,2} \d{4}", "%b %d %Y"),
]
TWO_DIGIT = "M/D/YY"


def profile_shapes(series: pd.Series, top: int = 15) -> pd.DataFrame:
    """Replace every digit with 9 and every letter with A, then count. Reveals the formats in use."""
    shapes = (series.dropna().str.replace(r"\d", "9", regex=True)
              .str.replace(r"[A-Za-z]", "A", regex=True))
    table = shapes.value_counts().head(top).rename("count").to_frame()
    table["example"] = [series.dropna()[shapes == s].iloc[0] for s in table.index]
    table.index.name = "shape"
    return table


def classify_formats(series: pd.Series) -> pd.Series:
    """Label each value with the date format its shape matches (NaN if none)."""
    labels = pd.Series(None, index=series.index, dtype="object")
    for label, regex, _ in DATE_FORMATS:
        match = series.str.fullmatch(regex, na=False).astype(bool) & labels.isna()
        labels[match] = label
    return labels


def parse_dates(series: pd.Series):
    """Parse text dates with explicit formats. Returns (dates, format_label_used)."""
    labels = classify_formats(series)
    lookup = {}
    for label, _, fmt in DATE_FORMATS:
        for value in series[labels == label].unique():
            try:
                lookup[value] = datetime.strptime(value, fmt).date()
            except ValueError:
                pass  # right shape, impossible date (e.g. 02/30/2020): stays null and gets reported
    values = [lookup.get(v) if isinstance(v, str) else None for v in series]
    return pd.Series(values, index=series.index, dtype=DATE_TYPE), labels


def _with_year(d: date, year: int):
    try:
        return d.replace(year=year)
    except ValueError:  # Feb 29 in a non-leap century
        return d.replace(year=year, day=28)


def fix_two_digit_years(dates: pd.Series, labels: pd.Series, rule: str, reference: pd.Series = None,
                        today: date = None) -> pd.Series:
    """Choose the right century for M/D/YY values.
    rule="past":  birth and sentence dates can't be in the future -> latest candidate not after today.
    rule="after": a release can't come before its sentence -> earliest candidate on/after the reference.
    """
    today = today or date.today()
    fixed = dates.tolist()
    ref = reference.tolist() if reference is not None else None
    for pos in [i for i, lab in enumerate(labels.tolist()) if isinstance(lab, str) and lab == TWO_DIGIT]:
        d = fixed[pos]
        if d is None or d is pd.NA:
            continue
        candidates = [_with_year(d, 1900 + d.year % 100), _with_year(d, 2000 + d.year % 100)]
        if rule == "past":
            ok = [c for c in candidates if c <= today]
            fixed[pos] = max(ok) if ok else min(candidates)
        elif rule == "after" and ref is not None and ref[pos] is not None and ref[pos] is not pd.NA:
            ok = [c for c in candidates if c >= ref[pos]]
            fixed[pos] = min(ok) if ok else max(candidates)
    return pd.Series(fixed, index=dates.index, dtype=DATE_TYPE)


def age_in_years(born: pd.Series, on: pd.Series) -> pd.Series:
    """Whole years between two date columns (birthday-aware)."""
    years = on.dt.year - born.dt.year
    before_birthday = (on.dt.month < born.dt.month) | ((on.dt.month == born.dt.month) & (on.dt.day < born.dt.day))
    return (years - before_birthday.astype("int64[pyarrow]")).astype("Int64")


def dob_flags(dob: pd.Series, age: pd.Series, today: date = None) -> pd.Series:
    """Why a date of birth needs a human to look at it (null means it looks fine)."""
    today = today or date.today()
    flags = pd.Series(pd.NA, index=dob.index, dtype="object")
    flags[(age > 100).fillna(False).astype(bool)] = "over 100 at sentencing"
    flags[(age < 17).fillna(False).astype(bool)] = "under 17 at sentencing"
    flags[(dob > today).fillna(False).astype(bool)] = "in the future"
    flags[(dob.dt.year < 1900).fillna(False).astype(bool)] = "before 1900"
    return flags


def session3(df: pd.DataFrame) -> pd.DataFrame:
    """Everything taught in session 3: typed dates, correct centuries, age, and DOB flags."""
    out = df.copy()
    dob, dob_fmt = parse_dates(out["date_of_birth"])
    sentence, sent_fmt = parse_dates(out["sentence_date"])
    release, rel_fmt = parse_dates(out["projected_release_date"])
    out["date_of_birth"] = fix_two_digit_years(dob, dob_fmt, "past")
    out["sentence_date"] = fix_two_digit_years(sentence, sent_fmt, "past")
    out["projected_release_date"] = fix_two_digit_years(release, rel_fmt, "after", reference=out["sentence_date"])
    out["age_at_sentencing"] = age_in_years(out["date_of_birth"], out["sentence_date"])
    out["dob_flag"] = dob_flags(out["date_of_birth"], out["age_at_sentencing"])
    return out


def date_report(before: pd.DataFrame, after: pd.DataFrame,
                columns=("date_of_birth", "sentence_date", "projected_release_date")) -> pd.DataFrame:
    """Per column: what was missing, what failed, what parsed, and how many centuries were corrected."""
    rows = {}
    for col in columns:
        raw = before[col]
        parsed, labels = parse_dates(raw)
        rows[col] = {
            "missing in source": int(raw.isna().sum()),
            "did not parse": int((raw.notna() & parsed.isna()).sum()),
            "parsed": int(after[col].notna().sum()),
            "two-digit years": int((labels == TWO_DIGIT).sum()),
            "century corrected": int((parsed.notna() & (parsed != after[col])).fillna(False).sum()),
        }
    return pd.DataFrame(rows).T
