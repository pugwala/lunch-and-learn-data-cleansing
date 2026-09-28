"""Session 4: every remaining column gets its real type."""
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd
import pyarrow as pa

MONEY_TYPE = pd.ArrowDtype(pa.decimal128(12, 2))  # exact to the cent, up to 9,999,999,999.99
TIME_TYPE = pd.ArrowDtype(pa.time64("us"))

NUMBER_WORDS = {w: str(i) for i, w in enumerate(
    ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"])}

TIME_FORMATS = [  # (label, shape, how to read it)
    ("HH:MM", r"\d{2}:\d{2}", "%H:%M"),
    ("HH:MM:SS", r"\d{2}:\d{2}:\d{2}", "%H:%M:%S"),
    ("H:MM AM", r"\d{1,2}:\d{2} ?[AaPp][Mm]", "%I:%M%p"),
    ("HHMM (military)", r"\d{4}", "%H%M"),
    ("HH.MM", r"\d{2}\.\d{2}", "%H.%M"),
]

# One mapping for every yes/no column: long forms, numbers, and the single characters most source
# systems use (Y/N and T/F). Values are compared after uppercasing, so y/n/t/f are covered too.
FLAG_COLUMNS = ["escape_risk", "protective_custody", "dampener_required"]
TRUE_VALUES = {"Y", "YES", "1", "TRUE", "T", "X"}
FALSE_VALUES = {"N", "NO", "0", "FALSE", "F"}

ORDERED = {
    "custody_level": ["G1", "G2", "G3", "G4", "G5"],
    "violence_code_std": ["NV", "V1", "V2", "V3"],   # the standardized code from session 2; violence_code stays as written
    "threat_level": ["DELTA", "BETA", "ALPHA", "OMEGA"],
}
UNORDERED = ["gender", "race_code", "religion_family", "stg_status", "offense_against"]


def to_integer(series: pd.Series):
    """'3', '03', '3.0', '3 pts', 'three' -> 3. Fractions are rejected, not truncated.
    Returns (integers, failed_mask)."""
    text = series.str.lower().str.replace("pts", "", regex=False).str.strip()
    text = text.replace(NUMBER_WORDS)
    numbers = pd.to_numeric(text, errors="coerce")
    fractional = numbers.notna() & (numbers % 1 != 0)
    result = numbers.mask(fractional).astype("Int64")
    failed = series.notna() & result.isna()
    return result, failed


def parse_money(value):
    """'$1,234.50' / 'USD 1,234.50' / '(12.00)' / '-' -> Decimal. Returns None if unreadable."""
    if not isinstance(value, str):
        return None
    text = value.upper().replace("USD", "").replace("$", "").replace(",", "").replace(" ", "")
    if text == "-":                      # accounting convention: a dash means zero
        return Decimal("0.00")
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()")
    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None
    if not amount.is_finite():
        return None
    amount = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return -amount if negative else amount


def to_money(series: pd.Series):
    """Returns (exact decimals, failed_mask)."""
    lookup = {v: parse_money(v) for v in series.dropna().unique()}
    values = [lookup.get(v) if isinstance(v, str) else None for v in series]
    result = pd.Series(values, index=series.index, dtype=MONEY_TYPE)
    return result, series.notna() & result.isna()


def to_time(series: pd.Series):
    """Seven time formats -> a real time-of-day. Returns (times, failed_mask)."""
    lookup = {}
    for value in series.dropna().unique():
        compact = value.replace(" ", "").upper()
        for _, shape, fmt in TIME_FORMATS:
            candidate = compact if "%p" in fmt else value
            if re.fullmatch(shape, value):
                try:
                    lookup[value] = datetime.strptime(candidate, fmt).time()
                    break
                except ValueError:
                    continue
    values = [lookup.get(v) if isinstance(v, str) else None for v in series]
    result = pd.Series(values, index=series.index, dtype=TIME_TYPE)
    return result, series.notna() & result.isna()


def to_boolean(series: pd.Series, true_values=TRUE_VALUES, false_values=FALSE_VALUES):
    """Map the many spellings of yes/no onto True/False. Returns (booleans, unmapped_values)."""
    upper = series.str.upper()
    result = pd.Series(pd.NA, index=series.index, dtype="boolean")
    result[upper.isin(true_values).fillna(False).astype(bool)] = True
    result[upper.isin(false_values).fillna(False).astype(bool)] = False
    unmapped = series[series.notna() & result.isna()]
    return result, unmapped


def to_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Codes become categories. Ordered ones support sorting and comparisons like >= 'G4'."""
    out = df.copy()
    for col, order in ORDERED.items():
        out[col] = pd.Categorical(out[col], categories=order, ordered=True)
    for col in UNORDERED:
        out[col] = out[col].astype("category")
    return out


def session4(df: pd.DataFrame) -> pd.DataFrame:
    """Everything taught in session 4."""
    out = df.copy()
    out["disciplinary_points"], _ = to_integer(out["disciplinary_points"])
    out["restitution_owed"], _ = to_money(out["restitution_owed"])
    out["intake_time"], _ = to_time(out["intake_time"])
    for col in FLAG_COLUMNS:
        out[col], _ = to_boolean(out[col])
    return to_categories(out)


def exceptions(before: pd.DataFrame) -> pd.DataFrame:
    """Every value that didn't convert, one row each, keyed by TDCJ number: the list to send back to the
    data owner. `before` is the data going into session 4 (indexed by tdcj_number). The TDCJ numbers are
    padded back to 8 digits because this list goes to people."""
    from .cleaning import format_id
    failed = {
        "disciplinary_points": to_integer(before["disciplinary_points"])[1],
        "restitution_owed": to_money(before["restitution_owed"])[1],
        "intake_time": to_time(before["intake_time"])[1],
    }
    for col in FLAG_COLUMNS:
        booleans, _ = to_boolean(before[col])
        failed[col] = before[col].notna() & booleans.isna()
    parts = []
    for col, mask in failed.items():
        rows = before.loc[mask.fillna(False).astype(bool), [col]].rename(columns={col: "value as written"})
        rows.insert(0, "column", col)
        parts.append(rows)
    result = pd.concat(parts).sort_index(kind="stable")
    result.index = format_id(result.index)
    return result


def conversion_report(before: pd.DataFrame) -> pd.DataFrame:
    """Per column: missing in source, failed conversion, converted."""
    results = {
        "disciplinary_points": to_integer(before["disciplinary_points"]),
        "restitution_owed": to_money(before["restitution_owed"]),
        "intake_time": to_time(before["intake_time"]),
    }
    rows = {}
    for col, (converted, failed) in results.items():
        rows[col] = {"missing in source": int(before[col].isna().sum()),
                     "failed conversion": int(failed.sum()), "converted": int(converted.notna().sum())}
    for col in FLAG_COLUMNS:
        booleans, unmapped = to_boolean(before[col])
        rows[col] = {"missing in source": int(before[col].isna().sum()),
                     "failed conversion": int(len(unmapped)), "converted": int(booleans.notna().sum())}
    return pd.DataFrame(rows).T
