"""Session 2: cleansing. Whitespace, codes that mean 'unknown', casing, names, keys, multi-value fields."""
import re

import pandas as pd

# Data dictionary: text the source system uses to mean "unknown", per column.
# Note what is NOT here: housing_restriction "NONE" means "no restriction required". That's a real
# answer, not missing data, so it stays. Decide these with the data owner, never by guessing.
NULL_TOKENS = {
    "disciplinary_points": ["N/A"],
    "escape_risk": ["U", "UNK"],
    "intake_time": ["99:99"],
    "protective_custody": ["U"],
}

# The record ID. One TDCJ number per incarceration: 8 digits, leading zeros included, never repeated.
# It is text, never a number, and from session 2 on it is the DataFrame's index.
RECORD_ID = "tdcj_number"
ID_PATTERN = r"\d{8}"

NAME_PARTICLES = {"DE", "LA", "DA", "DEL", "VAN", "VON", "DU", "DOS", "DI", "LE"}
NAME_SUFFIXES = {"JR": "Jr", "SR": "Sr", "II": "II", "III": "III", "IV": "IV"}
NAME_EXCEPTIONS = {"MACDONALD": "MacDonald"}


def text_columns(df: pd.DataFrame) -> list:
    """Columns holding text. Works on pandas 2 (object dtype) and pandas 3 (str dtype)."""
    return list(df.select_dtypes(include=["object", "string"]).columns)


def strip_and_null(df: pd.DataFrame) -> pd.DataFrame:
    """Trim spaces/tabs from both ends of every text value, then turn empty text into a real null."""
    out = df.copy()
    for col in text_columns(out):
        stripped = out[col].str.strip()
        out[col] = stripped.mask(stripped == "")
    return out


def apply_null_tokens(df: pd.DataFrame, tokens: dict = None) -> pd.DataFrame:
    """Replace documented 'unknown' codes with real nulls, column by column."""
    out = df.copy()
    for col, values in (tokens or NULL_TOKENS).items():
        if col in out.columns:
            out[col] = out[col].mask(out[col].str.upper().isin([v.upper() for v in values]))
    return out


def standardize_names(df: pd.DataFrame) -> pd.DataFrame:
    """Add *_std columns (uppercase, single spaces) for matching. Originals stay for display."""
    out = df.copy()
    for col in ["last_name", "first_name", "alias"]:
        out[f"{col}_std"] = out[col].str.upper().str.replace(r"\s+", " ", regex=True)
    return out


def _cap_word(word: str) -> str:
    if word in NAME_EXCEPTIONS:
        return NAME_EXCEPTIONS[word]
    if "-" in word:
        return "-".join(_cap_word(p) for p in word.split("-"))
    if "'" in word:
        head, tail = word.split("'", 1)
        return head.capitalize() + "'" + tail.capitalize()
    if word.startswith("MC") and len(word) > 3:
        return "Mc" + word[2:].capitalize()
    return word.capitalize()


def display_name(name):
    """Rule-based proper case for reports: MCDONALD -> McDonald, DE LA CRUZ -> De la Cruz.
    No rule set is perfect, which is why matching always uses the *_std columns instead."""
    if not isinstance(name, str):
        return name
    out = []
    for i, word in enumerate(name.upper().split()):
        if word.rstrip(".") in NAME_SUFFIXES:
            out.append(NAME_SUFFIXES[word.rstrip(".")])
        elif word in NAME_PARTICLES and i > 0:
            out.append(word.lower())
        else:
            out.append(_cap_word(word))
    return " ".join(out)


def record_ids(df: pd.DataFrame) -> pd.Series:
    """The TDCJ numbers, whether they are still a column or already the index."""
    return df.index.to_series() if df.index.name == RECORD_ID else df[RECORD_ID]


def set_record_id(df: pd.DataFrame) -> pd.DataFrame:
    """Make tdcj_number the index, after proving it is a real ID: present, 8 digits, never repeated.
    Refuses (raises) instead of guessing if any record fails."""
    if df.index.name == RECORD_ID:
        return df
    ids = df[RECORD_ID]
    problems = {
        "missing": int(ids.isna().sum()),
        "not 8 digits": int((~ids.dropna().str.fullmatch(ID_PATTERN)).sum()),
        "repeated": int(ids.dropna().duplicated().sum()),
    }
    failed = {name: count for name, count in problems.items() if count}
    if failed:
        raise ValueError(f"tdcj_number can't be the record ID yet: {failed}. Fix these with the data owner first.")
    return df.set_index(RECORD_ID, verify_integrity=True)


def key_checks(df: pd.DataFrame) -> pd.DataFrame:
    """Checks every report writer should run before trusting a join or a count."""
    tdcj, sid = record_ids(df), df["sid_number"]
    last_std = df["last_name_std"] if "last_name_std" in df else df["last_name"].str.upper()
    known = df.assign(_last=last_std).dropna(subset=["sid_number", "_last"])
    conflicts = int((known.groupby("sid_number")["_last"].nunique() > 1).sum())
    per_sid = df.dropna(subset=["sid_number"]).groupby("sid_number").size()
    checks = {
        "TDCJ number missing": int(tdcj.isna().sum()),
        "TDCJ number duplicated": int(tdcj.dropna().duplicated().sum()),
        "TDCJ number not 8 digits": int((~tdcj.dropna().str.fullmatch(r"\d{8}")).sum()),
        "SID missing": int(sid.isna().sum()),
        "SID not 8 digits": int((~sid.dropna().str.fullmatch(r"\d{8}")).sum()),
        "SIDs with conflicting last names": conflicts,
        "People (SIDs) with more than one record": int((per_sid > 1).sum()),
    }
    return pd.DataFrame({"count": checks})


def explode_powers(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (record, power) instead of 'FLIGHT;SUPER STRENGTH' packed into one cell."""
    base = df if df.index.name == RECORD_ID else df.set_index(RECORD_ID)
    long = base[["superpower"]].dropna().copy()
    long["power"] = long["superpower"].str.split(";")
    long = long.explode("power")
    long["power"] = long["power"].str.strip()
    return long[["power"]].reset_index()


def session2(df: pd.DataFrame) -> pd.DataFrame:
    """Everything taught in session 2, in order."""
    df = strip_and_null(df)
    df = set_record_id(df)
    df = apply_null_tokens(df)
    df = standardize_names(df)
    return df
