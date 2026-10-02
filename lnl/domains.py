"""Session 2: coded domains and free text.

Unit locations, race, religion, STG affiliation, offense codes and violence codes arrive spelled many ways. Each one is mapped
to agreed reference data (the CSV files in lnl/reference/) instead of being guessed at in code. Anything a
crosswalk doesn't know goes on an exceptions list for the data owner. Free text (names and officer notes)
gets its web-form and encoding damage repaired first, so matching and searching see what was actually typed.

Race and religion are sensitive: they're collected for required reporting and for accommodations, access to
them is restricted, and they are never inferred from a name, a photo or anything else. In the course data they
are assigned at random, independently of every other column.
"""
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

import pandas as pd

REFERENCE_DIR = Path(__file__).parent / "reference"

# Text damage, and what it should have been. Order matters: longest sequences first.
TEXT_REPAIRS = {
    "â€™": "'", "â€˜": "'", "â€œ": '"', "â€\u009d": '"',
    "â€“": "-", "â€”": "-",                        # â€™ â€˜ â€œ â€\x9d â€“ â€”
    "Ã‰": "É", "Ã“": "Ó", "Ã‘": "Ñ",  # Ã‰ Ã“ Ã‘ -> É Ó Ñ
    "Ã©": "é", "Ã³": "ó", "Ã±": "ñ",  # Ã© Ã³ Ã± -> é ó ñ
    "Ã¡": "á", "Ãº": "ú", "Ã­": "í",  # Ã¡ Ãº Ã\xad -> á ú í
    "&nbsp;": " ", "&amp;": "&", "<br />": " ", "<br/>": " ", "<br>": " ",
    "’": "'", "‘": "'", "“": '"', "”": '"', "–": "-", "—": "-", " ": " ",
}
TEXT_COLUMNS = ["last_name", "first_name", "alias", "conduct_notes"]

DOMAIN_COLUMNS = {                       # raw column -> the standardized columns session 2 adds
    "race": ["race_code", "race_omb"],
    "religion": ["religion_group", "religion_family"],
    "stg_affiliation": ["stg_group", "stg_status"],
    "offense_code": ["offense_code_std", "offense_against"],
    "violence_code": ["violence_code_std"],
    "unit_location": ["unit_name", "unit_code"],
}

# STG status written into the same field as the group. A bare group name means nobody recorded a status:
# the data owner decided that's "Unverified", never "Confirmed".
STG_STATUS = [
    ("Former", r"^(?:FORMER |EX-)|-RENOUNCED$| \(FORMER\)$"),
    ("Suspected", r"^SUSP(?::| )|-SUSPECTED$| \((?:SUSPECTED|S)\)$"),
    ("Confirmed", r"^CONFIRMED |-CONFIRMED$| \(C\)$"),
]
STG_ANY_STATUS = "|".join(pattern for _, pattern in STG_STATUS)


# ---- Reference data -------------------------------------------------------------------------
@lru_cache(maxsize=None)
def reference(name: str) -> pd.DataFrame:
    """One of the reference tables in lnl/reference/, e.g. reference("race_crosswalk")."""
    return pd.read_csv(REFERENCE_DIR / f"{name}.csv", dtype=str, keep_default_na=False)


def _lookup(keys: pd.Series, table: str, key_col: str, value_col: str) -> pd.Series:
    mapping = reference(table).set_index(key_col)[value_col]
    return keys.map(mapping)


# ---- Free text --------------------------------------------------------------------------------
def repair_text(series: pd.Series) -> pd.Series:
    """Undo encoding and web-form damage: â€™ -> ', MUÃ‘OZ -> MUÑOZ, <br> and &nbsp; -> space, curly quotes
    -> straight, runs of spaces -> one. Accents that were typed on purpose are kept."""
    out = series
    for bad, good in TEXT_REPAIRS.items():
        out = out.str.replace(bad, good, regex=False)
    return out.str.replace(r"\s+", " ", regex=True).str.strip()


def fold_accents(series: pd.Series) -> pd.Series:
    """GARCÍA -> GARCIA, MUÑOZ -> MUNOZ. For matching only; display columns keep the accents."""
    def fold(text):
        if not isinstance(text, str):
            return text
        return "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    return series.map(fold, na_action="ignore")


# ---- Coded domains ----------------------------------------------------------------------------
def domain_key(series: pd.Series) -> pd.Series:
    """The spelling-insensitive key every crosswalk is keyed on: upper case, no periods, straight apostrophes,
    single spaces, no spaces around - / and :. "H.Y.D.R.A." -> HYDRA, "W - WHITE" -> W-WHITE, "susp: hydra" ->
    SUSP:HYDRA, "Hell’s Kitchen" -> HELL'S KITCHEN."""
    key = series.str.upper().str.replace(".", "", regex=False).str.replace("[’‘]", "'", regex=True)
    key = key.str.replace(r"\s+", " ", regex=True).str.strip()
    return key.str.replace(r"\s*([-/:])\s*", r"\1", regex=True)


def offense_key(series: pd.Series) -> pd.Series:
    """NIBRS codes: 13a, 13-A, "13 A" -> 13A; 290.0 (Excel) -> 290; 9B -> 09B (the leading zero is part of the code)."""
    key = series.str.upper().str.replace(r"[\s-]", "", regex=True).str.replace(r"\.0$", "", regex=True)
    return key.mask(key.str.fullmatch(r"\d[A-Z]", na=False), "0" + key)


def split_stg(series: pd.Series) -> pd.DataFrame:
    """One field holding group and status -> stg_group + stg_status. "SUSP: H.Y.D.R.A." -> HYDRA, Suspected."""
    key = domain_key(series)
    status = pd.Series(pd.NA, index=series.index, dtype="object")
    for label, pattern in reversed(STG_STATUS):          # first pattern in STG_STATUS wins
        status = status.mask(key.str.contains(pattern, regex=True, na=False), label)
    rest = key.str.replace(STG_ANY_STATUS, "", regex=True).str.strip()
    group = _lookup(rest, "stg_groups", "alias", "stg_group")
    status = status.mask(group.notna() & status.isna(), "Unverified")
    status = status.mask(group == "None", "None")
    return pd.DataFrame({"stg_group": group, "stg_status": status.where(group.notna())})


def standardize_domains(df: pd.DataFrame) -> pd.DataFrame:
    """Add the standardized columns for every coded domain. The raw columns stay, as written."""
    out = df.copy()
    race = domain_key(out["race"])
    out["race_code"] = _lookup(race, "race_crosswalk", "source_value", "race_code")
    out["race_omb"] = _lookup(race, "race_crosswalk", "source_value", "race_omb")
    religion = domain_key(out["religion"])
    out["religion_group"] = _lookup(religion, "religion_crosswalk", "source_value", "religion_group")
    out["religion_family"] = _lookup(religion, "religion_crosswalk", "source_value", "religion_family")
    out[["stg_group", "stg_status"]] = split_stg(out["stg_affiliation"])
    code = offense_key(out["offense_code"])
    out["offense_code_std"] = code.where(code.isin(reference("nibrs_offenses")["code"]))
    out["offense_against"] = _lookup(out["offense_code_std"], "nibrs_offenses", "code", "crime_against")
    out["violence_code_std"] = _lookup(domain_key(out["violence_code"]), "violence_codes", "source_value", "violence_code")
    unit = domain_key(out["unit_location"])
    out["unit_name"] = _lookup(unit, "unit_crosswalk", "source_value", "unit_name")
    out["unit_code"] = _lookup(unit, "unit_crosswalk", "source_value", "unit_code")
    return out


def domain_exceptions(df: pd.DataFrame) -> pd.DataFrame:
    """Every coded value no crosswalk recognizes, keyed by TDCJ number (padded to 8 digits, because this list
    goes to people). Nothing on it is guessed or dropped: the data owner decides what each one means."""
    from .cleaning import format_id
    parts = []
    for raw, std_cols in DOMAIN_COLUMNS.items():
        failed = df[raw].notna() & df[std_cols[0]].isna()
        rows = df.loc[failed, [raw]].rename(columns={raw: "value as written"})
        reason = f"not in the {raw.replace('_', ' ')} reference data"
        if raw == "stg_affiliation":
            two = rows["value as written"].str.contains("/", regex=False) & ~domain_key(rows["value as written"]).eq("N/A")
            rows["reason"] = reason
            rows.loc[two, "reason"] = "two groups in one cell"
        else:
            rows["reason"] = reason
        rows.insert(0, "column", raw)
        parts.append(rows)
    result = pd.concat(parts).sort_index(kind="stable")
    result.index = format_id(result.index)
    return result


def race_conflicts(df: pd.DataFrame) -> pd.DataFrame:
    """People (SIDs, padded to 8 digits) whose records disagree on race. Religion is left out on purpose: people can change faith
    between incarcerations, so different religions across records aren't an error."""
    known = df.dropna(subset=["sid_number", "race_code"])
    known = known[~known["race_code"].isin(["U", "D"])]
    codes = known.groupby("sid_number")["race_code"].agg(lambda s: "/".join(sorted(set(s))))
    result = codes[codes.str.contains("/")].rename("race codes on file").to_frame()
    from .cleaning import format_id
    result.index = format_id(result.index)       # SIDs padded to 8 digits: this list goes to people
    return result


def offense_violence_conflicts(df: pd.DataFrame) -> pd.DataFrame:
    """Records whose offense code and violence code disagree: a crime against a person coded NV or V1, or a
    V2/V3 violence code on an offense that isn't against a person. Rule from the data owner; flag, don't fix."""
    violent = df["violence_code_std"].astype("string").isin(["V2", "V3"])
    against_person = df["offense_against"].eq("Person")
    known = df["violence_code_std"].notna() & df["offense_against"].notna()
    result = df.loc[known & (violent != against_person),
                    ["primary_offense", "offense_code", "offense_code_std", "offense_against", "violence_code_std"]]
    from .cleaning import format_id
    result.index = format_id(result.index)       # TDCJ numbers padded to 8 digits: this list goes to people
    return result
