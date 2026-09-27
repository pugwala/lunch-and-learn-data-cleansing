"""The Trust Test: questions a warden might ask, answered naively (session 1) and correctly (session 4)."""
import pandas as pd

QUESTIONS = {
    "Q1": "Records missing a custody level",
    "Q2": "Distinct gender values",
    "Q3": "Records with last name KENT",
    "Q4": "Earliest projected release date",
    "Q5": "Does TDCJ #02381457 exist?",
    "Q6": "Average age at sentencing",
    "Q7": "Records flagged as escape risk",
    "Q8": "Records in protective custody",
}


def naive_answers(df: pd.DataFrame) -> dict:
    """Answers from a pandas-defaults load with no cleansing (what session 1 starts with)."""
    return {
        "Q1": int(df["custody_level"].isna().sum()),
        "Q2": int(df["gender"].nunique(dropna=False)),
        "Q3": int((df["last_name"] == "KENT").sum()),
        "Q4": repr(df["projected_release_date"].dropna().min()),
        "Q5": bool((df["tdcj_number"].astype(str) == "02381457").any()),
        "Q6": "can't compute (dates are text)",
        "Q7": int((df["escape_risk"] == "Y").sum()),
        "Q8": int((df["protective_custody"] == "Y").sum()),
    }


def typed_answers(df: pd.DataFrame) -> dict:
    """Answers from fully cleansed, typed data (after session 4)."""
    trusted_ages = df.loc[df["dob_flag"].isna(), "age_at_sentencing"]
    return {
        "Q1": int(df["custody_level"].isna().sum()),
        "Q2": int(df["gender"].nunique(dropna=False)),
        "Q3": int((df["last_name_std"] == "KENT").sum()),
        "Q4": str(df["projected_release_date"].min()),
        "Q5": "02381457" in df.index,   # the TDCJ number is the index from session 2 on
        "Q6": round(float(trusted_ages.mean()), 1),
        "Q7": int(df["escape_risk"].sum()),
        "Q8": int(df["protective_custody"].sum()),
    }


def scorecard(naive: dict, typed: dict) -> pd.DataFrame:
    table = pd.DataFrame({
        "Question": [QUESTIONS[q] for q in QUESTIONS],
        "Session 1 (as loaded)": [str(naive[q]) for q in QUESTIONS],
        "Session 4 (cleansed + typed)": [str(typed[q]) for q in QUESTIONS],
    }, index=list(QUESTIONS))
    table["Changed?"] = ["yes" if a != b else "" for a, b in
                         zip(table["Session 1 (as loaded)"], table["Session 4 (cleansed + typed)"])]
    return table
