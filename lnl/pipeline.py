"""Load the data at any stage of the course. Each stage applies everything taught up to that session."""
import pandas as pd

from . import env


def load_naive() -> pd.DataFrame:
    """What a first-timer does: pandas defaults. Session 1 uses this on purpose."""
    return pd.read_csv(env.paths()["data"])


def load_raw() -> pd.DataFrame:
    """The right way to load untrusted text: everything as text, nothing auto-converted."""
    return pd.read_csv(env.paths()["data"], dtype=str, keep_default_na=False, na_filter=False)


def answer_key() -> pd.DataFrame:
    """True sentiment of each conduct note. Only the text sessions use this, for scoring.
    Its TDCJ numbers are converted exactly like the main data's, so the two join: a text key and a
    number key never match."""
    from .cleaning import ids_to_numbers
    key = pd.read_csv(env.paths()["answer_key"], dtype=str)
    key["tdcj_number"], _ = ids_to_numbers(key["tdcj_number"])
    return key


def clean_through(session: int) -> pd.DataFrame:
    """Raw data with every step from sessions 2..`session` applied."""
    from . import cleaning, conversions, dates
    df = load_raw()
    if session >= 2:
        df = cleaning.session2(df)
    if session >= 3:
        df = dates.session3(df)
    if session >= 4:
        df = conversions.session4(df)
    return df
