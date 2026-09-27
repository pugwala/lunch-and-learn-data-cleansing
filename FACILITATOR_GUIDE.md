# Facilitator Guide

## Every session

**Before:** start the compute 10 minutes early (a cold Databricks cluster can take 5 or more minutes), then run the notebook top to bottom once yourself so the dataset is already generated and any environment surprises happen to you, not the room.

**Timing (45 minutes):** 0-5 people get food and run the setup cell · 5-10 recap and the idea for today · 10-38 hands-on · 38-45 wrap-up and questions. If you're short on time, cut the last exercise, not the wrap-up.

**Common snags:**
- *"NameError: PATHS is not defined"*: they skipped the setup cell. Run it.
- *Weird results after editing a cell out of order*: restart (Colab: Runtime → Restart session; Databricks: Run → Clear state) and run from the top.
- *Colab asks to "Run anyway" for a GitHub notebook*: expected. The notebook is from our repo.
- *The setup cell says REPO_URL needs setting*: the one-time setup in the README wasn't finished.

All numbers below come from seed 2026 with 100,000 rows. If someone's numbers differ, they've edited something upstream.

## Session 1 · Meet the Mess

**Point to land:** clean-looking answers can be wrong, and the most dangerous wrong answer is the one that looks reasonable.

| What | Expected |
|---|---|
| Distinct custody values including blanks (#1) | 22 |
| Q1 missing custody level: as loaded → after strip | 1,536 → 2,991 |
| Q2 distinct genders: as loaded → after strip | 19 → 3 |
| Missing gender after strip (#2) | 2,995 |
| Most common unit (#3) | Cleveland |

**Talking points:** `tdcj_number` became an integer and lost its leading zero on load. Ask the room what happens when that column gets joined back to the source system. The `protective_custody` peek sets up session 4: a column that should have two values has more than a dozen.

## Session 2 · Cleansing

**Point to land:** "unknown" is a decision the data owner makes, not the analyst.

| What | Expected |
|---|---|
| `NONE` in housing_restriction (#1) | 4,539 (kept as a real answer) |
| Spellings of MCDONALD (#2) | 53 |
| People with more than one record (#3) | 8,499 |
| SIDs missing | 2,997 |
| KENT records after standardizing | 3 |
| Nulls after a CSV round trip vs Parquet | 0 vs 81,039 |

**Talking points:** spend real time on `NONE` versus blank; it's the single most transferable idea in the series. Ask what a blank flag means in their own systems: an unchecked box (No) or unknown? Most people have never asked. Then land the ID point: `set_record_id` proves `tdcj_number` is present, 8 digits and never repeated before making it the index, and stops rather than guessing if any record fails. From there every output is labeled by TDCJ number, and saves keep the index (no `index=False`).

## Session 3 · Dates

**Point to land:** readable is not the same as true.

| What | Expected |
|---|---|
| Two-digit sentence dates (#1) | 9,761 |
| Birth dates in the future before fixing (#2) | 1,634 |
| Birth-date centuries corrected | 1,633 |
| Birth dates flagged | 8 (Wonder Woman ×3, Wolverine, Spider-Man 2099, Thor, Winter Soldier ×2) |
| People with conflicting birth dates across records | 8 |
| Releases in 2027 (#3) | 11,974 |
| Earliest / latest projected release | 1999-11-02 / 2099-12-31 (Hulk) |

**Talking points:** the `format="mixed"` demo gives different answers on pandas 2 and 3 for Wonder Woman. That's the argument against guessing. Winter Soldier is the best discussion case: one row is right, one is wrong, and no single-row rule can tell which.

## Session 4 · Types and the Payoff

**Point to land:** types are what make answers trustworthy, and single-character flags are the easiest place to be silently wrong.

| What | Expected |
|---|---|
| Average disciplinary points (#1) | 8.62 |
| Total restitution (#2) | 1,333,587,950.77 |
| Failed conversions: points / money / time / escape / protective custody / dampener | 206 / 183 / 185 / 220 / 0 / 292 |
| `astype(bool)` on protective_custody | 96,744 of 96,744 True (every record) |
| Protective custody: `== "Y"` vs mapped | 5,168 vs 6,878 |
| Dampener required (#4) | 31,350 |
| Escape risk: `== "Y"` vs mapped | 5,635 vs 14,194 |
| Overnight intakes (10 PM to 6 AM) | 14,470 |

**Talking points:** the `astype(bool)` demo usually gets a reaction; let it. Then connect it to their own Y/N and T/F columns: which reports count only `"Y"`? The Trust Test scorecard changes all eight answers, which is the payoff for the first four sessions. Then show the exceptions list: 1,086 values that didn't convert, one row each, keyed by TDCJ number.

## Session 5 · PySpark

**Point to land:** same rules, different engine; reconcile before you trust either.

| What | Expected |
|---|---|
| Distinct genders: raw / `trim()` / regex | 19 / 5 / 4 |
| Missing gender (#1) | 2,995 |
| Total restitution (#2) | 1,333,587,950.77 |
| Reconciliation | 16 of 16 match, including distinct TDCJ numbers = rows |

**Talking points:** Spark's `trim()` leaving tabs behind and Spark's `yy` meaning 2000-2099 are both examples of "same name, different behavior." Spark SQL is there for people who think in SQL; point it out explicitly. This session runs slower in Colab (a local Spark starts up); start it early.

## Session 6 · Text I

**Point to land:** rules are transparent and brittle; always score against an answer key.

| Attempt | F1 |
|---|---|
| 1. contains "good" | 0.118 |
| 2. any capitalization | 0.225 |
| 3. whole word | 0.180 |
| 4. lemma = good | 0.205 |
| 5b. full vocabulary | 0.768 |
| 6. + negation | 0.847 |
| 7. + domain exclusions | 0.898 |

**Talking points:** attempt 3 goes *down*, and attempt 4's lemmatizer turns contraband "goods" into praise. Those two surprises are the lesson. "Good time credit" is a TDCJ-specific false positive that nobody outside the agency would think to exclude.

## Session 7 · Text II

**Point to land:** meaning-based methods plus a few hundred labels usually beat hand-built rules, and governance comes before any real narrative touches a model.

Results depend on which embedding method loads (the notebook prints it). In testing with the fallback method (Latent Semantic Analysis), few-shot with 300 labels scored F1 0.947 against 0.899 for the session 6 rules, and zero-shot scored 0.588. Expect zero-shot to be clearly better with the sentence-transformer model; few-shot should stay on top either way. Dry-run this session on your compute so you know which numbers the room will see.

**Talking points:** negation is a known weak spot for embeddings; if semantic search surfaces *not a good day*, use it. Close with the governance point: real conduct notes are criminal justice information, and any AI Function or model use goes through AI governance review first.
