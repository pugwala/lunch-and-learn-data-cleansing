# Facilitator Guide

## Before the first session: setup and dry run

The point of the dry run is simple: every number you show the room has already matched on your screen, so you can tell people with confidence that theirs will match too. Work top to bottom.

### 1. Finish the repo (10 minutes)

- [ ] Make sure the repo has the latest `lnl/spark.py` (it fixes the table display in session 5 on Databricks).
- [ ] Check the top level of the repo shows four folders (`generator`, `lnl`, `notebooks`, `wheels`) and `README.md`, `FACILITATOR_GUIDE.md`, `requirements.txt`.

### 2. Set up Databricks Free Edition (15 minutes)

- [ ] Create the volume: **Catalog > workspace > default > Create > Volume**, name `lunch_and_learn`, type managed.
- [ ] Nothing to edit: `lnl/config.py` ships set for Free Edition (`workspace` / `default` / `lunch_and_learn`).
- [ ] Clone or refresh the course: **Workspace > your user folder > Create > Git folder**, paste the repo's HTTPS URL. If the Git folder already exists, open its Git dialog and click **Pull**.
- [ ] Make pandas 2.2 the default: **Settings > Workspace admin > Compute > Base environments for serverless compute**, then star environment version 4 or newer.

### 3. Dry run every session (about 45 minutes each)

Run each notebook top to bottom once with the blanks empty, the way a student will: every "Your turn" cell should ask you to fill in the blank, and nothing should show a red error. Then fill in the answers from the bottom of the notebook and check that each checker says Correct.

| Session | Must match | Also expect |
|---|---|---|
| 1 | Exercise 1 = 22; Q1 goes 1,536 to 2,991 | Setup cell builds the data in about 15 seconds |
| 2 | Index: tdcj_number, unique: True; exercise 3 = 8,499 | Nulls: CSV 0, Parquet 81,039 |
| 3 | 1,633 centuries corrected; 8 birth dates flagged | Flagged rows labeled by TDCJ number |
| 4 | Restitution 1,333,587,950.77; 1,086 exceptions; all 8 Trust Test answers changed | Exceptions list keyed by TDCJ number |
| 5 | "tdcj_number is a valid record ID"; reconciliation 16 of 16 | A Delta table line naming workspace.default; a "skip" line on a constraint is fine |
| 6 | Final F1 about 0.898 | First run installs spaCy (a minute or two); attempt 4 may differ slightly |
| 7 | Few-shot scores highest | The notebook prints which embedding method loaded; on Free Edition expect the fallback |

- [ ] Sessions 1 to 5 match exactly. The data comes from a fixed seed, so a different number means something is wrong, not random.
- [ ] Sessions 6 and 7 are close; small differences there come from library versions.
- [ ] Time the hands-on part of each session at a newcomer's pace; it should fit in 28 minutes.
- [ ] Practice the recovery move: **Run > Clear state**, then run all cells from the top.

### 4. Pilot with one non-technical colleague

- [ ] Ask someone who fits your audience to do session 1 with only the notebook, no help. Watch where they pause; those are the moments to say out loud in the room.
- [ ] Fix or explain anything that confused them before the kickoff.

### 5. Decide where students will work

Recommended: the agency Databricks workspace, where an admin sets the default environment, volume, catalog and schema once and nobody in the room configures anything. Free Edition is fine for a dry run but doesn't permit commercial use, so check with the agency before using it for staff training. If students use their own Free Edition accounts, signing up, cloning the repo and setting the environment version become pre-work, and the pilot should test that path.

- [ ] Decide, and run the pilot on that same path.

### 6. Share and schedule

- [ ] Fill in the kickoff slides' placeholders: [date], [time], [room], [workspace link], [folder path].
- [ ] Share the course site and the slides. The site includes instructor keys; every notebook already has its answers at the bottom.

## Every session

**Before:** start the compute 10 minutes early (a cold Databricks cluster can take 5 or more minutes), then run the notebook top to bottom once yourself so the dataset is already generated and any environment surprises happen to you, not the room.

**Timing (45 minutes):** 0-5 people get food and run the setup cell · 5-10 recap and the idea for today · 10-38 hands-on · 38-45 wrap-up and questions. If you're short on time, cut the last exercise, not the wrap-up.

**Common snags:**
- *"NameError: PATHS is not defined"*: they skipped the setup cell. Run it.
- *Weird results after editing a cell out of order*: choose **Run → Clear state** and run from the top.
- *The setup cell says it can't find the course files*: the notebook was opened from outside the course's Git folder. Open it from inside the Git folder.
- *"This course needs pandas 2.0 or newer"*: the notebook is on an old serverless environment. Open the Environment panel on the right, pick version 4 or newer, click Apply, and rerun the setup cell.
- *Session 5 prints "No Unity Catalog volume configured"*: the volume is missing or its path in `lnl/config.py` has a typo. The session still runs; it hands the data to Spark through pandas.

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

**Talking points:** Spark's `trim()` leaving tabs behind and Spark's `yy` meaning 2000-2099 are both examples of "same name, different behavior." Spark SQL is there for people who think in SQL; point it out explicitly. The first Spark command takes a moment on serverless; start compute early.

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
