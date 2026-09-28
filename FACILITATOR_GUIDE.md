# Facilitator Guide

## Before the first session: setup and dry run

The point of the dry run is simple: every number you show the room has already matched on your screen, so you can tell people with confidence that theirs will match too. Work top to bottom.

### 1. Finish the repo (10 minutes)

- [ ] Make sure the repo has the latest `lnl/` folder, including `lnl/domains.py` and the crosswalk files in `lnl/reference/`, and the latest generator. The data file's name includes a version number, so the first setup cell after a pull rebuilds the data once (about 15 seconds).
- [ ] Check the top level of the repo shows six folders (`docs`, `extras`, `generator`, `lnl`, `notebooks`, `wheels`) and `README.md`, `FACILITATOR_GUIDE.md`, `requirements.txt`.
- [ ] When the student handout changes, export it again and replace `docs/student-handout.pdf`, so the printed copy matches.

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
| 2 | Index: tdcj_number, type: Int64, unique: True; exercise 3 = 8,499; exercise 4 = 554 | Text lookup False, `id_value` lookup True; 1,700 coded values on the exceptions list; nulls: CSV 0, Parquet 123,226 |
| 3 | 1,633 centuries corrected; 8 birth dates flagged | Flagged rows labeled by TDCJ number |
| 4 | Restitution 1,333,587,950.77; 1,086 exceptions; 228 offense/violence conflicts; all 8 Trust Test answers changed | Exceptions list keyed by TDCJ number, padded to 8 digits |
| 5 | "tdcj_number is a valid record ID (BIGINT)"; reconciliation 22 of 22 | A Delta table line naming workspace.default; a "skip" line on a constraint is fine. If a table was saved by an earlier version of the course, it is replaced (new columns, BIGINT IDs) |
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
| Distinct values as loaded: race / religion / STG / offense / violence | 74 / 86 / 459 / 59 / 40 |

**Talking points:** pandas turned `tdcj_number` into an integer on its own and left `sid_number` as text. The problem isn't the number type (our tables store both IDs as BIGINT); it's that nobody chose it or checked it. Ask the room what happens when a number key is joined to a text key from another system. The `protective_custody` peek sets up session 4: a column that should have two values has more than a dozen. The coded-columns peek sets up session 2: four violence codes written 40 ways, and pandas' defaults quietly turning STG `N/A` and `None` ("no known affiliation," a real answer) into missing for about 25,700 records.

## Session 2 · Cleansing

**Point to land:** "unknown" is a decision the data owner makes, not the analyst, and so is what every code means.

| What | Expected |
|---|---|
| `NONE` in housing_restriction (#1) | 4,539 (kept as a real answer) |
| Spellings of MCDONALD (#2) | 53 |
| People with more than one record (#3) | 8,499 |
| SIDs missing | 2,997 |
| KENT records after standardizing | 3 |
| TDCJ and SID number type after `set_record_id` | Int64 (pandas' BIGINT) |
| `"02381457" in df.index` vs `id_value("02381457") in df.index` | False vs True |
| Values repaired: last names / officer notes | 159 / 7,908 |
| GARCIA as written, after folding accents | GARCIA 286, GARCÍA 125 |
| `race == "W"` as written vs `race_code == "W"` | 14,069 vs 28,765 |
| HYDRA: `== "HYDRA"` as written / any HYDRA / Confirmed (#4) | 113 / 1,608 / 554 |
| STG status: None / Confirmed / Suspected / Unverified / Former / blank | 86,526 / 3,612 / 3,046 / 2,612 / 1,091 / 3,113 |
| Offense 13A, offense 09B, violence V3: as written → standardized | 11,531 → 12,318; 1,301 → 2,257; 13,880 → 14,795 |
| Coded values on the exceptions list | 1,700 (offense 1,379, violence 104, religion 86, STG 77 including 30 two-group cells, race 54) |
| People (SIDs) with more than one race code | 113 |
| Nulls after a CSV round trip vs Parquet | 0 vs 123,226 |

**Talking points:** spend real time on `NONE` versus blank; it's the single most transferable idea in the series. Ask what a blank flag means in their own systems: an unchecked box (No) or unknown? Most people have never asked. Then land the ID point: `set_record_id` proves `tdcj_number` is present, 8 digits and never repeated, converts the TDCJ and SID numbers to whole numbers (BIGINT), and makes the TDCJ number the index. It stops rather than guessing if any record fails. Show both lookups side by side: typing `"02381457"` finds nothing and `id_value("02381457")` finds Superman. That's the whole lesson: a text key and a number key only match when both sides are converted the same way. From there every output is labeled by TDCJ number, IDs shown to people are padded back to 8 digits with `format_id`, and saves keep the index (no `index=False`).

Then the coded values. The idea to land is the **crosswalk**: a reference table the data owner agrees to, kept in Git, that the code only looks values up in. Ask the room who owns the reference tables in their own systems, and where the list of valid codes actually lives. Three moments to slow down on:
- **`N/A` means different things in different columns.** In STG it means no known affiliation (a real answer); in religion it means not recorded. One global list of "null words" would get one of them wrong.
- **A bare STG group name is Unverified, not Confirmed.** STG status feeds housing and classification, so a guess here affects a person. The groups are fictional villains; say so if anyone asks.
- **Race and religion are sensitive.** Standardize what was recorded, restrict access, and never infer either one from a name or anything else. In the mock data they're assigned at random, so any pattern someone "finds" in them is noise. Race gets two outputs, the agency letter code and the 2024 federal (OMB) categories, and old combined values like Asian/Pacific Islander say "Needs review" instead of being guessed into one.

Religion changing between a person's records is allowed (people change faith); race changing is flagged. That difference is a business rule, and it's the data owner's to make.

**Timing:** session 2 is now the fullest session in the series. If the room is behind at the 25-minute mark, run section 7 (coded values) as a live demo from the front and leave exercise #4 as optional; don't cut the ID section or the wrap-up.

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
| Records at V2 or above (ordered category) | 35,447 |
| Offense and violence codes that disagree | 228 |

**Talking points:** the `astype(bool)` demo usually gets a reaction; let it. Then connect it to their own Y/N and T/F columns: which reports count only `"Y"`? The Trust Test scorecard changes all eight answers, which is the payoff for the first four sessions. Then show the exceptions list: 1,086 values that didn't convert, one row each, keyed by TDCJ number and padded back to 8 digits because it goes to people. The offense/violence crosstab makes the cross-column rule visible: nearly every crime against a person is V2 or V3, and the 228 that aren't (or the reverse) are flagged, not fixed, because the data can't say which column is wrong.

## Session 5 · PySpark

**Point to land:** same rules, different engine; reconcile before you trust either.

| What | Expected |
|---|---|
| Distinct genders: raw / `trim()` / regex | 19 / 5 / 4 |
| Missing gender (#1) | 2,995 |
| Total restitution (#2) | 1,333,587,950.77 |
| Reconciliation | 22 of 22 match, including distinct TDCJ numbers = rows, the TDCJ hash total, confirmed STG records (3,612), unrecognized coded values (1,700) and offense/violence conflicts (228) |

**Talking points:** Spark's `trim()` leaving tabs behind and Spark's `yy` meaning 2000-2099 are both examples of "same name, different behavior." Spark SQL is there for people who think in SQL; point it out explicitly. The hash total (the sum of every TDCJ number) is the one legitimate time to add IDs: as a checksum that both sides hold the same records. Both engines read the same crosswalk files, so a fix to reference data fixes both. The first Spark command takes a moment on serverless; start compute early.

## Session 6 · Text I: Finding "Good"

**Point to land:** rules are transparent and easy to audit, but brittle; score every attempt against an answer key.

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
