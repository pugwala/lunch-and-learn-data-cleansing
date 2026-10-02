# Data Cleansing Lunch & Learn

Seven 45-minute hands-on sessions that take report writers from "I loaded a CSV" to "I can prove these numbers are right." Everyone works on the same 100,000-row mock offender dataset (superheroes and villains, so there's zero chance of real data leaking), built on purpose to contain the problems our real source systems have: padded blanks, nine date formats, two-digit years, leading zeros, money written six ways, Y/N and T/F flags mixed in one column, race, religion, gang (STG), offense codes and unit names spelled dozens of ways, encoding damage in names, and free-text officer notes with web-form leftovers.

> ⚠️ **Mock data only.** Nothing in this course may ever be run against real offender data outside approved TDCJ systems.

## The sessions

| # | Notebook | What people learn |
|---|---|---|
| 1 | Meet the Mess | Look around a dataset; watch default loading give confident wrong answers (the Trust Test) |
| 2 | Cleansing | Load as text, strip and null, "unknown" codes and the data dictionary, text repair, names, keys, coded values mapped through crosswalks (unit locations, race, religion, STG affiliation, offense and violence codes), sensitive attributes, Parquet vs CSV |
| 3 | Dates | Profile shapes, explicit formats, century rules for two-digit years, impossible dates, cross-record checks |
| 4 | Types and the Payoff | Integers, exact money, time of day, yes/no (including single-character Y/N and T/F flags), ordered codes, rules that span two columns; the Trust Test rerun |
| 5 | PySpark | The same pipeline in Spark and Spark SQL, with the same crosswalk files, reconciled with pandas to the cent, saved as a Delta table |
| 6 | Text I: Finding "Good" | Keyword → case → whole word → lemma → vocabulary → negation → domain exclusions, each scored against an answer key |
| 7 | Text II: Meaning Over Words | Embeddings, semantic search, zero-shot and few-shot classification, topic modeling, AI governance |

Each session runs about 5 minutes of setup and recap, 30 minutes hands-on, and 10 minutes of wrap-up. Exercises check themselves, and answer keys are at the bottom of each notebook. See [FACILITATOR_GUIDE.md](FACILITATOR_GUIDE.md) for timing, talking points, and the numbers everyone should see.

## For staff: how to open a session

No command line needed anywhere. Open the course's Git folder in your Databricks Workspace, open the session notebook, attach it to the compute your instructor named (Serverless on Free Edition), and run the first cell with **Shift + Enter**.

The first cell builds the dataset (about 15 seconds). Everyone gets identical data because it's generated from a fixed seed.

## For the instructor: one-time setup

1. **Put the repo on GitHub.** A public repo clones into Databricks without Git credentials; a private repo needs a GitHub personal access token in your Databricks user settings (Linked accounts). The content is all mock data, but confirm publishing is acceptable.
2. **Databricks Git folder.** In the workspace: **Workspace → Create → Git folder**, paste the repo URL. Staff can each create their own, or you can share one read-only and have them clone it.
3. **Databricks settings in `lnl/config.py`** (shipped set for Free Edition; change them for the agency workspace):
   - `DATABRICKS_VOLUME`: a Unity Catalog volume the class can write to. Recommended. With it, the data is kept between sessions and session 5 reads the file directly with Spark, the way a real pipeline does. Without it, the data is rebuilt each session and session 5 hands it to Spark through pandas, and says so.
   - `DATABRICKS_CATALOG` and `DATABRICKS_SCHEMA`: where session 5 writes each person's Delta table (named with their username).
   - `EMBEDDING_MODEL`: if the workspace blocks huggingface.co, download the `sentence-transformers/all-MiniLM-L6-v2` model folder once, put it in a volume, and set this to that path. Without it, session 7 falls back to a weaker method and says so.
4. **Compute.** A recent **ML runtime (LTS)** is the easy choice; it includes pandas 2+, PyArrow, scikit-learn, spaCy, and sentence-transformers. Serverless also works if the volume is configured. Sessions 1 through 4 need pandas 2.0 or newer, which the setup cell checks.

### Databricks Free Edition (for dry runs)

Free Edition is serverless-only and limits outbound internet, so a few settings matter:

1. **Clone:** Workspace > Create > Git folder, paste the repo's HTTPS URL. A public repo needs no Git credentials.
2. **Environment:** open a notebook, open the Environment panel on the right, and pick environment version 4 or newer (pandas 2.2). Versions 1-3 ship pandas 1.5 and the setup cell will stop and say so. To make it the default for every notebook: Settings > Workspace admin > Compute > Base environments for serverless compute.
3. **Volume:** Catalog > `workspace` > `default` > Create > Volume, named `lunch_and_learn`. `lnl/config.py` already sets `DATABRICKS_VOLUME = "/Volumes/workspace/default/lunch_and_learn"`, `DATABRICKS_CATALOG = "workspace"`, `DATABRICKS_SCHEMA = "default"`, commit, and pull in the Git folder.
4. **Text sessions:** the spaCy model ships in `wheels/`, so session 6 doesn't need github.com. Session 7's sentence-transformer model is not in the Standard environment and Hugging Face may be blocked; expect the fallback method unless you stage the model in the volume (`EMBEDDING_MODEL`).

Free Edition is for learning and doesn't permit commercial use. Use it with this mock data only.

## Enterprise track (agency workspace)

After the sessions, [`enterprise/`](enterprise/README.md) runs the same cleansing rules the way the agency Databricks workspace is built to: data in Unity Catalog, a Lakeflow pipeline whose expectations warn, quarantine or stop, column masks and row filters on race and religion, a scheduled job that alerts by email and PagerDuty (ServiceNow or Vivantio by webhook), and data-quality monitoring. Six notebooks, about three hours in lunch-sized pieces, still on mock data. The settings are the `ENTERPRISE_*` block at the end of `lnl/config.py`; `enterprise/00_preflight` checks what your workspace allows and lists exactly what to ask an admin for. [`enterprise/README.md`](enterprise/README.md) covers setup, the bundle template, and the checklist for before you point it at real tables.

## What's in the repo

```
notebooks/          the seven sessions (ship without outputs; exercises use ___ blanks)
lnl/                course helper package; each session's cleansing lives here once taught
  config.py         the settings above
  env.py            environment detection, dependency checks, dataset generation
  pipeline.py       load_naive(), load_raw(), clean_through(n), answer_key()
  cleaning.py       session 2    dates.py        session 3    conversions.py  session 4
  domains.py        session 2: text repair, coded values, exceptions and cross-record checks
  reference/        the crosswalks: unit locations, race, religion, STG groups, NIBRS offense codes, violence codes (CSV)
  spark.py          session 5    text.py         sessions 6-7
  spark_pipeline.py sessions 2-4 in PySpark as functions, for the enterprise pipeline
  enterprise.py     enterprise track helpers: names, landing files, pipeline and job
  trust.py          the Trust Test questions      check.py        exercise checkers
generator/          gen_hero_offenders.py builds the dataset and the conduct-notes answer key
wheels/             spaCy's small English model, so session 6 works without github.com access
docs/               student-handout.pdf, the 10-page student handout, ready to print
extras/             supply_orders_type_conversion.ipynb + mock_supply_orders.csv: a standalone type-conversion walkthrough (keep the two files together)
enterprise/         the enterprise track: notebooks 00-05, pipeline/ (the Lakeflow pipeline source), tasks/ (the job's
                    quality check), databricks.yml (bundle template), README.md
```

## The dataset

27 columns, 100,000 rows, seed 2026. Highlights: `tdcj_number`, the record ID (written as 8 digits with leading zeros and never repeated; from session 2 on it is checked, stored as a whole number, BIGINT, and used as the index), and `sid_number`, written and stored the same way; names in random casing; dates in nine formats with two-digit years and a few impossible birth dates (Wonder Woman, 1213); `intake_time` in seven formats; `disciplinary_points` as `3`, `03`, `3.0`, `3 pts`, `three`; `restitution_owed` with `$`, `USD`, `(12.00)` credits, and `-` for zero; `escape_risk` with a dozen spellings of yes and no; **`protective_custody`** as single-character Y/N from a legacy system mixed with T/F from a newer one; **`dampener_required`** as single-character T/F; and `conduct_notes`, free-text officer notes whose true sentiment is in a separate answer key used only for scoring.

Seven data domains carry realistic damage, the same seven the agency's data-quality (IDMC) work defines:

| Domain | Column(s) | What's wrong with it | Standardized in session 2 as |
|---|---|---|---|
| Names | `last_name`, `first_name`, `alias` | random casing, accents typed only sometimes (García / GARCIA), encoding damage (MUÃ‘OZ) | `*_std` (upper case, accents folded) |
| Locations | `unit_location` | unit names in random casing, misspellings (Pittsburg, Gothem City), apostrophe variants (Hells Kitchen, Hell’s Kitchen), abbreviations (NYC, L.A., CDMX), three-letter unit codes typed in place of names (GOC), a stray "Unit" suffix, short names (Gotham), and placeholders that aren't units at all (`IN TRANSIT`, `TBD`, `UNASSIGNED`, `XX`) | `unit_name`, `unit_code` (through a unit crosswalk) |
| Race and ethnicity | `race` | 66 spellings (W, WHITE, Caucasian, W - WHITE, W/H…), legacy combined boxes, a few codes on no list | `race_code` (agency letter code) and `race_omb` (2024 federal categories) |
| Religious affiliation | `religion` | 80 spellings (RC, R.C., Cath, CATHLIC…), `NONE` vs `N/A` vs `DECLINED` | `religion_group`, `religion_family` |
| STG affiliation | `stg_affiliation` | fictional villain groups under many aliases, status written into the same field (`SUSP: H.Y.D.R.A.`, `INTERGANG (C)`), two groups in one cell | `stg_group`, `stg_status` |
| Offense and violence codes | `offense_code`, `primary_offense`, `violence_code` | NIBRS codes in lower case, with hyphens or spaces, `290.0` from a spreadsheet, lost leading zeros (`9B`), descriptions typed into the code field; violence codes (NV/V1/V2/V3) as `V-2`, `v2`, `2`, `N/V`, plus codes that don't exist (`V4`); some violence codes disagree with the offense | `offense_code_std`, `offense_against`, `violence_code_std` |
| Free text | `conduct_notes` | `<br>`, `&nbsp;`, curly quotes, doubled spaces, encoding damage (`â€™`) | repaired in place |

Race and religion are assigned at random, independently of every other column, and are never inferred. The STG groups are fictional. The crosswalks in `lnl/reference/` are the course's stand-in for reference data the data owner would maintain.

About 3% of every column is blank or whitespace-only.

To regenerate or make a different size: `python generator/gen_hero_offenders.py 100000 data.csv 2026`.

## How this was tested

All seven notebooks were executed end to end, both as delivered (blanks unfilled) and with the answer keys filled in, on pandas 3.0 with PySpark 4.2 in local mode. Sessions 1 through 4 were also run on pandas 2.2 with identical results. The Spark pipeline matches pandas on all 22 reconciliation metrics, and on every standardized coded value, record by record.

The enterprise track's Spark rules were run locally against the same data and match the pandas sessions; its Databricks SDK calls and bundle were checked against the SDK and bundle schemas, and the notebooks were reviewed against current Databricks documentation. None of it has run on a Databricks workspace yet.

Not yet tested: an actual Databricks workspace, and the sentence-transformer path in session 7 (the test environment couldn't reach Hugging Face, so the fallback method ran instead). Do a dry run of each notebook on your compute before the first session.
