# Data Cleansing Lunch & Learn

Seven 45-minute hands-on sessions that take report writers from "I loaded a CSV" to "I can prove these numbers are right." Everyone works on the same 100,000-row mock offender dataset (superheroes and villains, so there's zero chance of real data leaking), built on purpose to contain the problems our real source systems have: padded blanks, nine date formats, two-digit years, leading zeros, money written six ways, Y/N and T/F flags mixed in one column, and free-text officer notes.

> ⚠️ **Mock data only.** Nothing in this course may ever be run against real offender data outside approved TDCJ systems. Google Colab is **not** approved for criminal justice information.

## The sessions

| # | Notebook | What people learn | Open in Colab |
|---|---|---|---|
| 1 | Meet the Mess | Look around a dataset; watch default loading give confident wrong answers (the Trust Test) | [Open](https://colab.research.google.com/github/YOUR-ORG/lunch-and-learn-data-cleansing/blob/main/notebooks/01_meet_the_mess.ipynb) |
| 2 | Cleansing | Load as text, strip and null, "unknown" codes and the data dictionary, casing, names, keys, Parquet vs CSV | [Open](https://colab.research.google.com/github/YOUR-ORG/lunch-and-learn-data-cleansing/blob/main/notebooks/02_cleansing.ipynb) |
| 3 | Dates | Profile shapes, explicit formats, century rules for two-digit years, impossible dates, cross-record checks | [Open](https://colab.research.google.com/github/YOUR-ORG/lunch-and-learn-data-cleansing/blob/main/notebooks/03_dates.ipynb) |
| 4 | Types and the Payoff | Integers, exact money, time of day, yes/no (including single-character Y/N and T/F flags), ordered codes; the Trust Test rerun | [Open](https://colab.research.google.com/github/YOUR-ORG/lunch-and-learn-data-cleansing/blob/main/notebooks/04_types_and_payoff.ipynb) |
| 5 | PySpark | The same pipeline in Spark and Spark SQL, reconciled with pandas to the cent, saved as a Delta table | [Open](https://colab.research.google.com/github/YOUR-ORG/lunch-and-learn-data-cleansing/blob/main/notebooks/05_pyspark.ipynb) |
| 6 | Text I: Finding "Good" | Keyword → case → whole word → lemma → vocabulary → negation → domain exclusions, each scored against an answer key | [Open](https://colab.research.google.com/github/YOUR-ORG/lunch-and-learn-data-cleansing/blob/main/notebooks/06_text_finding_good.ipynb) |
| 7 | Text II: Meaning Over Words | Embeddings, semantic search, zero-shot and few-shot classification, topic modeling, AI governance | [Open](https://colab.research.google.com/github/YOUR-ORG/lunch-and-learn-data-cleansing/blob/main/notebooks/07_text_meaning.ipynb) |

Each session runs about 5 minutes of setup and recap, 30 minutes hands-on, and 10 minutes of wrap-up. Exercises check themselves, and answer keys are at the bottom of each notebook. See [FACILITATOR_GUIDE.md](FACILITATOR_GUIDE.md) for timing, talking points, and the numbers everyone should see.

## For staff: how to open a session

No command line needed anywhere.

**Databricks (recommended).** Open the course's Git folder in your Workspace, open the session notebook, attach it to the compute your instructor named, and run the first cell with **Shift + Enter**.

**Google Colab (backup option).** Click the session's **Open** link above, sign in with a Google account, and run the first cell. It copies the course files into your Colab session automatically.

The first cell builds the dataset (about 15 seconds). Everyone gets identical data because it's generated from a fixed seed.

## For the instructor: one-time setup

1. **Put the repo somewhere Git can reach.** For the Colab links to work, Colab must be able to clone it without signing in, so a public GitHub repo is simplest. The content is all mock data, but confirm publishing is acceptable. Databricks works with private repos through your Git credentials.
2. **Point everything at your repo.** From a copy of the repo, run `python tools/set_repo.py OWNER/REPO`. This updates the Colab links, `lnl/config.py`, and the setup cell in all seven notebooks. Commit and push.
3. **Databricks Git folder.** In the workspace: **Workspace → Create → Git folder**, paste the repo URL. Staff can each create their own, or you can share one read-only and have them clone it.
4. **Databricks settings in `lnl/config.py`:**
   - `DATABRICKS_VOLUME`: a Unity Catalog volume the class can write to. **Required for session 5 on serverless or shared compute**, because Spark there can't read files from the driver's local disk.
   - `DATABRICKS_CATALOG` and `DATABRICKS_SCHEMA`: where session 5 writes each person's Delta table (named with their username).
   - `EMBEDDING_MODEL`: if the workspace blocks huggingface.co, download the `sentence-transformers/all-MiniLM-L6-v2` model folder once, put it in a volume, and set this to that path. Without it, session 7 falls back to a weaker method and says so.
5. **Compute.** A recent **ML runtime (LTS)** is the easy choice; it includes pandas 2+, PyArrow, scikit-learn, spaCy, and sentence-transformers. Serverless also works if the volume is configured. Sessions 1 through 4 need pandas 2.0 or newer, which the setup cell checks.

### Databricks Free Edition (for dry runs)

Free Edition is serverless-only and limits outbound internet, so a few settings matter:

1. **Clone:** Workspace > Create > Git folder, paste the repo's HTTPS URL. A public repo needs no Git credentials.
2. **Environment:** open a notebook, open the Environment panel on the right, and pick environment version 4 or newer (pandas 2.2). Versions 1-3 ship pandas 1.5 and the setup cell will stop and say so. To make it the default for every notebook: Settings > Workspace admin > Compute > Base environments for serverless compute.
3. **Volume:** Catalog > `workspace` > `default` > Create > Volume, named `lunch_and_learn`. Then in `lnl/config.py` set `DATABRICKS_VOLUME = "/Volumes/workspace/default/lunch_and_learn"`, `DATABRICKS_CATALOG = "workspace"`, `DATABRICKS_SCHEMA = "default"`, commit, and pull in the Git folder.
4. **Text sessions:** the spaCy model ships in `wheels/`, so session 6 doesn't need github.com. Session 7's sentence-transformer model is not in the Standard environment and Hugging Face may be blocked; expect the fallback method unless you stage the model in the volume (`EMBEDDING_MODEL`).

Free Edition is for learning and doesn't permit commercial use. Use it with this mock data only.

## What's in the repo

```
notebooks/          the seven sessions (ship without outputs; exercises use ___ blanks)
lnl/                course helper package; each session's cleansing lives here once taught
  config.py         the settings above
  env.py            environment detection, dependency checks, dataset generation
  pipeline.py       load_naive(), load_raw(), clean_through(n), answer_key()
  cleaning.py       session 2    dates.py        session 3    conversions.py  session 4
  spark.py          session 5    text.py         sessions 6-7
  trust.py          the Trust Test questions      check.py        exercise checkers
generator/          gen_hero_offenders.py builds the dataset and the conduct-notes answer key
wheels/             spaCy's small English model, so session 6 works without github.com access
tools/set_repo.py   one-time repo URL update
```

## The dataset

23 columns, 100,000 rows, seed 2026. Highlights: `tdcj_number`, the record ID (8 digits with leading zeros, never repeated; the index from session 2 on), and `sid_number`, also with leading zeros; names in random casing; dates in nine formats with two-digit years and a few impossible birth dates (Wonder Woman, 1213); `intake_time` in seven formats; `disciplinary_points` as `3`, `03`, `3.0`, `3 pts`, `three`; `restitution_owed` with `$`, `USD`, `(12.00)` credits, and `-` for zero; `escape_risk` with a dozen spellings of yes and no; **`protective_custody`** as single-character Y/N from a legacy system mixed with T/F from a newer one; **`dampener_required`** as single-character T/F; and `conduct_notes`, free-text officer notes whose true sentiment is in a separate answer key used only for scoring. About 3% of every column is blank or whitespace-only.

To regenerate or make a different size: `python generator/gen_hero_offenders.py 100000 data.csv 2026`.

## How this was tested

All seven notebooks were executed end to end, both as delivered (blanks unfilled) and with the answer keys filled in, on pandas 3.0 with PySpark 4.2 in local mode. Sessions 1 through 4 were also run on pandas 2.2 with identical results. The Spark pipeline matches pandas on all 16 reconciliation metrics.

Not yet tested: an actual Databricks workspace or Colab session, and the sentence-transformer path in session 7 (the test environment couldn't reach Hugging Face, so the fallback method ran instead). Do a dry run of each notebook on your compute before the first session.
