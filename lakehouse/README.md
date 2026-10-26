# Home-lab DuckDB lakehouse

The course's mock data in a DuckDB lakehouse on a single Proxmox VM: DuckLake tables (Postgres catalog,
Parquet on local disk), loaded by the same generator and the same session 2–4 cleansing the class uses, so
every number matches the Facilitator Guide. dbt builds gold, and Dagster runs it all at 05:00 Central.

> ⚠️ **Mock data only**, same rule as the rest of the repo. DuckDB and DuckLake have no table grants, row filters
> or column masks: anyone with the catalog password can read every column, race and religion included.

## What's here

| Path | What it does |
| --- | --- |
| `compose.yaml` | `core` profile: Postgres catalog, Jupyter, Dagster (UI + daemon), a one-shot loader, Caddy |
| `.env.example` | Every setting and secret; copy to `.env` |
| `postgres/init/` | `01-roles.sh`: `ducklake`, `superset`, `dagster` databases and the `lake_rw` / `lake_ro` roles. `02-dagster.sh` and `03-superset.sh`: the Dagster and Superset roles (safe to re-run) |
| `jupyter/Dockerfile` | `pyspark-notebook` + course requirements + DuckDB 1.5.6, dbt-duckdb, Dagster; extensions pre-installed. One image for every Python process, so DuckDB versions never drift |
| `lake.py` | Helpers: `lake_connect()`, `lake_ro_connect()`, `run()`, `to_lake_frame()`, `exceptions_table()`, `trust_test_table()` |
| `sql.py` | `%load_ext lakehouse.sql`: jupysql's `%sql` on the lake, read-only as `lake_ro` |
| `load_lake.py` | Generates the data if needed and lands reference, bronze and silver as one DuckLake snapshot |
| `tests/test_parity.py` | 19 checks that the lake matches the Facilitator Guide's numbers |
| `dbt/` | Gold models and tests (dbt-duckdb, attaches DuckLake) |
| `orchestration/` | Dagster definitions and instance config |
| `scripts/set-dagster-ui-password.sh` | Puts a login in front of the Dagster UI (it has none of its own) |

## Tables

| Table | Contents |
| --- | --- |
| `ref.*` | The six crosswalks from `lnl/reference/`, plus `null_tokens` (the documented "unknown" codes from `lnl.cleaning`) |
| `bronze.hero_offenders_raw` | The generator CSV as written, all VARCHAR, plus `_run_id`, `_ingested_at`, `_source_file`, `_seed`, `_data_version` |
| `bronze.conduct_notes_answer_key` | The answer key as written |
| `silver.hero_offenders` | `clean_through(4)`: BIGINT keys, DATE, TIME, DECIMAL(12,2), BOOLEAN; ordered codes as text plus `<col>_rank` |
| `silver.hero_offenders_exceptions` | One row per flagged value: `domain_unmapped`, `conversion_failed`, `dob_check`, `offense_violence_conflict`. Records are flagged, never dropped |
| `silver.conduct_notes_answer_key` | Answer key with BIGINT `offender_number`, joins to silver |
| `gold.dim_unit`, `gold.dim_offense`, `gold.dim_stg_group` | Dimensions from the reference data |
| `gold.fct_offender_record` | One row per record: keys, codes, measures, `exception_count`. No names, aliases, notes or date of birth. Race, religion and STG columns carry `meta.sensitivity: restricted` for the semantic layer to mask |
| `gold.dq_scorecard` | Score per dimension × column × rule for the latest load: completeness, conformity, validity, consistency, uniqueness, timeliness. Accuracy isn't scored (no ground truth) |
| `ops.load_runs` | One row per load: counts, seconds, DuckLake snapshot ID |

## The daily run

```
load_lake ──► parity check (blocking) ──► dbt build (gold models + tests)
```

- **Parity check fails** → the run fails and gold isn't rebuilt. Silver has already been written; DuckLake time
  travel (`AT (VERSION => n)`) gets the previous version back if you need it.
- **dbt test at `error` severity fails** (keys, row counts, accepted values) → the run fails.
- **dbt test at `warn` severity fails** (reference lookups, the 95% quality floor) → recorded, the run succeeds.
  The floor fires today on `disciplinary_points` completeness (94.15%): blanks plus `N/A`.

Jobs: `daily_lake` (scheduled 05:00 Central, on by default) and `regenerate_lake` (rebuilds the CSV first).
One run at a time, so DuckLake writers never race.

## Setup, upgrades and health checks: `scripts/bootstrap.sh`

One script sets up a new VM, upgrades an existing one, and proves the result. Every step checks first and only
does what's missing, so it's safe to run again at any time; on a healthy install it changes nothing.

```bash
# New VM: Debian with Docker, your user in the docker group, the data disk formatted and mounted at /lake
# (see below). Then:
git clone -b duckdb-lakehouse https://github.com/pugwala/lunch-and-learn-data-cleansing.git ~/lunch-and-learn-data-cleansing
bash ~/lunch-and-learn-data-cleansing/lakehouse/scripts/bootstrap.sh

# Upgrade: pull, then run it again. It rebuilds the image (cached layers make this quick) and restarts what changed.
git -C ~/lunch-and-learn-data-cleansing pull && bash ~/lunch-and-learn-data-cleansing/lakehouse/scripts/bootstrap.sh

bash lakehouse/scripts/bootstrap.sh --check    # verify only, changes nothing
bash lakehouse/scripts/bootstrap.sh --reload   # also re-run the loader even if the lake has data
```

What it does, in order:

1. **Tools.** Checks Docker, Compose and your access to the Docker daemon; installs `acl` if it's missing.
2. **Data disk.** Stops unless `/lake` is a mounted disk, because data written to an unmounted `/lake` lands on the
   OS disk. It never formats a disk. Then creates the `/lake` folders and makes the container user (1000:100) own them.
3. **Repo permissions.** If you aren't uid 1000, grants you and the containers write access with ACLs.
4. **Secrets.** Creates `.env` from `.env.example`, generates any secret still `CHANGE_ME`, never overwrites one that's
   set, stops on duplicate keys, asks for the Dagster UI login if it's missing, and keeps `.env` at mode 600.
5. **Image and services.** Builds the image, starts Postgres, creates the `dagster` role (Postgres runs
   `postgres/init/` only on an empty volume), starts the six long-running services by name, and reloads Caddy.
6. **Lake data.** Runs the loader if silver or `ref.null_tokens` is missing.
7. **Proof.** Waits for every health check, runs the 19 parity tests, and checks that both sites answer through Caddy,
   on the Tailscale name with certificate checking on, so an untrusted certificate fails.

**Preparing the data disk** is the one step left to you, because formatting erases a disk. Find it by size
(`lsblk -o NAME,SIZE,FSTYPE,LABEL`), then use its stable name from `/dev/disk/by-id/`, never `sda`/`sdb`,
which can swap between boots:

```bash
sudo mkfs.ext4 -L lake /dev/disk/by-id/<data disk>
grep -q 'LABEL=lake' /etc/fstab || echo 'LABEL=lake /lake ext4 defaults,noatime,nofail 0 2' | sudo tee -a /etc/fstab
sudo mkdir -p /lake && sudo mount /lake && findmnt /lake
```

**Reaching the sites.** Install [Tailscale](https://tailscale.com/download/linux) on the VM and sign in with
`sudo tailscale up --accept-dns=false`. Keep `--accept-dns=false`: MagicDNS would rewrite the VM's resolver, which can
break containers' internet lookups. Turn on HTTPS certificates in the Tailscale admin console. `bootstrap.sh` sets
`LAKE_TS_HOST` from `tailscale status`, and Caddy then serves, with a real certificate from Tailscale:

- `https://<LAKE_TS_HOST>/jupyter/`: Jupyter, token login
- `https://<LAKE_TS_HOST>/dagster/`: Dagster, behind Caddy basic auth

Any device on your tailnet can open them, at home or away, with no hosts entries and no browser warnings. This is
the only name Caddy serves: the earlier `*.lake.home.arpa` LAN names were retired on 2026-10-25.

Two Compose habits the script follows: `docker compose --profile core up -d` with no service names also runs the
one-shot `load-lake` loader, so name the services; and `.env` is hidden inside the containers (dbt 1.12 would
otherwise load every secret in it), so give a container a setting through `compose.yaml`, not `.env` alone.

## Querying

From a notebook, the quickest way is SQL. One line connects jupysql to the lake, read-only, as `lake_ro`:

```
%load_ext lakehouse.sql
```

Then `%sql` for one line, or a cell that starts with `%%sql`:

```sql
%%sql
SELECT question_id, as_loaded, cleansed FROM gold.trust_test ORDER BY question_id
```

The lake is selected, so name tables by schema (`bronze`, `silver`, `gold`, `ref`, `ops`). Nothing can be written:
DuckLake is attached `READ_ONLY` and `lake_ro` has read grants only. A query that fails doesn't break the next one
(the extension rolls the failed transaction back). `lake_ro`'s password reaches DuckDB through a temporary secret,
never the `ATTACH` string, because DuckLake's connection errors print that string. Course notebooks don't load this.

The same connection in Python, as a DuckDB connection:

```python
from lakehouse import lake
con = lake.lake_ro_connect()
con.sql("SELECT dimension, round(100 * sum(passed) / sum(checked), 2) AS pct FROM gold.dq_scorecard GROUP BY 1").df()
```

Compose sets `PYTHONPATH=/home/jovyan/course` for Jupyter, so `lakehouse` imports from a notebook in any folder.

dbt by hand, inside the Jupyter container:

```bash
docker compose exec jupyter dbt build --project-dir lakehouse/dbt --profiles-dir lakehouse/dbt
```

### Through Cube, the governed path

Everyone except engineering queries through Cube (`cube/`), because DuckDB has no grants or masks of its own.
Cube attaches DuckLake read-only as `lake_ro` and exposes gold as cubes; its SQL API speaks the Postgres wire
protocol at `cube:15432` (user `lake`, password `CUBE_SQL_PASSWORD`), on the Compose network only. From a notebook:

```python
import os, psycopg2
cube = psycopg2.connect(host="cube", port=15432, dbname="cube", user="lake", password=os.environ["CUBE_SQL_PASSWORD"])
cur = cube.cursor()
cur.execute("SELECT dimension, MEASURE(pass_rate) FROM dq_scorecard GROUP BY 1")
cur.fetchall()
```

Cubes: `offender_records` (joined to `units` and `offenses`) and `dq_scorecard`. Columns dbt tags restricted (race,
religion, STG) are left out until Phase 5 adds masking by role, and the acceptance test fails if one appears.

Cube is pinned at v1.8.3, which bundles DuckDB **1.5.5**: a deliberate, read-only exception to "1.5.6 everywhere".
No Cube release ships 1.5.6 yet, and the 1.6 line's DuckDB is too old to read the DuckLake catalog. Drop the
exception when a Cube release bundles 1.5.6. After changing a model file, restart Cube:
`docker compose --profile core restart cube`.

### Superset: dashboards and SQL Lab

Superset runs at `https://<LAKE_TS_HOST>/superset/` (`SUPERSET_APP_ROOT=/superset`, a feature Superset still calls
beta). Log in as `admin`; the password is `SUPERSET_ADMIN_PASSWORD` in `.env`, so read it in your own SSH session,
never in chat. Its **only database is Cube**, registered on every start by `superset/start.sh`, so every chart and
SQL Lab query goes through the governed layer and nothing in Superset reads DuckLake directly. Its own metadata
(users, charts, dashboards) lives in catalog-pg's `superset` database, owned by the `superset` role
(`postgres/init/03-superset.sh`).

**Trust Test dashboard.** The course's Trust Test scorecard (session 1's eight questions, answered from a default load and from the cleansed, typed data) flows through every layer: the loader computes it with `lnl.trust` into `silver.trust_test`, dbt publishes `gold.trust_test`, Cube serves it as the `trust_test` cube, and `superset/dashboards.py` builds the **Trust Test** dashboard on it through Superset's API. `bootstrap.sh` runs that script (with `--check` when checking), and a re-run changes nothing.

In SQL Lab, query cubes with Cube's SQL: `SELECT MEASURE(record_count) FROM offender_records`.

Compose mounts `caddy/` and `superset/` as **folders**, not single files. Editors save by writing a new file, and a
single-file bind mount keeps showing the old one, which is how a Caddyfile change once went unseen.

## Test status

| Piece | Where it has run |
| --- | --- |
| Loader, 19 parity checks, DuckLake on Postgres, image build, Postgres roles | On the VM |
| Dagster webserver and daemon with Postgres storage, their healthchecks, Caddy basic auth | On the VM |
| `scripts/bootstrap.sh`: 22 bats tests (`bats lakehouse/tests/bootstrap.bats`, needs `apt install bats`), each shown to fail when the behaviour it covers is broken | On the VM, with stub commands; never touches the live stack |
| dbt gold models and their tests (25 today), Dagster job (both jobs, plus the blocking check stopping gold) | Sandbox, against a plain DuckDB file |
| Cube v1.8.3 over gold, read-only as `lake_ro`: 6 acceptance tests (`pytest lakehouse/acceptance/test_cube.py`); the restricted-column check was shown to fail on a deliberate leak | On the VM |
| Superset 6.1.0 under `/superset`, Cube as its only database, the Trust Test dashboard: 6 acceptance tests (`pytest lakehouse/acceptance/test_superset.py`); the only-Cube check was shown to fail with a decoy database added | On the VM |
| dbt on DuckLake: the first `daily_lake` run (load 7.5 s, gold 5.5 s, one expected `dq_score_floor` warning), checked by 14 acceptance tests: `docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance` | On the VM |
| Trust Test from loader to dashboard (`silver.trust_test` → `gold.trust_test` → Cube → Superset): 1 parity check, 4 acceptance tests and 3 bats tests, each shown failing before the code existed | On the VM |
| `%load_ext lakehouse.sql` (jupysql, read-only as `lake_ro`): 6 acceptance tests (`pytest lakehouse/acceptance/test_sql.py`), plus a real notebook kernel run from outside the repo; the password and read-only checks were shown to fail on deliberate breaks | On the VM |

## Next phases

Superset and Cube (semantic layer), then OpenMetadata (ontology), then the monitoring profile. See the plan
document for the order and the reasoning.
