# Home-lab DuckDB lakehouse

The course's mock data in a DuckDB lakehouse on a single Proxmox VM: DuckLake tables (Postgres catalog,
Parquet on local disk), loaded by the same generator and the same session 2–4 cleansing the class uses, so
every number matches the Facilitator Guide. dbt builds gold, and Dagster runs it all at 05:00 Central.

> ⚠️ **Mock data only**, same rule as the rest of the repo. DuckDB and DuckLake have no table grants, row filters
> or column masks: anyone with the catalog password can read every column, race and religion included.

## What's here

| Path | What it does |
| --- | --- |
| `compose.yaml` | `core` profile: Postgres catalog, Jupyter, Dagster (UI + daemon), a one-shot loader, Cube, Superset, the MCP server, OpenSearch, OpenMetadata (with a one-shot `om-migrate`), Fuseki, the monitoring stack (Prometheus, node-exporter, cAdvisor, postgres-exporter, blackbox, Pushgateway, Alertmanager, Grafana), ntfy (where alerts go), the nightly `catalog-backup`, Caddy |
| `.env.example` | Every setting and secret; copy to `.env` |
| `postgres/init/` | `01-roles.sh`: `ducklake`, `superset`, `dagster` databases and the `lake_rw` / `lake_ro` roles. `02-dagster.sh`, `03-superset.sh` and `04-openmetadata.sh`: the Dagster, Superset and OpenMetadata roles and databases (safe to re-run) |
| `jupyter/Dockerfile` | `pyspark-notebook` + course requirements + DuckDB 1.5.6, dbt-duckdb, Dagster; extensions pre-installed. One image for every Python process, so DuckDB versions never drift |
| `lake.py` | Helpers: `lake_connect()`, `lake_ro_connect()`, `run()`, `to_lake_frame()`, `exceptions_table()`, `trust_test_table()` |
| `sql_magic.py` | `%load_ext lakehouse.sql_magic`: jupysql's `%sql` on the lake, read-only as `lake_ro` |
| `load_lake.py` | Generates the data if needed and lands reference, bronze and silver as one DuckLake snapshot |
| `tests/test_parity.py` | 19 checks that the lake matches the Facilitator Guide's numbers |
| `dbt/` | Gold models and tests (dbt-duckdb, attaches DuckLake) |
| `orchestration/` | Dagster definitions and instance config |
| `scripts/set-dagster-ui-password.sh` | Puts a login in front of the Dagster UI (it has none of its own) |
| `scripts/stage_model.py` | Stages session 7's embedding model on the lake disk at a pinned revision (bootstrap.sh runs it) |
| `mcp/` | The MCP server (`server.py`, Dockerfile, pinned requirements): `query_metric` over Cube, `define_term` over OpenMetadata's glossary |
| `openmetadata/` | `setup_admin.py` (replaces the default admin password, creates the read-only `mcp-reader`) `om_sync.py` (pushes the glossary, the Sensitivity tags and every lake table into OpenMetadata) and `om_to_rdf.py` (publishes that meaning to Fuseki as RDF) |
| `monitoring/` | Prometheus config, probe targets and alert rules (`prometheus/`), Alertmanager config (`alertmanager/`), blackbox modules (`blackbox/`), Grafana provisioning, the Home Lakehouse dashboard and `set_home.py` (`grafana/`) |
| `ntfy/` | ntfy, where alerts go: `server.yml` (deny-all, which user may do what), `start.sh` (declares the users from `.env`), `templates/lakehouse.yml` (the notification and its priority) |
| `ontology/` | Business rules over the Fuseki graphs: `rules/*.rq` (deductions, one SPARQL CONSTRUCT each), `shapes.ttl` (SHACL constraints), `check_rules.py` (asks Fuseki to validate them) |
| `products/` | The data products, one descriptor each (`custody-population.yml`, `custody-offenses.yml`), and their domains (`domains/`) |
| `metrics.py` | `push()`: Dagster's pipeline and data-quality numbers to the Pushgateway (never fails a run) |
| `backup/backup.sh` | The nightly catalog backup: `pg_dump` of every app database, checked, 14 days kept |
| `fuseki/` | Apache Jena Fuseki 6.2.0, built from Apache's verified release: `Dockerfile`, `config.ttl` (one dataset; writes need the writer user), `shiro.ini` (no users; admin only inside the container), `start.sh` |
| `cube/sync_masks.py` | Makes Cube mask exactly the columns OpenMetadata tags `Sensitivity.Restricted` |
| `glossary/glossary.yml` | The glossary: 15 terms with definitions, synonyms, columns, metrics and typed relations; `om_sync.py` loads it into OpenMetadata |
| `08_from_lake.ipynb` | Session 8 for the lakehouse: the Trust Test answered in SQL over silver, checked against `gold.trust_test` |

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
| `gold.dim_person` | One row per known person (SID): record count, first and latest sentence date, repeat, concurrent records. Derived facts only, no personal data |
| `gold.fct_offender_record` | One row per record: keys, codes, measures, `exception_count`. No names, aliases, notes or date of birth. Race, religion and STG columns carry `meta.sensitivity: restricted` for the semantic layer to mask |
| `gold.dq_scorecard` | Score per dimension × column × rule for the latest load: completeness, conformity, validity, consistency, uniqueness, timeliness. Accuracy isn't scored (no ground truth) |
| `ops.load_runs` | One row per load: counts, seconds, DuckLake snapshot ID |
| `mart_custody.custody_snapshot_monthly` | The custody mart: one row per offender record per month-end it is open, carrying the SID (4.24 M rows, Oct 1996 to the last complete month). Enforced dbt contract |
| `mart_custody.snapshot_coverage` | One row saying what the snapshot covers and leaves out (records with no sentence or release date). Enforced dbt contract |

## The daily run

```
load_lake ──► parity check (blocking) ──► dbt build (gold, then mart_custody) ──► openmetadata/lake ──► fuseki/glossary
                                                                                  ├──► monitoring/data_quality
                                                                                  └──► monitoring/data_products
```

- **Parity check fails** → the run fails and gold isn't rebuilt. Silver has already been written; DuckLake time
  travel (`AT (VERSION => n)`) gets the previous version back if you need it.
- **dbt test at `error` severity fails** (keys, row counts, accepted values) → the run fails.
- **dbt test at `warn` severity fails** (reference lookups, the 95% quality floor) → recorded, the run succeeds.
  The floor fires today on `disciplinary_points` completeness (94.15%): blanks plus `N/A`.
- **`openmetadata/lake`** runs `om_sync.py`, so OpenMetadata knows today's tables. Its non-blocking check,
  `cube_masks_match_openmetadata`, warns when a steward has tagged a column Restricted that the Cube model doesn't
  mask yet; fixing that (rewriting the model, restarting Cube) stays a reviewed `bootstrap.sh` step.
- **`fuseki/glossary`** runs `om_to_rdf.py`, so the SPARQL graphs carry the same day's meaning, then its non-blocking
  check `business_rules_hold` validates `ontology/shapes.ttl` and pushes the violations per rule to Prometheus.

Jobs: `daily_lake` (scheduled 05:00 Central, on by default), `regenerate_lake` (rebuilds the CSV first) and
`notebook_smoke` (Sundays 06:00 Central, on by default): it runs the seven course notebooks as delivered plus
`08_from_lake.ipynb` with papermill, saves the executed copies in `/lake/notebook-runs/<date>_<run>/`, and fails
if any cell errors. It's an op job rather than an asset, so `daily_lake`'s all-asset selection never includes it.
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
   `postgres/init/` only on an empty volume), generates OpenMetadata's own JWT signing keys once (under
   `/lake/openmetadata/keys`, never in the repo), starts the twenty-one long-running services by name, reloads Caddy, makes Prometheus and Alertmanager reread
   their config (SIGHUP), and restarts Dagster if catalog-pg was recreated (its pooled connections would be dead).
6. **Lake data and meaning.** Runs the loader if silver or `ref.null_tokens` is missing, stages session 7's model,
   replaces OpenMetadata's default admin password, runs `om_sync.py`, `om_to_rdf.py` (Fuseki's graphs), `ontology/check_rules.py` (the business rules), then
   `cube/sync_masks.py` (restarting Cube if the masks changed), builds the Trust Test and Custody dashboards, and
   makes Home Lakehouse Grafana's home page.
7. **Proof.** Waits for every health check, runs the 19 parity tests, and checks that every site answers through
   Caddy, on the Tailscale name with certificate checking on, so an untrusted certificate fails.

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
- `https://<LAKE_TS_HOST>/superset/`: Superset, its own login
- `https://<LAKE_TS_HOST>/openmetadata/`: OpenMetadata, its own login
- `https://<LAKE_TS_HOST>/fuseki/`: Fuseki, SPARQL over the glossary and column tags (reads open, writes need `writer`)
- `https://<LAKE_TS_HOST>/mcp`: the MCP server, bearer token
- `https://<LAKE_TS_HOST>/grafana/`: Grafana, its own login (`admin` / `GRAFANA_ADMIN_PASSWORD`)
- `https://<LAKE_TS_HOST>/prometheus/` and `/alertmanager/`: behind the Dagster basic-auth login

Any device on your tailnet can open them, at home or away, with no hosts entries and no browser warnings. This is
the only name Caddy serves: the earlier `*.lake.home.arpa` LAN names were retired on 2026-10-25.

Two Compose habits the script follows: `docker compose --profile core up -d` with no service names also runs the
one-shot `load-lake` loader, so name the services; and `.env` is hidden inside the containers (dbt 1.12 would
otherwise load every secret in it), so give a container a setting through `compose.yaml`, not `.env` alone.

**Pinned versions.** Nothing floats: every image and base image names an exact release, chosen at least two weeks
old when it was pinned, and `acceptance/test_pins.py` fails if one doesn't. The pins live in `compose.yaml` (the
`${X_TAG:-default}` defaults) and the Dockerfiles; `.env` can override one with `X_TAG=<exact release>`, and
`bootstrap.sh` fails on a floating override (`latest` or a bare major such as `17`), which would undo the pins.

| Image | Pinned at | Notes |
| --- | --- | --- |
| `quay.io/jupyter/pyspark-notebook` | `2026-10-12` | A dated docker-stacks build; until 2026-10-26 it was `latest`, and each rebuild pulled that day's |
| `postgres` (catalog-pg, catalog-backup) | `17.11-trixie` | PostgreSQL 17.11, released 2026-08-13; the tag names the OS because `17` and `17.11` get rebuilt |
| `caddy` | `2.11.7` | Released 2026-10-03 |
| `torch` (CPU build) | `2.14.1` | Released 2026-09-30; from PyTorch's own index, which the date cutoff below can't apply to |
| `dbt-core` | `1.12.5` | Released 2026-09-15; 1.12.6 (2026-10-14) was pinned earlier without the two-week check |
| Everything else | as in `compose.yaml` and the Dockerfiles | Each chosen in its phase; the Work Log has the dates |

Python packages resolve **as of a date**: every `pip install` from PyPI in `jupyter/Dockerfile` runs with
`--uploaded-prior-to` the `PIP_CUTOFF` build arg, which is the base image's date (`2026-10-12`). So the course's
open-ended `requirements.txt` (a course file, left as the course ships it) and every transitive dependency resolve
to what existed then, never to what was uploaded the day of a rebuild; before the cutoff, today's rebuilds had
pulled spaCy and transformers released that same morning. The lakehouse's own packages
(`jupyter/requirements-lake.txt`) are pinned with `==` as well, each released before the cutoff (a test checks with
PyPI). The cutoff is build-only, so the course's setup cells can still install what they need at run time.

## Querying

From a notebook, the quickest way is SQL. One line connects jupysql to the lake, read-only, as `lake_ro`:

```
%load_ext lakehouse.sql_magic
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

Compose sets `PYTHONPATH=/home/jovyan/course` for Jupyter and Dagster, so `lakehouse` imports from a notebook in
any folder. `08_from_lake.ipynb` is a worked example: the eight Trust Test questions in SQL over silver.

### The course notebooks on the VM

All seven run unmodified in the lakehouse's Jupyter. Session 7's embedding model (`all-MiniLM-L6-v2`, the one
`lnl/config.py` names) is staged once on the lake disk by `bootstrap.sh` at a pinned revision, and compose sets
`HF_HOME=/lake/models/huggingface` and `HF_HUB_OFFLINE=1`, so the notebooks never download anything and the
model survives image rebuilds. The course's own setting is unchanged.

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

Cubes: `offender_records` (joined to `units` and `offenses`), `dq_scorecard` and `trust_test`. Names, aliases,
birth dates and notes never reach Cube (gold leaves them out). The restricted columns (race, religion, STG) are in
the model but **masked**: every query, SQL or REST, gets `***` for them, unless its token's `groups` include
`restricted_reader` (`cube.js` maps `groups` to Cube's groups; `access_policy` in `model/offender_records.yml` does
the rest). Which columns are restricted is decided in OpenMetadata, not here: see the tags-to-masks loop below. In
the model, a mask must be SQL (`mask: {sql: "'***'"}`); Cube's YAML compiler turns a bare `"***"` into null.

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

**Custody dashboard.** The same script builds **Custody** (`/superset/dashboard/custody/`) on the custody data products' Cube views: people in custody at each month-end, people by custody level and by offense category at the latest month-end, and new sentences per month. Each chart's metric is Cube's own measure, written as `MEASURE(people_in_custody)`, so people stay distinct SIDs counted by Cube, not a sum Superset makes up. "Latest" is Cube's `is_latest_month_end` flag, so the tables move on by themselves when a month completes. Each dashboard is one entry in `DASHBOARDS` in `superset/dashboards.py`; add a dict there to add one.

In SQL Lab, query cubes with Cube's SQL: `SELECT MEASURE(record_count) FROM offender_records`.

Compose mounts `caddy/` and `superset/` as **folders**, not single files. Editors save by writing a new file, and a
single-file bind mount keeps showing the old one, which is how a Caddyfile change once went unseen.

### MCP: governed metrics and definitions for an AI agent

An MCP server at `https://<LAKE_TS_HOST>/mcp` gives an agent two tools instead of SQL access:

- `query_metric(metric, group_by=[], limit=100)`: a Cube measure by name (`record_count`, `total_restitution`,
  `answers_changed`, ...), optionally grouped by dimensions (`custody_level`, `unit_name`, `offense_against`, ...).
  It goes through Cube's REST API, so it sees only Cube's model; an unknown name lists the real ones.
- `define_term(term)`: a glossary term from OpenMetadata, matched by name or synonym in any case, with its typed
  relations in both directions. An edit made in OpenMetadata's UI shows up at once.

Every request needs `Authorization: Bearer <MCP_TOKEN>` (from `.env`). The container holds no lake credentials and
no OpenMetadata admin: it reads as `mcp-reader` (role DataConsumer only), and its Cube token has no groups, so
restricted columns come back masked.
To connect Claude Code on a machine on the tailnet, without the token ever appearing on screen:

```bash
claude mcp add --transport http lakehouse https://<LAKE_TS_HOST>/mcp \
  --header "Authorization: Bearer $(ssh <vm> 'grep ^MCP_TOKEN= ~/lunch-and-learn-data-cleansing/lakehouse/.env | cut -d= -f2-')"
```

### OpenMetadata: what the lake means, and what's restricted

OpenMetadata 2.0.5 runs at `https://<LAKE_TS_HOST>/openmetadata/`, with OpenSearch 2.19.6 for search and its own
`openmetadata_db` on catalog-pg. Log in as `admin@open-metadata.org` with `OM_ADMIN_PASSWORD` from `.env` (read it
in your own SSH session). Nothing from the quickstart's published defaults survives: `setup_admin.py` replaces the
admin password, `bootstrap.sh` generates the Fernet key and the JWT signing keys, and self-signup is off.

OpenMetadata has no DuckLake connector, so `openmetadata/om_sync.py` pushes everything in, and a re-run changes
nothing (`--check` only compares):

- the **Lakehouse glossary** from `glossary/glossary.yml`, with synonyms, the columns and metrics each term maps to
  (custom properties), and typed relations (`identifies`, `broader`, `partOf`, ...);
- the **Sensitivity** classification: `Restricted` (race, religion, STG) and `Personal` (names, birth dates, notes);
- every table in the lake (`lakehouse.lake.<schema>.<table>`), with dbt's descriptions, a Sensitivity tag on each
  column dbt's `meta.sensitivity` marks (bronze and silver included), and glossary-term links.

**The tags-to-masks loop.** dbt's tags are the floor, not the ceiling. A steward can tag another column
`Sensitivity.Restricted` in OpenMetadata's UI, and `om_sync.py` keeps it (it only adds tags). `cube/sync_masks.py`
then rewrites the `offender_records` access policy so Cube masks exactly the restricted columns it serves, adding
or dropping a dimension's `mask:` to match. `bootstrap.sh` runs it and restarts Cube when the masks change. So:

1. Tag a column Restricted in OpenMetadata.
2. Run `scripts/bootstrap.sh` (or `docker compose --profile core exec -T -u "$(id -u):users" jupyter python
   lakehouse/cube/sync_masks.py`, then `docker compose --profile core restart cube`).
3. Review `git diff lakehouse/cube/model/` and commit it. `bootstrap.sh --check` fails while the model is behind.

### Fuseki: SPARQL over the meaning

Apache Jena Fuseki 6.2.0 serves one dataset, `lakehouse`, at `https://<LAKE_TS_HOST>/fuseki/` (TDB2 under
`/lake/fuseki`). The image is built from Apache's own release: `fuseki/Dockerfile` pins its SHA-512 and checks its
signature against Apache's Jena KEYS on every build. `openmetadata/om_to_rdf.py` reads OpenMetadata (as the
read-only `mcp-reader`) and replaces one named graph, `<https://lakehouse.invalid/graph/openmetadata>` (the
rulebook and deduced facts sit beside it, in graphs of their own: see Business rules below):

- the glossary as SKOS: each term a `skos:Concept` (`term:offender-number`, ...) with `skos:prefLabel`,
  `skos:definition` and `skos:altLabel`; `broader`, `narrower` and `relatedTo` as SKOS's own relations, the rest
  (`partOf`, `identifies`, ...) as `lh:` properties;
- every lake column as an `lh:Column` with `lh:table`, `lh:name`, `lh:sensitivity` (`lh:Restricted`,
  `lh:Personal`) and `lh:means` (its glossary term).

The IRIs use `lakehouse.invalid`, a name reserved never to resolve. Reads need no login, because the graph holds
meaning and tags, never records; writes need the `writer` user (`FUSEKI_WRITER_PASSWORD`), and the admin endpoints
answer only inside the container. One question no single system answers, run from the browser UI or with curl:

```bash
curl -s https://<LAKE_TS_HOST>/fuseki/lakehouse/sparql -H 'Accept: text/csv' --data-urlencode 'query=
PREFIX lh: <https://lakehouse.invalid/ontology#>
PREFIX term: <https://lakehouse.invalid/glossary/>
SELECT ?column WHERE {   # restricted columns that hold part of an offender record
  ?c lh:table "gold.fct_offender_record" ; lh:name ?column ; lh:sensitivity lh:Restricted ; lh:means ?t .
  ?t lh:partOf term:offender-record }'
```

It answers `stg_group` and `stg_status`.

### Business rules in the ontology

Rules about meaning and governance live in the ontology; rules that compute numbers stay in dbt and Cube, and
thresholds in Prometheus. All are files in the repo: Fuseki's graphs are rebuilt on every run, so anything typed into
it by hand is gone the next morning. Three kinds, each with its own graph or check:

| Kind | Where you write it | What it does |
| --- | --- | --- |
| The rulebook | `rules:` in `glossary/glossary.yml` | Each rule's statement, the terms it is about and what enforces it, as `lh:BusinessRule` in `graph/rulebook`. `enforced_by` must name something real (`cube:`, `dbt:`, `prometheus:`, `shacl:`, `sparql:`, `test:`). |
| Deductions | `ontology/rules/<name>.rq`, a SPARQL `CONSTRUCT` | New facts from the published graphs, run until nothing new appears, into `graph/inferred/<name>`, apart from stated ones. Delete the file and its graph goes too. |
| Constraints | `ontology/shapes.ttl`, SHACL | What must be true. Fuseki validates them (`/fuseki/lakehouse/shacl`); `check_rules.py` names each violation, `bootstrap.sh` fails on one, and `BusinessRuleViolated` alerts. |

Today's deduction: **a person's identifier is a pseudonymous key.** Any column that means a term which
`identifies` Person (today every `sid_number`, six tables) is `lh:Pseudonymous`: fine in tables, for joining and
counting people, never shown to a consumer, and a test holds Cube to that. Today's shapes: every glossary term has
one name and a definition of 40 characters or more, and every rule states itself and names its enforcer.

Which rules involve Person, and where is each enforced:

```sparql
PREFIX lh: <https://lakehouse.invalid/ontology#>
PREFIX term: <https://lakehouse.invalid/glossary/>
PREFIX dct: <http://purl.org/dc/terms/>
SELECT ?rule ?enforcedBy WHERE { ?r a lh:BusinessRule ; dct:title ?rule ; lh:about term:person ; lh:enforcedBy ?enforcedBy }
```

**Adding a rule**, test first: write it in `rules:` with what enforces it; for a deduction, add
`ontology/rules/<name>.rq`; for a constraint, a shape in `shapes.ttl` with named property shapes (`lh:<Shape>-<what>`,
so a violation names its rule); then run `scripts/bootstrap.sh`. `acceptance/test_business_rules.py` shows how to
prove a shape catches a violation with a scratch graph.

## Monitoring and alerting

Everything reports to Prometheus 3.15.0 on this VM (30 days kept on `/lake/prometheus`), and **Home Lakehouse** at
`https://<LAKE_TS_HOST>/grafana/` shows it in three rows: platform, pipeline, data quality. A pipeline that succeeds
while quietly flagging far more records than usual is a failure too, so the data is watched as closely as the
services. None of the monitoring pieces has a host port (the tailnet can reach anything bound to the VM's
`0.0.0.0`): Prometheus and Alertmanager are reached only through Caddy, behind the Dagster basic-auth login.

| What | Where it comes from |
| --- | --- |
| The VM: CPU, memory, disks (`/`, `/lake`) | node-exporter, reading the host through `/host` (its network counters are its own container's) |
| Every container, by Compose service | cAdvisor (privileged with the Docker socket: root-equivalent, the usual way to run it) |
| catalog-pg | postgres-exporter, as `lake_monitor` (`pg_monitor`: statistics, never table data) |
| Every site, as a tailnet user sees it, and the certificate | blackbox, through Caddy on the Tailscale name (`monitoring/prometheus/sites.yml` holds only a placeholder; `fill-targets.sh` fills it in at run time) |
| Job outcomes and durations | Dagster's `push_run_success` and `push_run_failure` sensors, through the Pushgateway |
| Load numbers and quality pass rates | `load_lake` and the `monitoring/data_quality` asset, through the Pushgateway |
| The nightly catalog backup | `catalog-backup`, through node-exporter's textfile collector |

**Alerts.** 21 rules in `monitoring/prometheus/rules/lakehouse.yml`, with thresholds well clear of today's values:
platform (a target or site down, the certificate under 14 days, a disk over 85% or 95%, memory under 10%, Postgres
down), pipeline (a job whose last run failed, `daily_lake` not succeeding in 26 hours, `notebook_smoke` in 8 days),
data (silver holding fewer rows than bronze, over 10% of records flagged where about 3% is normal, a quality
dimension under 95%, more than the one expected rule under the floor), backups (none in 26 hours, or the last one
failed), and **Watchdog**, which always fires on purpose. A healthy lakehouse fires only Watchdog.

**Where alerts go: ntfy, on this VM.** Alertmanager posts each alert to ntfy v2.30.0, which pushes it to the ntfy
phone app. It is reached only on the tailnet, at `https://<LAKE_TS_HOST>:8443/` through Caddy: ntfy can't sit under
a path, so it has its own port on the same name and certificate. Nothing is open by default (deny-all), and
`ntfy/start.sh` declares three users from the plain passwords in `.env`, hashed at start: `alertmanager` may only
publish, `ian` (the phone) and `checker` (the tests) may only read, and only the `lakehouse-*` topics.

| Topic | What goes there | Who reads it |
| --- | --- | --- |
| `lakehouse-alerts` | Every alert, except the two below | Ian's phone |
| `lakehouse-heartbeat` | Watchdog, every 4 hours, at min priority | The tests: silence there means alerting is down |
| `lakehouse-selftest` | Whatever `monitoring_selftest` raises (the tests fail it on purpose) | The tests, so testing never buzzes the phone |

`ntfy/templates/lakehouse.yml` turns Alertmanager's JSON into the notification and sets its priority from the
alert's severity: critical while firing is high (sound and a banner), warning default, resolved low, Watchdog min.
`AlertDeliveryFailing` fires when Alertmanager can't hand a notification over (Prometheus scrapes Alertmanager and
ntfy too), and `SiteDown` and `TargetDown` cover ntfy itself.

**On the phone.** Install Tailscale and the ntfy app. In ntfy, add a server: `https://<LAKE_TS_HOST>:8443`, user
`ian`, with the password from `.env`, read in your own SSH session:
`grep '^NTFY_IAN_PASSWORD=' ~/lunch-and-learn-data-cleansing/lakehouse/.env | cut -d= -f2-`. Then subscribe to
`lakehouse-alerts` on that server. The same login works in a browser at `https://<LAKE_TS_HOST>:8443/`. On an iPhone, keep
Tailscale on (its VPN On Demand setting does that): iOS wakes an app only through Apple's push service, so ntfy
forwards each message's ID and a SHA-256 hash of its topic URL to ntfy.sh (`upstream-base-url` in
`ntfy/server.yml`), never the title or text, and the app then fetches the alert itself from this server.

**Test an alert.** Launch `monitoring_selftest` in Dagster with config `{"ops": {"selftest": {"config": {"fail":
true}}}}`: within about a minute `JobFailing` fires for it. Launch it again with `false` and the alert resolves.

**Catalog backups.** `catalog-backup` runs `pg_dump` 17 (the same image as catalog-pg) at 02:00 Central, and at
start when the newest backup is over 23 hours old, as `lake_backup` (`pg_read_all_data`: reads every database,
writes nothing). Every app database (`ducklake`, `dagster`, `superset`, `openmetadata_db`) goes to
`/lake/catalog-backups/<UTC stamp>/` in custom format, read back with `pg_restore --list`, listed in
`manifest.json`, and kept 14 days. The service runs as uid 999 and the dumps are mode 600: they hold every app
database, user password hashes included, so notebooks (uid 1000) can see that a backup happened but not read one.
Roles aren't dumped (that needs a superuser); `postgres/init/` recreates them from `.env`. To restore the catalog
into a scratch database and compare:

```bash
dump=$(sudo ls -d /lake/catalog-backups/2*/ | tail -1)ducklake.dump
docker compose --profile core exec -T catalog-pg createdb -U postgres ducklake_restore_check
sudo cat "$dump" | docker compose --profile core exec -T catalog-pg pg_restore -U postgres --no-owner --no-privileges -d ducklake_restore_check
docker compose --profile core exec -T catalog-pg psql -U postgres -d ducklake_restore_check -c 'select count(*) from ducklake_snapshot'
docker compose --profile core exec -T catalog-pg dropdb -U postgres ducklake_restore_check
```

## Data products

A data product is a promise about some tables: what question they answer, who owns them, what they mean, that
their shape won't change silently, and how fresh they are. Each one is a single file in `products/`, and every
system holds to it:

| The promise | Where it lives |
| --- | --- |
| The question, owner, description | `products/<name>.yml`, published to OpenMetadata as a Data Product in its Domain |
| The tables it delivers | dbt models in a mart schema (`mart_custody`); none may hold a restricted or personal column |
| A shape that won't change silently | dbt enforced contracts, mirrored as OpenMetadata Data Contracts with the SLA |
| What it means | Glossary terms on its columns; the product as a DCAT dataset in Fuseki |
| Who may see what | One Cube view per question, with no restricted members |
| Fresh by when | `max_age_hours`; `monitoring/data_products` pushes its age, and `DataProductStale` alerts past it |

Two products today, in the **Custody Operations** domain:

- **Custody Population**: who is held at each month-end, by unit, custody level and threat level (Cube view
  `custody_population`).
- **Custody Offenses**: what they're held for, and new sentences each month (views `custody_offenses`,
  `custody_admissions`).

**People, not records.** A person can hold several records, sometimes two at once (1,008 overlapping pairs), so
the products count people as distinct SIDs at query time, from a snapshot that carries the SID, and never store
people counts, which don't add up across units. On 2026-09-30: 84,106 open records, 81,409 people.
`gold.dim_person` and the glossary's Person term (identified by SID Number; an Offender Record is about one) make
that explicit; "inmate" and "offender" are roles, so synonyms.

**Adding a product**, test first as everything here:

1. Model its tables in dbt under `dbt/models/marts/<subject>/`, with `contract: {enforced: true}` and a type on
   every column; give the folder a `+schema` in `dbt_project.yml`.
2. Add a Cube view per question it answers (`cube/model/`), with no restricted members.
3. Write `products/<name>.yml` (copy one of the two) and, for a new domain, `products/domains/<name>.yml`.
4. Run `scripts/bootstrap.sh` (or wait for 05:00): OpenMetadata, Fuseki and monitoring pick it up.
   `acceptance/test_products.py` checks every table, view and term it names is real.

## Test status

| Piece | Where it has run |
| --- | --- |
| Loader, 19 parity checks, DuckLake on Postgres, image build, Postgres roles | On the VM |
| Dagster webserver and daemon with Postgres storage, their healthchecks, Caddy basic auth | On the VM |
| `scripts/bootstrap.sh`: 55 bats tests (`bats lakehouse/tests/bootstrap.bats`, needs `apt install bats`), each shown to fail when the behaviour it covers is broken | On the VM, with stub commands; never touches the live stack |
| dbt gold models and their tests (25 today), Dagster job (both jobs, plus the blocking check stopping gold) | Sandbox, against a plain DuckDB file |
| Cube v1.8.3 over gold, read-only as `lake_ro`, restricted columns masked except for `restricted_reader`: 15 acceptance tests (`pytest lakehouse/acceptance/test_cube.py`); unmasking everyone made 8 of them fail | On the VM |
| Superset 6.1.0 under `/superset`, Cube as its only database, the Trust Test and Custody dashboards: 8 acceptance tests (`pytest lakehouse/acceptance/test_superset.py`); the only-Cube check was shown to fail with a decoy database added, the Custody chart checks failed before the dashboard existed, and `--check` exits 1 on a changed layout | On the VM |
| dbt on DuckLake: the first `daily_lake` run (load 7.5 s, gold 5.5 s, one expected `dq_score_floor` warning), checked by 14 acceptance tests: `docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance` | On the VM |
| Trust Test from loader to dashboard (`silver.trust_test` → `gold.trust_test` → Cube → Superset): 1 parity check, 4 acceptance tests and 3 bats tests, each shown failing before the code existed | On the VM |
| `%load_ext lakehouse.sql_magic` (jupysql, read-only as `lake_ro`): 6 acceptance tests (`pytest lakehouse/acceptance/test_sql.py`), plus a real notebook kernel run from outside the repo; the password and read-only checks were shown to fail on deliberate breaks | On the VM |
| Phase 3 notebooks: model staged offline, `08_from_lake.ipynb`, the `notebook_smoke` job: 7 acceptance tests (`pytest lakehouse/acceptance/test_notebooks.py`) and 3 bats tests, each seen failing first; the first smoke run (all 8 notebooks, 110 s) passed with session 5's 22 of 22 reconciliation and session 6's F1 0.898 | On the VM |
| MCP server (`query_metric`, `define_term`): 13 acceptance tests (`pytest lakehouse/acceptance/test_mcp.py`) and 4 bats tests, each seen failing first; the token check and synonym matching were shown to fail on deliberate breaks; checked end to end through Caddy (total restitution 1,333,587,950.77) | On the VM |
| OpenMetadata 2.0.5 with OpenSearch 2.19.6 under `/openmetadata`, no default credentials left: 8 acceptance tests (`pytest lakehouse/acceptance/test_openmetadata.py`) and 5 bats tests, each seen failing first; restarting with `--check` kept the same signing keys | On the VM |
| `om_sync.py` (glossary with typed relations, Sensitivity tags, every lake table) and `define_term` reading OpenMetadata: 9 acceptance tests (`pytest lakehouse/acceptance/test_om_sync.py`); an edit in OpenMetadata reached `define_term`, and `om_sync.py` put it back with the relations intact | On the VM |
| Tags-to-masks loop (`cube/sync_masks.py`): 4 acceptance tests (`pytest lakehouse/acceptance/test_masks.py`) and 3 bats tests, each seen failing first; live, tagging `gender` Restricted in OpenMetadata masked it in Cube (`***` for all 100,000), and removing the tag restored the real values and a byte-identical model | On the VM |
| Daily `om_sync` and Fuseki refresh in `daily_lake` (`openmetadata/lake`, `fuseki/glossary`, the non-blocking masks check): 7 acceptance tests across `test_om_sync.py`, `test_glossary_rdf.py` and `test_daily_run.py`, each seen failing first; real runs succeeded | On the VM |
| Fuseki 6.2.0 under `/fuseki`: 7 acceptance tests (`pytest lakehouse/acceptance/test_fuseki.py`) and 4 bats tests, each seen failing first; removing the writer-only rule made the write test fail | On the VM |
| Business rules (rulebook, the pseudonymous-key deduction, SHACL shapes through Fuseki, the daily check and `BusinessRuleViolated`): 12 acceptance tests (`pytest lakehouse/acceptance/test_business_rules.py`) and 2 bats tests, each seen failing first; a "TBD" definition in a scratch graph broke `TermShape`, and an inferred graph without its rule file was removed | On the VM |
| Glossary as RDF (`om_to_rdf.py`): 8 acceptance tests (`pytest lakehouse/acceptance/test_glossary_rdf.py`) and 1 bats test, each seen failing first; a stray triple was flagged by `--check` and removed by the next load | On the VM |
| Platform metrics (Prometheus 3.15.0, node-exporter, cAdvisor), Grafana 13.2.3 and the Home Lakehouse dashboard, Postgres and blackbox probes: 22 acceptance tests (`pytest lakehouse/acceptance/test_monitoring.py`), each seen failing first; stopping Fuseki dropped its probe to 0 and back; every dashboard panel's query returns data | On the VM |
| Pipeline and data-quality metrics (Pushgateway, run sensors, `monitoring_selftest`): 8 acceptance tests (`pytest lakehouse/acceptance/test_pipeline_metrics.py`), each seen failing first | On the VM |
| Alerting (Alertmanager 0.34.1, 21 rules): 9 acceptance tests (`pytest lakehouse/acceptance/test_alerting.py`), each seen failing first; a failed `monitoring_selftest` raised `JobFailing`, ntfy got it at high priority on the selftest topic, and a passing run resolved it, at low priority | On the VM |
| Alert delivery (ntfy 2.30.0 on port 8443, deny-all, three declared users, the lakehouse template): 7 acceptance tests (`pytest lakehouse/acceptance/test_ntfy.py`), each seen failing first; the Watchdog heartbeat arrives at min priority | On the VM |
| Nightly catalog backup: 6 acceptance tests (`pytest lakehouse/acceptance/test_backup.py`), each seen failing first; a restore drill matched the live catalog row for row | On the VM |
| Pinned versions: 28 acceptance tests (`pytest lakehouse/acceptance/test_pins.py`) and 2 bats tests, each seen failing first; the jupyter image was shown by layer match to be built on the `2026-10-12` base, and `daily_lake` and `notebook_smoke` succeeded on it | On the VM |
| Docstring rule (every Python function and class has a docstring, every shell function a comment above it): 2 acceptance tests (`pytest lakehouse/acceptance/test_docstrings.py`), seen failing first on 23 Python and 9 shell functions written before the rule | On the VM |
| Person model (glossary term, `isAbout` relation, `gold.dim_person`): 8 acceptance tests (`test_person.py`) and 6 dbt tests, each seen failing first | On the VM |
| Custody mart (`mart_custody`, enforced contracts): 7 acceptance tests (`test_custody_marts.py`) and 3 dbt tests, each seen failing first; a deliberate type change was refused by dbt | On the VM |
| Custody products on Cube (three views; the MCP server picks the view that has the dimension): 10 acceptance tests (`test_custody_cube.py`), each seen failing first | On the VM |
| Product descriptors, OpenMetadata domain, data products and contracts, DCAT in Fuseki, product freshness and coverage alerts: 14 acceptance tests (`test_products.py`), each seen failing first; a made-up view in a descriptor failed its test, and a 30-hour-old refresh fired `DataProductStale` | On the VM |

## Next phases

Phases 0 to 6 are done: OpenMetadata holds the glossary and the sensitivity tags, the tags drive Cube's masks,
Fuseki serves the same meaning as SPARQL, and Prometheus, Grafana and Alertmanager watch the platform, the pipeline
and the data, with a nightly catalog backup. Still open: where alerts go (PagerDuty, email or both). Phase 7, upgrades,
has started: every image is pinned. DuckDB 2.0 and DuckLake 1.1 wait for DuckDB 2.0.1 to be out at least two weeks
(2.0.0 shipped 2026-10-21), with a Proxmox snapshot first, since older DuckDB can't read 2.0 files. See the plan
document for the order and the reasoning.
