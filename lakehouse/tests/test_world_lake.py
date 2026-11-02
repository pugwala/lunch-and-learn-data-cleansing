"""Acceptance tests for the expanded world's lake layers (dataset expansion, chunk 7).

    pytest lakehouse/tests/test_world_lake.py     # inside the jupyter container (or CI's sandbox); needs the lake

One scale-S world (with its mess) is built in a temporary folder, then dbt builds only the world's models
(tag:world: bronze, silver, gold, and the answer-key tests) into a temporary DuckDB lake that holds a copy of
ref.nibrs_offenses and silver.hero_offenders' keys and sentence dates. The tests check that:
  - dbt builds every world model and every world test passes, the answer-key tests among them: silver equals the
    clean truth in every cell except where the key says the truth was lost (unknown) or kept as written and flagged
    (orphans, out-of-order dates, links to a duplicate), and silver's flags are the key's defects;
  - every messy row lands in bronze and in silver, nothing dropped;
  - silver finds the duplicate people and the out-of-order dates the key lists (its precision and recall);
  - gold keeps names and birth dates out, and its facts hold every silver row;
  - a month rebuilt on its own (vars world_from/world_to, as a Dagster partition runs it) gives the same rows.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lakehouse import lake  # noqa: E402
from lakehouse.universe import build, manifest  # noqa: E402

DBT = Path(__file__).resolve().parents[1] / "dbt"
MONTH = ("2015-06-01", "2015-07-01")
MONTHLY = ["participant", "power_use", "damage", "charge", "disposition", "mission_incident"]


def dbt(tmp: Path, db: Path, root: Path, *args, extra_vars=None) -> subprocess.CompletedProcess:
    """Run dbt on the temporary lake, with the world's root (and any partition vars) set."""
    env = {**os.environ, "LAKE_CATALOG": str(db), "DBT_TARGET": "local"}
    vars_ = {"world_root": str(root), **(extra_vars or {})}
    return subprocess.run(["dbt", *args, "--project-dir", str(DBT), "--profiles-dir", str(DBT), "--target", "local",
                           "--vars", json.dumps(vars_), "--target-path", str(tmp / "target"),
                           "--log-path", str(tmp / "logs")], env=env, capture_output=True, text=True)


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A scale-S world, a temporary lake with ref.nibrs_offenses and silver.hero_offenders' keys and sentence dates
    (the world's rule tests check its convictions against them), and dbt's build of tag:world into it."""
    tmp = tmp_path_factory.mktemp("world")
    root = tmp / "universe" / "S"
    src = lake.lake_connect(read_only=True)
    build.build(root, scale_name="S", seed=2026, workers=4, chunk_rows=100_000)
    nibrs = src.execute("SELECT * FROM ref.nibrs_offenses").arrow().read_all()
    records = src.execute("SELECT offender_number, sid_number, sentence_date FROM silver.hero_offenders").arrow().read_all()
    db = tmp / "world.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE SCHEMA ref; CREATE SCHEMA silver")
    con.register("nibrs", nibrs)
    con.register("records", records)
    con.execute("CREATE TABLE ref.nibrs_offenses AS SELECT * FROM nibrs")
    con.execute("CREATE TABLE silver.hero_offenders AS SELECT * FROM records")
    con.close()
    result = dbt(tmp, db, root, "build", "--select", "tag:world")
    return tmp, db, root, result


def query(world, sql):
    """Rows from the temporary lake (read-only, attached as lake, as dbt sees it), with the world's folder as $root."""
    _, db, root, _ = world
    con = duckdb.connect()
    con.execute(f"ATTACH '{db}' AS lake (READ_ONLY); USE lake")
    try:
        return con.execute(sql.replace("$root", str(root))).fetchall()
    finally:
        con.close()


def one(world, sql):
    """The single value a query returns."""
    return query(world, sql)[0][0]


def test_dbt_builds_every_world_model_and_every_world_test_passes(world):
    """dbt build --select tag:world exits 0: every model built, every test (answer-key tests included) passed."""
    result = world[3]
    assert result.returncode == 0, (result.stdout + result.stderr)[-6000:]
    assert "PASS=" in result.stdout


def test_every_messy_row_lands_in_bronze_and_silver(world):
    """Bronze and silver hold exactly the messy CSV's rows, per table (person with its duplicates)."""
    mess = manifest.read(world[2])["mess"]
    for t in build.ORDER:
        rows = mess[f"messy/{t}"]["rows"]
        assert one(world, f"SELECT count(*) FROM world_bronze.{t}") == rows, t
        assert one(world, f"SELECT count(*) FROM world_silver.{t}") == rows, t


def test_silver_finds_the_duplicate_people_and_out_of_order_dates(world):
    """Duplicates: at least 90% of the key's are found and at least 99% of those flagged are real. Out-of-order
    dates: every flag is in the key, and at least 95% of the key's are flagged (the rest hide behind a token or a
    missing link, so there's nothing to compare)."""
    found = one(world, """SELECT count(*) FROM world_silver.exceptions e JOIN read_parquet('$root/answer_key/person/*.parquet') k
                          ON k.id = e.id AND k.kind = 'duplicate_person'
                          WHERE e.table_name = 'person' AND e.rule = 'duplicate_person'""")
    flagged = one(world, "SELECT count(*) FROM world_silver.exceptions WHERE rule = 'duplicate_person'")
    keyed = one(world, "SELECT count(*) FROM read_parquet('$root/answer_key/person/*.parquet') WHERE kind = 'duplicate_person'")
    assert keyed > 0 and found / keyed >= 0.9 and found / flagged >= 0.99, (found, flagged, keyed)
    ooo_keyed = one(world, "SELECT count(*) FROM read_parquet('$root/answer_key/*/*.parquet') WHERE kind = 'out_of_order'")
    ooo_flagged = one(world, "SELECT count(*) FROM world_silver.exceptions WHERE rule = 'out_of_order'")
    assert ooo_keyed > 0 and ooo_flagged / ooo_keyed >= 0.95, (ooo_flagged, ooo_keyed)


def test_gold_keeps_personal_data_out_and_its_facts_hold_every_silver_row(world):
    """No names or birth dates in gold; one gold person per resolved person; facts row for row with silver."""
    cols = {r[0] for r in query(world, "SELECT column_name FROM information_schema.columns WHERE table_schema = 'world_gold'")}
    assert not cols & {"last_name", "first_name", "alias", "birth_date", "codename"}
    assert one(world, "SELECT count(*) FROM world_gold.dim_person") == one(
        world, "SELECT count(DISTINCT person_key) FROM world_silver.person")
    for fact, table in [("fct_incident", "incident"), ("fct_participation", "participant"), ("fct_power_use", "power_use"),
                        ("fct_damage", "damage"), ("fct_charge", "charge"), ("fct_mission", "mission")]:
        assert one(world, f"SELECT count(*) FROM world_gold.{fact}") == one(world, f"SELECT count(*) FROM world_silver.{table}"), fact


def fingerprint(world, schema, table, month):
    """A month's rows of a table, as (count, order-free checksum of every column but _partition)."""
    return query(world, f"""SELECT count(*), sum(hash(struct_pack(*COLUMNS(* EXCLUDE (_partition))))) FROM {schema}.{table}
                            WHERE incident_month >= DATE '{month[0]}' AND incident_month < DATE '{month[1]}'""")[0]


def test_a_month_rebuilt_alone_gives_the_same_rows(world):
    """One Dagster partition (vars world_from/world_to) rebuilds only that month's rows of the monthly models (and
    the undated ones no month owns), gives the same rows, and leaves the other months as they were."""
    tmp, db, root, _ = world
    before = {t: fingerprint(world, "world_silver", t, MONTH) for t in MONTHLY}
    totals = {t: one(world, f"SELECT count(*) FROM world_silver.{t}") for t in MONTHLY}
    assert all(n > 0 for n, _ in before.values())
    result = dbt(tmp, db, root, "run", "--select", "tag:world_monthly",
                 extra_vars={"world_from": MONTH[0], "world_to": MONTH[1]})
    assert result.returncode == 0, (result.stdout + result.stderr)[-4000:]
    assert {t: fingerprint(world, "world_silver", t, MONTH) for t in MONTHLY} == before
    assert {t: one(world, f"SELECT count(*) FROM world_silver.{t}") for t in MONTHLY} == totals
    for t in MONTHLY:
        owned = one(world, f"""SELECT count(*) FROM world_silver.{t} WHERE incident_month IS NULL
                               OR (incident_month >= DATE '{MONTH[0]}' AND incident_month < DATE '{MONTH[1]}')""")
        written = one(world, f"SELECT count(*) FROM world_silver.{t} WHERE _partition = '{MONTH[0]}'")
        assert written == owned < totals[t], (t, written, owned)
