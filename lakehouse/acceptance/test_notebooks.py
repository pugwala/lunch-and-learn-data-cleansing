"""Acceptance test for Phase 3: the course notebooks on the VM.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_notebooks.py

Session 7's embedding model is staged on the lake disk (HF_HOME=/lake/models/huggingface) at a pinned revision,
and the notebooks run offline (HF_HUB_OFFLINE=1), so nothing depends on reaching Hugging Face. The course's own
setting (EMBEDDING_MODEL in lnl/config.py) is unchanged: the course files stay identical to main.
"""
import os
import subprocess
from pathlib import Path

import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
HF_HOME = "/lake/models/huggingface"
MODEL_REPO = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"   # 2026-06-01, Apache-2.0
NOTEBOOK_SERVICES = ("jupyter", "dagster-webserver", "dagster-daemon")


def compose_env(service):
    """The environment compose gives a service, with YAML merge keys (the shared lake-env) resolved."""
    return yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"][service]["environment"]


# --- the embedding model ---------------------------------------------------------------------------------------

def test_notebook_services_share_the_notebook_environment():
    """Jupyter and both Dagster services read Hugging Face models from /lake/models without downloading, and can
    import lakehouse and read the lake as lake_ro (08_from_lake.ipynb loads lakehouse.sql_magic) from any folder."""
    for service in NOTEBOOK_SERVICES:
        env = compose_env(service)
        assert env.get("HF_HOME") == HF_HOME, service
        assert str(env.get("HF_HUB_OFFLINE")) == "1", service
        assert env.get("PYTHONPATH") == "/home/jovyan/course", service
        assert "LAKE_RO_PASSWORD" in str(env.get("LAKE_RO_PASSWORD", "")), service   # 08 reads as lake_ro


def test_embedding_model_is_staged_at_the_pinned_revision():
    """The model session 7 names is on the lake disk at the pinned revision."""
    ref = Path(HF_HOME) / "hub" / f"models--{MODEL_REPO.replace('/', '--')}" / "refs" / "main"
    assert ref.exists(), f"{MODEL_REPO} is not staged: run bootstrap.sh"
    assert ref.read_text().strip() == MODEL_REVISION


def test_session_7_loads_the_real_model_offline():
    """With downloads switched off, the course's Embedder loads the sentence-transformer, not the LSA fallback."""
    code = "from lnl import text; print(text.Embedder().method)"
    env = {**os.environ, "HF_HOME": HF_HOME, "HF_HUB_OFFLINE": "1"}
    r = subprocess.run(["python", "-c", code], capture_output=True, text=True, env=env, timeout=300)
    assert r.returncode == 0, r.stderr[-500:]
    assert r.stdout.strip().splitlines()[-1] == "sentence-transformers (all-MiniLM-L6-v2)"


# --- the bridge notebook ---------------------------------------------------------------------------------------

FROM_LAKE = LAKEHOUSE / "08_from_lake.ipynb"


def test_from_lake_notebook_ships_without_outputs():
    """Like the course notebooks, 08_from_lake.ipynb is stored clean: no outputs, no execution counts."""
    import nbformat
    nb = nbformat.read(str(FROM_LAKE), as_version=4)
    code = [c for c in nb.cells if c.cell_type == "code"]
    assert code and not any(c.outputs or c.execution_count for c in code)


def test_from_lake_notebook_rebuilds_the_trust_test(tmp_path):
    """08_from_lake.ipynb runs top to bottom, and its SQL over silver gives the course's 8 cleansed answers."""
    import papermill
    out = tmp_path / "08_from_lake.ipynb"
    papermill.execute_notebook(str(FROM_LAKE), str(out), cwd=str(LAKEHOUSE), kernel_name="python3",
                               progress_bar=False)   # raises on any failing cell
    assert "8 of 8 answers match gold.trust_test" in out.read_text()


# --- the weekly smoke run --------------------------------------------------------------------------------------

from test_daily_run import graphql  # noqa: E402

NOTEBOOK_RUNS = Path("/lake/notebook-runs")
SMOKE_NOTEBOOKS = sorted([*(p.name for p in (LAKEHOUSE.parent / "notebooks").glob("0*.ipynb")), "08_from_lake.ipynb"])


def test_notebook_smoke_job_runs_weekly():
    """Dagster has a notebook_smoke job, scheduled Sundays at 06:00 Central (after the daily run) and switched on."""
    data = graphql('{ repositoriesOrError { ... on RepositoryConnection { nodes { '
                   'jobs { name } schedules { name cronSchedule executionTimezone scheduleState { status } } } } } }')
    repo = data["repositoriesOrError"]["nodes"][0]
    assert "notebook_smoke" in {j["name"] for j in repo["jobs"]}
    sched = {s["name"]: s for s in repo["schedules"]}.get("notebook_smoke_schedule")
    assert sched, "no notebook_smoke_schedule"
    assert (sched["cronSchedule"], sched["executionTimezone"], sched["scheduleState"]["status"]) == \
        ("0 6 * * 0", "America/Chicago", "RUNNING")


def test_latest_notebook_smoke_run_executed_every_notebook():
    """The latest notebook_smoke run succeeded and saved all 8 executed notebooks, none with an error output."""
    import nbformat
    runs = graphql('{ runsOrError(filter: {pipelineName: "notebook_smoke"}, limit: 1) '
                   '{ ... on Runs { results { runId status } } } }')["runsOrError"]["results"]
    assert runs, "no notebook_smoke run yet: launch one from the Dagster UI"
    assert runs[0]["status"] == "SUCCESS"
    folders = list(NOTEBOOK_RUNS.glob(f"*_{runs[0]['runId'][:8]}"))
    assert len(folders) == 1, f"no output folder for run {runs[0]['runId'][:8]}"
    assert sorted(p.name for p in folders[0].glob("*.ipynb")) == SMOKE_NOTEBOOKS
    for path in folders[0].glob("*.ipynb"):
        outputs = [o for c in nbformat.read(str(path), as_version=4).cells for o in c.get("outputs", [])]
        assert outputs and not [o for o in outputs if o.output_type == "error"], path.name
