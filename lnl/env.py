"""Environment detection, dependency checks, and dataset generation."""
import importlib
import os
import subprocess
import sys
import time
from pathlib import Path

from . import config

REPO_ROOT = Path(__file__).resolve().parent.parent
# /Workspace exists on every Databricks compute type, serverless (and Free Edition) included
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ or Path("/Workspace").is_dir()

SESSION_TITLES = {
    1: "Meet the Mess", 2: "Cleansing", 3: "Dates", 4: "Types and the Payoff",
    5: "The Same Pipeline in PySpark", 6: "Text I: Finding 'Good'", 7: "Text II: Meaning Over Words",
}


def environment_name() -> str:
    if IS_DATABRICKS:
        runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION")
        return f"Databricks (runtime {runtime})" if runtime else "Databricks (serverless)"
    return "Local Jupyter (instructor testing)"


def data_dir() -> Path:
    """Unity Catalog volume on Databricks if configured, otherwise a local temp folder."""
    if IS_DATABRICKS and config.DATABRICKS_VOLUME and Path(config.DATABRICKS_VOLUME).exists():
        return Path(config.DATABRICKS_VOLUME)
    path = Path(os.environ.get("LNL_DATA_DIR", "/tmp/lnl_data"))
    path.mkdir(parents=True, exist_ok=True)
    return path


# Bump whenever the generator's output changes, so a data file cached from an older version is never reused.
DATA_VERSION = 4


def paths() -> dict:
    stem = f"hero_offender_data_v{DATA_VERSION}_s{config.SEED}_r{config.ROWS}"
    d = data_dir()
    return {"data": d / f"{stem}.csv", "answer_key": d / f"{stem}_answer_key.csv", "dir": d}


def pip_install(*packages: str) -> bool:
    """Install packages from inside the notebook (no terminal needed). Returns True on success."""
    print(f"Installing {', '.join(packages)} (one-time, about a minute)...")
    result = subprocess.run([sys.executable, "-m", "pip", "install", "-q", *packages],
                            capture_output=True, text=True)
    importlib.invalidate_caches()
    if result.returncode != 0:
        print(result.stderr[-1500:])
    return result.returncode == 0


def ensure(module: str, package: str = None) -> bool:
    try:
        importlib.import_module(module)
        return True
    except ImportError:
        return pip_install(package or module)


def _check_core() -> None:
    import pandas as pd
    major = int(pd.__version__.split(".")[0])
    if major < 2:
        raise RuntimeError(
            f"This course needs pandas 2.0 or newer; this environment has {pd.__version__}. "
            "On Databricks serverless (including Free Edition): open the Environment panel on the right, "
            "set the environment version to 4 or newer, click Apply, and run this cell again. "
            "On classic compute: use a recent ML runtime (LTS).")
    ensure("pyarrow")


def generate(force: bool = False) -> dict:
    p = paths()
    if force or not (p["data"].exists() and p["answer_key"].exists()):
        sys.path.insert(0, str(REPO_ROOT / "generator"))
        gen = importlib.import_module("gen_hero_offenders")
        start = time.time()
        print(f"Generating {config.ROWS:,} mock records (seed {config.SEED})...", end=" ", flush=True)
        gen.generate(rows=config.ROWS, out=str(p["data"]), seed=config.SEED, answer_key=str(p["answer_key"]))
        print(f"done in {time.time() - start:.0f}s")
    return p


def prepare(session: int) -> dict:
    """Run at the top of every notebook: check libraries, build the data, print a banner."""
    _check_core()
    if session == 5 and not IS_DATABRICKS:
        ensure("pyspark")
    if session in (6, 7):
        ensure("spacy")
    p = generate()
    import pandas as pd
    print(f"Session {session}: {SESSION_TITLES.get(session, '')}")
    print(f"  Environment : {environment_name()}")
    print(f"  pandas      : {pd.__version__}")
    print(f"  Data file   : {p['data']}")
    print("  Ready. Everyone in the room has the identical dataset.")
    return p
