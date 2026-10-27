#!/usr/bin/env bash
# Every lakehouse check that needs no secrets and no running stack, in one command: for GitHub Actions
# (.github/workflows/lakehouse-ci.yml) and for running by hand before a push.
#
#   bash lakehouse/ci/checks.sh
#
# Needs python3 (with venv), docker and bats. It works on a copy of the working tree (tracked and new files, never
# .env or dbt's target/), so nothing it writes touches the live stack. Checks, in order:
#   python      a virtualenv with the lakehouse's own pins (jupyter/requirements-lake.txt, plus the image's pandas
#               and pyarrow), installed only from releases before the image's PIP_CUTOFF date
#   compile     every Python file compiles
#   yaml        every YAML file parses
#   docstrings  every function has a docstring (acceptance/test_docstrings.py)
#   ontology    the SHACL shapes parse, each deduction rule is valid SPARQL, the rulebook and readiness graphs build
#   bats        bootstrap.sh's and restore_drill.sh's tests, with stub commands
#   prometheus  promtool checks prometheus.yml and the alert rules
#   alertmanager amtool checks alertmanager.yml
#   compose     compose.yaml validates against a dummy .env made from .env.example (nothing printed: -q)
#   load        the generator and the course's cleansing load a plain DuckDB file (the sandbox lake)
#   readiness   closes.py syncs the declared inputs and plays the units' closes
#   dbt         dbt build on the sandbox lake (--target local): every model and test
#   parity      the 19 checks against the Facilitator Guide's numbers, on the sandbox lake
#   dagster     dagster definitions validate: the definitions load (what a compile can't catch)
# Exits 1 if any check fails, after running them all.
set -uo pipefail

SRC_REPO=$(cd "$(dirname "$0")/../.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
FAILED=()

# check NAME COMMAND...: run one check, print ok or FAIL (with the end of its output), and remember a failure.
check() {
  local name=$1 out
  shift
  if out=$("$@" 2>&1); then
    printf '  ok    %s\n' "$name"
  else
    printf '  FAIL  %s\n' "$name"
    printf '%s\n' "$out" | tail -25 | sed 's/^/        /'
    FAILED+=("$name")
  fi
}

# image_tag IMAGE: the tag compose.yaml pins for an image (its ${X:-tag} default), so CI checks with the same release.
image_tag() { grep -oP "image: $1:\\\$\{\w+:-\K[^}]+" "$REPO/lakehouse/compose.yaml" | head -1; }

# copy_tree: the working tree as git sees it (tracked and new files; nothing ignored, so no .env) into $WORK/repo.
copy_tree() {
  mkdir -p "$WORK/repo"
  (cd "$SRC_REPO" && git ls-files -co --exclude-standard -z | tar --null -T - -cf -) | tar -xf - -C "$WORK/repo"
}

# python_env: a virtualenv with the lakehouse's pinned packages, from releases before the image's PIP_CUTOFF.
python_env() {
  local cutoff
  cutoff=$(grep -oP '^ARG PIP_CUTOFF=\K\S+' "$REPO/lakehouse/jupyter/Dockerfile")
  python3 -m venv "$WORK/venv" &&
    "$PY" -m pip install -q pip==26.2.1 &&
    "$PY" -m pip install -q --uploaded-prior-to "$cutoff" pandas==2.3.3 pyarrow==25.0.0 \
      -r "$REPO/lakehouse/jupyter/requirements-lake.txt"
}

# yaml_parses: every YAML file under lakehouse/ parses.
yaml_parses() {
  (cd "$REPO" && "$PY" -c '
import pathlib, sys, yaml
bad = []
for f in pathlib.Path("lakehouse").rglob("*.y*ml"):
    try:
        yaml.safe_load(f.read_text())
    except Exception as e:
        bad.append(f"{f}: {e}")
print("\n".join(bad)); sys.exit(1 if bad else 0)')
}

# ontology_parses: the SHACL shapes and deduction rules parse, and the rulebook and readiness graphs build from files.
ontology_parses() {
  (cd "$REPO" && "$PY" -c '
import pathlib, sys
from rdflib import Graph
from rdflib.plugins.sparql import prepareQuery
sys.path.insert(0, ".")
Graph().parse("lakehouse/ontology/shapes.ttl", format="turtle")
for rule in pathlib.Path("lakehouse/ontology/rules").glob("*.rq"):
    prepareQuery(rule.read_text())
sys.path.insert(0, "lakehouse/openmetadata")
import om_to_rdf
assert len(om_to_rdf.rulebook()) > 0 and len(om_to_rdf.readiness()) > 0')
}

# compose_validates: compose.yaml against a dummy .env built from .env.example (fake secrets, a placeholder host).
compose_validates() {
  sed 's/=CHANGE_ME.*/=ci-dummy/' "$REPO/lakehouse/.env.example" > "$WORK/ci.env"
  printf 'LAKE_TS_HOST=lakehouse.example.invalid\nDAGSTER_UI_USER=admin\nDAGSTER_UI_HASH=ci-dummy\n' >> "$WORK/ci.env"
  docker compose -f "$REPO/lakehouse/compose.yaml" --env-file "$WORK/ci.env" --profile core config -q
}

# sandbox: run a command in the repo copy with the sandbox lake (a plain DuckDB file) and sandbox data folders.
sandbox() {
  (cd "$REPO" && LAKE_CATALOG="$WORK/lake.duckdb" LNL_DATA_DIR="$WORK/data" DBT_TARGET=local \
     PYTHONPATH="$REPO" PATH="$WORK/venv/bin:$PATH" "$@")
}

echo "Lakehouse checks"
copy_tree
REPO=$WORK/repo
PY=$WORK/venv/bin/python
check python python_env
check compile "$PY" -m compileall -q "$REPO/lakehouse" "$REPO/lnl" "$REPO/generator"
check yaml yaml_parses
check docstrings "$PY" -m pytest -q -p no:cacheprovider "$REPO/lakehouse/acceptance/test_docstrings.py"
check ontology ontology_parses
check bats bats "$REPO/lakehouse/tests/bootstrap.bats" "$REPO/lakehouse/tests/restore_drill.bats"
check prometheus docker run --rm -v "$REPO/lakehouse/monitoring/prometheus:/etc/prometheus:ro" --entrypoint promtool \
  "prom/prometheus:$(image_tag prom/prometheus)" check rules /etc/prometheus/rules/lakehouse.yml
check alertmanager docker run --rm -v "$REPO/lakehouse/monitoring/alertmanager:/etc/alertmanager:ro" --entrypoint amtool \
  "prom/alertmanager:$(image_tag prom/alertmanager)" check-config /etc/alertmanager/alertmanager.yml
check compose compose_validates
check load sandbox python -c 'from lakehouse import lake; print(lake.run())'
check readiness sandbox sh -c 'python lakehouse/readiness/closes.py sync && python lakehouse/readiness/closes.py simulate'
check dbt sandbox dbt build --project-dir lakehouse/dbt --profiles-dir lakehouse/dbt --target local \
  --target-path "$WORK/dbt-target" --log-path "$WORK/dbt-logs"
check parity sandbox python -m pytest -q -p no:cacheprovider lakehouse/tests
check dagster sandbox dagster definitions validate -f lakehouse/orchestration/definitions.py

echo
if [ ${#FAILED[@]} = 0 ]; then echo "All lakehouse checks passed."; else echo "${#FAILED[@]} failed: ${FAILED[*]}"; exit 1; fi
