"""Acceptance test for Apache Jena Fuseki, the lakehouse's SPARQL endpoint (Phase 5, optional piece).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_fuseki.py

Fuseki 6.2.0 serves one dataset, lakehouse (TDB2 under /lake/fuseki), at https://<LAKE_TS_HOST>/fuseki/. Reads are
open to the tailnet because the dataset only ever holds the glossary and which columns are tagged, never records;
writes need the writer user, whose password (FUSEKI_WRITER_PASSWORD) reaches the container as an environment
variable and is written to a private password file at start, never into the image or the repo.
"""
import os
import re
import uuid
from pathlib import Path

import pytest
import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
FUSEKI = os.environ.get("FUSEKI_URL", "http://fuseki:3030/fuseki")
DATASET = f"{FUSEKI}/lakehouse"
WRITER = ("writer", os.environ.get("FUSEKI_WRITER_PASSWORD", ""))


def ask(query, **kw):
    """POST a SPARQL query to the dataset's read endpoint; returns the response."""
    return requests.post(f"{DATASET}/sparql", data={"query": query}, timeout=30,
                         headers={"Accept": "application/sparql-results+json"}, **kw)


def update(statement, **kw):
    """POST a SPARQL update to the dataset's write endpoint; returns the response."""
    return requests.post(f"{DATASET}/update", data={"update": statement}, timeout=30, **kw)


def test_server_is_up_at_the_pinned_version():
    """Fuseki answers its ping under /fuseki and reports 6.2.0 (it sends no Server header; /$/server says)."""
    requests.get(f"{FUSEKI}/$/ping", timeout=30).raise_for_status()
    assert requests.get(f"{FUSEKI}/$/server", timeout=30).json()["version"] == "6.2.0"


def test_anyone_can_read_the_dataset():
    """A SPARQL query with no login gets an answer."""
    r = ask("ASK { }")
    r.raise_for_status()
    assert r.json()["boolean"] in (True, False)


def test_writes_need_the_writer_password():
    """An update with no login, or the wrong password, is refused; the writer's is accepted and lands."""
    graph = f"urn:test:{uuid.uuid4()}"
    insert = f"INSERT DATA {{ GRAPH <{graph}> {{ <urn:a> <urn:b> 'c' }} }}"
    assert update(insert).status_code == 401
    assert update(insert, auth=("writer", "not-the-password")).status_code == 401
    assert update(insert, auth=("user1", "passwd1")).status_code == 401   # the default shiro.ini's sample user
    assert WRITER[1], "FUSEKI_WRITER_PASSWORD isn't in the jupyter container"
    try:
        update(insert, auth=WRITER).raise_for_status()
        assert ask(f"ASK {{ GRAPH <{graph}> {{ ?s ?p ?o }} }}").json()["boolean"] is True
    finally:
        update(f"DROP SILENT GRAPH <{graph}>", auth=WRITER).raise_for_status()


def test_admin_endpoints_are_refused_from_the_network():
    """Dataset admin (create, drop, backup, compact, stats) answers only inside the container, never through
    Caddy or from another container."""
    for path in ("$/datasets", "$/backup/lakehouse", "$/compact/lakehouse", "$/stats"):
        assert requests.post(f"{FUSEKI}/{path}", timeout=30).status_code == 403, path


def test_shiro_config_is_ours_with_no_users():
    """start.sh installs the repo's shiro.ini, which has no users (the default ships a sample user1) and keeps
    the admin endpoints to localhost."""
    ini = (LAKEHOUSE / "fuseki" / "shiro.ini").read_text()
    users = ini.split("[users]")[1].split("[")[0]
    assert not [line for line in users.splitlines() if line.strip() and not line.strip().startswith("#")]
    assert re.search(r"(?m)^/\$/\*\* *= *localhostFilter", ini)
    assert "shiro.ini" in (LAKEHOUSE / "fuseki" / "start.sh").read_text()


def test_data_lives_on_the_lake_disk():
    """The dataset is TDB2 under /lake/fuseki, so it survives a container rebuild."""
    svc = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]["fuseki"]
    assert "${LAKE_ROOT:-/lake}/fuseki:/fuseki/databases" in svc["volumes"], svc["volumes"]
    assert 'tdb2:location "/fuseki/databases/lakehouse"' in (LAKEHOUSE / "fuseki" / "config.ttl").read_text()


def test_password_stays_out_of_the_image_and_repo():
    """The image runs as a non-root user and holds no password file; the repo only names the variable."""
    svc = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]["fuseki"]
    assert "${FUSEKI_WRITER_PASSWORD" in str(svc["environment"]["FUSEKI_WRITER_PASSWORD"])
    dockerfile = (LAKEHOUSE / "fuseki" / "Dockerfile").read_text()
    assert "USER 1000" in dockerfile
    assert not re.search(r"(?m)^COPY .*passwd", dockerfile), "a password file is baked into the image"
    for f in (LAKEHOUSE / "fuseki").iterdir():
        assert os.environ["FUSEKI_WRITER_PASSWORD"] not in f.read_text(), f.name
