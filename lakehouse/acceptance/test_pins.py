"""Acceptance test that nothing the lakehouse runs floats (Phase 7).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_pins.py

Every image in compose.yaml and every base image in a Dockerfile names an exact release, never "latest" or a bare
major version, so a rebuild can't silently bring new software. Each pin was a release at least two weeks old when
it was chosen (README, Pinned versions). Postgres tags name the minor version and the OS, because the "17" tag moves.
The lakehouse's own Python requirements are pinned with ==. The course's requirements.txt is a course file and
stays as the course ships it.
"""
import os
import re
from pathlib import Path

import psycopg2
import pytest
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
EXACT = re.compile(r"v?\d+\.\d+\.\d+([-_.][\w.]+)*|\d{4}-\d{2}-\d{2}")   # 1.2.3, v1.2.3-x, 21.0.12_8-jre-noble, 2026-10-12
POSTGRES = re.compile(r"\d+\.\d+-[a-z]+")                                  # 17.11-trixie


def default_tag(ref):
    """The tag an image reference resolves to with nothing set in .env: ${X:-tag} gives tag, no tag gives latest."""
    ref = re.sub(r"\$\{\w+:-([^}]+)\}", r"\1", ref)   # fill in defaults first: they contain a ":" of their own
    name = ref.split("/")[-1]
    return name.split(":", 1)[1] if ":" in name else "latest"


def compose_images():
    """(service, image reference) for every service that pulls an image rather than building one."""
    services = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]
    return [(name, s["image"]) for name, s in services.items() if "image" in s and not s["image"].endswith(":local")]


def dockerfile_bases():
    """(Dockerfile, base image with its ARG default filled in) for every FROM in the lakehouse's Dockerfiles."""
    out = []
    for f in sorted(LAKEHOUSE.glob("*/Dockerfile")):
        text = f.read_text()
        args = dict(re.findall(r"(?m)^ARG (\w+)=(\S+)", text))
        for ref in re.findall(r"(?m)^FROM (\S+)", text):
            out.append((f.parent.name, re.sub(r"\$\{(\w+)\}", lambda m: args.get(m.group(1), m.group(0)), ref)))
    return out


@pytest.mark.parametrize("service,ref", compose_images())
def test_compose_images_are_exact(service, ref):
    """No service's image floats: its default tag is an exact release."""
    tag = default_tag(ref)
    pattern = POSTGRES if ref.startswith("postgres:") else EXACT
    assert pattern.fullmatch(tag), f"{service}: {ref} resolves to {tag!r}, which floats"


def test_build_args_are_exact():
    """Tags compose.yaml passes to Dockerfiles as build args default to exact releases too."""
    services = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]
    for name, s in services.items():
        for key, value in ((s.get("build") or {}).get("args") or {}).items():
            if key.endswith("_TAG"):
                tag = re.sub(r"\$\{\w+:-([^}]+)\}", r"\1", str(value))
                assert EXACT.fullmatch(tag), f"{name}: build arg {key} defaults to {tag!r}, which floats"


def test_env_example_sets_no_floating_tags():
    """.env.example doesn't override the pins with a floating tag (an override there would win over compose.yaml)."""
    tags = re.findall(r"(?m)^(\w+_TAG)=(\S*)", (LAKEHOUSE / ".env.example").read_text())
    assert not [(k, v) for k, v in tags if v == "latest" or re.fullmatch(r"v?\d+", v)], tags


@pytest.mark.parametrize("dockerfile,ref", dockerfile_bases())
def test_dockerfile_bases_are_exact(dockerfile, ref):
    """No Dockerfile builds on a floating base image."""
    tag = ref.rsplit(":", 1)[1]
    assert EXACT.fullmatch(tag), f"{dockerfile}/Dockerfile: FROM {ref} floats"


def test_lake_python_requirements_are_pinned():
    """Every package in jupyter/requirements-lake.txt is pinned with ==."""
    lines = [l.split("#")[0].strip() for l in (LAKEHOUSE / "jupyter" / "requirements-lake.txt").read_text().splitlines()]
    loose = [l for l in lines if l and "==" not in l]
    assert not loose, loose


def test_catalog_runs_the_pinned_postgres():
    """catalog-pg reports the minor version compose.yaml pins."""
    ref = dict(compose_images())["catalog-pg"]
    want = default_tag(ref).split("-")[0]
    with psycopg2.connect(host="catalog-pg", dbname="ducklake", user="lake_rw",
                          password=os.environ["PGPASSWORD"]) as pg, pg.cursor() as cur:
        cur.execute("SHOW server_version")
        assert cur.fetchone()[0].split()[0] == want


# --- Python packages resolve as of a date ----------------------------------------------------------------------

DOCKERFILE = (LAKEHOUSE / "jupyter" / "Dockerfile").read_text()
# Packages pip installs into the image (the course's open-ended requirements and what they pull in), as opposed to
# the ones the pinned base image already ships.
PIP_INSTALLED = ["spacy", "sentence-transformers", "transformers", "tokenizers", "huggingface-hub", "torch",
                 "scikit-learn", "matplotlib"]


def cutoff():
    """The date the image's pip installs resolve as of (the PIP_CUTOFF build arg), as YYYY-MM-DD."""
    m = re.search(r"(?m)^ARG PIP_CUTOFF=(\d{4}-\d{2}-\d{2})", DOCKERFILE)
    assert m, "jupyter/Dockerfile has no PIP_CUTOFF"
    return m.group(1)


def uploaded(name, version):
    """When PyPI received a release (its earliest file), as YYYY-MM-DD."""
    import json
    import urllib.request
    data = json.loads(urllib.request.urlopen(f"https://pypi.org/pypi/{name}/{version}/json", timeout=30).read())
    return min(u["upload_time"][:10] for u in data["urls"])


def test_pip_resolves_as_of_the_base_image_date():
    """Every pip install from PyPI in the image uses --uploaded-prior-to the cutoff, which is the base image's date,
    so the course's open-ended requirements and every transitive dependency are what existed then."""
    base = re.search(r"(?m)^ARG JUPYTER_BASE_TAG=(\S+)", DOCKERFILE).group(1)
    assert cutoff() == base
    for line in re.findall(r"(?m)^RUN pip install .*-r .*$", DOCKERFILE):
        assert '--uploaded-prior-to "$PIP_CUTOFF"' in line, line


def test_torch_is_pinned():
    """torch comes from PyTorch's CPU index, which the date cutoff can't apply to, so it names its version."""
    assert re.search(r"(?m)^RUN pip install .*download\.pytorch\.org.* torch==\d+\.\d+\.\d+", DOCKERFILE)


def test_lake_pins_predate_the_cutoff():
    """Each == pin in requirements-lake.txt was on PyPI before the cutoff (else the build couldn't resolve it)."""
    late = []
    for name, version in re.findall(r"(?m)^([A-Za-z0-9_.-]+)==(\S+)", (LAKEHOUSE / "jupyter" / "requirements-lake.txt").read_text()):
        if uploaded(name, version) >= cutoff():
            late.append(f"{name}=={version}")
    assert not late, late


def test_installed_packages_predate_the_cutoff():
    """The packages pip put in this image were released before the cutoff, none on the day of a rebuild."""
    from importlib.metadata import version
    late = {p: version(p) for p in PIP_INSTALLED if uploaded(p, version(p).split("+")[0]) >= cutoff()}
    assert not late, late
