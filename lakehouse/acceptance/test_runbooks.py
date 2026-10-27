"""Acceptance test for the runbooks: every alert says what it means and what to do, one tap from the notification.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_runbooks.py

One page per alert in lakehouse/runbooks/<AlertName>.md, each with the same four sections (what it means, what to
check, how to fix it, when it clears). Every rule in monitoring/prometheus/rules/lakehouse.yml carries a
runbook_url annotation pointing at its page on GitHub, and ntfy's lakehouse template puts that link at the end of
the notification, so the phone shows where to go. The pages hold no secrets and no Tailscale names (the repo is
public); they name the files and commands to use, and every file they name must exist.
"""
import os
import re
from pathlib import Path

import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
REPO = LAKEHOUSE.parent
RUNBOOKS = LAKEHOUSE / "runbooks"
BASE = "https://github.com/pugwala/lunch-and-learn-data-cleansing/blob/duckdb-lakehouse/lakehouse/runbooks/"
SECTIONS = ["## What it means", "## What to check", "## How to fix it", "## When it clears"]
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
RULES = {r["alert"]: r for g in yaml.safe_load((LAKEHOUSE / "monitoring" / "prometheus" / "rules" /
                                                 "lakehouse.yml").read_text())["groups"] for r in g["rules"]}


def pages():
    """{alert name: runbook text} for every page in lakehouse/runbooks/ (the index excluded)."""
    return {p.stem: p.read_text() for p in sorted(RUNBOOKS.glob("*.md")) if p.name != "README.md"}


def test_every_alert_has_a_runbook_and_every_runbook_an_alert():
    """One page per alert rule, named after it, and no page for an alert that doesn't exist."""
    assert set(pages()) == set(RULES)


def test_every_rule_links_to_its_own_page():
    """Each rule's runbook_url is its page on GitHub, so the link in the notification opens the right one."""
    for name, rule in RULES.items():
        assert rule.get("annotations", {}).get("runbook_url") == f"{BASE}{name}.md", name


def test_every_page_has_the_four_sections_in_order():
    """What it means, what to check, how to fix it, when it clears: the same shape on every page."""
    for name, text in pages().items():
        assert text.startswith(f"# {name}\n"), name
        positions = [text.find(s) for s in SECTIONS]
        assert all(p >= 0 for p in positions) and positions == sorted(positions), (name, positions)


def test_every_file_a_runbook_names_exists():
    """A path like lakehouse/readiness/closes.py in a runbook must be real, so the pages can't quietly rot."""
    for name, text in pages().items():
        for path in re.findall(r"lakehouse/[\w./-]+\.(?:py|sh|yml|yaml|md|ttl|rq|bats)", text):
            assert (REPO / path).exists(), (name, path)


def test_the_index_lists_every_runbook():
    """lakehouse/runbooks/README.md links each page, grouped as the rules are."""
    index = (RUNBOOKS / "README.md").read_text()
    assert all(f"({name}.md)" in index for name in pages())


def test_the_notification_ends_with_the_runbook_link():
    """ntfy's lakehouse template prints the alerts' runbook_url last, as a line of its own."""
    template = (LAKEHOUSE / "ntfy" / "templates" / "lakehouse.yml").read_text()
    assert ".commonAnnotations.runbook_url" in template and "Runbook: " in template


def test_running_prometheus_has_the_links():
    """The rules Prometheus has loaded carry the same runbook_url (it reread its rules)."""
    groups = requests.get(f"{PROMETHEUS}/api/v1/rules", timeout=30).json()["data"]["groups"]
    loaded = {r["name"]: r.get("annotations", {}).get("runbook_url") for g in groups for r in g["rules"]}
    assert loaded == {name: f"{BASE}{name}.md" for name in RULES}
