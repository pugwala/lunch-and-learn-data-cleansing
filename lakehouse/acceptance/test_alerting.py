"""Acceptance test for alerting (Phase 6): Prometheus's rules and Alertmanager.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_alerting.py

Prometheus evaluates monitoring/prometheus/rules/lakehouse.yml (platform, pipeline, data quality, backups) and sends
what fires to Alertmanager v0.34.1, served at https://<LAKE_TS_HOST>/alertmanager/ behind Caddy's basic auth. Where
alerts go is still Ian's call, so every route ends at the "undecided" receiver, which sends nothing anywhere: alerts
show in Alertmanager and Grafana until PagerDuty or email is added there. Watchdog always fires, on purpose: once a
real receiver exists, its silence means the alerting itself is down.
"""
import os
import time
from pathlib import Path

import requests
import yaml

from test_pipeline_metrics import launch

LAKEHOUSE = Path(__file__).resolve().parents[1]
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
ALERTMANAGER = os.environ.get("ALERTMANAGER_URL", "http://alertmanager:9093/alertmanager")
SERVICES = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]
RULES = {"TargetDown", "SiteDown", "CertificateExpiresSoon", "DiskFilling", "DiskAlmostFull", "MemoryLow",
         "PostgresDown", "JobFailing", "DailyLakeStale", "NotebookSmokeStale", "SilverDroppedRows",
         "FlaggedShareHigh", "DataQualityBelowFloor", "MoreRulesBelowFloor", "CatalogBackupStale",
         "CatalogBackupFailed", "Watchdog"}


def firing(**labels):
    """Names of the alerts Alertmanager holds as active, optionally only those with these labels."""
    alerts = requests.get(f"{ALERTMANAGER}/api/v2/alerts", params={"active": "true"}, timeout=30).json()
    return {a["labels"]["alertname"] for a in alerts if all(a["labels"].get(k) == v for k, v in labels.items())}


def test_alertmanager_is_up_at_the_pinned_version():
    """Alertmanager answers under /alertmanager and reports 0.34.1."""
    status = requests.get(f"{ALERTMANAGER}/api/v2/status", timeout=30).json()
    assert status["versionInfo"]["version"] == "0.34.1"


def test_prometheus_sends_to_alertmanager():
    """Prometheus has Alertmanager as its one active target."""
    am = requests.get(f"{PROMETHEUS}/api/v1/alertmanagers", timeout=30).json()["data"]["activeAlertmanagers"]
    assert [a["url"] for a in am] == ["http://alertmanager:9093/alertmanager/api/v2/alerts"], am


def test_every_rule_loads_and_evaluates():
    """All the lakehouse's rules are loaded, and each evaluates without error."""
    groups = requests.get(f"{PROMETHEUS}/api/v1/rules", timeout=30).json()["data"]["groups"]
    rules = {r["name"]: r for g in groups for r in g["rules"]}
    assert RULES <= set(rules), RULES - set(rules)
    assert all(r["health"] == "ok" for r in rules.values()), {n: r.get("lastError") for n, r in rules.items()
                                                              if r["health"] != "ok"}


def test_a_healthy_lakehouse_fires_only_the_watchdog():
    """With everything up, the only alert is Watchdog, the always-on proof that alerting works."""
    assert firing() == {"Watchdog"}


def test_alerts_go_nowhere_until_ian_chooses():
    """Every route ends at "undecided", and that receiver has no integrations: nothing leaves the VM yet."""
    config = yaml.safe_load(requests.get(f"{ALERTMANAGER}/api/v2/status", timeout=30).json()["config"]["original"])
    assert config["route"]["receiver"] == "undecided"
    assert all(r.get("receiver", "undecided") == "undecided" for r in config["route"].get("routes", []))
    receiver = next(r for r in config["receivers"] if r["name"] == "undecided")
    assert not [k for k in receiver if k.endswith("_configs")], receiver   # no PagerDuty, email, webhook...


def test_alertmanager_keeps_silences_on_the_lake_disk():
    """Silences and notification state live on /lake/alertmanager; no host port."""
    svc = SERVICES["alertmanager"]
    assert "${LAKE_ROOT:-/lake}/alertmanager:/alertmanager" in svc["volumes"]
    assert "ports" not in svc


def test_a_failed_job_raises_an_alert_and_a_success_clears_it():
    """monitoring_selftest set to fail makes JobFailing fire in Alertmanager; set to pass, the alert resolves."""
    assert launch("monitoring_selftest", {"ops": {"selftest": {"config": {"fail": True}}}}) == "FAILURE"
    deadline = time.time() + 240
    while "JobFailing" not in firing(pipeline="monitoring_selftest"):
        assert time.time() < deadline, "JobFailing never fired for monitoring_selftest"
        time.sleep(5)
    assert launch("monitoring_selftest", {"ops": {"selftest": {"config": {"fail": False}}}}) == "SUCCESS"
    deadline = time.time() + 240
    while "JobFailing" in firing(pipeline="monitoring_selftest"):
        assert time.time() < deadline, "JobFailing never resolved for monitoring_selftest"
        time.sleep(5)
