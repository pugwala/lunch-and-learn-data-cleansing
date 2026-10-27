"""Acceptance test for alerting (Phase 6): Prometheus's rules and Alertmanager.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_alerting.py

Prometheus evaluates monitoring/prometheus/rules/lakehouse.yml (platform, pipeline, data quality, backups) and sends
what fires to Alertmanager v0.34.1, served at https://<LAKE_TS_HOST>/alertmanager/ behind Caddy's basic auth.
Alertmanager delivers to ntfy on this VM (acceptance/test_ntfy.py): alerts to lakehouse-alerts, the topic Ian's
phone subscribes to; Watchdog, which always fires on purpose, to lakehouse-heartbeat,
so its silence means alerting itself is down; and the alert monitoring_selftest raises in these tests to
lakehouse-selftest, so testing never buzzes Ian's phone. ntfy/templates/lakehouse.yml turns each into a notification
and sets its priority from the alert's severity (critical firing: high). Prometheus scrapes Alertmanager too, and
AlertDeliveryFailing fires when a notification fails.
"""
import os
import time
from pathlib import Path

import requests
import yaml

from test_ntfy import messages
from test_pipeline_metrics import launch

LAKEHOUSE = Path(__file__).resolve().parents[1]
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
ALERTMANAGER = os.environ.get("ALERTMANAGER_URL", "http://alertmanager:9093/alertmanager")
SERVICES = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]
RULES = {"TargetDown", "SiteDown", "CertificateExpiresSoon", "DiskFilling", "DiskAlmostFull", "MemoryLow",
         "PostgresDown", "JobFailing", "DailyLakeStale", "NotebookSmokeStale", "SilverDroppedRows",
         "FlaggedShareHigh", "DataQualityBelowFloor", "MoreRulesBelowFloor", "CatalogBackupStale",
         "CatalogBackupFailed", "AlertDeliveryFailing", "BusinessRuleViolated", "BusinessInputLate", "LakeHousekeepingStale", "OffsiteBackupStale", "OffsiteBackupFailed",
         "RestoreDrillStale", "RestoreDrillFailed", "Watchdog"}
NTFY_URL = "http://ntfy:8080/"


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


def running_config():
    """The configuration Alertmanager is running now (secrets shown as <secret>)."""
    return yaml.safe_load(requests.get(f"{ALERTMANAGER}/api/v2/status", timeout=30).json()["config"]["original"])


def test_alerts_go_to_ntfy_and_tests_stay_off_ians_phone():
    """By default alerts go to lakehouse-alerts; Watchdog to the heartbeat topic and anything from
    monitoring_selftest to the selftest topic, so neither reaches the phone. All use the lakehouse template.
    Alertmanager shows webhook URLs as <secret>, so the topics are read from the file, and the running config is
    checked to be that file (same routes, same receivers)."""
    config = yaml.safe_load((LAKEHOUSE / "monitoring" / "alertmanager" / "alertmanager.yml").read_text())
    running = running_config()
    assert running["route"]["receiver"] == config["route"]["receiver"]
    assert [(r["matchers"], r["receiver"]) for r in running["route"]["routes"]] == \
        [(r["matchers"], r["receiver"]) for r in config["route"]["routes"]]
    assert [r["name"] for r in running["receivers"]] == [r["name"] for r in config["receivers"]]
    route = config["route"]
    topic = {r["name"]: r["webhook_configs"][0]["url"] for r in config["receivers"]}
    assert topic[route["receiver"]] == NTFY_URL + "lakehouse-alerts?template=lakehouse"
    by_match = {tuple(r["matchers"]): r["receiver"] for r in route["routes"]}
    assert set(by_match) == {('alertname="Watchdog"',), ('pipeline="monitoring_selftest"',)}
    assert topic[by_match[('alertname="Watchdog"',)]] == NTFY_URL + "lakehouse-heartbeat?template=lakehouse"
    assert topic[by_match[('pipeline="monitoring_selftest"',)]] == NTFY_URL + "lakehouse-selftest?template=lakehouse"


def test_every_receiver_logs_in_with_a_password_file():
    """Each receiver is one ntfy webhook, as the publish-only alertmanager user, its password read from a file that
    Compose fills from .env (never written into the config)."""
    for r in running_config()["receivers"]:
        assert [k for k in r if k.endswith("_configs")] == ["webhook_configs"], r["name"]
        auth = r["webhook_configs"][0]["http_config"]["basic_auth"]
        assert auth == {"username": "alertmanager", "password_file": "/run/secrets/ntfy_alertmanager_password"}, auth


def test_prometheus_watches_alert_delivery():
    """Prometheus scrapes Alertmanager, so AlertDeliveryFailing can see failed notifications; none have failed."""
    got = requests.get(f"{PROMETHEUS}/api/v1/query", timeout=30, params={
        "query": 'sum(alertmanager_notifications_failed_total{integration="webhook"})'}).json()["data"]["result"]
    assert got and float(got[0]["value"][1]) == 0, got


def test_alertmanager_keeps_silences_on_the_lake_disk():
    """Silences and notification state live on /lake/alertmanager; no host port."""
    svc = SERVICES["alertmanager"]
    assert "${LAKE_ROOT:-/lake}/alertmanager:/alertmanager" in svc["volumes"]
    assert "ports" not in svc


def selftest_notices(after):
    """JobFailing notifications for monitoring_selftest on the selftest topic since a time, as (message, priority)."""
    return [(m["message"], m.get("priority", 3)) for m in messages("lakehouse-selftest", since="1h")
            if m["time"] >= after and "JobFailing" in m.get("title", "")]


def test_a_failed_job_raises_an_alert_and_a_success_clears_it():
    """monitoring_selftest set to fail makes JobFailing fire in Alertmanager and arrive in ntfy (on the selftest
    topic, not Ian's) at high priority, as JobFailing is critical; set to pass, the alert resolves and ntfy gets the
    resolved notice, at low priority."""
    started = int(time.time())
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
    deadline = time.time() + 120   # Alertmanager sends the resolved notice on its next group interval
    while not any("Status: Resolved" in m for m, _ in selftest_notices(started)):
        assert time.time() < deadline, f"ntfy never got both notices: {selftest_notices(started)}"
        time.sleep(5)
    notices = selftest_notices(started)
    assert {p for m, p in notices if "Status: Firing" in m} == {4}, notices
    assert {p for m, p in notices if "Status: Resolved" in m} == {2}, notices
    assert all("runbooks/JobFailing.md" in m for m, _ in notices), notices   # the link to what to do, on the phone
    assert not [m for m in messages("lakehouse-alerts", since="1h") if m["time"] >= started
                and "monitoring_selftest" in m["message"]], "the selftest's alert reached Ian's phone topic"
