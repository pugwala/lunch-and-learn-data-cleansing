"""Acceptance test for pipeline and data-quality metrics (Phase 6).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_pipeline_metrics.py

Dagster pushes to a Pushgateway (persisted on /lake/pushgateway), which Prometheus scrapes keeping the pushed
labels. Two run sensors, on by default, push every job's last success and last failure; load_lake pushes the load's
numbers; monitoring/data_quality pushes gold.dq_scorecard's pass rates. monitoring_selftest fails on request, so the
failure path (and later the alert on it) can be exercised on demand.
"""
import json
import os
import time
import urllib.request
from pathlib import Path

import pytest
import requests
import yaml
from prometheus_client.parser import text_string_to_metric_families

from lakehouse import lake
from test_daily_run import EXPECTED_DIMENSIONS, graphql

LAKEHOUSE = Path(__file__).resolve().parents[1]
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
PUSHGATEWAY = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
SERVICES = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]


def query(promql):
    """Run an instant PromQL query; returns {frozenset(labels): float value}."""
    r = requests.get(f"{PROMETHEUS}/api/v1/query", params={"query": promql}, timeout=30)
    r.raise_for_status()
    return {frozenset(x["metric"].items()): float(x["value"][1]) for x in r.json()["data"]["result"]}


def pushed(name, **labels):
    """The value the Pushgateway holds for a metric with these labels, or None."""
    text = requests.get(f"{PUSHGATEWAY}/metrics", timeout=30).text
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == name and all(s.labels.get(k) == v for k, v in labels.items()):
                return s.value
    return None


def launch(job, config=None):
    """Launch a Dagster job through GraphQL, wait for it to finish, and return its final status."""
    repo = graphql("{ repositoriesOrError { ... on RepositoryConnection { nodes { name location { name } } } } }")
    node = repo["repositoriesOrError"]["nodes"][0]
    params = {"selector": {"repositoryLocationName": node["location"]["name"], "repositoryName": node["name"],
                           "jobName": job}, **({"runConfigData": config} if config else {})}
    body = json.dumps({"query": "mutation($p: ExecutionParams!) { launchRun(executionParams: $p) "
                                "{ __typename ... on LaunchRunSuccess { run { runId } } } }", "variables": {"p": params}})
    req = urllib.request.Request(os.environ.get("DAGSTER_GRAPHQL", "http://dagster-webserver:3000/dagster/graphql"),
                                 data=body.encode(), headers={"Content-Type": "application/json"})
    run_id = json.loads(urllib.request.urlopen(req, timeout=60).read())["data"]["launchRun"]["run"]["runId"]
    for _ in range(120):
        status = graphql(f'{{ runOrError(runId: "{run_id}") {{ ... on Run {{ status }} }} }}')["runOrError"]["status"]
        if status in ("SUCCESS", "FAILURE", "CANCELED"):
            return status
        time.sleep(2)
    raise AssertionError(f"{job} run {run_id} didn't finish")


def wait_for(fn, seconds=120):
    """Poll fn() until it returns something truthy (sensors run every 15 s); returns that value or fails."""
    for _ in range(seconds // 3):
        got = fn()
        if got:
            return got
        time.sleep(3)
    raise AssertionError(f"timed out waiting for {fn.__doc__ or fn}")


# --- the Pushgateway -------------------------------------------------------------------------------------------

def test_pushgateway_is_up_at_the_pinned_version():
    """The Pushgateway answers and reports 1.11.4."""
    status = requests.get(f"{PUSHGATEWAY}/api/v1/status", timeout=30).json()["data"]
    assert status["build_information"]["version"] == "1.11.4"


def test_prometheus_scrapes_the_pushgateway_keeping_pushed_labels():
    """The pushgateway target is up, and honor_labels keeps each pushed metric's own job label."""
    targets = requests.get(f"{PROMETHEUS}/api/v1/targets", timeout=30).json()["data"]["activeTargets"]
    assert any(t["labels"]["job"] == "pushgateway" and t["health"] == "up" for t in targets)
    config = yaml.safe_load(requests.get(f"{PROMETHEUS}/api/v1/status/config", timeout=30).json()["data"]["yaml"])
    job = next(j for j in config["scrape_configs"] if j["job_name"] == "pushgateway")
    assert job["honor_labels"] is True


def test_pushed_metrics_survive_a_restart():
    """The Pushgateway persists to /lake/pushgateway, and like every monitoring piece has no host port."""
    svc = SERVICES["pushgateway"]
    assert "--persistence.file=/pushgateway/metrics.store" in svc["command"]
    assert "${LAKE_ROOT:-/lake}/pushgateway:/pushgateway" in svc["volumes"]
    assert "ports" not in svc


# --- Dagster ---------------------------------------------------------------------------------------------------

def test_run_sensors_are_on():
    """The success and failure sensors are switched on without anyone touching the UI."""
    data = graphql("{ repositoriesOrError { ... on RepositoryConnection { nodes { sensors { name "
                   "sensorState { status } } } } } }")
    sensors = {s["name"]: s["sensorState"]["status"] for s in data["repositoriesOrError"]["nodes"][0]["sensors"]}
    assert sensors.get("push_run_success") == "RUNNING" and sensors.get("push_run_failure") == "RUNNING", sensors


def test_latest_daily_run_pushed_its_success():
    """The latest successful daily_lake run's end time is what Prometheus has as its last success."""
    runs = graphql('{ runsOrError(filter: {pipelineName: "daily_lake", statuses: [SUCCESS]}, limit: 1) '
                   '{ ... on Runs { results { endTime } } } }')["runsOrError"]["results"]
    assert runs, "no successful daily_lake run yet"
    got = query('lakehouse_job_last_success_timestamp_seconds{pipeline="daily_lake"}')
    assert got and abs(max(got.values()) - runs[0]["endTime"]) < 5, (got, runs)


def test_load_numbers_match_the_latest_load():
    """Rows and flagged values in Prometheus are the latest ops.load_runs row's."""
    con = lake.lake_ro_connect()
    silver, exceptions = con.sql("SELECT rows_silver, exceptions FROM ops.load_runs "
                                 "ORDER BY finished_at DESC LIMIT 1").fetchone()
    assert list(query('lakehouse_load_rows{layer="silver"}').values()) == [float(silver)]
    assert list(query("lakehouse_load_flagged_values").values()) == [float(exceptions)]


def test_data_quality_pass_rates_match_gold():
    """Per-dimension pass rates in Prometheus equal gold.dq_scorecard's, and one rule sits under the 95% floor."""
    got = {dict(k)["dimension"]: round(v, 2) for k, v in query("lakehouse_dq_pass_rate_percent").items()}
    assert got == EXPECTED_DIMENSIONS
    assert list(query("lakehouse_dq_rules_below_floor").values()) == [1.0]


def test_a_failed_run_is_pushed_then_cleared_by_a_success():
    """monitoring_selftest set to fail pushes a failure; set to pass, its success is newer than that failure."""
    before = pushed("lakehouse_job_last_failure_timestamp_seconds", pipeline="monitoring_selftest") or 0
    assert launch("monitoring_selftest", {"ops": {"selftest": {"config": {"fail": True}}}}) == "FAILURE"

    def failure_pushed():
        """a newer failure timestamp for monitoring_selftest"""
        now = pushed("lakehouse_job_last_failure_timestamp_seconds", pipeline="monitoring_selftest")
        return now if now and now > before else None

    failed_at = wait_for(failure_pushed)
    assert launch("monitoring_selftest", {"ops": {"selftest": {"config": {"fail": False}}}}) == "SUCCESS"

    def success_pushed():
        """a success timestamp for monitoring_selftest newer than its failure"""
        now = pushed("lakehouse_job_last_success_timestamp_seconds", pipeline="monitoring_selftest")
        return now if now and now > failed_at else None

    wait_for(success_pushed)
