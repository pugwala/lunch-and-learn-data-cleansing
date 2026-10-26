"""Acceptance test for the lakehouse's monitoring (Phase 6).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_monitoring.py

Prometheus 3.15.0 scrapes the VM (node-exporter) and every container (cAdvisor), keeps 30 days on /lake/prometheus,
and is served at https://<LAKE_TS_HOST>/prometheus/ behind Caddy's basic auth (it has no login of its own). None of
the exporters has a host port: the tailnet can reach anything bound to the VM's 0.0.0.0, so they stay on the
Compose network.
"""
import os
from pathlib import Path

import pytest
import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
SERVICES = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]
EXPORTERS = ("node-exporter", "cadvisor")


def query(promql):
    """Run an instant PromQL query; returns the result vector."""
    r = requests.get(f"{PROMETHEUS}/api/v1/query", params={"query": promql}, timeout=30)
    r.raise_for_status()
    return r.json()["data"]["result"]


def test_prometheus_is_up_at_the_pinned_version():
    """Prometheus is ready under /prometheus and reports 3.15.0."""
    requests.get(f"{PROMETHEUS}/-/ready", timeout=30).raise_for_status()
    info = requests.get(f"{PROMETHEUS}/api/v1/status/buildinfo", timeout=30).json()["data"]
    assert info["version"] == "3.15.0"


def test_every_scrape_target_is_up():
    """Prometheus scrapes itself, the VM and the containers, and every target answers."""
    targets = requests.get(f"{PROMETHEUS}/api/v1/targets", timeout=30).json()["data"]["activeTargets"]
    jobs = {t["labels"]["job"]: t["health"] for t in targets}
    assert {"prometheus", "node", "cadvisor"} <= set(jobs), jobs
    assert all(h == "up" for h in jobs.values()), jobs


def test_node_metrics_describe_the_vm():
    """node-exporter reads the host, not its container: the VM's memory, and the /lake data disk."""
    mem = float(query("node_memory_MemTotal_bytes")[0]["value"][1])
    assert 28 * 2**30 < mem < 34 * 2**30, mem   # the VM has about 31 GiB
    assert query('node_filesystem_size_bytes{mountpoint="/lake"}'), "no /lake filesystem in node-exporter"


def test_cadvisor_sees_each_service_by_name():
    """Container metrics carry the Compose service name, for every long-running service."""
    got = {r["metric"]["container_label_com_docker_compose_service"]
           for r in query("container_memory_usage_bytes{container_label_com_docker_compose_service!=\"\"}")}
    want = {"catalog-pg", "jupyter", "dagster-webserver", "dagster-daemon", "cube", "superset", "mcp",
            "opensearch", "openmetadata-server", "fuseki", "caddy", "prometheus"}
    assert want <= got, want - got


def test_thirty_days_on_the_lake_disk():
    """The time series live on /lake/prometheus and are kept 30 days."""
    flags = requests.get(f"{PROMETHEUS}/api/v1/status/flags", timeout=30).json()["data"]
    assert flags["storage.tsdb.retention.time"] == "30d"
    assert flags["storage.tsdb.path"] == "/prometheus"
    assert "${LAKE_ROOT:-/lake}/prometheus:/prometheus" in SERVICES["prometheus"]["volumes"]


@pytest.mark.parametrize("service", ["prometheus", *EXPORTERS])
def test_no_monitoring_port_reaches_the_host(service):
    """Prometheus and the exporters publish no host ports; Caddy is the only way in."""
    assert "ports" not in SERVICES[service] and SERVICES[service].get("network_mode") != "host"
