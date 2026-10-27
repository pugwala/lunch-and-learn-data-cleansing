"""Acceptance test for the lakehouse's monitoring (Phase 6).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_monitoring.py

Prometheus 3.15.0 scrapes the VM (node-exporter) and every container (cAdvisor), keeps 30 days on /lake/prometheus,
and is served at https://<LAKE_TS_HOST>/prometheus/ behind Caddy's basic auth (it has no login of its own). None of
the exporters has a host port: the tailnet can reach anything bound to the VM's 0.0.0.0, so they stay on the
Compose network.
"""
import os
import re
import subprocess
from pathlib import Path

import psycopg2

import pytest
import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
SERVICES = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]
EXPORTERS = ("node-exporter", "cadvisor", "postgres-exporter", "blackbox")


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


# --- Grafana ---------------------------------------------------------------------------------------------------

GRAFANA = os.environ.get("GRAFANA_URL", "http://grafana:3000/grafana")
ADMIN = ("admin", os.environ.get("GRAFANA_ADMIN_PASSWORD", ""))


def test_grafana_is_up_at_the_pinned_version():
    """Grafana answers its health check under /grafana and reports 13.2.3."""
    health = requests.get(f"{GRAFANA}/api/health", timeout=30).json()
    assert health["database"] == "ok" and health["version"] == "13.2.3", health


def test_grafana_needs_a_login():
    """No anonymous access: the API refuses a request without credentials, and a wrong password."""
    assert requests.get(f"{GRAFANA}/api/datasources", timeout=30).status_code == 401
    assert requests.get(f"{GRAFANA}/api/datasources", auth=("admin", "admin"), timeout=30).status_code == 401


def test_prometheus_is_grafanas_provisioned_data_source():
    """The admin password from .env logs in, and the provisioned Prometheus data source answers."""
    assert ADMIN[1], "GRAFANA_ADMIN_PASSWORD isn't in the jupyter container"
    r = requests.get(f"{GRAFANA}/api/datasources/uid/prometheus/health", auth=ADMIN, timeout=30)
    assert r.status_code == 200 and r.json()["status"] == "OK", r.text


def test_grafana_does_not_phone_home():
    """Usage reporting, update checks and the news feed are off, and nobody can sign themselves up."""
    env = SERVICES["grafana"]["environment"]
    for key in ("GF_ANALYTICS_REPORTING_ENABLED", "GF_ANALYTICS_CHECK_FOR_UPDATES",
                "GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES", "GF_NEWS_NEWS_FEED_ENABLED",
                "GF_USERS_ALLOW_SIGN_UP", "GF_AUTH_ANONYMOUS_ENABLED"):
        assert str(env.get(key)).lower() == "false", key


def test_grafana_keeps_its_data_on_the_lake_disk():
    """Dashboards and users live in /lake/grafana; the admin password comes only from .env."""
    svc = SERVICES["grafana"]
    assert "${LAKE_ROOT:-/lake}/grafana:/var/lib/grafana" in svc["volumes"]
    assert "${GRAFANA_ADMIN_PASSWORD" in str(svc["environment"]["GF_SECURITY_ADMIN_PASSWORD"])
    assert "ports" not in svc


# --- service probes --------------------------------------------------------------------------------------------

def bootstrap_sites():
    """The sites bootstrap.sh checks through Caddy (its "path:status" pairs), by path."""
    line = next(l for l in (LAKEHOUSE / "scripts" / "bootstrap.sh").read_text().splitlines() if l.startswith("for pair in"))
    return {pair.split(":")[0] for pair in re.findall(r'"([a-z]+:\d+)"', line)}


def test_postgres_exporter_sees_the_catalog():
    """The exporter is connected (pg_up 1) and reports the lake's catalog database."""
    assert query("pg_up")[0]["value"][1] == "1"
    assert query('pg_database_size_bytes{datname="ducklake"}'), "no ducklake database in postgres-exporter"


def test_postgres_exporter_reads_statistics_not_data():
    """It logs in as lake_monitor, which holds pg_monitor and can't read the catalog's tables."""
    with psycopg2.connect(host="catalog-pg", dbname="ducklake", user="lake_rw",
                          password=os.environ["PGPASSWORD"]) as pg, pg.cursor() as cur:
        cur.execute("SELECT pg_has_role('lake_monitor', 'pg_monitor', 'MEMBER'), "
                    "has_table_privilege('lake_monitor', 'public.ducklake_snapshot', 'SELECT')")
        assert cur.fetchone() == (True, False)
    assert SERVICES["postgres-exporter"]["environment"]["DATA_SOURCE_USER"] == "lake_monitor"


def test_every_site_is_probed_through_caddy():
    """Blackbox probes exactly the sites bootstrap.sh checks, through Caddy on the Tailscale name, and each succeeds."""
    probes = {r["metric"]["site"]: r["value"][1] for r in query('probe_success{job="sites"}')}
    assert set(probes) == bootstrap_sites(), probes
    assert all(v == "1" for v in probes.values()), probes


def test_certificate_has_more_than_a_week_left():
    """The Tailscale certificate Caddy serves expires more than 7 days from now."""
    left = query('min(probe_ssl_earliest_cert_expiry{job="sites"}) - time()')
    assert left and float(left[0]["value"][1]) > 7 * 86400, left


def test_tailscale_name_stays_out_of_the_repo():
    """The probe targets name the host only as a placeholder; start.sh fills it in at run time, outside the repo."""
    for f in (LAKEHOUSE / "monitoring").rglob("*"):
        if f.is_file():
            text = f.read_text()
            assert not re.search(r"\.ts\.net|100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.", text), f
    assert "LAKE_TS_HOST" in (LAKEHOUSE / "monitoring" / "prometheus" / "sites.yml").read_text()


# --- the Home Lakehouse dashboard ------------------------------------------------------------------------------

DASHBOARD = LAKEHOUSE / "monitoring" / "grafana" / "dashboards" / "home-lakehouse.json"


def panels():
    """The dashboard's panels (rows excluded), from the JSON in the repo."""
    import json
    return [p for p in json.loads(DASHBOARD.read_text())["panels"] if p["type"] != "row"]


def test_dashboard_is_provisioned_with_its_three_rows():
    """Grafana serves "Home Lakehouse" from the repo's JSON, rows Platform, Pipeline, Data quality and Data
    products, as its home."""
    r = requests.get(f"{GRAFANA}/api/dashboards/uid/home-lakehouse", auth=ADMIN, timeout=30)
    assert r.status_code == 200, r.text
    dash = r.json()["dashboard"]
    assert dash["title"] == "Home Lakehouse"
    assert [p["title"] for p in dash["panels"] if p["type"] == "row"] == ["Platform", "Pipeline", "Data quality",
                                                                          "Data products"]
    assert r.json()["meta"]["provisioned"] is True
    prefs = requests.get(f"{GRAFANA}/api/org/preferences", auth=ADMIN, timeout=30).json()
    assert prefs.get("homeDashboardUID") == "home-lakehouse", prefs   # the provisioned one, not a second copy
    home = requests.get(f"{GRAFANA}/api/dashboards/home", auth=ADMIN, timeout=30).json()
    assert "/d/home-lakehouse/" in home.get("redirectUri", ""), home


def test_every_panel_has_data():
    """Each panel reads the provisioned Prometheus, and each of its queries returns something today."""
    for p in panels():
        assert p["datasource"]["uid"] == "prometheus", p["title"]
        for t in p["targets"]:
            assert query(t["expr"]), f'{p["title"]}: {t["expr"]} returned nothing'
