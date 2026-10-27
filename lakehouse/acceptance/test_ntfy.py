"""Acceptance test for ntfy, where the lakehouse's alerts go (Phase 6, alert delivery).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_ntfy.py

ntfy v2.30.0 runs on this VM and is reached only on the tailnet, at https://<LAKE_TS_HOST>:8443/ through Caddy
(ntfy can't live under a sub-path). Nothing is open to anyone: default access is deny-all, and three users are
declared in config with only what each needs. alertmanager may only publish to the lakehouse-* topics; ian reads
them from the ntfy phone app; checker reads them for these tests. Alertmanager posts its webhook JSON and
ntfy/templates/lakehouse.yml turns it into a notification whose priority follows the alert's severity. Topics: lakehouse-alerts (Ian's phone),
lakehouse-heartbeat (Watchdog, every 4 hours, so silence there means alerting is down) and lakehouse-selftest
(the alert monitoring_selftest raises in tests, kept off Ian's phone).
"""
import json
import os
import time
from pathlib import Path

import requests
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
NTFY = os.environ.get("NTFY_URL", "http://ntfy:8080")
CHECKER = ("checker", os.environ.get("NTFY_CHECKER_PASSWORD", ""))
SERVICES = yaml.safe_load((LAKEHOUSE / "compose.yaml").read_text())["services"]


def messages(topic, since="12h", auth=CHECKER):
    """The cached messages on a topic since a duration ago, read as checker: a list of ntfy message dicts."""
    r = requests.get(f"{NTFY}/{topic}/json", params={"poll": "1", "since": since}, auth=auth, timeout=30)
    r.raise_for_status()
    return [m for m in map(json.loads, r.text.splitlines()) if m.get("event") == "message"]


def test_ntfy_is_up_at_the_pinned_version():
    """ntfy answers its health check and reports 2.30.0."""
    assert requests.get(f"{NTFY}/v1/health", timeout=10).json() == {"healthy": True}
    assert "binwiederhier/ntfy:${NTFY_TAG:-v2.30.0}" == SERVICES["ntfy"]["image"]


def test_nobody_gets_in_without_a_user():
    """Deny-all by default: anonymous can neither publish to nor read the alert topics."""
    for topic in ("lakehouse-alerts", "lakehouse-heartbeat", "lakehouse-selftest", "anything-else"):
        assert requests.post(f"{NTFY}/{topic}", data="hi", timeout=10).status_code == 403, topic
        assert requests.get(f"{NTFY}/{topic}/json", params={"poll": "1"}, timeout=10).status_code == 403, topic


def test_each_user_has_only_what_it_needs():
    """checker reads but can't publish; a wrong password is refused; no user reaches topics outside lakehouse-*."""
    assert requests.get(f"{NTFY}/lakehouse-alerts/json", params={"poll": "1"}, auth=CHECKER, timeout=10).ok
    assert requests.post(f"{NTFY}/lakehouse-alerts", data="hi", auth=CHECKER, timeout=10).status_code == 403
    assert requests.get(f"{NTFY}/other-topic/json", params={"poll": "1"}, auth=CHECKER, timeout=10).status_code == 403
    wrong = requests.get(f"{NTFY}/lakehouse-alerts/json", params={"poll": "1"}, auth=("checker", "wrong"), timeout=10)
    assert wrong.status_code == 401


def test_no_password_or_hash_is_in_the_repo():
    """Users come from plain passwords in .env, hashed by ntfy/start.sh at start: compose names only variables."""
    env = SERVICES["ntfy"]["environment"]
    for user in ("ALERTMANAGER", "IAN", "CHECKER"):
        assert env[f"NTFY_{user}_PASSWORD"].startswith(f"${{NTFY_{user}_PASSWORD:?"), user
    assert not any("$2a$" in str(v) or "$2b$" in str(v) for v in env.values())


def test_ntfy_keeps_its_data_on_the_lake_disk_and_runs_unprivileged():
    """Messages and users live on /lake/ntfy; the container isn't root and publishes no host port (Caddy fronts it)."""
    svc = SERVICES["ntfy"]
    assert "${LAKE_ROOT:-/lake}/ntfy:/var/lib/ntfy" in svc["volumes"]
    assert "./ntfy:/etc/ntfy:ro" in [v.split("   #")[0].strip() for v in svc["volumes"]]
    assert svc["user"] == "1000:100" and "ports" not in svc


def test_caddy_serves_ntfy_on_its_own_port():
    """ntfy can't sit under a path, so Caddy gives it port 8443 on the same Tailscale name and certificate."""
    assert "8443:8443" in SERVICES["caddy"]["ports"]
    caddyfile = (LAKEHOUSE / "caddy" / "Caddyfile").read_text()
    assert "{$LAKE_TS_HOST}:8443 {" in caddyfile and "reverse_proxy ntfy:8080" in caddyfile


def test_the_watchdog_heartbeat_arrives():
    """Watchdog always fires and is re-sent every 4 hours to lakehouse-heartbeat: alerting works end to end, from
    Prometheus through Alertmanager to ntfy, right now. It never goes to Ian's phone topic."""
    deadline = time.time() + 120   # right after a fresh start, the first one lands after Alertmanager's group_wait
    while not (beats := [m for m in messages("lakehouse-heartbeat", since="5h") if "Watchdog" in m.get("title", "")]):
        assert time.time() < deadline, "no Watchdog notification on lakehouse-heartbeat in the last 5 hours"
        time.sleep(10)
    assert {m.get("priority", 3) for m in beats} == {1}   # min priority: silent even if someone subscribes
    assert not [m for m in messages("lakehouse-alerts") if "Watchdog" in m.get("title", "")]


def test_iphone_wakeups_relay_through_ntfy_sh_without_the_alert():
    """iOS only wakes an app through Apple's push service, so ntfy forwards a poll request to ntfy.sh (a hash of the
    topic URL and the message ID, never the text); the phone then fetches the alert from this server over Tailscale."""
    config = yaml.safe_load((LAKEHOUSE / "ntfy" / "server.yml").read_text())
    assert config["upstream-base-url"] == "https://ntfy.sh"
