"""Push metrics to the lakehouse's Pushgateway (Phase 6), where Prometheus scrapes them.

Every sample is a gauge, pushed with POST, which replaces only the same-named metrics in its group: a job's success
push leaves its last-failure time alone. PUSHGATEWAY_URL (default http://pushgateway:9091) says where. A push never
raises: monitoring must not fail a load or a run, so a failed push comes back as an error message to log.
"""
import os

from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

PUSHGATEWAY = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")


def push(group: str, samples: list[tuple[str, str, float, dict]], grouping_key: dict | None = None) -> str | None:
    """Push gauges, each (name, help, value, labels), under job=<group> plus grouping_key; None, or what went wrong."""
    registry = CollectorRegistry()
    gauges = {}
    for name, doc, value, labels in samples:
        if name not in gauges:
            gauges[name] = Gauge(name, doc, sorted(labels), registry=registry)
        (gauges[name].labels(**labels) if labels else gauges[name]).set(value)
    try:
        pushadd_to_gateway(PUSHGATEWAY, job=group, registry=registry, grouping_key=grouping_key or {}, timeout=10)
    except Exception as e:   # noqa: BLE001 (any failure to reach the Pushgateway is only worth a warning)
        return f"push to {PUSHGATEWAY} failed: {e}"
    return None
