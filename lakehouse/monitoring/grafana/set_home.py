"""Make the provisioned Home Lakehouse dashboard Grafana's home page (an org preference; provisioning can't set it).

    docker compose --profile core exec -T jupyter python lakehouse/monitoring/grafana/set_home.py [--check]

Grafana's own home-path setting would load the JSON a second time, as a hidden copy; pointing the org preference at
the provisioned dashboard's uid keeps one copy. Waits up to a minute for provisioning to load it. --check only
compares, and exits 1 when the home page is something else.
"""
import os
import sys
import time

import requests

GRAFANA = os.environ.get("GRAFANA_URL", "http://grafana:3000/grafana") + "/api"
AUTH = ("admin", os.environ["GRAFANA_ADMIN_PASSWORD"])
UID = "home-lakehouse"


def main(argv):
    """Set (or --check) the org's home dashboard; print one summary line."""
    for _ in range(20):
        if requests.get(f"{GRAFANA}/dashboards/uid/{UID}", auth=AUTH, timeout=30).status_code == 200:
            break
        time.sleep(3)
    else:
        print(f"Grafana home: dashboard {UID} isn't provisioned")
        return 1
    current = requests.get(f"{GRAFANA}/org/preferences", auth=AUTH, timeout=30).json().get("homeDashboardUID")
    if current == UID:
        print("Grafana home: Home Lakehouse")
        return 0
    if "--check" in argv:
        print(f"Grafana home: {current or 'Grafana default'}, not Home Lakehouse; run monitoring/grafana/set_home.py")
        return 1
    requests.patch(f"{GRAFANA}/org/preferences", json={"homeDashboardUID": UID}, auth=AUTH, timeout=30).raise_for_status()
    print("Grafana home: set to Home Lakehouse")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
