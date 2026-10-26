"""Superset dashboards for the lakehouse, created or brought up to date through Superset's REST API.

    python lakehouse/superset/dashboards.py           # create what's missing, update what drifted
    python lakehouse/superset/dashboards.py --check   # change nothing; exit 1 if anything is missing or out of date

bootstrap.sh runs it in the jupyter container, as Superset's admin, after the lake has data. Every chart reads a
cube from the Cube database that superset/start.sh registers, so dashboards only ever see the governed model.
Running it again changes nothing: each object is found by name first and only written when it differs.
"""
import json
import os
import sys

import requests

SUPERSET = os.environ.get("SUPERSET_URL", "http://superset:8088/superset")
TIMEOUT = 60

TRUST_TABLE = "trust_test"          # the Cube cube over gold.trust_test
TRUST_COLUMNS = ["question_id", "question", "as_loaded", "cleansed", "changed"]
TRUST_CHART = "Trust Test scorecard"
TRUST_DASHBOARD = "Trust Test"
TRUST_SLUG = "trust-test"
TRUST_INTRO = """## The Trust Test

Eight questions a warden might ask, answered twice from the same 100,000 mock records: once from a default
load (session 1) and once after sessions 2 to 4 cleansed and typed the data. **Every answer changes.** The
*as loaded* column is what an untrusted load reports, with full confidence.

Computed by the course's own `lnl.trust`, loaded by the daily run, and read through Cube."""


def login():
    """A requests session logged in to Superset as admin, with the CSRF token write endpoints need."""
    s = requests.Session()
    r = s.post(f"{SUPERSET}/api/v1/security/login", timeout=TIMEOUT,
               json={"username": "admin", "password": os.environ["SUPERSET_ADMIN_PASSWORD"], "provider": "db",
                     "refresh": False})
    r.raise_for_status()
    s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    csrf = s.get(f"{SUPERSET}/api/v1/security/csrf_token/", timeout=TIMEOUT)
    csrf.raise_for_status()
    s.headers.update({"X-CSRFToken": csrf.json()["result"], "Referer": SUPERSET})
    return s


def call(api, method, path, **kwargs):
    """One REST call; raises with Superset's own message when it refuses."""
    r = api.request(method, f"{SUPERSET}/api/v1/{path}", timeout=TIMEOUT, **kwargs)
    if not r.ok:
        raise RuntimeError(f"{method} {path}: {r.status_code} {r.text[:300]}")
    return r.json()


def find(api, resource, column, value):
    """The first object of a resource (dataset, chart, dashboard) whose column equals value, or None."""
    q = f"(filters:!((col:{column},opr:eq,value:'{value}')),page_size:100)"
    found = call(api, "GET", f"{resource}/", params={"q": q})["result"]
    return found[0] if found else None


def cube_database(api):
    """ID of the Cube database (registered by superset/start.sh)."""
    db = find(api, "database", "database_name", "Cube")
    if db is None:
        raise RuntimeError("Superset has no Cube database yet; is superset/start.sh done?")
    return db["id"]


def ensure_dataset(api, db_id, check):
    """The dataset over the trust_test cube: (id or None, status)."""
    ds = find(api, "dataset", "table_name", TRUST_TABLE)
    if ds:
        return ds["id"], "up to date"
    if check:
        return None, "missing"
    created = call(api, "POST", "dataset/", json={"database": db_id, "schema": "public", "table_name": TRUST_TABLE})
    return created["id"], "created"


def chart_spec(ds_id):
    """(params, query_context) for a plain table of the scorecard, one row per question in order."""
    params = {"datasource": f"{ds_id}__table", "viz_type": "table", "query_mode": "raw",
              "all_columns": TRUST_COLUMNS, "order_by_cols": [json.dumps(["question_id", True])],
              "row_limit": 100, "server_pagination": False, "include_search": False, "adhoc_filters": []}
    query = {"columns": TRUST_COLUMNS, "orderby": [["question_id", True]], "row_limit": 100, "filters": [],
             "extras": {"having": "", "where": ""}, "metrics": [], "post_processing": []}
    context = {"datasource": {"id": ds_id, "type": "table"}, "force": False, "queries": [query],
               "form_data": params, "result_format": "json", "result_type": "full"}
    return params, context


def ensure_chart(api, ds_id, check):
    """The scorecard table chart, created or brought back to chart_spec: (id or None, status)."""
    params, context = chart_spec(ds_id) if ds_id else ({}, {})
    body = {"slice_name": TRUST_CHART, "viz_type": "table", "datasource_id": ds_id, "datasource_type": "table",
            "params": json.dumps(params), "query_context": json.dumps(context),
            "description": "The Trust Test, answered from a default load and from the cleansed, typed data."}
    found = find(api, "chart", "slice_name", TRUST_CHART)
    if found is None:
        if check:
            return None, "missing"
        return call(api, "POST", "chart/", json=body)["id"], "created"
    current = call(api, "GET", f"chart/{found['id']}")["result"]
    if json.loads(current.get("params") or "{}") == params:
        return found["id"], "up to date"
    if check:
        return found["id"], "out of date"
    call(api, "PUT", f"chart/{found['id']}", json=body)
    return found["id"], "updated"


def layout(chart_id):
    """Dashboard position_json: the explanation on top, the scorecard table below, both full width."""
    def node(kind, ident, parents, meta, children=()):
        """One layout component in Superset's v2 dashboard layout format."""
        return {"type": kind, "id": ident, "children": list(children), "parents": parents, "meta": meta}
    root, grid = ["ROOT_ID"], ["ROOT_ID", "GRID_ID"]
    return {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": node("GRID", "GRID_ID", root, {}, ["ROW-intro", "ROW-scorecard"]),
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": TRUST_DASHBOARD}},
        "ROW-intro": node("ROW", "ROW-intro", grid, {"background": "BACKGROUND_TRANSPARENT"}, ["MARKDOWN-intro"]),
        "MARKDOWN-intro": node("MARKDOWN", "MARKDOWN-intro", grid + ["ROW-intro"],
                               {"width": 12, "height": 30, "code": TRUST_INTRO}),
        "ROW-scorecard": node("ROW", "ROW-scorecard", grid, {"background": "BACKGROUND_TRANSPARENT"},
                              ["CHART-scorecard"]),
        "CHART-scorecard": node("CHART", "CHART-scorecard", grid + ["ROW-scorecard"],
                                {"chartId": chart_id, "width": 12, "height": 60, "sliceName": TRUST_CHART}),
    }


def ensure_dashboard(api, chart_id, check):
    """The Trust Test dashboard, published, laid out by layout(): (id or None, status)."""
    body = {"dashboard_title": TRUST_DASHBOARD, "slug": TRUST_SLUG, "published": True,
            "position_json": json.dumps(layout(chart_id)), "json_metadata": json.dumps({"color_scheme": ""})}
    found = find(api, "dashboard", "dashboard_title", TRUST_DASHBOARD)
    if found is None:
        if check or chart_id is None:
            return None, "missing"
        return call(api, "POST", "dashboard/", json=body)["id"], "created"
    current = call(api, "GET", f"dashboard/{found['id']}")["result"]
    if json.loads(current.get("position_json") or "{}") == layout(chart_id) and current.get("published"):
        return found["id"], "up to date"
    if check:
        return found["id"], "out of date"
    call(api, "PUT", f"dashboard/{found['id']}", json=body)
    return found["id"], "updated"


def ensure_link(api, chart_id, dashboard_id, check):
    """The chart belongs to the dashboard (what /dashboard/<id>/charts lists): status."""
    if chart_id is None or dashboard_id is None:
        return "missing"
    linked = {d["id"] for d in call(api, "GET", f"chart/{chart_id}")["result"].get("dashboards", [])}
    if dashboard_id in linked:
        return "up to date"
    if check:
        return "missing"
    call(api, "PUT", f"chart/{chart_id}", json={"dashboards": sorted(linked | {dashboard_id})})
    return "updated"


def main(argv):
    """Bring the Trust Test dashboard into place (or just check it); print one line per object and a summary."""
    check = "--check" in argv
    api = login()
    ds_id, ds = ensure_dataset(api, cube_database(api), check)
    chart_id, chart = ensure_chart(api, ds_id, check)
    dash_id, dash = ensure_dashboard(api, chart_id, check)
    link = ensure_link(api, chart_id, dash_id, check)
    states = {"dataset": ds, "chart": chart, "dashboard": dash, "chart on dashboard": link}
    for name, state in states.items():
        print(f"  {name}: {state}")
    bad = [name for name, state in states.items() if state in ("missing", "out of date")]
    print(f"Trust Test dashboard: {'needs ' + ', '.join(bad) if bad else 'in place'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
