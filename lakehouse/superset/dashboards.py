"""Superset dashboards for the lakehouse, created or brought up to date through Superset's REST API.

    python lakehouse/superset/dashboards.py           # create what's missing, update what drifted
    python lakehouse/superset/dashboards.py --check   # change nothing; exit 1 if anything is missing or out of date

bootstrap.sh runs it in the jupyter container, as Superset's admin, after the lake has data. Every chart reads a
cube from the Cube database that superset/start.sh registers, so dashboards only ever see the governed model.
Running it again changes nothing: each object is found by name first and only written when it differs.
Two dashboards, each a spec in DASHBOARDS: the Trust Test, and Custody (the custody data products' views). Custody
charts use Cube's own MEASURE(...) as their metric, so people are still counted as distinct SIDs, by Cube.
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
CUSTODY_DASHBOARD = "Custody"
CUSTODY_SLUG = "custody"
CUSTODY_INTRO = """## Custody

Who is held, and for what, from the **Custody Population** and **Custody Offenses** data products. People are
distinct SIDs, counted by Cube: a person held under two records is one person, so these counts are never added up
from smaller ones. Each month-end is a snapshot of the records open that day; records with no sentence or release
date (about 6%) can't be placed in time and aren't in it. "Latest month-end" is the last complete month.

A month's numbers are **final** only once every business unit that feeds them has closed the month (Records,
Classification, Courts liaison); until then they are **provisional**. *Period status* says which, and who is late."""


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


def ensure_dataset(api, db_id, table, check):
    """The dataset over one Cube cube or view: (id or None, status)."""
    ds = find(api, "dataset", "table_name", table)
    if ds:
        return ds["id"], "up to date"
    if check:
        return None, "missing"
    created = call(api, "POST", "dataset/", json={"database": db_id, "schema": "public", "table_name": table})
    return created["id"], "created"


def context(ds_id, params, query):
    """A chart's query_context: what /chart/<id>/data/ runs."""
    return {"datasource": {"id": ds_id, "type": "table"}, "force": False, "queries": [query],
            "form_data": params, "result_format": "json", "result_type": "full"}


def raw_table(columns, order_by, ascending, limit):
    """A builder of (params, query_context) for a plain table of rows, sorted by one column."""
    def build(ds_id):
        """The table's spec for one dataset."""
        params = {"datasource": f"{ds_id}__table", "viz_type": "table", "query_mode": "raw",
                  "all_columns": columns, "order_by_cols": [json.dumps([order_by, ascending])],
                  "row_limit": limit, "server_pagination": False, "include_search": False, "adhoc_filters": []}
        query = {"columns": columns, "orderby": [[order_by, ascending]], "row_limit": limit, "filters": [],
                 "extras": {"having": "", "where": ""}, "metrics": [], "post_processing": []}
        return params, context(ds_id, params, query)
    return build


trust_table = raw_table(TRUST_COLUMNS, "question_id", True, 100)   # the scorecard, one row per question in order


def measure(name, label):
    """A Superset metric that asks Cube for one of its measures, so Cube's definition (distinct SIDs) applies."""
    return {"expressionType": "SQL", "sqlExpression": f"MEASURE({name})", "label": label, "hasCustomLabel": True,
            "optionName": f"metric_{name}"}


def monthly_line(time_column, metric):
    """A builder of (params, query_context) for a line of one Cube measure by month."""
    def build(ds_id):
        """The line chart's spec for one dataset."""
        label = metric["label"]
        params = {"datasource": f"{ds_id}__table", "viz_type": "echarts_timeseries_line", "x_axis": time_column,
                  "time_grain_sqla": "P1M", "metrics": [metric], "groupby": [], "row_limit": 10000,
                  "adhoc_filters": [{"expressionType": "SIMPLE", "subject": time_column, "operator": "TEMPORAL_RANGE",
                                     "comparator": "No filter", "clause": "WHERE"}],
                  "show_legend": False, "rich_tooltip": True, "truncate_metric": True}
        query = {"columns": [{"timeGrain": "P1M", "columnType": "BASE_AXIS", "sqlExpression": time_column,
                              "label": time_column, "expressionType": "SQL"}],
                 "metrics": [metric], "orderby": [[metric, False]], "row_limit": 10000, "series_columns": [],
                 "filters": [{"col": time_column, "op": "TEMPORAL_RANGE", "val": "No filter"}],
                 "extras": {"having": "", "where": ""},
                 "post_processing": [{"operation": "pivot", "options": {"index": [time_column], "columns": [],
                                      "aggregates": {label: {"operator": "mean"}}, "drop_missing_columns": False}},
                                     {"operation": "flatten"}]}
        return params, context(ds_id, params, query)
    return build


def latest_table(groupby, metric):
    """A builder of (params, query_context) for a table of one Cube measure by one dimension, latest month-end."""
    def build(ds_id):
        """The table chart's spec for one dataset."""
        latest = {"expressionType": "SIMPLE", "subject": "is_latest_month_end", "operator": "==", "comparator": True,
                  "clause": "WHERE"}
        params = {"datasource": f"{ds_id}__table", "viz_type": "table", "query_mode": "aggregate",
                  "groupby": [groupby], "metrics": [metric], "all_columns": [], "adhoc_filters": [latest],
                  "row_limit": 100, "order_desc": True, "server_pagination": False, "include_search": False}
        query = {"columns": [groupby], "metrics": [metric], "orderby": [[metric, False]], "row_limit": 100,
                 "filters": [{"col": "is_latest_month_end", "op": "==", "val": True}],
                 "extras": {"having": "", "where": ""}, "post_processing": []}
        return params, context(ds_id, params, query)
    return build


PEOPLE = measure("people_in_custody", "People in custody")
DASHBOARDS = [
    {"title": TRUST_DASHBOARD, "slug": TRUST_SLUG, "intro": TRUST_INTRO,
     "charts": [{"name": TRUST_CHART, "cell": "scorecard", "table": TRUST_TABLE, "viz": "table", "spec": trust_table,
                 "height": 60,
                 "description": "The Trust Test, answered from a default load and from the cleansed, typed data."}],
     "rows": {"scorecard": [TRUST_CHART]}},
    {"title": CUSTODY_DASHBOARD, "slug": CUSTODY_SLUG, "intro": CUSTODY_INTRO,
     "charts": [
         {"name": "Period status by month-end", "cell": "status", "table": "period_readiness", "viz": "table",
          "spec": raw_table(["month_end", "product", "period_status", "waiting_on"], "month_end", False, 12),
          "height": 40,
          "description": "Whether each product's numbers for a month-end are final yet, and who they still wait on."},
         {"name": "People in custody, by month-end", "cell": "population", "table": "custody_population", "viz": "echarts_timeseries_line",
          "spec": monthly_line("month_end", PEOPLE), "height": 50,
          "description": "Distinct people held at each month-end (Custody Population)."},
         {"name": "People in custody by custody level, latest month-end", "cell": "levels", "table": "custody_population",
          "viz": "table", "spec": latest_table("custody_level", PEOPLE), "height": 50,
          "description": "G1 (least) to G5 (most supervised), at the last complete month-end."},
         {"name": "People in custody by offense category, latest month-end", "cell": "offenses",
          "table": "custody_offenses",
          "viz": "table", "spec": latest_table("offense_against", PEOPLE), "height": 50,
          "description": "What people are held for, at the last complete month-end (Custody Offenses)."},
         {"name": "New sentences per month", "cell": "admissions", "table": "custody_admissions", "viz": "echarts_timeseries_line",
          "spec": monthly_line("sentence_date", measure("admissions", "New sentences")), "height": 50,
          "description": "Records sentenced each month (Custody Offenses)."}],
     "rows": {"status": ["Period status by month-end"],
              "population": ["People in custody, by month-end"],
              "now": ["People in custody by custody level, latest month-end",
                      "People in custody by offense category, latest month-end"],
              "admissions": ["New sentences per month"]}},
]


def ensure_chart(api, chart, ds_id, check):
    """One chart, created or brought back to its spec: (id or None, status)."""
    params, ctx = chart["spec"](ds_id) if ds_id else ({}, {})
    body = {"slice_name": chart["name"], "viz_type": chart["viz"], "datasource_id": ds_id, "datasource_type": "table",
            "params": json.dumps(params), "query_context": json.dumps(ctx), "description": chart["description"]}
    found = find(api, "chart", "slice_name", chart["name"])
    if found is None:
        if check or ds_id is None:
            return None, "missing"
        return call(api, "POST", "chart/", json=body)["id"], "created"
    current = call(api, "GET", f"chart/{found['id']}")["result"]
    if json.loads(current.get("params") or "{}") == params:
        return found["id"], "up to date"
    if check:
        return found["id"], "out of date"
    call(api, "PUT", f"chart/{found['id']}", json=body)
    return found["id"], "updated"


def layout(spec, ids):
    """Dashboard position_json: the explanation on top, then each row of charts, sharing the 12-column width."""
    def node(kind, ident, parents, meta, children=()):
        """One layout component in Superset's v2 dashboard layout format."""
        return {"type": kind, "id": ident, "children": list(children), "parents": parents, "meta": meta}
    root, grid = ["ROOT_ID"], ["ROOT_ID", "GRID_ID"]
    rows = [f"ROW-{r}" for r in spec["rows"]]
    out = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
           "GRID_ID": node("GRID", "GRID_ID", root, {}, ["ROW-intro", *rows]),
           "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": spec["title"]}},
           "ROW-intro": node("ROW", "ROW-intro", grid, {"background": "BACKGROUND_TRANSPARENT"}, ["MARKDOWN-intro"]),
           "MARKDOWN-intro": node("MARKDOWN", "MARKDOWN-intro", grid + ["ROW-intro"],
                                  {"width": 12, "height": 30, "code": spec["intro"]})}
    charts = {c["name"]: c for c in spec["charts"]}
    for row, names in zip(rows, spec["rows"].values()):
        cells = [f"CHART-{charts[n]['cell']}" for n in names]
        out[row] = node("ROW", row, grid, {"background": "BACKGROUND_TRANSPARENT"}, cells)
        for cell, name in zip(cells, names):
            out[cell] = node("CHART", cell, grid + [row], {"chartId": ids[name], "width": 12 // len(names),
                                                           "height": charts[name]["height"], "sliceName": name})
    return out


def ensure_dashboard(api, spec, ids, check):
    """One dashboard, published, laid out by layout(): (id or None, status)."""
    body = {"dashboard_title": spec["title"], "slug": spec["slug"], "published": True,
            "position_json": json.dumps(layout(spec, ids)), "json_metadata": json.dumps({"color_scheme": ""})}
    found = find(api, "dashboard", "dashboard_title", spec["title"])
    if found is None:
        if check or None in ids.values():
            return None, "missing"
        return call(api, "POST", "dashboard/", json=body)["id"], "created"
    current = call(api, "GET", f"dashboard/{found['id']}")["result"]
    if json.loads(current.get("position_json") or "{}") == layout(spec, ids) and current.get("published"):
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
    """Bring every dashboard into place (or just check them); print one line per object and a summary."""
    check = "--check" in argv
    api = login()
    db_id, bad, datasets = cube_database(api), [], {}
    for spec in DASHBOARDS:
        states, ids = {}, {}
        for chart in spec["charts"]:
            if chart["table"] not in datasets:
                datasets[chart["table"]], states[f"dataset {chart['table']}"] = ensure_dataset(api, db_id, chart["table"], check)
            ids[chart["name"]], states[f"chart {chart['name']}"] = ensure_chart(api, chart, datasets[chart["table"]], check)
        dash_id, states["dashboard"] = ensure_dashboard(api, spec, ids, check)
        for chart in spec["charts"]:
            states[f"on dashboard: {chart['name']}"] = ensure_link(api, ids[chart["name"]], dash_id, check)
        print(f"{spec['title']}:")
        for name, state in states.items():
            print(f"  {name}: {state}")
        gaps = sum(state in ("missing", "out of date") for state in states.values())
        bad += [f"{spec['title']} ({gaps} of {len(states)} objects)"] if gaps else []
    print(f"Superset dashboards ({', '.join(d['title'] for d in DASHBOARDS)}): "
          f"{'needs ' + ', '.join(bad) if bad else 'in place'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
