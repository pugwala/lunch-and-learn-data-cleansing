"""MCP server for the home-lab lakehouse: governed metrics from Cube, meanings from the glossary, and the rulebook.

Tools
    list_metrics()                                    every metric query_metric offers: its view, data product,
                                                      description, aggregation and dimensions
    query_metric(metric, group_by=None, filters=None, time_dimension=None, date_from=None, date_to=None,
                 granularity=None, limit=100)
                                                      a Cube measure by name, optionally grouped by dimensions,
                                                      filtered, and over a time range by day, week, month, quarter
                                                      or year; a data product's metric also says which months are
                                                      final. Filters on a masked column compare the masked value
    define_term(term)                                 a glossary term from OpenMetadata: definition, synonyms,
                                                      columns, metrics, sensitivity and typed relations
    business_rules(about=None)                        the rulebook from Fuseki (all of it, or the rules about one
                                                      term or synonym): each rule's statement, terms and enforcers,
                                                      and whether a shape or alert holds today (from Prometheus)
    data_product_info(product=None)                   every data product, or one by name: its domain,
                                                      question, tables, views and metrics, glossary terms, the
                                                      business inputs it waits on, its data contracts, which months
                                                      are final, and how fresh it is against its promise
    data_quality(area=None, table=None)               the course data's quality scorecard (pass rate per
                                                      dimension, rules under the floor), the expanded world's
                                                      flagged values by silver table and rule (counts only), and
                                                      the data-quality alerts firing now

Served over streamable HTTP at /mcp (stateless, JSON responses) and through Caddy at https://<LAKE_TS_HOST>/mcp.
Every request needs `Authorization: Bearer <MCP_TOKEN>`. The server holds no lake credentials: list_metrics and
query_metric go
through Cube's REST API, so an agent sees only Cube's governed model, never the lake. define_term reads the
Lakehouse glossary in OpenMetadata (loaded there by om_sync.py) as mcp-reader, a read-only account.
business_rules reads Fuseki's public SPARQL endpoint and Prometheus, both read-only and without credentials.
data_product_info combines Fuseki (the catalog's DCAT and readiness graphs), OpenMetadata (contracts, as
mcp-reader), Cube (views, readiness) and Prometheus (freshness); data_quality reads Cube and Prometheus.
"""
import base64
import hmac
import html
import json
import os
import time
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import jwt
import uvicorn
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel
from starlette.responses import PlainTextResponse

CUBE_API = os.environ.get("CUBE_API", "http://cube:4000/cubejs-api/v1")
OM_API = os.environ.get("OM_API", "http://openmetadata-server:8585/openmetadata/api/v1")
OM_READER = "mcp-reader@open-metadata.org"
GLOSSARY = "Lakehouse"
FUSEKI = os.environ.get("FUSEKI_URL", "http://fuseki:3030/fuseki") + "/lakehouse"
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090/prometheus")
RULEBOOK = "https://lakehouse.invalid/graph/rulebook"
OPERATORS = ["equals", "notEquals", "contains", "notContains", "startsWith", "endsWith", "gt", "gte", "lt", "lte",
             "set", "notSet"]
GRANULARITIES = ["day", "week", "month", "quarter", "year"]
CHECKED_ELSEWHERE = {"dbt": "a dbt test, run with the lake's dbt builds (lakehouse/dbt/tests)",
                     "test": "an acceptance test (lakehouse/acceptance)",
                     "sparql": "a deduction rule Fuseki applies on every load (lakehouse/ontology/rules)",
                     "cube": "a Cube measure that computes it the governed way"}
PRODUCT_IRI = "https://lakehouse.invalid/product/"
INPUT_IRI = "https://lakehouse.invalid/input/"
DQ_FLOOR = 95   # the daily run's quality floor (dbt/tests/dq_score_floor.sql, DataQualityBelowFloor)
DQ_ALERTS = ["DataQualityBelowFloor", "MoreRulesBelowFloor", "FlaggedShareHigh", "SilverDroppedRows"]
AREAS = ["course", "world"]
_om_token = {"value": None}

server = MCPServer(
    name="lakehouse",
    instructions="Governed metrics, definitions and business rules for the home-lab lakehouse's mock offender "
                 "data. Ask list_metrics for what can be asked, query_metric for a metric by name (grouped, "
                 "filtered or over a time range; an unknown name lists the real ones), define_term for what a term "
                 "means, and business_rules for the rules about a term and whether they hold today. There is no raw "
                 "SQL access, by design. data_product_info says what a data product promises and whether this "
                 "month's number is final and fresh; data_quality says how clean the data is.")


# ---- Results (typed, so clients get structured content with a schema) -----------------------------------------
class PeriodStatus(BaseModel):
    """One month-end of a data product: final, or provisional and the business units it still waits on."""
    month_end: str
    status: str
    waiting_on: list[str]


class Readiness(BaseModel):
    """Whether a data product's numbers are final: its latest month-end, and every month still provisional."""
    product: str
    latest: PeriodStatus
    provisional: list[PeriodStatus]


class MetricFilter(BaseModel):
    """Keep only rows whose dimension compares with values by the operator (equals, notEquals, contains,
    notContains, startsWith, endsWith, gt, gte, lt, lte; set and notSet take no values)."""
    dimension: str
    operator: str = "equals"
    values: list[str] = []


class TimeWindow(BaseModel):
    """The time range a metric was asked over: the time dimension, the first and last day (inclusive), and the
    granularity its rows are by (none: one total for the range)."""
    dimension: str
    date_from: str | None = None
    date_to: str | None = None
    granularity: str | None = None


class MetricResult(BaseModel):
    """A metric's rows: one per combination of the group_by dimensions (and time period, when asked by a
    granularity), each with the metric's value, after the filters. When the metric comes from a data product's
    view, readiness says which of its month-ends are final yet."""
    metric: str
    group_by: list[str]
    rows: list[dict[str, Any]]
    filters: list[MetricFilter] = []
    time: TimeWindow | None = None
    readiness: Readiness | None = None


class Metric(BaseModel):
    """One metric in one Cube view: what it counts, how it aggregates, and the dimensions it can be grouped by."""
    name: str
    view: str
    title: str
    description: str
    aggregation: str
    product: str | None
    dimensions: list[str]


class MetricList(BaseModel):
    """Every metric query_metric offers, one entry per view that has it."""
    metrics: list[Metric]


class Relation(BaseModel):
    """A typed link to another glossary term (identifies, partOf, broader, relatedTo)."""
    type: str
    term: str


class Enforcer(BaseModel):
    """What enforces a rule (kind: cube, dbt, prometheus, shacl, sparql or test; name), whether it holds today
    (holds, broken, unknown, or not checked here) and how that's known."""
    kind: str
    name: str
    status: str
    detail: str


class BusinessRule(BaseModel):
    """One rule in the rulebook: what must hold, the glossary terms it's about, and what enforces it."""
    name: str
    statement: str
    about: list[str]
    enforced_by: list[Enforcer]


class RuleBook(BaseModel):
    """The rules asked for: all of them, or those about one glossary term."""
    about: str | None
    rules: list[BusinessRule]


class BusinessInputInfo(BaseModel):
    """A business input a data product waits on: who closes it, in which system, and within how many days."""
    name: str
    title: str
    unit: str
    system: str
    closes_within_days: int


class ContractInfo(BaseModel):
    """A data contract OpenMetadata holds for one of the product's tables: its refresh promise and its columns."""
    table: str
    refresh_every: int
    refresh_unit: str
    available_by: str
    timezone: str
    max_latency_hours: int
    columns: int


class Freshness(BaseModel):
    """How fresh a product is against its promise: fresh, stale, or unknown when Prometheus has no figure."""
    status: str
    refreshed_at: str | None = None
    refreshed_epoch: float | None = None
    age_hours: float | None = None
    max_age_hours: float | None = None


class Coverage(BaseModel):
    """Records a custody snapshot draws on, and how many of them it can't place in time."""
    records_total: int
    records_without_dates: int


class ProductInfo(BaseModel):
    """One data product: what it promises (catalog), what it serves (views, metrics), what it waits on (inputs),
    what holds its shape (contracts), and whether today's numbers can be trusted (readiness, freshness)."""
    name: str
    title: str
    domain: str
    question: str
    description: str
    tables: list[str]
    views: list[str]
    metrics: list[str]
    terms: list[str]
    inputs: list[BusinessInputInfo]
    contracts: list[ContractInfo]
    uncontracted_tables: list[str]
    readiness: Readiness | None
    freshness: Freshness
    coverage: Coverage | None


class DataProducts(BaseModel):
    """Every data product asked about."""
    products: list[ProductInfo]


class DimensionScore(BaseModel):
    """One quality dimension of the course data: its pass rate, how many rules check it, how many are under the floor."""
    dimension: str
    pass_rate: float
    rules: int
    rules_below_floor: int


class RuleScore(BaseModel):
    """One quality rule's score on one column."""
    dimension: str
    column: str
    rule: str
    score_pct: float


class Scorecard(BaseModel):
    """The course data's quality scorecard (gold.dq_scorecard, through Cube) for the latest load."""
    built_at: str | None
    floor: int
    dimensions: list[DimensionScore]
    below_floor: list[RuleScore]


class TableIssues(BaseModel):
    """One world_silver table's flagged values, by rule, and the rows they touch (counts only)."""
    table: str
    flagged_values: int
    rows_flagged: int
    by_rule: dict[str, int]


class WorldIssues(BaseModel):
    """The expanded world's flagged values (world_silver.exceptions, through Cube), most-flagged table first."""
    flagged_values: int
    tables: list[TableIssues]


class DataQuality(BaseModel):
    """How clean the data is: the course scorecard, the world's flags, and the data-quality alerts firing now."""
    course: Scorecard | None = None
    world: WorldIssues | None = None
    alerts_firing: list[str]
    note: str


class TermDefinition(BaseModel):
    """A glossary term: what it means, its synonyms, and the columns, metrics and terms it connects to."""
    term: str
    definition: str
    synonyms: list[str]
    columns: list[str]
    metrics: list[str]
    sensitivity: str
    relations: list[Relation]


# ---- Cube ----------------------------------------------------------------------------------------------------
def cube(path, payload=None):
    """One call to Cube's REST API with a short-lived token signed with CUBEJS_API_SECRET; returns its JSON."""
    token = jwt.encode({"exp": int(time.time()) + 300}, os.environ["CUBEJS_API_SECRET"], algorithm="HS256")
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"{CUBE_API}/{path}", data=data, method="POST" if data else "GET",
                                 headers={"Authorization": token, "Content-Type": "application/json"})
    for _ in range(30):   # Cube answers "Continue wait" while a query is still running
        try:
            body = json.loads(urllib.request.urlopen(req, timeout=60).read())
        except urllib.error.HTTPError as e:
            raise ToolError(f"Cube refused the query: {e.read().decode()[:300]}") from None
        if body.get("error") != "Continue wait":
            return body
        time.sleep(1)
    raise ToolError("Cube didn't finish the query in time")


def catalog():
    """Every public measure and dimension Cube offers, as {"measures": [...], "dimensions": [...]} of full names,
    plus "time_dimensions" (those of type time) and "products": {view: data product} for views tagged meta.product."""
    meta = cube("meta")
    out = {kind: [m["name"] for c in meta["cubes"] for m in c.get(kind, []) if m.get("public", True)]
           for kind in ("measures", "dimensions")}
    out["time_dimensions"] = [d["name"] for c in meta["cubes"] for d in c.get("dimensions", [])
                              if d.get("public", True) and d.get("type") == "time"]
    out["products"] = {c["name"]: c["meta"]["product"] for c in meta["cubes"] if (c.get("meta") or {}).get("product")}
    return out


def readiness(product):
    """A data product's readiness from Cube's period_readiness view: its latest month-end and any still
    provisional (a month is final once every business input the product depends on has closed it)."""
    dims = ["period_readiness.month_end", "period_readiness.period_status", "period_readiness.waiting_on"]
    data = cube("load", {"query": {"dimensions": dims, "order": {"period_readiness.month_end": "desc"}, "limit": 5000,
                                   "filters": [{"member": "period_readiness.product", "operator": "equals",
                                                "values": [product]}]}})
    periods = [PeriodStatus(month_end=str(r[dims[0]])[:10], status=r[dims[1]],
                            waiting_on=[u.strip() for u in (r[dims[2]] or "").split(",") if u.strip()])
               for r in data.get("data", [])]
    if not periods:
        return None
    return Readiness(product=product, latest=periods[0], provisional=[p for p in periods if p.status == "provisional"])


def resolve(name, options, prefer_cube=None):
    """A full Cube member name from a full or short name; a short name shared by several cubes prefers prefer_cube."""
    if name in options:
        return name
    matches = [o for o in options if o.split(".", 1)[1] == name]
    if len(matches) > 1 and prefer_cube:
        matches = [m for m in matches if m.startswith(prefer_cube + ".")] or matches
    return matches[0] if len(matches) == 1 else None


def number(value):
    """Cube sends measures as strings: whole numbers become int, others float; anything else is left alone."""
    try:
        d = Decimal(str(value))
    except Exception:
        return value
    return int(d) if d == d.to_integral_value() else float(d)


def pick(metric, group_by, cat):
    """(measure, dimensions) as full Cube names, or (None, []) for an unknown metric. Several views can offer the
    same metric (people_in_custody is in custody_population and custody_offenses): the first that also has every
    dimension asked for wins; otherwise the first, its dimensions resolved across Cube, where joins reach them."""
    measures = [m for m in cat["measures"] if m == metric or m.split(".", 1)[1] == metric]
    if not measures:
        return None, []
    for measure in measures:
        own = [d for d in cat["dimensions"] if d.startswith(measure.split(".", 1)[0] + ".")]
        dims = [resolve(name, own) for name in group_by]
        if all(dims):
            return measure, dims
    measure = measures[0]
    return measure, [resolve(name, cat["dimensions"], prefer_cube=measure.split(".", 1)[0]) for name in group_by]


@server.tool()
def list_metrics() -> MetricList:
    """Every governed metric you can ask query_metric for, one entry per Cube view that offers it: its name, view,
    title, description, aggregation, data product (if the view is one) and the dimensions you can group it by. The
    same name can sit in several views with different dimensions; pass "view.name" to query_metric to pick one.
    aggregation countDistinct counts distinct people or things: never add its values across groups."""
    short = lambda full: full.split(".", 1)[1]   # noqa: E731
    metrics = []
    for c in cube("meta")["cubes"]:
        dims = [short(d["name"]) for d in c.get("dimensions", []) if d.get("public", True)]
        for m in c.get("measures", []):
            if m.get("public", True):
                metrics.append(Metric(name=short(m["name"]), view=c["name"],
                                      title=m.get("shortTitle") or short(m["name"]),
                                      description=m.get("description") or "", aggregation=m.get("aggType") or "",
                                      product=(c.get("meta") or {}).get("product"), dimensions=dims))
    return MetricList(metrics=metrics)


def a_day(value, what):
    """A YYYY-MM-DD date as text, or ToolError naming the format."""
    if value is None:
        return None
    try:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError
        date.fromisoformat(value)
    except ValueError:
        raise ToolError(f"{what} must be a date written YYYY-MM-DD, not {value!r}") from None
    return value


def time_window(measure, time_dimension, date_from, date_to, granularity, cat):
    """The time dimension (a full Cube name) to ask over, or None when no time range or granularity was asked:
    the one named, or the measure's view's only time dimension. ToolError lists what would work."""
    date_from, date_to = a_day(date_from, "date_from"), a_day(date_to, "date_to")
    if granularity is not None and granularity not in GRANULARITIES:
        raise ToolError(f"No granularity {granularity!r}. Granularities: {', '.join(GRANULARITIES)}")
    if time_dimension is None and date_from is None and date_to is None and granularity is None:
        return None
    view = measure.split(".", 1)[0]
    times = [t for t in cat["time_dimensions"] if t.startswith(view + ".")] or cat["time_dimensions"]
    if time_dimension is None:
        if len(times) != 1:
            raise ToolError("Name a time_dimension: " + ", ".join(sorted(t.split(".", 1)[1] for t in times)))
        return times[0]
    found = resolve(time_dimension, times, prefer_cube=view)
    if found is None:
        raise ToolError(f"No time dimension named {time_dimension!r}. Time dimensions: " +
                        ", ".join(sorted({t.split(".", 1)[1] for t in cat["time_dimensions"]})))
    return found


@server.tool()
def query_metric(metric: str, group_by: list[str] | None = None, filters: list[MetricFilter] | None = None,
                 time_dimension: str | None = None, date_from: str | None = None, date_to: str | None = None,
                 granularity: str | None = None, limit: int = 100) -> MetricResult:
    """A governed metric from Cube by name (for example record_count, total_restitution, people_in_custody),
    optionally grouped by dimensions (for example custody_level, unit_name, offense_against), filtered (for
    example [{"dimension": "custody_level", "operator": "equals", "values": ["G5"]}]; notEquals and notContains
    keep rows where the dimension is empty, as Cube does), and over a time range:
    date_from and date_to (YYYY-MM-DD, inclusive) on a time_dimension (for example sentence_date; a view with one
    time dimension uses it on its own), by granularity day, week, month, quarter or year (none: one total for the
    range). Short names work; an unknown name returns the real ones. Only Cube's model is reachable: restricted
    columns come back masked, and a filter on one compares the masked value. A metric from a data product also
    returns readiness: whether its latest month-end is final, and which months are still provisional and who they
    wait on; say so when you report a provisional month."""
    cat = catalog()
    filters = filters or []
    for f in filters:
        if f.operator not in OPERATORS:
            raise ToolError(f"No filter operator {f.operator!r}. Operators: {', '.join(OPERATORS)}")
    names = (group_by or []) + [f.dimension for f in filters]
    measure, dims = pick(metric, names + ([time_dimension] if time_dimension else []), cat)
    if measure is None:
        raise ToolError(f"No metric named {metric!r}. Metrics: " +
                         ", ".join(sorted({m.split(".", 1)[1] for m in cat["measures"]})))
    for name, dim in zip(names, dims):
        if dim is None:
            raise ToolError(f"No dimension named {name!r}. Dimensions: " +
                             ", ".join(sorted({d.split(".", 1)[1] for d in cat["dimensions"]})))
    group_dims, filter_dims = dims[:len(group_by or [])], dims[len(group_by or []):len(names)]
    tdim = time_window(measure, time_dimension, date_from, date_to, granularity, cat)
    query = {"measures": [measure], "dimensions": group_dims, "limit": max(1, min(limit, 1000)),
             "filters": [{"member": d, "operator": f.operator, **({"values": f.values} if f.operator not in
                                                                   ("set", "notSet") else {})}
                         for d, f in zip(filter_dims, filters)]}
    if tdim:
        window = {"dimension": tdim}
        if date_from or date_to:
            window["dateRange"] = [date_from or "1900-01-01", date_to or "2999-12-31"]
        if granularity:
            window["granularity"] = granularity
            query["order"] = {tdim: "asc"}
        query["timeDimensions"] = [window]
    data = cube("load", {"query": query})
    short = lambda full: full.split(".", 1)[1]   # noqa: E731
    period = f"{tdim}.{granularity}" if tdim and granularity else None
    rows = []
    for row in data.get("data", []):
        out = {short(k): (number(v) if k == measure else v) for k, v in row.items() if k in group_dims + [measure]}
        if period:
            out[short(tdim)] = str(row.get(period) or "")[:10]
        rows.append(out)
    product = cat["products"].get(measure.split(".", 1)[0])
    return MetricResult(metric=short(measure), group_by=[short(d) for d in group_dims], rows=rows, filters=filters,
                        time=TimeWindow(dimension=short(tdim), date_from=date_from, date_to=date_to,
                                        granularity=granularity) if tdim else None,
                        readiness=readiness(product) if product else None)


# ---- Glossary (OpenMetadata) ----------------------------------------------------------------------------------
def om(path):
    """GET an OpenMetadata API path as mcp-reader, logging in when there's no token yet or it has expired."""
    for attempt in (1, 2):
        if not _om_token["value"]:
            body = json.dumps({"email": OM_READER, "password": base64.b64encode(
                os.environ["OM_MCP_PASSWORD"].encode()).decode()}).encode()
            req = urllib.request.Request(f"{OM_API}/users/login", data=body, method="POST",
                                         headers={"Content-Type": "application/json"})
            try:
                _om_token["value"] = json.loads(urllib.request.urlopen(req, timeout=30).read())["accessToken"]
            except (urllib.error.URLError, KeyError) as e:
                raise ToolError(f"OpenMetadata is unavailable or refused mcp-reader ({e})") from None
        req = urllib.request.Request(f"{OM_API}/{path}", headers={"Authorization": f"Bearer {_om_token['value']}"})
        try:
            return json.loads(urllib.request.urlopen(req, timeout=30).read())
        except urllib.error.HTTPError as e:
            if e.code == 401 and attempt == 1:
                _om_token["value"] = None   # expired: log in again once
                continue
            raise ToolError(f"OpenMetadata refused {path}: {e.code}") from None
        except urllib.error.URLError as e:
            raise ToolError(f"OpenMetadata is unavailable ({e.reason})") from None


def text(value):
    """Description text as a person reads it: OpenMetadata returns descriptions HTML-escaped."""
    return " ".join(html.unescape(value or "").split())


def listed(value):
    """A comma-separated custom property as a list."""
    return [v.strip() for v in (value or "").split(",") if v.strip()]


@server.tool()
def define_term(term: str) -> TermDefinition:
    """What a lakehouse term means, from the Lakehouse glossary in OpenMetadata: its definition, synonyms, the
    columns and metrics it maps to, its sensitivity, and its typed relations to other terms (both directions).
    Matches the term or any synonym, in any case; an unknown term lists the glossary."""
    glossary = om(f"glossaries/name/{GLOSSARY}")
    terms = om(f"glossaryTerms?glossary={glossary['id']}&fields=relatedTerms,synonyms,extension,tags&limit=200")["data"]
    wanted = term.strip().casefold()
    for t in terms:
        if wanted in {t["name"].casefold(), *(s.casefold() for s in t.get("synonyms") or [])}:
            ext = t.get("extension") or {}
            restricted = any(x["tagFQN"] == "Sensitivity.Restricted" for x in t.get("tags") or [])
            return TermDefinition(term=t["name"], definition=text(t.get("description")),
                                  synonyms=t.get("synonyms") or [], columns=listed(ext.get("lakehouseColumns")),
                                  metrics=listed(ext.get("lakehouseMetrics")),
                                  sensitivity="restricted" if restricted else "internal",
                                  relations=[Relation(type=r.get("relationType", "relatedTo"), term=r["term"]["name"])
                                             for r in t.get("relatedTerms") or []])
    raise ToolError(f"No term {term!r} in the glossary. Terms: " + ", ".join(sorted(t["name"] for t in terms)))


# ---- The rulebook (Fuseki) and today's checks (Prometheus) ---------------------------------------------------
def sparql(query):
    """Rows of a SPARQL SELECT on Fuseki's public endpoint, as [{variable: value}]."""
    body = urllib.parse.urlencode({"query": query}).encode()
    req = urllib.request.Request(f"{FUSEKI}/sparql", data=body, method="POST",
                                 headers={"Accept": "application/sparql-results+json",
                                          "Content-Type": "application/x-www-form-urlencoded"})
    try:
        result = json.loads(urllib.request.urlopen(req, timeout=30).read())
    except (urllib.error.URLError, ValueError) as e:
        raise ToolError(f"Fuseki is unavailable ({e})") from None
    return [{k: v["value"] for k, v in row.items()} for row in result["results"]["bindings"]]


def prometheus(query):
    """The series a PromQL query returns now, or None when Prometheus can't be reached."""
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    try:
        return json.loads(urllib.request.urlopen(url, timeout=15).read())["data"]["result"]
    except (urllib.error.URLError, ValueError, KeyError):
        return None


def enforcer(ref):
    """One enforced_by entry ("kind:name") with whether it holds today: a SHACL shape by the daily rule check's
    violations, a Prometheus alert by whether it's firing; anything else is checked elsewhere."""
    kind, name = ref.split(":", 1)
    if kind == "shacl":
        series = prometheus(f'lakehouse_business_rule_violations{{shape="{name}"}}')
        if series is None:
            return Enforcer(kind=kind, name=name, status="unknown", detail="Prometheus is unavailable")
        if not series:
            return Enforcer(kind=kind, name=name, status="unknown", detail="the daily rule check hasn't reported it yet")
        n = int(float(series[0]["value"][1]))
        return Enforcer(kind=kind, name=name, status="holds" if n == 0 else "broken",
                        detail=f"{n} violation{'s' * (n != 1)} in the daily rule check")
    if kind == "prometheus":
        series = prometheus(f'ALERTS{{alertname="{name}", alertstate="firing"}}')
        if series is None:
            return Enforcer(kind=kind, name=name, status="unknown", detail="Prometheus is unavailable")
        return Enforcer(kind=kind, name=name, status="broken" if series else "holds",
                        detail="the alert is firing" if series else "the alert isn't firing")
    return Enforcer(kind=kind, name=name, status="not checked here",
                    detail=f"checked by {CHECKED_ELSEWHERE.get(kind, kind)}")


@server.tool()
def business_rules(about: str | None = None) -> RuleBook:
    """The lakehouse's business rules, from the rulebook in Fuseki: every rule, or those about one glossary term
    (its name or any synonym, in any case). Each rule says what must hold, which terms it's about, and what
    enforces it: a SHACL shape or a Prometheus alert also says whether it holds today; a dbt test, an acceptance
    test, a deduction or a Cube measure says it's checked elsewhere. An unknown term lists the terms rules are about."""
    # A rule's terms and their names are matched together, in one OPTIONAL keyed on the rule: matched apart, a rule
    # about no term would pick up every term's name.
    rows = sparql(f"""PREFIX lh: <https://lakehouse.invalid/ontology#>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?title ?statement ?term ?by WHERE {{
          GRAPH <{RULEBOOK}> {{ ?r a lh:BusinessRule ; dct:title ?title ; dct:description ?statement ;
                                   lh:enforcedBy ?by }}
          OPTIONAL {{ GRAPH <{RULEBOOK}> {{ ?r lh:about ?t }} ?t skos:prefLabel ?term }} }}""")
    rules = {}
    for row in rows:
        r = rules.setdefault(row["title"], {"statement": row["statement"], "about": set(), "by": set()})
        r["by"].add(row["by"])
        if row.get("term"):
            r["about"].add(row["term"])
    wanted = None
    if about is not None:
        labels = sparql("""PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT ?name ?alt WHERE { ?t a skos:Concept ; skos:prefLabel ?name . OPTIONAL { ?t skos:altLabel ?alt } }""")
        names = {}
        for row in labels:
            names[row["name"].casefold()] = row["name"]
            if row.get("alt"):
                names.setdefault(row["alt"].casefold(), row["name"])
        wanted = names.get(about.strip().casefold())
        if wanted is None:
            raise ToolError(f"No glossary term or synonym {about!r}. Rules are about: " +
                            ", ".join(sorted({t for r in rules.values() for t in r["about"]})))
    return RuleBook(about=wanted, rules=[
        BusinessRule(name=title, statement=r["statement"], about=sorted(r["about"]),
                     enforced_by=[enforcer(ref) for ref in sorted(r["by"])])
        for title, r in sorted(rules.items()) if wanted is None or wanted in r["about"]])


# ---- Data products (Fuseki, OpenMetadata, Cube, Prometheus) -------------------------------------------------
def product_catalog():
    """{product name: its catalog entry} from Fuseki: the DCAT dataset (title, description, domain, question,
    tables, Cube views, glossary terms) and the readiness graph's inputs (title, unit, system, days to close)."""
    rows = sparql("""PREFIX lh: <https://lakehouse.invalid/ontology#>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX dcat: <http://www.w3.org/ns/dcat#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?p ?title ?description ?domain ?question ?table ?view ?term WHERE {
          ?p a dcat:Dataset ; dct:title ?title .
          OPTIONAL { ?p dct:description ?description } OPTIONAL { ?p lh:domain ?domain }
          OPTIONAL { ?p lh:question ?question } OPTIONAL { ?p lh:usesTable ?table }
          OPTIONAL { ?p dcat:accessService/dct:title ?view } OPTIONAL { ?p lh:usesTerm/skos:prefLabel ?term } }""")
    out = {}
    for r in rows:
        p = out.setdefault(r["p"].removeprefix(PRODUCT_IRI), {
            "title": r["title"], "description": r.get("description", ""), "domain": r.get("domain", ""),
            "question": r.get("question", ""), "tables": set(), "views": set(), "terms": set(), "inputs": {}})
        for key, var in (("tables", "table"), ("views", "view"), ("terms", "term")):
            if r.get(var):
                p[key].add(r[var])
    for r in sparql("""PREFIX lh: <https://lakehouse.invalid/ontology#>
        PREFIX dct: <http://purl.org/dc/terms/>
        SELECT ?p ?i ?title ?unit ?system ?days WHERE {
          ?p lh:dependsOn ?i . ?i dct:title ?title ; lh:unit ?unit ; lh:system ?system ; lh:closesWithinDays ?days }"""):
        name = r["p"].removeprefix(PRODUCT_IRI)
        if name in out:
            i = r["i"].removeprefix(INPUT_IRI)
            out[name]["inputs"][i] = BusinessInputInfo(name=i, title=r["title"], unit=r["unit"], system=r["system"],
                                                       closes_within_days=int(r["days"]))
    return out


def contracts_by_table():
    """{"schema.table": ContractInfo} for every data contract OpenMetadata holds (its name sits under its table's
    FQN: lakehouse.lake.<schema>.<table>.dataContract_<name>)."""
    out = {}
    for c in om("dataContracts?limit=500&fields=sla").get("data", []):
        table = c["fullyQualifiedName"].removeprefix("lakehouse.lake.").split(".dataContract_")[0]
        sla = c.get("sla") or {}
        every, latency = sla.get("refreshFrequency") or {}, sla.get("maxLatency") or {}
        out[table] = ContractInfo(table=table, refresh_every=int(every.get("interval", 0)),
                                  refresh_unit=every.get("unit", ""), available_by=sla.get("availabilityTime", ""),
                                  timezone=sla.get("timezone", ""), columns=len(c.get("schema") or []),
                                  max_latency_hours=int(latency.get("value", 0)) * (24 if latency.get("unit") == "day" else 1))
    return out


def by_product(query):
    """{product: value} of a per-product metric in Prometheus ({} when it can't be reached)."""
    return {x["metric"].get("product"): float(x["value"][1]) for x in prometheus(query) or []}


def central(epoch):
    """An epoch time as ISO 8601 in Central time (UTC if the zone data is missing)."""
    try:
        from zoneinfo import ZoneInfo
        zone = ZoneInfo("America/Chicago")
    except Exception:
        zone = timezone.utc
    return datetime.fromtimestamp(epoch, zone).isoformat(timespec="seconds")


def find_product(product, names):
    """The product name asked for, in any case, hyphens or spaces alike ("Court Outcomes" is court-outcomes, which
    is also how every title reads); None if there is no such product."""
    key = lambda s: " ".join(s.replace("-", " ").casefold().split())   # noqa: E731
    return next((name for name in names if key(product) == key(name)), None)


@server.tool()
def data_product_info(product: str | None = None) -> DataProducts:
    """What a data product is and whether its numbers can be trusted today: every product, or one by name (in any
    case, with hyphens or spaces: "court outcomes" is court-outcomes). Each says its domain and the question it answers, the tables it delivers and the Cube views and
    metrics that serve it (ask query_metric for those), the glossary terms it speaks in, the business inputs it waits
    on (which unit closes each month, within how many days), the data contracts OpenMetadata holds for its tables
    (and which tables have none), whether its latest month-end is final or still provisional (and every provisional
    month, with who it waits on), and how fresh it is against its promise (refreshed when, how many hours old, and
    the most it promises). The custody products also say how many records their snapshot can't place in time."""
    entries = product_catalog()
    if product is not None:
        name = find_product(product, entries)
        if name is None:
            raise ToolError(f"No data product {product!r}. Products: " +
                            ", ".join(f"{n} ({p['title']})" for n, p in sorted(entries.items())))
        entries = {name: entries[name]}
    contracts = contracts_by_table()
    meta = cube("meta")["cubes"]
    refreshed, max_age = by_product("lakehouse_product_refreshed_timestamp_seconds"), \
        by_product("lakehouse_product_max_age_seconds")
    totals, undated = by_product("lakehouse_product_records_total"), by_product("lakehouse_product_records_without_dates")
    now, out = time.time(), []
    for name, p in sorted(entries.items()):
        views = sorted(p["views"])
        metrics = sorted(m["name"] for c in meta if c["name"] in views for m in c.get("measures", [])
                         if m.get("public", True))
        if name in refreshed:
            age, limit = (now - refreshed[name]) / 3600, max_age.get(name, 0) / 3600 or None
            freshness = Freshness(status="unknown" if limit is None else "fresh" if age <= limit else "stale",
                                  refreshed_at=central(refreshed[name]), refreshed_epoch=refreshed[name],
                                  age_hours=round(age, 2), max_age_hours=limit)
        else:
            freshness = Freshness(status="unknown")
        tables = sorted(p["tables"])
        out.append(ProductInfo(
            name=name, title=p["title"], domain=p["domain"], question=p["question"], description=p["description"],
            tables=tables, views=views, metrics=metrics, terms=sorted(p["terms"]),
            inputs=[p["inputs"][i] for i in sorted(p["inputs"])],
            contracts=[contracts[t] for t in tables if t in contracts],
            uncontracted_tables=[t for t in tables if t not in contracts],
            readiness=readiness(name), freshness=freshness,
            coverage=Coverage(records_total=int(totals[name]), records_without_dates=int(undated[name]))
            if name in totals and name in undated else None))
    return DataProducts(products=out)


# ---- Data quality (Cube, Prometheus) -------------------------------------------------------------------------
def scorecard():
    """The course data's scorecard through Cube's dq_scorecard: pass rate per dimension, every rule's score."""
    rates = cube("load", {"query": {"measures": ["dq_scorecard.pass_rate"], "dimensions": ["dq_scorecard.dimension"]}})
    dims = ["dq_scorecard.dimension", "dq_scorecard.column_name", "dq_scorecard.rule", "dq_scorecard.score_pct",
            "dq_scorecard.built_at"]
    rules = cube("load", {"query": {"dimensions": dims, "limit": 5000}}).get("data", [])
    below = sorted((RuleScore(dimension=r[dims[0]], column=r[dims[1]], rule=r[dims[2]], score_pct=float(r[dims[3]]))
                    for r in rules if float(r[dims[3]]) < DQ_FLOOR),
                   key=lambda r: (r.score_pct, r.dimension, r.column, r.rule))
    dimensions = [DimensionScore(dimension=r["dq_scorecard.dimension"], pass_rate=float(r["dq_scorecard.pass_rate"]),
                                 rules=sum(1 for x in rules if x[dims[0]] == r["dq_scorecard.dimension"]),
                                 rules_below_floor=sum(1 for x in below if x.dimension == r["dq_scorecard.dimension"]))
                  for r in rates.get("data", [])]
    built = max((str(r[dims[4]]) for r in rules if r.get(dims[4])), default=None)
    return Scorecard(built_at=built, floor=DQ_FLOOR, dimensions=sorted(dimensions, key=lambda d: d.dimension),
                     below_floor=below)


def world_issues(table=None):
    """The world's flagged values through Cube's world_exceptions, counts only: per table and rule, and the rows
    each table has flagged (counted per table, since distinct rows can't be added up). table narrows it to one."""
    m, d = "world_exceptions.", ["world_exceptions.table_name", "world_exceptions.rule"]
    per_table = cube("load", {"query": {"measures": [m + "flagged_values", m + "rows_flagged"], "dimensions": d[:1],
                                        "limit": 5000}}).get("data", [])
    known = sorted(r[d[0]] for r in per_table)
    if table is not None and table not in known:
        raise ToolError(f"No world_silver table {table!r} has flags. Tables: {', '.join(known)}")
    per_rule = cube("load", {"query": {"measures": [m + "flagged_values"], "dimensions": d, "limit": 5000}}).get("data", [])
    tables = [TableIssues(table=r[d[0]], flagged_values=int(r[m + "flagged_values"]),
                          rows_flagged=int(r[m + "rows_flagged"]),
                          by_rule={x[d[1]]: int(x[m + "flagged_values"]) for x in per_rule if x[d[0]] == r[d[0]]})
              for r in per_table if table is None or r[d[0]] == table]
    tables.sort(key=lambda t: (-t.flagged_values, t.table))
    return WorldIssues(flagged_values=sum(t.flagged_values for t in tables), tables=tables)


@server.tool()
def data_quality(area: str | None = None, table: str | None = None) -> DataQuality:
    """How clean the lakehouse's data is. area "course" is the course's 100,000 offender records: the pass rate of
    each quality dimension (completeness, conformity, consistency, validity, uniqueness, timeliness) on the latest
    load, how many rules check it, and every rule scoring under the 95% floor, worst first. area "world" is the
    expanded world: how many values its silver layer flagged (kept as written, never dropped), per silver table and
    rule (missing_token, two_digit_year, orphan, ...), and how many rows they touch; table narrows it to one silver
    table. Counts only: no flagged value is ever returned, since one can be a name. With no area, both. Either way,
    the data-quality alerts firing now (from Prometheus)."""
    if area is not None and area not in AREAS:
        raise ToolError(f"No area {area!r}. Areas: {', '.join(AREAS)} (or none, for both)")
    if table is not None and area == "course":
        raise ToolError("table narrows the world's flags (area world); the course scorecard is by column instead")
    alerts = prometheus('ALERTS{alertstate="firing"}')
    firing = sorted({x["metric"]["alertname"] for x in alerts or []} & set(DQ_ALERTS))
    note = ("Prometheus is unavailable, so firing alerts are unknown" if alerts is None
            else f"data-quality alerts checked: {', '.join(DQ_ALERTS)}")
    return DataQuality(course=scorecard() if area in (None, "course") else None,
                       world=world_issues(table) if area in (None, "world") else None,
                       alerts_firing=firing, note=note)


# ---- HTTP ----------------------------------------------------------------------------------------------------
@server.custom_route("/healthz", methods=["GET"])
async def healthz(_request):
    """Liveness for the container healthcheck; the only path that needs no token."""
    return PlainTextResponse("ok")


class BearerAuth:
    """ASGI middleware: every HTTP request except /healthz must carry the right bearer token, or it gets 401."""

    def __init__(self, app, token):
        """Wrap app; token is MCP_TOKEN."""
        self.app, self.expected = app, f"Bearer {token}".encode()

    async def __call__(self, scope, receive, send):
        """Check the Authorization header in constant time, then hand over to the MCP app."""
        if scope["type"] == "http" and scope["path"] != "/healthz":
            sent = dict(scope.get("headers", [])).get(b"authorization", b"")
            if not hmac.compare_digest(sent, self.expected):
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-type", b"application/json"), (b"www-authenticate", b"Bearer")]})
                await send({"type": "http.response.body", "body": b'{"error": "unauthorized"}'})
                return
        await self.app(scope, receive, send)


def app():
    """The MCP app behind bearer-token auth, accepting only this VM's Tailscale name and the Compose name."""
    ts = os.environ["LAKE_TS_HOST"]
    security = TransportSecuritySettings(allowed_hosts=[ts, f"{ts}:*", "mcp:*", "localhost:*"],
                                         allowed_origins=[f"https://{ts}"])
    inner = server.streamable_http_app(streamable_http_path="/mcp", json_response=True, stateless_http=True,
                                       transport_security=security, host="0.0.0.0")
    return BearerAuth(inner, os.environ["MCP_TOKEN"])


if __name__ == "__main__":
    uvicorn.run(app(), host="0.0.0.0", port=8000, log_level="info", proxy_headers=True)
