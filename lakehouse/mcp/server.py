"""MCP server for the home-lab lakehouse: governed metrics from Cube, meanings from the glossary.

Tools
    query_metric(metric, group_by=None, limit=100)   a Cube measure by name, optionally grouped by dimensions
    define_term(term)                                 a glossary term: definition, synonyms, columns, relations

Served over streamable HTTP at /mcp (stateless, JSON responses) and through Caddy at https://<LAKE_TS_HOST>/mcp.
Every request needs `Authorization: Bearer <MCP_TOKEN>`. The server holds no lake credentials: query_metric goes
through Cube's REST API, so an agent sees only Cube's governed model, never the lake. The glossary is read on
every call from /glossary/glossary.yml (a folder mount), so edits show up without a restart.
"""
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from decimal import Decimal
from pathlib import Path
from typing import Any

import jwt
import uvicorn
import yaml
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel
from starlette.responses import PlainTextResponse

CUBE_API = os.environ.get("CUBE_API", "http://cube:4000/cubejs-api/v1")
GLOSSARY = Path(os.environ.get("GLOSSARY", "/glossary/glossary.yml"))

server = MCPServer(
    name="lakehouse",
    instructions="Governed metrics and definitions for the home-lab lakehouse's mock offender data. Ask "
                 "query_metric for a metric by name (an unknown name lists the real ones) and define_term "
                 "for what a term means. There is no raw SQL access, by design.")


# ---- Results (typed, so clients get structured content with a schema) -----------------------------------------
class MetricResult(BaseModel):
    """A metric's rows: one per combination of the group_by dimensions, each with the metric's value."""
    metric: str
    group_by: list[str]
    rows: list[dict[str, Any]]


class Relation(BaseModel):
    """A typed link to another glossary term (identifies, partOf, broader, relatedTo)."""
    type: str
    term: str


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
    """Every public measure and dimension Cube offers, as {"measures": [...], "dimensions": [...]} of full names."""
    meta = cube("meta")
    return {kind: [m["name"] for c in meta["cubes"] for m in c.get(kind, []) if m.get("public", True)]
            for kind in ("measures", "dimensions")}


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


@server.tool()
def query_metric(metric: str, group_by: list[str] | None = None, limit: int = 100) -> MetricResult:
    """A governed metric from Cube by name (for example record_count, total_restitution, answers_changed),
    optionally grouped by dimensions (for example custody_level, unit_name, offense_against). Short names work;
    an unknown name returns the list of real ones. Only Cube's model is reachable: restricted and personal
    columns aren't in it."""
    cat = catalog()
    measure = resolve(metric, cat["measures"])
    if measure is None:
        raise ToolError(f"No metric named {metric!r}. Metrics: " +
                         ", ".join(sorted(m.split(".", 1)[1] for m in cat["measures"])))
    dims = []
    for name in group_by or []:
        dim = resolve(name, cat["dimensions"], prefer_cube=measure.split(".", 1)[0])
        if dim is None:
            raise ToolError(f"No dimension named {name!r}. Dimensions: " +
                             ", ".join(sorted(d.split(".", 1)[1] for d in cat["dimensions"])))
        dims.append(dim)
    data = cube("load", {"query": {"measures": [measure], "dimensions": dims, "limit": max(1, min(limit, 1000))}})
    short = lambda full: full.split(".", 1)[1]   # noqa: E731
    rows = [{short(k): (number(v) if k == measure else v) for k, v in row.items() if k in dims + [measure]}
            for row in data.get("data", [])]
    return MetricResult(metric=short(measure), group_by=[short(d) for d in dims], rows=rows)


# ---- Glossary ------------------------------------------------------------------------------------------------
@server.tool()
def define_term(term: str) -> TermDefinition:
    """What a lakehouse term means: its definition, synonyms, the columns and metrics it maps to, and how it
    relates to other terms. Matches the term or any synonym, in any case; an unknown term lists the glossary."""
    terms = yaml.safe_load(GLOSSARY.read_text())["terms"]
    wanted = term.strip().casefold()
    for t in terms:
        if wanted in {t["name"].casefold(), *(s.casefold() for s in t.get("synonyms", []))}:
            return TermDefinition(term=t["name"], definition=" ".join(t["definition"].split()),
                                  synonyms=t.get("synonyms", []), columns=t.get("columns", []),
                                  metrics=t.get("metrics", []), sensitivity=t.get("sensitivity", "internal"),
                                  relations=[Relation(**r) for r in t.get("relations", [])])
    raise ToolError(f"No term {term!r} in the glossary. Terms: " + ", ".join(t["name"] for t in terms))


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
