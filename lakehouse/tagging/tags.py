"""The tagger: governance and metadata tags for every lake table and column, as data lands.

    python -m lakehouse.tagging.tags      # tag the lake now (the daily run's governance/tags step does this)

For each table:
  - its source (sources.yml) gives Governance (CJIS), Source and Refresh tags; a table built by dbt inherits
    Governance and Source from every source upstream of it, through the manifest's lineage;
  - its schema gives a Layer tag (Bronze, Silver, Gold, Reference, Operations);
  - if its rows describe people, each column is checked against rules.yml (by name, or by a sample of its text),
    and any personal column makes the whole table Governance.PII.
Every tag carries how it was set (rule, source, lineage, layer, rollup) and why. The result goes to the lake as
ops.table_tags and ops.column_tags, which om_sync.py publishes to OpenMetadata.
"""
import fnmatch
import json
import re
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
RULES = HERE / "rules.yml"
SOURCES = HERE / "sources.yml"
MANIFEST = HERE.parent / "dbt" / "target" / "manifest.json"
SAMPLE_ROWS = 10_000
LAYERS = {"bronze": "Bronze", "world_bronze": "Bronze", "silver": "Silver", "world_silver": "Silver",
          "gold": "Gold", "world_gold": "Gold", "mart_custody": "Gold", "mart_readiness": "Gold",
          "ref": "Reference", "ops": "Operations"}
OUTPUT = {"ops.table_tags", "ops.column_tags"}   # the tagger's own tables, never tagged themselves

_LOADED = {}


def _load(path: Path) -> dict:
    """A YAML file, read once per process."""
    if path not in _LOADED:
        _LOADED[path] = yaml.safe_load(path.read_text())
    return _LOADED[path]


def rules() -> list:
    """The column rules (rules.yml)."""
    return _load(RULES)["rules"]


def families() -> dict:
    """The tag vocabulary (rules.yml families), with each PIIType tag's description taken from its rule and each
    Source tag's from its source: {family: {"description": ..., "tags": {name: description}}}."""
    out = {name: {"description": f["description"], "tags": dict(f.get("tags") or {})}
           for name, f in _load(RULES)["families"].items()}
    for rule in rules():
        family, name = rule["tag"].split(".")
        out[family]["tags"][name] = rule["why"]
    for src in sources():
        out["Source"]["tags"][src["tag"]] = src["title"]
    return out


def sources() -> list:
    """The ingested sources (sources.yml)."""
    return _load(SOURCES)["sources"]


def source_of(table: str):
    """The source that lands a table (schema.table), or None for a table no source claims."""
    for src in sources():
        if any(fnmatch.fnmatchcase(table, pattern) for pattern in src["tables"]):
            return src
    return None


def about_people(table: str, upstream: set) -> bool:
    """Whether a table's rows describe people: its source says; a built table does if any source upstream does;
    a table with neither is assumed to (safer to look than to assume)."""
    src = source_of(table)
    if src:
        return src.get("people", True)
    roots = [s for s in (source_of(u) for u in upstream) if s]
    return any(r.get("people", True) for r in roots) if roots else True


def _values_match(spec: dict, sample: list) -> bool:
    """Whether a column's sampled text looks like a values rule says: enough values, enough of them long enough,
    and varied enough."""
    if len(sample) < spec["min_values"]:
        return False
    long_enough = sum(len(str(v).split()) >= spec["min_words"] for v in sample) / len(sample)
    varied = len(set(sample)) / len(sample)
    return long_enough >= spec["share"] and varied >= spec["distinct"]


def column_tags(column: str, dtype: str, sample: list) -> list:
    """[(tag, why)] for one column: every rule whose name pattern matches it, or whose values rule its sampled text
    (VARCHAR columns only) passes."""
    out = []
    for rule in rules():
        if "column" in rule and re.fullmatch(rule["column"], column):
            out.append((rule["tag"], f"rule {rule['name']}: {rule['why']}"))
        elif "values" in rule and dtype == "VARCHAR" and _values_match(rule["values"], sample):
            out.append((rule["tag"], f"rule {rule['name']}: {rule['why']}"))
    return out


def upstream(manifest: dict) -> dict:
    """{model table: every table it's built from, directly or not} from a dbt manifest, tables named as in the lake
    (schema.alias for models and seeds, schema.identifier for sources)."""
    names = {uid: f"{n['schema']}.{n['alias']}" for uid, n in manifest["nodes"].items()
             if n.get("resource_type", "model") in ("model", "seed", "snapshot")}
    names |= {uid: f"{n['schema']}.{n['identifier']}" for uid, n in manifest.get("sources", {}).items()}
    parents, memo = manifest["parent_map"], {}

    def walk(uid):
        """Every named node above uid."""
        if uid not in memo:
            memo[uid] = set()
            for p in parents.get(uid, []):
                if p in names:
                    memo[uid] |= {names[p]} | walk(p)
        return memo[uid]

    return {names[uid]: walk(uid) for uid, n in manifest["nodes"].items() if uid in names}


def table_tags(table: str, columns: dict, upstream_tables: set) -> list:
    """[(tag, why)] for one table, each why starting with how it was set: its source's tags (or, for a built
    table, the Governance and Source tags of every source upstream, through lineage), its layer, and
    Governance.PII when any column has a PIIType tag. columns: {column: [(tag, why)]}."""
    out = {}
    src = source_of(table)
    if src:
        for g in src.get("governance", []):
            out[f"Governance.{g}"] = f"source {src['name']}: {src['title']}"
        out[f"Source.{src['tag']}"] = f"source {src['name']}: {src['title']}"
        if src.get("refresh"):
            out[f"Refresh.{src['refresh']}"] = f"source {src['name']} lands {src['refresh'].lower()}"
    else:
        for u in sorted(upstream_tables):
            up = source_of(u)
            if up:
                for g in up.get("governance", []):
                    out.setdefault(f"Governance.{g}", f"lineage: built from {u} ({up['name']})")
                out.setdefault(f"Source.{up['tag']}", f"lineage: built from {u} ({up['name']})")
    schema = table.split(".")[0]
    if schema in LAYERS:
        out[f"Layer.{LAYERS[schema]}"] = f"layer: schema {schema}"
    personal = sorted(c for c, found in columns.items() if any(t.startswith("PIIType.") for t, _ in found))
    if personal:
        out["Governance.PII"] = "rollup: personal columns " + ", ".join(personal)
    return sorted(out.items())


def _samples(con, relation: str, text_columns: list) -> dict:
    """{column: its non-null values} from a repeatable sample of up to SAMPLE_ROWS rows of relation."""
    if not text_columns:
        return {}
    cols = ", ".join(f'"{c}"' for c in text_columns)
    rows = con.execute(f"SELECT {cols} FROM {relation} USING SAMPLE {SAMPLE_ROWS} ROWS (reservoir, 42)").fetchall()
    return {c: [r[i] for r in rows if r[i] is not None] for i, c in enumerate(text_columns)}


def classify(con, tables: dict, manifest: dict = None, catalog: str = "") -> tuple:
    """Tag tables ({schema.table: [(column, duckdb type)]}) read through con (prefixed with catalog, e.g. "lake.").
    Returns (table rows, column rows): (schema, table, tag, how, why) and (schema, table, column, tag, how, why)."""
    up = upstream(manifest) if manifest else {}
    table_rows, column_rows = [], []
    for table, columns in sorted(tables.items()):
        ups = up.get(table, set())
        found = {}
        if about_people(table, ups):
            sample = _samples(con, f"{catalog}{table}", [c for c, d in columns if d == "VARCHAR"])
            found = {c: t for c, d in columns if (t := column_tags(c, d, sample.get(c, [])))}
        schema, name = table.split(".", 1)
        column_rows += [(schema, name, c, tag, "rule", why) for c, t in found.items() for tag, why in t]
        table_rows += [(schema, name, tag, why.split(":")[0].split()[0], why)
                       for tag, why in table_tags(table, found, ups)]
    return table_rows, column_rows


def tag_lake(manifest_path: Path = MANIFEST) -> dict:
    """Tag every table in the lake and replace ops.table_tags and ops.column_tags with the result, in one
    transaction; returns counts for the run's log and metrics."""
    sys.path.insert(0, str(HERE.parents[1]))
    from lakehouse import lake
    con = lake.lake_connect()
    rows = con.execute("SELECT table_schema || '.' || table_name, column_name, data_type FROM information_schema.columns "
                       "WHERE table_catalog = 'lake' ORDER BY table_schema, table_name, ordinal_position").fetchall()
    tables = {}
    for table, column, dtype in rows:
        if table not in OUTPUT:
            tables.setdefault(table, []).append((column, dtype))
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
    table_rows, column_rows = classify(con, tables, manifest, catalog="lake.")
    con.execute("BEGIN TRANSACTION")
    try:
        for name, cols, data in (("table_tags", "table_schema VARCHAR, table_name VARCHAR, tag VARCHAR, how VARCHAR, "
                                  "why VARCHAR", table_rows),
                                 ("column_tags", "table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, "
                                  "tag VARCHAR, how VARCHAR, why VARCHAR", column_rows)):
            con.execute(f"DROP TABLE IF EXISTS lake.ops.{name}")
            con.execute(f"CREATE TABLE lake.ops.{name} ({cols})")
            if data:
                con.executemany(f"INSERT INTO lake.ops.{name} VALUES ({', '.join('?' * len(data[0]))})", data)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    con.close()
    return {"tables": len(tables), "table_tags": len(table_rows), "column_tags": len(column_rows),
            "personal_columns": len({(s, t, c) for s, t, c, *_ in column_rows}),
            "pii_tables": sum(1 for r in table_rows if r[2] == "Governance.PII"),
            "cjis_tables": sum(1 for r in table_rows if r[2] == "Governance.CJIS")}


if __name__ == "__main__":
    print(tag_lake())
