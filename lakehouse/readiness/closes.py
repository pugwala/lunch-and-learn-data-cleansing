"""Business inputs and their closes: what a data product needs from each business unit before a month is final.

    docker compose --profile core exec -T jupyter python lakehouse/readiness/closes.py status
    docker compose --profile core exec -T jupyter python lakehouse/readiness/closes.py report classification 2026-09-30 --by "Classification"
    docker compose --profile core exec -T jupyter python lakehouse/readiness/closes.py sync [--check]
    docker compose --profile core exec -T jupyter python lakehouse/readiness/closes.py simulate

Declared in files, so adding one is teaching material, not code:
  products/inputs/<input>.yml   a business input: its unit, system, and days to close a month (closes_within_days)
  depends_on: in products/<product>.yml   the inputs that product's numbers need

Recorded in the lake (schema ops):
  business_inputs   one row per input (sync copies the files)
  product_inputs    one row per product and input it depends on (sync copies the descriptors)
  input_closes      every close a unit reports: complete_through a month-end, by whom, when. Append-only: the latest
                    report counts, so reporting an earlier month-end reopens a month, and the history stays.

The dbt view mart_readiness.period_readiness turns these into final or provisional per product and month-end.
The units here are fictional, so simulate plays each one: on its simulate.closes_after_days after a month ends, it
reports that month closed. The daily run calls sync, simulate and status (see orchestration/definitions.py).
"""
import argparse
import datetime as dt
import sys
from pathlib import Path

import pandas as pd
import yaml

LAKEHOUSE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAKEHOUSE.parent))
from lakehouse import lake  # noqa: E402

PRODUCTS = LAKEHOUSE / "products"
CENTRAL = "America/Chicago"


# ---- What the files declare -----------------------------------------------------------------------------------
def inputs():
    """{input name: its file's fields} from products/inputs/."""
    return {f.stem: yaml.safe_load(f.read_text()) for f in sorted((PRODUCTS / "inputs").glob("*.yml"))}


def dependencies():
    """[(product, input)] from each product descriptor's depends_on:."""
    return [(p["name"], i) for f in sorted(PRODUCTS.glob("*.yml"))
            for p in [yaml.safe_load(f.read_text())] for i in p.get("depends_on", [])]


def declared():
    """The two ops tables as the files say they should be: (business_inputs, product_inputs) DataFrames."""
    found = inputs()
    unknown = sorted({i for _, i in dependencies()} - set(found))
    if unknown:
        raise SystemExit(f"depends_on names inputs with no file in products/inputs/: {', '.join(unknown)}")
    business = pd.DataFrame([(n, i["title"], i["unit"], i["system"], int(i["closes_within_days"]))
                             for n, i in found.items()],
                            columns=["input", "title", "unit", "system", "closes_within_days"])
    product = pd.DataFrame(dependencies(), columns=["product", "input"])
    return business, product


# ---- Month-end arithmetic (pure, so the tests can pin the dates) ----------------------------------------------
def month_end(day):
    """The last day of day's month."""
    return (day.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)


def is_month_end(day):
    """True for the last day of a month."""
    return day == month_end(day)


def last_month_end(today):
    """The latest month-end strictly before today's month: the last complete month."""
    return today.replace(day=1) - dt.timedelta(days=1)


def days_late(complete_through, closes_within_days, today):
    """How many days past its deadline an input is: the first month it hasn't closed was due closes_within_days
    after that month ended. 0 when nothing is overdue. Never reported counts from the last complete month."""
    first_open = month_end(complete_through + dt.timedelta(days=1)) if complete_through else last_month_end(today)
    due = first_open + dt.timedelta(days=closes_within_days)
    return max(0, (today - due).days)


def simulated_close(complete_through, closes_after_days, today):
    """The month-end a simulated unit reports closed today, or None: the latest month-end at least
    closes_after_days old, if it is later than what the unit has already closed."""
    month = last_month_end(today)
    if (today - month).days < closes_after_days:
        month = last_month_end(month)
    return month if complete_through is None or month > complete_through else None


# ---- The lake -------------------------------------------------------------------------------------------------
def ensure_closes(con):
    """Create ops.input_closes if it's missing (append-only, so nothing ever replaces it)."""
    con.execute(f"""CREATE TABLE IF NOT EXISTS {lake.LAKE}.ops.input_closes (
        input VARCHAR, complete_through DATE, reported_at TIMESTAMPTZ, reported_by VARCHAR, note VARCHAR)""")


def watermarks(con):
    """{input: (complete_through, reported_by)} from the latest close each input reported."""
    rows = con.execute("SELECT input, arg_max(complete_through, reported_at), arg_max(reported_by, reported_at) "
                       "FROM ops.input_closes GROUP BY input").fetchall()
    return {i: (through, by) for i, through, by in rows}


def sync(check=False):
    """Make ops.business_inputs and ops.product_inputs match the files (one transaction); with check, only compare.
    Returns the differences found, as text."""
    business, product = declared()
    con = lake.lake_ro_connect() if check else lake.lake_connect()
    have = {}
    for table, frame in (("business_inputs", business), ("product_inputs", product)):
        try:
            current = con.execute(f"SELECT * FROM ops.{table}").fetchdf()
            same = sorted(map(tuple, current.astype(str).values)) == sorted(map(tuple, frame.astype(str).values))
        except Exception:   # the table doesn't exist yet
            same = False
        have[table] = same
    drift = [t for t, same in have.items() if not same]
    if drift and not check:
        con.execute("BEGIN TRANSACTION")
        try:
            for table, frame in (("business_inputs", business), ("product_inputs", product)):
                lake._replace(con, f"ops.{table}", f"{table}_df", frame)
            ensure_closes(con)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    elif not check:
        ensure_closes(con)
    con.close()
    return drift


def report(name, through, by, note=None, con=None):
    """Record that input name is complete through a month-end, as reported by someone. Refuses an input with no
    file or a date that isn't a month-end."""
    if name not in inputs():
        raise SystemExit(f"No input {name!r}: inputs are {', '.join(inputs())} (products/inputs/)")
    if not is_month_end(through):
        raise SystemExit(f"{through} isn't a month-end: a close is reported through the last day of a month")
    own = con is None
    con = con or lake.lake_connect()
    ensure_closes(con)
    con.execute(f"INSERT INTO {lake.LAKE}.ops.input_closes VALUES (?, ?, now(), ?, ?)", [name, through, by, note])
    if own:
        con.close()


def simulate(today):
    """Play the fictional units: each reports the month it would have closed by today. Returns what was reported."""
    con = lake.lake_connect()
    ensure_closes(con)
    marks, said = watermarks(con), []
    for name, i in inputs().items():
        month = simulated_close((marks.get(name) or (None, None))[0], i["simulate"]["closes_after_days"], today)
        if month:
            report(name, month, f"{i['unit']} (simulated)", "the lakehouse plays this fictional unit", con=con)
            said.append(f"{i['unit']} closed {name} through {month} (simulated)")
    con.close()
    return said


def status(today):
    """One dict per input: its unit, what it is complete through and who said so, and how many days late it is."""
    con = lake.lake_ro_connect()
    marks = watermarks(con)
    con.close()
    out = []
    for name, i in inputs().items():
        through, by = marks.get(name, (None, None))
        out.append({"input": name, "unit": i["unit"], "complete_through": through, "reported_by": by,
                    "days_late": days_late(through, i["closes_within_days"], today)})
    return out


def today_central():
    """Today's date in Central time, the lakehouse's business day."""
    from zoneinfo import ZoneInfo
    return dt.datetime.now(ZoneInfo(CENTRAL)).date()


def main(argv):
    """The command line: sync, report, simulate or status."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("sync", help="copy products/inputs/ and depends_on: into the ops tables")
    s.add_argument("--check", action="store_true", help="only compare; exit 1 if the lake is behind the files")
    r = sub.add_parser("report", help="record that an input is complete through a month-end")
    r.add_argument("input")
    r.add_argument("through", type=dt.date.fromisoformat, help="a month-end, e.g. 2026-09-30")
    r.add_argument("--by", required=True, help="who reports it (a person or a unit)")
    r.add_argument("--note")
    sub.add_parser("simulate", help="play the fictional units' scheduled closes")
    sub.add_parser("status", help="each input's close and lateness")
    args = parser.parse_args(argv)

    if args.command == "sync":
        drift = sync(check=args.check)
        n_in, n_dep = len(inputs()), len(dependencies())
        if args.check:
            print(f"Readiness: {'behind the files (' + ', '.join(drift) + '); run readiness/closes.py sync' if drift else f'inputs and dependencies in step with the files ({n_in} inputs, {n_dep} dependencies)'}")
            return 1 if drift else 0
        print(f"Readiness: synced {n_in} inputs and {n_dep} dependencies" + (f" ({', '.join(drift)} changed)" if drift else ""))
    elif args.command == "report":
        report(args.input, args.through, args.by, args.note)
        print(f"Readiness: {args.input} complete through {args.through}, reported by {args.by}")
    elif args.command == "simulate":
        said = simulate(today_central())
        print("\n".join(said) or "Readiness: no simulated close due today")
    else:
        for s in status(today_central()):
            late = f", {s['days_late']} days late" if s["days_late"] else ""
            print(f"{s['input']:<16} {s['unit']:<16} complete through {s['complete_through']} ({s['reported_by']}){late}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
