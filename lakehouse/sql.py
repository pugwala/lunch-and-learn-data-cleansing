"""IPython extension: jupysql's %sql connected to the lake, read-only, as the lake_ro role.

In any notebook in the lakehouse's Jupyter:

    %load_ext lakehouse.sql
    %sql SELECT question_id, as_loaded, cleansed FROM gold.trust_test ORDER BY question_id

or a whole cell starting with %%sql. Schemas are bronze, silver, gold, ref and ops (the lake is selected, so no
"lake." prefix is needed). Nothing can be written: DuckLake is attached READ_ONLY, and lake_ro has read grants
only. Course notebooks don't load this; it is opt-in.
"""
from sqlalchemy import create_engine, event

from lakehouse import lake

ALIAS = "lake"


def lake_engine():
    """A SQLAlchemy engine whose every DuckDB connection attaches the lake read-only as lake_ro and selects it.
    An event, not a one-off USE, because USE only reaches the connection that ran it."""
    engine = create_engine("duckdb:///:memory:")

    @event.listens_for(engine, "connect")
    def attach(dbapi_connection, _record):
        """Attach the lake to each new DuckDB connection the engine opens."""
        lake.attach_lake_ro(dbapi_connection)

    @event.listens_for(engine, "handle_error")
    def recover(context):
        """Roll back after a failed statement. DuckDB aborts the open transaction when a query fails while running,
        and jupysql doesn't roll that back, so without this one bad query would break every query after it."""
        if context.connection is not None:
            context.connection.rollback()

    return engine


def load_ipython_extension(ip):
    """Load jupysql and make the read-only lake engine its current connection, named `lake`."""
    ip.run_line_magic("load_ext", "sql")
    ip.user_ns["lake_engine"] = lake_engine()
    ip.run_line_magic("sql", f"lake_engine --alias {ALIAS}")
