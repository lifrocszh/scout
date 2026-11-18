"""DuckDB over the local Delta tables.

Each table is read with delta-rs and registered as a DuckDB view named
``<layer>_<table>`` (``silver_pages``, ``gold_origin_freshness``,
``lineage_loads``...). delta-rs resolves the Delta log, so DuckDB only sees the
rows of the current table version, and no DuckDB extension has to be
downloaded. Tables that do not exist yet are registered empty with their schema.

Tables are read into memory: the lakehouse holds per-snapshot metadata (no Page
bodies), which stays small next to a Corpus. For larger lakes, DuckDB's own
``delta`` extension (``delta_scan('<lake>/silver/pages')``) reads the same tables.
"""

from pathlib import Path

import duckdb
from deltalake import DeltaTable

from . import tables as t

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def connect(lake):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for spec in t.ALL_TABLES:
        if t.exists(lake, spec):
            data = DeltaTable(str(spec.path(lake))).to_pyarrow_table()
        else:
            data = spec.schema.empty_table()
        con.register(spec.qualified, data)
    return con


def sql_file(name):
    """A committed SQL file under analytics/sql, e.g. ``examples/freshness_by_origin.sql``."""
    return (SQL_DIR / name).read_text(encoding="utf-8")


def example_files():
    return sorted((SQL_DIR / "examples").glob("*.sql"))


def format_table(relation):
    """A small fixed-width rendering of a DuckDB relation for the terminal."""
    data = relation.to_arrow_table()
    columns = data.column_names

    def cell(value):
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.3f}"
        if hasattr(value, "isoformat"):
            return value.isoformat().replace("+00:00", "Z")
        return str(value)

    rows = [[cell(value) for value in row.values()] for row in data.to_pylist()]
    widths = [max([len(column)] + [len(row[i]) for row in rows]) for i, column in enumerate(columns)]

    def line(values):
        return "  ".join(value.ljust(width) for value, width in zip(values, widths)).rstrip()

    return "\n".join([line(columns), line(["-" * width for width in widths])] + [line(row) for row in rows])
