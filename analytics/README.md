# analytics

The local lakehouse: Delta tables on the filesystem (delta-rs), queried with DuckDB, with
data-quality checks and lineage. See [docs/lakehouse.md](../docs/lakehouse.md).

```bash
python3 -m pip install -r analytics/requirements.txt
python3 -m analytics.lakehouse demo
python3 -B analytics/tests/test_lakehouse.py
```

- `lakehouse/`: the loader, quality rules, tables and CLI (`python3 -m analytics.lakehouse --help`)
- `sql/gold/`: the SQL that builds the Gold tables
- `sql/examples/`: example DuckDB queries
- `fixtures/`: the committed fixture crawl ([README](fixtures/README.md))
