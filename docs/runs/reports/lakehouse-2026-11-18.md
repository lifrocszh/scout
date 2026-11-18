# Local lakehouse: Delta tables, DuckDB queries, data quality (issue #14)

Adds a local analytics path next to the Databricks pipeline. Corpus snapshots, near-duplicate
reports and durable crawl events land as Delta tables (delta-rs) on the local filesystem in Bronze,
Silver and Gold layers. DuckDB queries them, data-quality rules fail loudly, and lineage records
which crawl run and snapshot produced each table version. The design is in
[docs/lakehouse.md](../../lakehouse.md).

## What changed

| Where | What |
| --- | --- |
| `analytics/lakehouse/` (new) | Loader, quality rules, Delta tables, DuckDB access, CLI (`python3 -m analytics.lakehouse`) |
| `analytics/sql/gold/`, `analytics/sql/examples/` (new) | Gold table SQL (freshness by Origin, change rate, duplicate rate) and example queries |
| `analytics/fixtures/` (new) | Two real fixture crawls, derived events, and real outbox output from a distributed crawl |
| `analytics/tests/test_lakehouse.py` (new) | 34 tests |
| `analytics/requirements.txt` (new) | `deltalake==1.6.6`, `duckdb==2.0.0`, `pyarrow==26.0.1` |
| `.github/workflows/ci.yml` | New `lakehouse` job: tests, `demo`, and every broken fixture must exit 1 |
| `Makefile` | `lakehouse-demo`, `lakehouse-test` |
| `README.md`, `docs/design.md`, `docs/lakehouse.md` (new) | Docs, lineage diagram, "Left out of the picture" and "Not built yet" updated |

No Rust code changed. Snapshot input goes through `scripts/databricks/snapshot_batch.py`
(export and validate), and event batches through `crawl_event_batch.py validate`. Both are
imported unchanged, so the Databricks path and the lakehouse accept the same inputs.

## Decisions

- **Delta, not Iceberg.** Delta is the format the Databricks path already writes, and it needs no
  catalog. delta-rs has insert-only `MERGE`, which gives idempotent loads (a merge that inserts
  nothing commits no version), and per-commit custom metadata, which carries the lineage. Details
  are in [docs/lakehouse.md](../../lakehouse.md#why-delta-delta-rs-and-not-iceberg).
- **No DuckDB extension.** Tables are read with delta-rs and registered in DuckDB, so CI and
  offline runs download nothing at query time.
- **Lineage comes from commit metadata.** `_lineage/loads` is rebuilt from the `scout.*` metadata
  of each Delta commit. A load that stops between a commit and its lineage row is repaired by the
  next load.
- **No Kafka dependency.** Events enter through `EventSource.records()`. Two transports exist:
  `scout events consume` JSONL and `scout events export` batches. A Kafka source (#13) would add a
  third without changing the tables or the rules.

## Verification (local, 2026-11-18)

| Check | Result |
| --- | --- |
| `python -B analytics/tests/test_lakehouse.py` (Python 3.12 and 3.13) | 34 tests OK, about 27 s |
| `python -m analytics.lakehouse demo` | exit 0, every rule PASS. The reload adds nothing |
| `demo --broken schema-drift / event-conflict / event-schema / stale` | each exits 1 on `schema.drift` / `event.identity` / `event.schema` / `freshness.sla_24h` |
| `ruff check --select F analytics` | clean |

Fixture results (`analytics/fixtures`, two `scout crawl` runs of a 7-Page, 2-Origin site):

- **Change rate.** For the docs Source: 1 Page added, 1 removed, 2 changed and 1 unchanged, so the
  change rate is 0.667 and the churn rate 0.800. For the reference Source: 1 added, 1 removed,
  1 changed and 1 unchanged. Its `cli.html` became an alias of the identical `cli-mirror.html`, so
  the old Page ID counts as removed.
- **Duplicate rate.** On the docs Origin, 1 near-duplicate Page of 4 (0.25, from
  `near-duplicates.json`). On the reference Origin, 1 exact-duplicate alias (0.25 of captured URLs).
- **Idempotency.** Loading both snapshots and all events again leaves every table's row count and
  Delta version unchanged. So does loading the same events as JSONL and as batches.

Real outbox check: PostgreSQL 16 on localhost, `scout coordinator` with one `scout worker` on the v2
fixture site, then `scout crawl --materialize-spool`. `scout events consume` printed 10 events and
`scout events export --max-records 4` published 3 batches. Loading the snapshot, then the JSONL,
then the batches inserted the 10 events once. The batch load added no rows, and every lake rule
passed, including `counts.events_match_snapshot`. That run reproduced the fixture's v2 snapshot ID,
which exposed a bug: the rule compared run IDs, but snapshot IDs are content-derived, so another
run can publish the same snapshot. The rule now compares counts only. The outbox output is
committed under `analytics/fixtures/outbox/` and loaded by the tests.

## Not verified

- CI on GitHub (the PR's checks), and the pip install time on the runner.
- A large Corpus. Tables are read into memory for DuckDB, and only the fixture sizes were run.
- The Gold SQL on Databricks SQL. It avoids DuckDB-only functions, but it was run only on DuckDB.
- Dead letters are not loaded (they live in Postgres; `scout dlq list` prints them).
