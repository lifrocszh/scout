# Lakehouse fixture

Committed input for the lakehouse tests, `python3 -m analytics.lakehouse demo` and CI
([docs/lakehouse.md](../../docs/lakehouse.md)).

| Path | What | How it was made |
| --- | --- | --- |
| `data/` | Two Corpus snapshots of the same two Sources (`docs` on `127.0.0.1:18111`, `reference` on `127.0.0.1:18112`), with their Crawl Manifests and `near-duplicates.json` reports | `build_fixture.py`: two real `scout crawl` runs. Between them one docs Page is edited, one is removed, a near-duplicate Page is added, and an exact duplicate of a reference Page is added (it becomes an alias). Body files and operational logs are dropped. |
| `events.jsonl` | Crawl events for both runs, in the format `scout events consume` prints | Derived by `build_fixture.py` from the two crawls' manifests, because a local crawl has no outbox: one `capture_committed` per captured Page, then `crawl_completed` and `corpus_snapshot_published`. The schema, run IDs, snapshot IDs and checksums are real. The event IDs and task IDs are made up. |
| `outbox/` | `scout events consume` output and `scout events export` batches (4 records per batch) | One distributed crawl (coordinator, one worker, PostgreSQL 16) of the v2 site, then `scout crawl --materialize-spool`. It reproduced the v2 snapshot ID, because snapshot IDs are content-derived. |
| `snapshots.json` | The snapshot IDs `build_fixture.py` produced | |

Rebuild `data/`, `events.jsonl` and `snapshots.json`:

```bash
cargo build --locked
python3 analytics/fixtures/build_fixture.py      # serves the sites on ports 18111 and 18112
```

The ports are fixed because Page IDs derive from URLs. Snapshot times change on every rebuild, and
the tests and `demo` read their clock from the fixture (`fixture.fixture_as_of()`). Rebuild
`outbox/` with the distributed stack (`scripts/distributed-local.sh`, or a coordinator and a
worker against any PostgreSQL), crawling the v2 site from `build_fixture.site(2)`:

```bash
scout coordinator --config sources.toml --data-dir D --bind 127.0.0.1:50071 &
scout worker --coordinator http://127.0.0.1:50071 --data-dir D --worker-id w1
scout crawl --config sources.toml --data-dir D --materialize-spool "$RUN_ID"
scout events consume --run-id "$RUN_ID" --consumer-id lake-jsonl --follow > outbox/events.jsonl
scout events export --run-id "$RUN_ID" --consumer-id lake-batches --data-dir D --max-records 4 --follow
cp -r D/crawl-event-batches outbox/
```
