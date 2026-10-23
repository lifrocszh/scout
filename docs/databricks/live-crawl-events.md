# Live crawl-event ingestion

Scout exports committed crawl events as immutable
`scout.databricks-crawl-event-batch.v1` directories. These batches are separate
from completed Corpus snapshot exports. Ingestion stores raw event envelopes,
accepted event identities, provisional capture observations, quarantines, and
an attempt audit. It does not create Pages or Passages, extract captured body
artifacts, or perform exact-content deduplication. Snapshot ingestion and its
history tables remain unchanged.

## Export while a crawl is active

Set `SCOUT_DATABASE_URL` for the coordinator database. Start the exporter while
the distributed crawl is running; each published folder is durable locally
before the event consumer acknowledges its checkpoint:

```sh
RUN_ID='run-<64-lowercase-hex-characters>'
DATA_DIR=./data
SCOUT_DATABASE_URL='postgresql://...' scout events export \
  --run-id "$RUN_ID" \
  --consumer-id databricks-live-v1 \
  --data-dir "$DATA_DIR" \
  --max-records 32 \
  --max-bytes 67108864 \
  --max-local-bytes 4294967296 \
  --max-elapsed-ms 5000 \
  --poll-interval-ms 250 \
  --follow
```

The exporter writes
`$DATA_DIR/crawl-event-batches/$RUN_ID/<batch-id>/` with exactly
`batch-manifest.json`, `records.jsonl`, and `_PUBLISHED`. Record count, payload
bytes, and collection elapsed time bound each batch; the v1 manifest records
the record and byte bounds. Omit `--follow` to export one currently available
batch and exit. Add `--max-batches 1..10000` for bounded replay of one run;
rerun with the same consumer ID to continue. The default local queue ceiling is
4 GiB across all runs under `crawl-event-batches`; it can be raised to at most
1 TiB. Keep one exporter process active per data directory so concurrent
consumers cannot race the shared ceiling. At the limit, export exits with an
error before publishing or acknowledging its next batch. Clear or archive
retained batches only under the retention rules in
[analytics recovery](analytics-recovery.md). Crawl and its event outbox stay
independent of Databricks availability.

## Validate, upload, and submit

Create a Unity Catalog Volume and configure the Databricks CLI OAuth profile as
described in [Snapshot ingestion](snapshot-ingestion.md). Validate a published
batch before uploading it:

```sh
BATCH_DIR="$DATA_DIR/crawl-event-batches/$RUN_ID/<batch-id>"
python3 scripts/databricks/crawl_event_batch.py validate --batch-dir "$BATCH_DIR"
python3 scripts/databricks/crawl_event_batch.py upload \
  --batch-dir "$BATCH_DIR" \
  --volume dbfs:/Volumes/workspace/default/scout_exports \
  --profile portfolio-trial
```

Upload prints an immutable Volume path ending in
`/scout/crawl-events/<batch-id>/uploads/<attempt-id>`. Submit one bounded
serverless task using that printed path:

```sh
python3 scripts/databricks/crawl_event_batch.py run \
  --batch-dir "$BATCH_DIR" \
  --volume dbfs:/Volumes/workspace/default/scout_exports \
  --remote-path dbfs:/Volumes/workspace/default/scout_exports/scout/crawl-events/<batch-id>/uploads/<attempt-id> \
  --catalog workspace \
  --schema default \
  --volume-name scout_exports \
  --workspace-path /Users/<your-email>/scout-crawl-event-ingest \
  --profile portfolio-trial
```

Run validation and upload for each new local batch while the crawl continues.
Then query `gold_provisional_crawl_run_status` to see captures arrive before
`crawl_completed` or `corpus_snapshot_published`. The local immutable batch
folders remain available when Databricks is offline. After recovery, upload
and submit the same batch again; a new upload-attempt folder is safe. A batch
whose ID has a different manifest or payload is rejected.

## Tables and reports

- `scout_crawl_event_records` retains raw event envelopes by batch and line,
  including valid-looking rows rejected for identity conflicts. Its
  `disposition` marks each stored row as accepted, duplicate, or identity
  conflict; rejected rows never enter accepted identity or Gold counts. A batch
  cannot complete until every input line is represented in this table or
  quarantine and every rejected identity has a quarantine row.
- `scout_crawl_event_identity` keeps the first accepted immutable event for
  `(crawl_run_id, event_id)`. Exact replays are idempotent.
- `scout_provisional_capture_observations` contains only validated
  `capture_committed` events, including Source, commit time, hashes, and the
  immutable body-artifact reference.
- `scout_crawl_event_quarantine` retains invalid rows and identity conflicts
  with reason, checksum, full raw JSONL row, and bounded excerpt.
- `scout_crawl_event_batches` tracks complete, processing, or failed batches,
  retry count, accepted counts, conflicts, and last error.
- `scout_crawl_event_ingestion_attempts` records duration, processed bytes,
  event/byte throughput, quarantine and replay counts, plus nullable runtime
  and capacity measurements.

`gold_provisional_source_capture_summary` reports per-Source provisional
capture counts and observation age. `gold_provisional_crawl_run_status`
reports run counts, latest event age, event-to-ingestion latency, completion
and snapshot-publication events, and quarantined rows. Gold includes accepted
or exact-duplicate event identities only after their source batch is complete;
identity and observation rows left by a failed attempt stay out until a
complete replay exists. Valid-looking raw rows rejected for an identity
conflict do not count as accepted captures.
`evidence_status = 'snapshot_publication_event_seen'` means only that the
durable event was observed. It does not validate or ingest the referenced
snapshot and cannot authorize final history; ticket 07 performs that
reconciliation after snapshot validation.
`gold_crawl_event_ingestion_status` exposes remote pending batches, retries,
errors, per-attempt throughput, and duration. Locally pending batches that
have not reached the Volume are visible only in the local batch directory.

Durable crawl-event v1 has no fetch-failure or Page-rejection totals, so
`failed_count` and `rejected_count` remain `NULL` with an explanatory reason.
Serverless runtime, capacity wait, driver/executor memory, and CPU are also
`NULL`; measured notebook execution duration, bytes, and event throughput are
available per attempt. A quarantine count describes invalid analytics input,
not upstream crawl failures.

The deterministic fixture defines expected report values; no live Free Edition
workspace run or serverless resource sample is recorded in this change. Run the
workflow above in a workspace to demonstrate in-flight reporting and observe
its actual job-level limits.

Inspect the active run and latest ingestion attempts:

```sql
SELECT * FROM workspace.default.gold_provisional_crawl_run_status
WHERE crawl_run_id = 'run-<64-lowercase-hex-characters>';

SELECT * FROM workspace.default.gold_provisional_source_capture_summary
WHERE crawl_run_id = 'run-<64-lowercase-hex-characters>'
ORDER BY source_id;

SELECT * FROM workspace.default.gold_crawl_event_ingestion_status
WHERE crawl_run_id = 'run-<64-lowercase-hex-characters>'
ORDER BY started_at_utc DESC;
```

[`fixtures/live-crawl-events-demo.json`](fixtures/live-crawl-events-demo.json)
defines deterministic report expectations. It is explanatory input, not an
ingestable batch or a claim of live workspace execution.
