# Databricks analytics recovery

Databricks is downstream of Scout's durable Crawl event feed. A workspace or
network outage stops export/upload work; it does not stop Crawl writes. Use
this runbook to resume one run or backfill a bounded list of runs and
snapshots.

## Bounded run and snapshot backfill

Select at most 100 run IDs and 100 snapshot IDs for one operation. Store one
ID per line in `run-ids.txt` and `snapshot-ids.txt`; keep the files with the
recovery report. The helper validates every ID and runs retained-history
preflight for every run before it exports anything. `complete` means the
requested consumer can replay from PostgreSQL or its verified local batch;
`replayable` lists gaps covered by a registered batch or archive; `unrecoverable`
lists gaps with no verified replay source. Resolve any listed gap before running
the bounded export. The helper caps each event run at 10,000 batches:

```sh
export SCOUT_DATABASE_URL='postgresql://...'
python3 scripts/databricks/analytics_backfill.py \
  --data-dir ./data \
  --consumer-id databricks-backfill-2026-10 \
  --run-ids-file run-ids.txt \
  --max-batches-per-run 256 \
  --snapshot-ids-file snapshot-ids.txt \
  --snapshot-output-root ./databricks-snapshot-exports \
  --max-local-bytes 4294967296
```

`pause_state=batch_limit` means repeat the same command with the same consumer
ID to continue. A new consumer ID intentionally replays that run from the
beginning with independent progress and new immutable batch IDs. Unknown IDs
register as optional consumers. The production consumer is pre-registered as
required with ID `databricks-live-v1`; temporary backfills remain optional unless
explicitly registered with `--required`. Set its replay window at registration
with `--replay-days` (default 90 days, allowed 1–3650). Retiring a consumer is
explicit and preserves its history:

```sh
scout events consumer-register --consumer-id databricks-live-v1 --required --replay-days 90 --data-dir ./data
scout events consumer-retire --consumer-id databricks-backfill-2026-10 --data-dir ./data
```

Do not use `--follow` for bounded backfill. Export status `no_pending_records`
means only that the local exporter checkpoint has no due rows; it is not a
Databricks ingestion receipt. `pending_backoff` reports the next eligible retry
time and must not be treated as caught up. Retained event history is stored
independently from crawl and event rows, so deleting both cannot produce a
false caught-up result.

Snapshot IDs are explicit and exported into the same output root. Existing
output is reused only when the regenerated manifest and every payload file
match byte-for-byte; conflicting content stops the backfill.

Then validate, upload, and submit each event or snapshot batch using its
existing `crawl_event_batch.py` or `snapshot_batch.py` commands. Snapshot jobs
submit with queueing disabled. After the Databricks event ingestion job
succeeds, fetch its notebook output with the Databricks CLI helper and register
the resulting workspace attestation in PostgreSQL:

```sh
python3 scripts/databricks/crawl_event_receipt.py --run-id <databricks-run-id> \
  --output ./data/analytics-receipts/<batch-id>.json
scout events acknowledge --receipt-file ./data/analytics-receipts/<batch-id>.json --data-dir ./data
```

The notebook emits this attestation only after it verifies the completed Delta
batch row and matching ingestion receipt row. The acknowledgement checks batch
and consumer identity, manifest and records checksums, event-coverage checksum,
record count, complete status, and zero quarantine or conflict counts. Treat the
receipt file as a trusted operator-imported workspace attestation: PostgreSQL
validates its contents but does not cryptographically authenticate who produced
the file. Failed or partial batches cannot release retained artifacts. This
ingestion receipt is separate from the local export checkpoint. Event batches
must be reconciled again after event or snapshot
ingestion. Ticket 07's
reconciliation views stay provisional until its completed-batch, checksum,
count, and capture-identity gates pass. Rerunning ingestion with the same
batch is idempotent; do not edit an exported folder to repair it.

## Resume by interruption point

| Last completed step | Recovery |
| --- | --- |
| Local event export | Validate and upload the published local batch. The exporter fsyncs `_PUBLISHED`, registers its immutable ID/checksums/coverage, then advances the separate local-export checkpoint. If it stops before checkpointing, rerun with the same consumer ID; it verifies/reuses the same batch ID. |
| Volume upload | A partial upload has no final `_PUBLISHED` marker and is ignored. Run `upload` again; it uses a fresh attempt folder. Keep the prior folder for diagnosis. |
| Event or snapshot ingestion | Submit the same immutable batch again. Bronze/Silver merges and batch identity make the logical result idempotent; wait for batch `status='complete'` before treating it as committed. |
| Snapshot reconciliation | Rerun the ticket-07 reconciliation SQL/notebook after both sides are complete. Its views are derived from retained event and snapshot tables, so rerunning does not change Page IDs or Generation state. |

Use one exporter process per data directory. Upload and job submission use
durable bounded retry state with request identity, timeouts, and a per-operation
lock. Job submissions keep queueing disabled and use a one-hour notebook task
timeout. One-attempt serverless task specs set `max_retries: 0` and
`disable_auto_optimization: true`; serverless auto-optimization can retry a
failed task outside the configured retry policy. See the [Jobs Run API
reference](https://docs.databricks.com/api/jobs/v2/run) and [serverless
auto-optimization guidance](https://docs.databricks.com/gcp/en/jobs/run-serverless-jobs).
After attempts are exhausted, follow the command printed by the
transport helper; it preserves upload or Databricks run identity unless an
operator confirms a terminal failure and explicitly starts a new run. See the
[transport retry guide](transport-retries.md) for limits and recovery commands.

## Capacity and retention

`scout events export` limits each batch to 128 records and 64 MiB. Its local
queue defaults to 4 GiB across all runs under
`$DATA_DIR/crawl-event-batches`; `--max-local-bytes` accepts up to 1 TiB. The
capacity check counts existing files and the candidate batch. At the limit,
the exporter returns an explicit error before publishing or acknowledging
that batch. PostgreSQL keeps it pending with normal retry backoff. Databricks
availability is never part of Crawl result acceptance.

The retention contract is per registered consumer. Each active required
consumer keeps its replay window open for 90 days by default; set the duration
from 1 to 3650 days when registering or updating a consumer. Required consumers
need a verified Databricks receipt for each batch covering an event before
local event and artifact copies can be pruned after that window closes.
Optional backfills do not block production retention unless explicitly
registered required. Never delete PostgreSQL history, batch coverage, or
ingestion receipt rows.

Before removing any exported batch or referenced artifact, copy and verify the
batch and its body/manifest dependencies:

```sh
scout events archive --batch-id <batch-id> --data-dir ./data
```

The default archive is `./data/analytics-archive` on the same local disk. It
stores immutable batches and content-addressed artifacts; PostgreSQL records
each path, checksum, size, and verification time. This archive does not protect
against loss of that disk. Copy it to independent durable storage before
retiring the project disk. Preflight verifies archived copies before marking a
missing source row replayable.

Inspect a run before resuming or backfilling it:

```sh
scout events preflight --run-id <run-id> --consumer-id databricks-live-v1 --data-dir ./data
```

The independent event-history and artifact-dependency ledgers are written
atomically with each new event and do not cascade when run or event metadata is
deleted. Preflight returns replayable and unrecoverable gaps; it never infers a
missing capture or marks the run caught up without retained evidence.

The exporter and preflight compare durable retained-history identities with
the current event feed and verified batch coverage. Ticket 07's snapshot gate
also leaves reconciliation provisional when accepted capture identities do
not match the validated Crawl Manifest count. For either mismatch, replay a
retained immutable batch; if none exists, record the run, missing count, and
retention action as unrecoverable. Never infer a missing capture or mark the
run caught up.

The local queue limit bounds only analytics batch storage; it is not a disk
quota for the Crawl body spool. The coordinator also measures tracked spool,
event-batch, and archive usage plus filesystem free space before granting task
leases. It automatically pauses new claims at the configured reserve and
resumes after two-capture headroom returns; existing permits and accepted
results continue through normal completion handling. See the
[storage pressure runbook](storage-pressure.md) for thresholds, quota settings,
and monitored paths. No live workspace capacity measurement is demonstrated
here.

[`fixtures/analytics-recovery-demo.json`](fixtures/analytics-recovery-demo.json)
is a deterministic operator scenario, not a live Databricks execution report.
