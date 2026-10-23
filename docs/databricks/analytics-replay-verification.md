# Analytics replay verification

`scripts/databricks/analytics_replay_verify.py` is a Databricks notebook that
compares actual Bronze, Silver, Gold, and Crawl/snapshot reconciliation results
from two isolated schemas. One schema must contain an uninterrupted load; the
other must contain the recovered/replayed load. The notebook reads the Delta
tables and views written by the existing ingestion and reconciliation
notebooks. It does not simulate their Spark logic.

## Prepare the two lanes

The configured CLI profile is `portfolio-trial`; it selects workspace/auth only, not a Lakebase project. Never target the Investment Portfolio Lakebase project. The profile is not a Scout-owned Unity Catalog namespace.

Create two disposable schemas in the same Unity Catalog. Each schema must hold
only the selected Crawl run and Corpus snapshot, including the source history
needed by the Gold views. Use the same immutable event and snapshot inputs in
both lanes. For event exports, separate consumer IDs are allowed: their batch
IDs include the consumer ID, while the event payloads and event IDs stay the
same. The snapshot export has a stable content-derived batch ID.

1. Export and validate the selected event batches and snapshot batch using the
   procedures in [analytics recovery](analytics-recovery.md),
   [live Crawl events](live-crawl-events.md), and
   [snapshot ingestion](snapshot-ingestion.md).
2. Ingest the selected events and snapshot once into the baseline schema. Run
   `crawl_snapshot_reconcile.py` after both ingestions complete.
3. Ingest the same logical event set and snapshot into the replay schema. For a
   replay scenario, submit at least one already processed immutable batch
   again, or resume after an interrupted attempt. Run reconciliation after
   recovery completes.
4. Run the verification notebook below with both schema names and the selected
   `run-*` and `snapshot-*` IDs.

Use schemas dedicated to this single run and snapshot. The notebook checks
that constraint because some history Gold views are not keyed by snapshot ID.
It fails if either lane has incomplete event or snapshot batches, quarantined
or conflicting event rows, conflicting snapshot provenance, a missing
reconciliation row, or any non-authoritative capture result.

## Import and submit

Import the notebook with a configured Databricks CLI profile:

```sh
databricks workspace import /Users/<you>/scout-analytics-replay-verify \
  --file scripts/databricks/analytics_replay_verify.py \
  --format SOURCE --language PYTHON --overwrite --profile portfolio-trial
```

Create a one-time job request with the selected IDs and schemas. Set
`report_uri` to a new JSON path in a Unity Catalog Volume if the report should
be stored there; otherwise, use an empty string and retain the notebook output.

```json
{
  "run_name": "scout-analytics-replay-verification",
  "queue": {"enabled": false},
  "performance_target": "STANDARD",
  "tasks": [{
    "task_key": "verify_replay",
    "timeout_seconds": 3600,
    "max_retries": 0,
    "disable_auto_optimization": true,
    "environment_key": "scout_serverless",
    "notebook_task": {
      "notebook_path": "/Users/<you>/scout-analytics-replay-verify",
      "source": "WORKSPACE",
      "base_parameters": {
        "baseline_catalog": "workspace",
        "baseline_schema": "analytics_baseline",
        "replay_catalog": "workspace",
        "replay_schema": "analytics_replay",
        "crawl_run_id": "run-<64-lowercase-hex>",
        "snapshot_id": "snapshot-<64-lowercase-hex>",
        "report_uri": ""
      }
    }
  }],
  "environments": [{
    "environment_key": "scout_serverless",
    "spec": {"environment_version": "4"}
  }]
}
```

Save the JSON as `analytics-replay-verification-job.json`, replace the
placeholders, then submit:

```sh
databricks jobs submit --json @analytics-replay-verification-job.json \
  --profile portfolio-trial
```

Keep `max_retries: 0` and `disable_auto_optimization: true` for bounded
one-attempt recovery tasks. Serverless auto-optimization can retry a failed
task outside its retry policy; see the [Jobs Run API reference](https://docs.databricks.com/api/jobs/v2/run)
and [serverless auto-optimization guidance](https://docs.databricks.com/gcp/en/jobs/run-serverless-jobs).

The successful notebook output is a JSON report with schema/version, IDs,
batch and authority gates, per-table row counts, excluded operational columns,
and the exact logical columns compared. A mismatch raises an error with a
small row sample. Store the report with the run evidence under `docs/runs/`;
do not describe this procedure as completed until the workspace job succeeds.

## Comparison rules

The notebook compares full row multisets, not only aggregate counts. It
normalizes rows into schema-checked JSON strings and counts each exact row on
both sides. Two delivery projections can contain repeated deliveries that do
not represent new logical events. For each, the notebook verifies one full
substantive row version per logical event across both sides, then compares
distinct full logical rows. Physical and logical row counts are both included
in the report. It compares these outputs:

- Snapshot batch provenance, Bronze and Silver records, and stored Source,
  Page, alias, and history-evidence observations.
- Snapshot Gold count, scope, change, presence, alias, coverage, freshness,
  frequency, operational, error-rate, Crawl-run, and effectiveness views.
- Durable event records, event identity, provisional capture observations,
  complete-event rows, and provisional event Gold views.
- Reconciliation status, authoritative Page and alias observations, capture
  reconciliation, and rejected-task observations.

Normalization excludes only columns whose current schemas make them
execution-dependent. Snapshot batch IDs and immutable manifest/checksum
provenance remain compared because snapshot batch IDs are content-derived.
Event `batch_id` and its checksums are excluded from logical event rows because
the batch ID includes `consumer_id`; the comparison keeps each durable event
ID, Source, source timestamp, payload checksum, and full event JSON. Event
line number, raw envelope, disposition, and ingestion time describe that
delivery. `disposition` can change from `accepted` to `duplicate` on an exact
replay. Batch completion, expected/ingested counts, quarantine count, and
identity-conflict count are checked separately. Ingestion timestamps, event
age/latency, publication timestamps, and freshness age are also omitted from
row equality; event and snapshot source timestamps remain included.

The raw `scout_crawl_event_records` table stores deliveries by
`(batch_id, line_number)`. The `scout_reconciliation_complete_events`
projection joins completed batch records, so it can also repeat an event
delivered in separate batches. For these two projections the verifier retains
all non-execution columns, including event identity, Source, source timestamp,
schema/type, payload checksum, and full event JSON. It collapses only exact
full-row duplicates after rejecting any logical event identity with
conflicting substantive versions. It reports physical delivery counts
separately from logical distinct counts. The event batch/attempt audit
and `gold_crawl_event_ingestion_status` are not compared as logical analytics.
Event batches must all be complete, account
for their declared row counts, and have zero quarantine and conflict counts.
`gold_ingestion_metrics` is execution telemetry and is excluded. The
reconciliation gate must be `authoritative` on both sides, with matching
publication evidence, valid snapshot metadata, exact capture count and task
identities, and Page/alias counts equal to the snapshot manifest. Every capture
reconciliation row must have an authoritative disposition.

This is a live Spark/Delta check. Local syntax checks, the illustrative
fixtures, and subprocess tests cannot establish Bronze/Silver/Gold replay
equivalence or Databricks reconciliation behavior. It does not measure
workspace resources, prove workspace disconnection during Crawl, or replace
the required live execution evidence.
