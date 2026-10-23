# Live Crawl to snapshot reconciliation

Run `scripts/databricks/crawl_snapshot_reconcile.py` after the crawl-event and
snapshot ingestion notebooks have created their tables. It creates dynamic
views; later event or snapshot batches change their results without rebuilding
stored history. Rerun the notebook only if its views were dropped or changed.

## Authority gate

`gold_crawl_snapshot_reconciliation` reports each run/snapshot candidate and
its `authority_status`. A row becomes `authoritative` only when all of these
conditions hold:

- A snapshot ingestion batch has `status = 'complete'` and its Crawl Manifest
  and snapshot records agree on the complete run and snapshot IDs, checksums,
  and Page, alias, and Passage counts.
- A `corpus_snapshot_published` event is present in a complete event batch with
  an `accepted` or exact `duplicate` disposition. Its snapshot ID, source
  snapshot SHA-256, Crawl Manifest SHA-256, and three record counts match that
  completed snapshot batch.
- Distinct accepted `capture_committed` event IDs from complete event batches
  equal the Crawl Manifest's `summary.captured_page_count`; each has a unique,
  non-null task ID.

The join uses `(crawl_run_id, event_id)` and exact event JSON to find a complete
replay. The identity ledger can retain rows from a failed notebook attempt, so
its original `batch_id` does not authorize an event. `crawl_completed`, an
unmatched publication event, a partial event export, and provisional capture
analytics never authorize final history. Incomplete Crawls without a validated
snapshot remain provisional.

Status values include `awaiting_publication_event`,
`awaiting_validated_snapshot_batch`, `awaiting_capture_events`,
`publication_binding_mismatch`, `capture_event_count_mismatch`,
`snapshot_manifest_mismatch`, and `provisional_no_validated_snapshot`.

## Views

- `gold_authoritative_page_observations` exposes the existing canonical Page
  history only for authoritative snapshots.
- `gold_authoritative_page_alias_observations` preserves Scout's recorded
  alias and representative IDs. Page totals come from canonical Page history;
  do not count capture events as Pages.
- `gold_crawl_capture_reconciliation` links a capture only when the durable
  event task ID, Source, body hash, and content hash match a successful
  `capture.accepted` operational record and a unique Crawl Manifest `pages[]`
  row by requested URL and hashes. It retains Scout's `capture_id`, admission
  outcome, selected capture, and canonical Page ID. Multiple task events may
  point to one logical `capture_id`; ambiguous or unmatched links stay
  provisional. No association is inferred from a body hash alone.
- `gold_crawl_rejected_task_observations` retains rejected task records without
  inventing a Page or capture.

The authoritative Page and alias views are analytics only. They do not feed
search, change local Corpus artifacts, or alter Generation builds or activation.

## Deterministic fixture

[`fixtures/crawl-snapshot-reconciliation-demo.json`](fixtures/crawl-snapshot-reconciliation-demo.json)
shows event-first and snapshot-first arrival, replay, missing and mismatched
batches, incomplete Crawls, capture-count gating, alias admission, and ambiguous
capture matching. It describes expected view states; its IDs are scenario
labels, not ingestible Scout artifacts. No live Databricks execution was
performed for this implementation.
