# Crawl coverage and effectiveness reports

Snapshot ingestion publishes the ticket-02 batch audit and ticket-03 history
views, then adds reproducible Gold reports for coverage, freshness, content
change frequency, operational events, and crawl effectiveness. Every row keeps
its `snapshot_id` and `batch_id`; the run report also exposes the snapshot and
Crawl Manifest checksums plus `source_config_sha256`.

## Coverage and freshness

`gold_source_coverage_report` reports `observed_page_count` separately from
`coverage_numerator`, `coverage_denominator`, `coverage_percent`, and
`coverage_basis`:

- A validated complete-scope assertion with `expected_page_count` uses that
  count as its denominator. The exporter checks it against the Source's Page
  count and rejects assertions on bounded or errored Crawls.
- A complete-scope assertion without a count reports
  `coverage_status = 'attested_complete'` with `NULL` numerator, denominator,
  and percentage. This assertion takes precedence over a finite allowed-URL
  list because it supplies no denominator for a percentage.
- A finite `allowed_urls` list uses exact `requested_url` matches from the
  Crawl Manifest as its numerator and the list size as its denominator.
- Open Sources without either denominator report `coverage_status = 'unknown'`
  and a reason. A successful Crawl or a Page count alone never means full
  Source coverage.

`gold_page_freshness_report` selects the latest observation for each
`(source_id, page_id)` and reports `observation_age_days`. The timestamp is the
Corpus snapshot seal time: Crawl Manifests do not contain an exact Page fetch
time, so the view marks `freshness_basis = 'snapshot_seal_time'` and
`timestamp_limit = 'page fetch timestamp unavailable'`. The
`current_presence_status` column distinguishes a Page in the latest Source
snapshot from a historically observed Page whose latest Source scope or
extraction contract changed.

## Changes, errors, and effectiveness

`gold_source_page_change_frequency` groups Page observations by Source,
compatible scope/extraction hashes, and UTC observation month. It reports new,
changed, unchanged, and comparable Pages, plus the changed percentage among
Pages with a previous compatible observation. Change timestamps remain
interval-bounded as documented in [Historical Page comparisons](snapshot-history.md).

`gold_crawl_operational_event_summary` parses structured operational JSONL and
deduplicates records by `(snapshot_id, record_id)`, so an evidence-bearing
re-export does not double count the same run log. Use it to inspect
`fetch.failed`, `page.rejected`, `task.claimed`, `capture.accepted`, and
`worker.task_failed` events by Source and outcome. Synthetic `history.*`
evidence events are excluded from crawl activity and error rates.

`gold_crawl_error_rate_report` computes the failure-outcome percentage over
observed operational event rows, with separate counts for fetch failures,
worker task failures, and rejected Pages. This is an event-log rate, not an
HTTP request error rate; runs without operational events have no rate row,
rather than being treated as zero errors.

`gold_crawl_run_report` contains the Crawl Manifest's stop reason, configured
Page target, captured/Page/Passage/alias/rejected/failed counts, start and end
times, duration, Source count, checksums, and `operational_log_present`. Old
batches where availability was not recorded report `NULL` for that field. The
`failed_count` comes from `fetch.failed`; `rejected_count` comes from
`page.rejected`.

`gold_crawl_effectiveness_report` reports Pages per minute and, when
`task.claimed` events exist, captured Pages and Crawl Manifest fetch failures
per task claim. It keeps `worker.task_failed` event counts separate from the
manifest's fetch-failure count to avoid counting one failed attempt twice.
When the operational log is absent, its availability is unknown for a legacy
batch, or claim events are absent, the attempt denominator and derived ratios
are `NULL` with an explanatory status. A present but empty log is distinguished
from an absent log. Failed runs that never produced a complete snapshot are
outside this snapshot export and cannot appear in these reports.

## Ingestion audit metrics

`scout_ingestion_metrics` records one row per successful notebook execution;
`gold_ingestion_metrics` exposes those rows with the batch and snapshot IDs.
It includes validated record counts, input bytes processed (records, manifest,
and optional evidence sidecar), notebook execution milliseconds, and
`DESCRIBE DETAIL` storage sizes for Bronze, Silver, and history Delta tables.
If storage details are unavailable, `storage_measurements_json` records `NULL`
availability with the reason. Bronze/Silver new and existing row counts are
measured before their idempotent `MERGE`; during concurrent duplicate ingestion
they are candidate counts, not a claim about committed Delta insert metrics.
Each replay appends a new ingestion-attempt row, while the logical Source/Page
history and its comparison reports remain deduplicated.

## Reproducible SQL

Replace the IDs with the batch or snapshot under review:

```sql
SELECT * FROM workspace.default.gold_crawl_run_report
WHERE snapshot_id = 'snapshot-<64-hex-characters>';

SELECT * FROM workspace.default.gold_source_coverage_report
WHERE snapshot_id = 'snapshot-<64-hex-characters>'
ORDER BY source_id;

SELECT * FROM workspace.default.gold_page_freshness_report
WHERE source_id = '<source-id>'
ORDER BY observation_age_days DESC;

SELECT * FROM workspace.default.gold_source_page_change_frequency
WHERE source_id = '<source-id>'
ORDER BY observation_month_utc;

SELECT * FROM workspace.default.gold_crawl_operational_event_summary
WHERE snapshot_id = 'snapshot-<64-hex-characters>'
ORDER BY event_name, source_id;

SELECT * FROM workspace.default.gold_crawl_error_rate_report
WHERE snapshot_id = 'snapshot-<64-hex-characters>'
ORDER BY source_id;

SELECT * FROM workspace.default.gold_crawl_effectiveness_report
WHERE snapshot_id = 'snapshot-<64-hex-characters>';

SELECT * FROM workspace.default.gold_ingestion_metrics
WHERE batch_id = 'batch-<64-hex-characters>'
ORDER BY started_at_utc DESC;
```

The deterministic logical inputs and expected report values are in
[`fixtures/crawl-report-demo.json`](fixtures/crawl-report-demo.json), alongside
the multi-snapshot Page fixture. They document expected semantics without
claiming a live Databricks SQL execution; verify the SQL in the target Free
Edition workspace before relying on a dashboard.

Content screening signals, Scout-owned exact alias groups, and an opt-in
bounded near-duplicate query are documented in
[Content quality and duplicate reports](content-quality-reports.md).
