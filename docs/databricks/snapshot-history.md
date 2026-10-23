# Historical Page comparisons

Snapshot ingestion maintains a logical history per `snapshot_id` in addition to
the ticket-02 per-batch audit tables. Re-exporting one snapshot with evidence
adds another batch audit row, while the historical Source, Page, alias, and
evidence observations merge by snapshot identity and do not count twice.
Arrival order does not determine comparison order: views order snapshots by the
snapshot seal timestamp and a stable snapshot-ID tie-breaker.

## Operator-supplied evidence

Normal Scout logs do not emit deletion evidence. A data custodian must create a
sidecar from authoritative knowledge outside the normal crawl log, then pass
it to the exporter. The exporter reads the file, copies its exact bytes into
the immutable batch, hashes it into the manifest, and includes that checksum
in the batch ID. Do not edit a published sidecar or batch in place; changed
evidence requires a fresh export.

The sidecar envelope is:

```json
{
  "schema_version": "scout.page-history-evidence.v1",
  "snapshot_id": "snapshot-<64 lowercase hex characters>",
  "run_id": "run-<64 lowercase hex characters>",
  "source_snapshot_manifest_sha256": "<64 lowercase hex characters>",
  "crawl_manifest_sha256": "<64 lowercase hex characters>",
  "records": []
}
```

`records` must contain at least one versioned assertion. A tombstone names one
Page that is absent from this snapshot:

```json
{
  "schema_version": "scout.page-tombstone.v1",
  "snapshot_id": "snapshot-<same snapshot ID>",
  "run_id": "run-<same run ID>",
  "source_id": "<Source ID>",
  "scope_sha256": "<Source scope hash>",
  "page_id": "<64 lowercase hex Page ID, absent from this snapshot>",
  "tombstoned_at_utc": "2026-10-13T04:15:00Z"
}
```

A complete-scope assertion attests that the Source was fully checked. The
optional `expected_page_count` is the number of Pages in this Source in the
current snapshot; when supplied it must match the exported Page count. Without
that value the comparison can be marked attested-complete, but it has no
denominator for a coverage percentage.

```json
{
  "schema_version": "scout.source-coverage-assertion.v1",
  "snapshot_id": "snapshot-<same snapshot ID>",
  "run_id": "run-<same run ID>",
  "source_id": "<Source ID>",
  "scope_sha256": "<Source scope hash>",
  "complete": true,
  "expected_page_count": 17,
  "asserted_at_utc": "2026-10-13T04:15:00Z"
}
```

Both evidence forms must have a valid RFC 3339 UTC timestamp ending in `Z`,
match the snapshot and run IDs, and bind to the snapshot-manifest and Crawl
Manifest checksums in the envelope. Their `scope_sha256` is SHA-256 of compact,
key-sorted UTF-8 JSON containing `source_id`, `seeds`, `allowed_urls`,
`allowed_origins`, `path_prefixes`, `deny_path_prefixes`, and
`sitemap_url_prefixes`. Array values are sorted; an absent `allowed_urls` is
`null`, and absent `sitemap_url_prefixes` is `[]`. This binds assertions to the
actual Source scope rather than just its display name.

The exporter accepts the sidecar as read-only input:

```sh
python3 scripts/databricks/snapshot_batch.py export \
  --data-dir ./data \
  --snapshot-id "$SNAPSHOT_ID" \
  --output-root ./databricks-exports \
  --history-evidence ./history-evidence.json
```

It validates the sidecar before writing a package. Coverage assertions are
accepted only when the Crawl Manifest says `frontier_exhausted` and both
`failed_count` and `rejected_count` are zero. A bounded, errored, or incomplete
crawl cannot assert complete coverage. A specific tombstone can still record
an independently verified removal from a bounded crawl, but it only applies
to that prior Page ID. Missing Pages without supported evidence remain
`unobserved`.

## Comparison contract

The Source scope hash includes the allowed URL/origin and path constraints,
seeds, and sitemap URL prefixes. The extraction-contract hash includes
`min_content_chars`, `max_decoded_body_bytes`, `max_extraction_bytes`,
`max_extraction_millis`, and `extraction_schema`. A Source comparison is
compatible only when both hashes match. A changed scope or extraction policy
starts a new comparison window; a Page absent across that boundary is not
called removed.

`observed_at_utc` comes from `snapshot.created_at_utc`. Page JSON and the Crawl
Manifest do not identify an exact fetch time for each Page, so this is the
snapshot seal/observation time, not a capture timestamp. A changed content
hash is known to have changed within `(previous Page observation time, current
Page observation time]`; the exact edit time is unknown.

For a missing Page, presence comparison looks only at a previously observed
Page ID in the same uninterrupted compatible Source scope. A missing Page in a
bounded or errored snapshot is `unobserved`; a later complete snapshot can
compare against that last prior Page observation. It becomes `removed` only if
the current snapshot carries a matching explicit tombstone or a valid
complete-scope assertion. The complete-scope assertion can support all prior
Pages in that Source that are absent from the asserted snapshot.

## Tables and views

The notebook creates these logical-history Delta tables:

- `scout_source_observations`: one row per `(snapshot_id, source_id)`, with
  `scope_sha256`, `extraction_contract_sha256`, `observed_at_utc`, source
  snapshot and Crawl Manifest checksums, crawl stop/failure/rejection state,
  `page_count`, and `source_json`.
- `scout_page_observations`: one row per `(snapshot_id, source_id, page_id)`,
  with batch lineage, both compatibility hashes, seal time, content hash, URL,
  and `record_json`.
- `scout_page_alias_observations`: one row per
  `(snapshot_id, source_id, alias_page_id)`, preserving the observed
  representative Page per snapshot.
- `scout_page_history_evidence`: validated versioned evidence per snapshot,
  Source, evidence schema, and Page (coverage assertions use an empty
  `page_id`).

Gold views expose these columns:

- `gold_source_scope_comparisons`: all Source observation columns plus
  `previous_snapshot_id`, `previous_scope_sha256`,
  `previous_extraction_contract_sha256`, and `comparison_status`
  (`first_observation`, `compatible`, or `incompatible_scope_or_extraction_contract`).
- `gold_page_change_observations`: Page observation columns plus
  `previous_snapshot_id`, `previous_content_sha256`,
  `previous_observed_at_utc`, `change_status` (`new`, `changed`, or
  `unchanged`), and the two `observed_change_interval_*_utc` bounds for
  changed content.
- `gold_page_presence_comparisons`: prior/current snapshot IDs, Source and Page
  IDs, scope hash, prior/current seal times, current crawl coverage fields,
  tombstone/coverage assertion times, `presence_status` (`removed` or
  `unobserved`), `removal_basis`, `removal_evidence_at_utc`, and the observed
  interval bounds.
- `gold_page_alias_observations`: per-snapshot alias, representative, URL,
  compatibility hashes, seal time, batch, and source record JSON.
- `gold_snapshot_history_count_report`: one row per logical snapshot with
  `snapshot_id`, `observed_at_utc`, `source_count`, `page_count`, and
  `alias_count`. These counts deduplicate evidence-bearing re-exports.

Ticket-02's `gold_snapshot_count_report` remains a per-batch audit report.

## Deterministic demonstration

[`fixtures/page-history-demo.json`](fixtures/page-history-demo.json) records a
fixed four-snapshot scenario and expected comparisons. It covers a bounded
missing Page staying unobserved, a later tombstone and complete-scope
assertion, an incompatible Source scope, a representative alias change,
replaying one batch, and batches arriving out of timestamp order. It is a
logical fixture, not a ready-to-ingest batch; its labels are stable scenario
IDs rather than exported Scout artifacts.
