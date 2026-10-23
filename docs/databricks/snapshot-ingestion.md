# Databricks snapshot ingestion

Scout exports one completed Corpus snapshot as an immutable JSONL batch. The exporter reads existing snapshot artifacts and its Crawl Manifest; it does not modify canonical storage or generate Page, Passage, or alias identities.

## Prepare a Unity Catalog Volume

Use a Databricks Free Edition workspace with Unity Catalog. Create a Volume in a schema you can use:

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.scout_exports;
```

Replace `workspace.default.scout_exports` with the catalog, schema, and Volume you created. Install the Databricks CLI on the Scout machine, then configure OAuth user authentication:

```sh
databricks auth login --host https://<your-workspace-url> --profile portfolio-trial
```

The CLI stores the profile and refreshable OAuth credentials in its user config. Scout's exporter accepts a profile name and does not read or print tokens. The machine needs outbound access to the Databricks workspace only; it does not expose Scout services.

## Export and validate one batch

Find a completed snapshot ID under `DATA_DIR/corpora/`, then export it:

```sh
python3 scripts/databricks/snapshot_batch.py export \
  --data-dir ./data \
  --snapshot-id snapshot-<64-hex-characters> \
  --output-root ./databricks-exports
```

The command creates `./databricks-exports/batch-<64-hex-characters>/` with exactly:

- `batch-manifest.json`: schema version `scout.databricks-snapshot-batch.v1`, stable batch and source snapshot IDs, raw Crawl Manifest file checksum and size, source artifact checksums, record counts, bounds, and payload checksum.
- `records.jsonl`: versioned envelopes for the snapshot, Sources, Pages, Passages, Page aliases, full Crawl Manifest provenance, and available structured operational events.
- `_PUBLISHED`: the batch manifest checksum. It is written after the payload has been validated; the package is then atomically renamed into place.

Operational records come from the existing `logs/<run_id>.jsonl` stream (`scout.operational-event.v1`) when present. The transactional durable crawl-event feed is a separate interface and is not part of this snapshot batch; see [Durable Crawl events](durable-crawl-events.md) for its consumer and recovery workflow.

Export and validation reject incomplete snapshots, unsupported schemas, corrupt checksums, malformed records, missing Source/Page relationships, count mismatches, and batches over 64 MiB or 100,000 records. The exporter preserves Scout's canonical IDs and verifies content hashes without deriving new identities.

New batch IDs include the raw Crawl Manifest file checksum, so a corrected export gets a new immutable folder even when its logical snapshot is unchanged. Older published folders stay unchanged and remain valid inputs, but they have no raw file byte count and do not pass the authoritative Crawl snapshot reconciliation gate. Re-export them with the updated exporter to produce a raw-bound batch; do not rewrite or delete the older folder.

Validate before upload:

```sh
python3 scripts/databricks/snapshot_batch.py validate \
  --batch-dir ./databricks-exports/batch-<batch-id>
```

## Upload and run

Upload is explicit and authenticated through the Databricks CLI profile. It uses a unique upload-attempt folder and sends `_PUBLISHED` last, so interrupted copies remain invisible to the ingestion notebook.

```sh
python3 scripts/databricks/snapshot_batch.py upload \
  --batch-dir ./databricks-exports/batch-<batch-id> \
  --volume dbfs:/Volumes/workspace/default/scout_exports \
  --profile portfolio-trial
```

Copy the `dbfs:/Volumes/.../uploads/<attempt-id>` path printed by the command, then submit one serverless run:

```sh
python3 scripts/databricks/snapshot_batch.py run \
  --batch-dir ./databricks-exports/batch-<batch-id> \
  --volume dbfs:/Volumes/workspace/default/scout_exports \
  --remote-path dbfs:/Volumes/workspace/default/scout_exports/scout/snapshots/<batch-id>/uploads/<attempt-id> \
  --catalog workspace \
  --schema default \
  --volume-name scout_exports \
  --workspace-path /Users/<your-email>/scout-snapshot-ingest \
  --profile portfolio-trial
```

The command imports `scripts/databricks/snapshot_ingest.py` at the selected workspace path and submits a one-time serverless job with a 1-hour task bound and the cost-efficient `STANDARD` performance target. It does not create a saved or scheduled job. The notebook rechecks the publication marker, checksums, schema, record bounds, canonical IDs, content hashes, Source/Page relationships, and snapshot counts before writing Delta data.

The notebook creates these batch-level assets in the selected schema:

- `scout_bronze_records`: insert-only raw envelopes with batch, snapshot, checksum, and ingestion-time lineage.
- `scout_silver_records`: insert-only canonical IDs plus Source, parent Page, URL, and content-hash projections. Each row retains its batch and snapshot IDs for historical comparisons.
- `scout_ingestion_batches`: batch validation and publication state. A batch appears as authoritative only after Bronze, Silver, and count checks finish.
- `gold_snapshot_count_report`: a view with Page, Passage, and alias counts for complete batches only.

It also maintains idempotent per-snapshot Source, Page, alias, and evidence
history, with Gold views for compatible-scope comparisons and evidence-backed
presence changes. The operator-supplied tombstone and complete-coverage
contracts, exact table/view columns, and a deterministic multi-snapshot
fixture are documented in [Historical Page comparisons](snapshot-history.md).
Coverage, freshness, event/error, change-frequency, effectiveness, and
per-ingestion audit queries are documented in [Crawl coverage and effectiveness reports](crawl-reports.md).

The same batch and manifest can be submitted again; insert-only Delta merges make the existing rows a logical no-op. A reused batch ID with a different manifest or record payload fails. Incomplete ingestion stays unpublished and can be resumed by submitting the same validated batch again.

Inspect the Gold report in a SQL editor:

```sql
SELECT *
FROM workspace.default.gold_snapshot_count_report
WHERE batch_id = 'batch-<64-hex-characters>';
```

Its Page, Passage, and alias counts must match the source snapshot manifest.

## Recovery and limits

If export stops after staging, rerun the same export command. A matching published batch is reused only when its manifest and payload files match byte-for-byte. If upload stops before printing a published path, run `upload` again; it creates a fresh attempt folder and does not overwrite earlier files. The notebook ignores folders without a valid final marker. If a serverless run stops partway through, resubmit the same batch and upload path. The processing marker is not authoritative, and insert-only merges resume the missing records. If validation reports a conflicting batch ID, retain both folders for diagnosis and export from the original immutable snapshot again; do not edit a package in place.

For bounded event and snapshot range backfill, interrupted workflow recovery,
and artifact retention prerequisites, see [Databricks analytics recovery](analytics-recovery.md).

Free Edition provides serverless compute only, has limited fair-use quotas, and allows up to five concurrent job tasks per account. Quota exhaustion can suspend compute until a later reset, while files remain in the Volume. This path runs one bounded task and makes no network calls from Databricks. Review the [current Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), [CLI Volume file commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/fs-commands), and [CLI OAuth setup](https://docs.databricks.com/aws/en/dev-tools/cli/authentication) before operating; quotas and supported features can change.

Databricks is optional downstream analytics. Export, upload, or ingestion failures do not affect Scout crawling, Corpus snapshots, indexing, or search.
