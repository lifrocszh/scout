# Analytics retention pruning

Pruning is an explicit operator action. The command inspects at most 128 events
per call and defaults to a dry run:

```sh
cargo run -- events prune --run-id run-<64-hex-id> --data-dir data
cargo run -- events prune --run-id run-<64-hex-id> --data-dir data --max-events 128 --execute
cargo run -- events prune --batch-id <batch-id> --data-dir data --execute
```

`SCOUT_DATABASE_URL` must point to the Crawl database. A run target selects the
oldest unpruned events up to `--max-events`; a batch target selects that
batch's complete event coverage and blocks if it exceeds the limit. Only the
selected local batch copy is removed for a batch target. Other consumer copies
are preserved, except a previously recorded incomplete deletion is resumed.
Review the JSON report before adding `--execute`; `status` is `ready`,
`blocked`, or `pruned`, and blockers identify the failed condition.

Execution requires a completed run whose retained event count and independent
coverage fingerprint match. Every selected event must retain its payload hash,
be covered by a clean verified archive batch, and have verified archives for
all registered body, crawl-manifest, or snapshot artifacts. Active required
consumer replay windows must be closed, and each historically required
consumer must have a validated downstream receipt for the event. A retired
required consumer remains applicable to event-history rows recorded before
retirement; later events do not inherit that consumer's receipt obligation.
Optional consumer receipts do not block pruning. The imported receipt is
trusted operator-provided workspace evidence, not a cryptographic attestation.
Quarantined or conflicting batches block pruning.

Before removing anything, execution persists checksum-bound intents in
PostgreSQL. Event payload rows and their completion intents change in one
transaction. Local batch files are checked against the archive and removed
outside the database transaction; an incomplete intent allows a later command
to resume deletion only when every remaining entry is an expected regular file
with the recorded checksum. Unexpected files, symlinks, changed checksums,
unverified archives, or changed event-history coverage stop the operation.

Pruning removes event payload rows and redundant local event-batch copies. It
preserves run/event history, receipts, artifact dependencies, verified archive
copies, body blobs, crawl manifests, snapshots, and Generation inputs. It does
not prune automatically, delete artifacts, or retire consumers. Keep the
`$DATA_DIR/analytics-archive` directory intact; a verified local export is not
the same as a downstream ingestion receipt.
