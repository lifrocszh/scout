# Durable Crawl events

`scout events consume` reads the PostgreSQL outbox for one Crawl. Set
`SCOUT_DATABASE_URL` to the same database used by the coordinator; the command
applies pending Scout migrations before reading.

```sh
export SCOUT_DATABASE_URL='postgresql://...'
RUN_ID='run-<64-hex-crawl-id>' # Replace with the coordinator's run ID.
scout events consume \
  --run-id "$RUN_ID" \
  --consumer-id bronze-export-v1 \
  --batch-size 32 \
  --poll-interval-ms 1000 \
  --follow
```

The command writes one versioned JSON event per stdout line and flushes it
before acknowledging that event. Batch size is bounded to 1–128 (default 32);
idle follow polls wait 1 second by default. `--follow` keeps polling until it
outputs and acknowledges `corpus_snapshot_published`. Without `--follow`, one
bounded batch is read and the command exits. Run ID and consumer ID are
required; keep the consumer ID stable across restarts to resume its independent
checkpoint.

For distributed Crawls, `crawl_completed` does not itself publish the final
snapshot event. After the coordinator has completed the run, materialize its
durable capture spool with the same config and data directory:

```sh
CONFIG=./data/configs/sources.toml
DATA_DIR=./data
scout crawl --config "$CONFIG" --data-dir "$DATA_DIR" --materialize-spool "$RUN_ID"
```

This seals and validates the Corpus snapshot, then writes
`corpus_snapshot_published`. If the filesystem seal succeeds but database
publication fails, rerun the same materialization command to reconcile it.

Delivery is at least once. If the process stops after flushing an event but
before its checkpoint commits, the next invocation can print that event again.
Downstream writers should deduplicate by `event_id`. Reuse the same command to
resume after a failure; retries use database backoff. To replay a run from the
beginning, use a new consumer ID:

```sh
scout events consume --run-id "$RUN_ID" --consumer-id bronze-export-v2 --follow
```

Use one process per consumer ID. Separate consumers keep independent
checkpoints. This CLI only reads the durable event feed; it does not upload to
Databricks or alter Crawl results.
