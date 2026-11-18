# Design notes: leases, the Postgres frontier, and delivery guarantees

Four trade-offs behind the crawl pipeline. The picture is in the
[README](../README.md#how-it-fits-together); what each component owns and the crawl flow are in
[ADR 0002](adr/0002-coordinator-led-distributed-crawling.md). This page covers why it is built
this way, what it costs, and what is not built yet.

## 1. Leases with fencing, not locks held by workers

A worker can stall past its deadline: a slow site, a paused container, a network partition.
Meanwhile another worker is given the same task. When the first wakes up and reports, both
results must not land.

- **A claim is one SQL statement.** It sets `status = 'leased'`, `lease_owner`,
  `lease_deadline = clock_timestamp() + lease` and `lease_epoch = lease_epoch + 1`. Only
  Postgres's clock decides whether a lease is live; worker clocks never do.
- **Every later write is fenced.** Renewing, failing, asking for a request permit and accepting a
  result all re-read the task row with `FOR UPDATE` and require the same
  `lease_owner`, the same `lease_epoch`, status `leased` and an unexpired deadline. Otherwise the
  call returns `Stale`, writes nothing and counts in `scout_stale_results_total`. The epoch makes
  each lease a distinct generation, so even an earlier lease of the same worker is refused.
- **There is no reaper.** An expired `leased` row is claimable like a `pending` one. The next
  claim bumps the epoch, which is what retires the old holder. Attempts are bounded by
  `max_task_attempts`; a lease that lapses on the last attempt marks the task `failed`.
- **Timing rules.** `LeaseTiming` rejects a lease no longer than the request timeout; the default
  is twice the request timeout, renewed after a third of it.

Cost: a stalled worker's work is thrown away, and a page can be fetched twice (each fetch still
needs a request permit, so politeness limits hold). Multi-host deployments still need
synchronized clocks, because workers check the permit and lease deadlines they are handed
(ADR 0002, Consequences). The contract is exercised by the `postgres_*` tests in
`src/distributed/frontier.rs`, which `scripts/ci/postgres-tests.sh` runs in CI, and end to end by
the failure-injection suite (`scripts/chaos/run.sh`): killed and stalled workers, a killed
coordinator, a paused Postgres and a partitioned network, each checked against a clean run. What
each failure looks like in operation is in the [runbook](runbook.md).

## 2. Postgres `SKIP LOCKED` instead of a broker for the frontier

The frontier is a set of URLs with state, not a stream of independent messages. A claim has to
respect more than "deliver this once":

- **Deduplication.** `UNIQUE (crawl_run_id, source_id, normalized_url)` makes link discovery
  idempotent: the queue and the seen-set are the same table.
- **Fairness and politeness.** A claim picks the next origin (or politeness group) after a per-run
  cursor, and global concurrency, per-origin concurrency, start spacing and retry budgets are rows
  in Postgres that grant request permits transactionally.
- **Atomic transitions.** Accepting a result, enqueueing the links it found, completing the task
  and writing its outbox event commit in one transaction. With a broker, acknowledging the
  message and writing the database cannot be one transaction.
- **One stateful service.** Postgres is already required for the frontier state; a broker would
  be a second system holding a copy of that state that can disagree with it.

The claim query ends in `FOR UPDATE OF task SKIP LOCKED`, so concurrent claims never wait on a
row another claim holds and one row is never leased twice. The honest limit: a claim also locks
the run's scheduler row to advance the fair cursor, and permits lock the run's request-policy row,
so some decisions of one run serialize by design. The
[scaling report](runs/reports/kubernetes-scaling-2026-11-18.md) measured 6.4x throughput from 1 to
8 workers and points at those row locks, not CPU, for the bend.

A broker wins where Postgres does not: many independent consumers replaying a long history,
fan-out at high message rates, or claim rates beyond one database. That is the case for a log
next to the frontier, not instead of it, and that is how the optional Kafka log is built
([section 4](#4-kafka-behind-the-outbox-not-in-front-of-postgres),
[ADR 0003](adr/0003-kafka-ingestion-log.md)).

## 3. At-least-once delivery, idempotent effects

Every hop may repeat; no hop may change the outcome when it does. Exactly-once would need one
transaction across worker, coordinator and sink. At-least-once plus an idempotent receiver gives
the same visible result with failure cases we can test.

1. **Worker to coordinator.** A task's `idempotency_key` is fixed when the URL is enqueued and a
   result carries its `result_sha256`. `accept_task_result` returns `Accepted` the first time,
   `Duplicate` (with the original counts) when the same lease resubmits the same payload, an error
   when the same key arrives with a different payload, and `Stale` when the lease epoch has moved.
2. **One transaction per result.** The result row, content-group deduplication, newly discovered
   URLs, task completion and the `capture_committed` event commit together or not at all.
3. **Outbox publishing.** Events are rows in `scout_crawl_events`, unique on
   `(crawl_run_id, event_type, dedupe_key)`, so publishing the same fact twice is a no-op.
   `scout events consume` keeps a checkpoint per consumer, flushes an event, then acknowledges it;
   a crash in between redelivers it, so consumers deduplicate on `event_id`
   ([durable events](databricks/durable-crawl-events.md)).
4. **Immutable outputs.** A Corpus snapshot is sealed and never changed, and an index Generation
   is built from one snapshot. Activation publishes a Generation only after validation, so
   `serve` answers from one complete Generation at a time.

## 4. Kafka behind the outbox, not in front of Postgres

The optional ingestion log (Compose `kafka` profile, chart `kafka.enabled`) is a copy of the
outbox, never the place a result is decided:

```
worker -> coordinator -> one Postgres tx (result + outbox event)      authority, fenced
                              -> scout kafka relay -> Kafka            replayable copy
                                                   -> scout kafka reduce -> replica snapshot
```

- **Why a relay and not workers producing to Kafka.** A result is a fact only after the
  coordinator accepts it under the current lease epoch (section 1), in the same transaction as its
  outbox event (section 3). Workers writing to Kafka would put fenced-off and duplicate-lease
  results into the log, make every consumer re-check fencing against Postgres, give workers broker
  credentials, and turn "accepted" into a dual write. The relay reads only what committed.
- **Ordered, at-least-once relay.** It sends the oldest unacknowledged outbox events first and
  retries the head event rather than skipping it (unlike `scout events consume`), so a run's
  `crawl_completed` never overtakes one of its captures. It acknowledges the outbox checkpoint
  only after Kafka acknowledged the write; a crash in between sends the event again.
- **Idempotent reducer, offsets stored with its effects.** `scout kafka reduce` writes the body
  (content-addressed), inserts the capture into its own spool (a repeat is a no-op), then records
  the event ID and next offset in one SQLite transaction. A crash replays at most the records after
  the last recorded offset; a duplicate from the relay is skipped by `event_id`.
- **Parity is checked, not assumed.** The reducer seals a snapshot with the same materializer once
  the run's `crawl_completed` and every capture before it are applied, and compares it with the
  live `corpus_snapshot_published` event: the snapshot ID is a digest over the config and every
  Page, Alias and Passage record, so equal IDs mean equal content.
  [`tests/kafka_log.rs`](../tests/kafka_log.rs) crashes the relay and the reducer once each and
  still gets the live snapshot from Kafka alone, with the site switched off.
- **Lag, not backpressure on the crawl.** A slow reducer grows `scout_kafka_consumer_lag_records`;
  an unavailable broker grows `scout_kafka_relay_pending_events`. Neither slows the crawl or
  enlarges worker memory, because nothing upstream waits for Kafka.

## Not built yet

- **The lakehouse reading from Kafka.** The [local lakehouse](lakehouse.md) (#14) loads outbox
  events from `scout events consume` output and exported batches; the Databricks Delta pipeline
  ([Phase 2](phase2.md)) also reads the outbox. Neither reads Kafka yet. The lakehouse's
  transport-neutral `EventSource` makes it a small step: the `crawl-events` topic carries each
  outbox payload byte for byte, so a Kafka source would yield those values with
  `topic/partition/offset` as the reference, and its `(crawl_run_id, event_id)` deduplication
  already absorbs the relay's at-least-once repeats.
- **Dead letters in the lakehouse.** Dead-lettered tasks stay in Postgres (`scout dlq list`). The
  lakehouse loads snapshots, near-duplicate reports and outbox events, but not dead letters yet.
- **More from the Kafka log**: a multi-broker cluster with replication, TLS/SASL, consumer-group
  offsets for third-party consumers, and replay of scheduled (Source-scoped) runs in the reducer.
- **More than one coordinator per run**, and a highly available Postgres. ADR 0002 explains the
  first; [Kubernetes](kubernetes.md) explains why the chart runs a single Postgres.
