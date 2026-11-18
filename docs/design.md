# Design notes: leases, the Postgres frontier, and delivery guarantees

Three trade-offs behind the crawl pipeline. The picture is in the
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
next to the frontier, not instead of it. [Issue #13](https://github.com/lifrocszh/scout/issues/13)
plans Kafka for replay and fan-out and requires an ADR to justify it first; the direct-to-Postgres
path stays the default until the two produce identical snapshots.

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

## Not built yet

- **Kafka.** Today's log is the Postgres outbox. Its consumers are the optional Databricks Delta
  pipeline ([Phase 2](phase2.md)) and the [local lakehouse](lakehouse.md) (#14): local Delta
  tables queried with DuckDB. Kafka under the Compose `data` profile (#13) is planned. The
  lakehouse reads events through a transport-neutral `EventSource`, so a Kafka source can be
  added to it without changing the tables.
- **Dead letters in the lakehouse.** Dead-lettered tasks stay in Postgres (`scout dlq list`). The
  lakehouse loads snapshots, near-duplicate reports and outbox events, but not dead letters yet.
- **More than one coordinator per run**, and a highly available Postgres. ADR 0002 explains the
  first; [Kubernetes](kubernetes.md) explains why the chart runs a single Postgres.
