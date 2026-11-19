# Kafka as an optional ingestion log behind the outbox

**Status:** accepted

## Context

Accepted crawl results reach downstream consumers through the Postgres outbox
(`scout_crawl_events`), delivered by `scout events consume` and `scout events export`. That path is
one consumer at a time per checkpoint, it reads Postgres for every delivery, and an event only
references the page body by its content address in the coordinator's data directory. Issue #13 asks
for a replayable log between fetching and processing: rebuild a snapshot from the log without a
recrawl, fan out to more consumers (the lakehouse in #14), and show backpressure.

Postgres already works as the queue for the frontier ([design notes](../design.md#2-postgres-skip-locked-instead-of-a-broker-for-the-frontier)),
and nothing here changes that. The question is only whether a log next to the frontier pays for
itself.

### What a log adds

- **Replay.** A Kafka topic keeps every accepted result, body included, for its retention period.
  A new consumer, or a rebuilt one, reads from offset 0 and reproduces a Corpus snapshot with no
  HTTP fetch and no access to Postgres or the coordinator's volume.
- **Fan-out.** Each consumer tracks its own offsets. Adding the lakehouse (#14) or a second
  replica costs no extra Postgres reads and no new outbox checkpoint table rows per event.
- **Backpressure isolation.** A slow consumer only grows its own lag. The crawl, the coordinator
  and the workers do not wait for it, and their memory does not depend on it.

### What it costs

- **One more stateful service**: a JVM broker with its own volume, retention and upgrades. It is
  single-broker here (replication factor 1), so it is not highly available, and losing its volume
  loses the replay history (Postgres and the coordinator's volume still hold the authoritative
  copy, and the relay can be re-run from the outbox).
- **At-least-once, again.** Kafka adds two more hops that may repeat (relay to broker, broker to
  consumer). Consumers must deduplicate, exactly as outbox consumers already do.
- **Ordering is per partition only**, so anything that needs a total order must share a key.
- **Operations**: lag to watch, topic sizing, broker message-size limits for large bodies.

These costs are acceptable only if Kafka stays optional and never becomes part of the write path
that decides whether a result is accepted.

## Decision

Add Apache Kafka (KRaft mode, one combined broker/controller, official `apache/kafka` image) as an
**optional** log fed from the outbox:

```
workers --gRPC--> coordinator --1 tx--> Postgres (result + outbox event)
                                              |
                               scout kafka relay (outbox consumer "kafka-relay")
                                              v
                      Kafka: <prefix>.crawl-results   <prefix>.crawl-events
                                              |
                               scout kafka reduce (offsets stored with its effects)
                                              v
                         replica capture spool -> Corpus snapshot -> parity check
```

1. **The relay reads the outbox; workers do not write to Kafka.** A worker's result is only a
   fact once the coordinator has accepted it under the current lease epoch, in the same
   transaction as its outbox event. If workers produced to Kafka, the log would hold stale-lease
   results and duplicate leases that Postgres rejected, every consumer would have to re-implement
   fencing against Postgres, workers would need broker credentials (ADR 0002 keeps them to the
   gRPC endpoint), and "accepted in Postgres" and "in the log" would be a dual write that can
   disagree. Relaying from the outbox has none of these problems: only accepted results enter the
   log, in commit-consistent form.
2. **Two topics.** `crawl-results` holds one record per accepted capture: the raw body as the
   value, the accepted capture metadata and identifiers as headers, keyed by the task's normalized
   URL, so captures spread over partitions and repeats of one URL stay in order. `crawl-events`
   holds every outbox event byte for byte, keyed by Crawl run ID, so one run's events are totally
   ordered in one partition (the reducer relies on `crawl_completed` following every
   `capture_committed`). Partitions are chosen with Kafka's default murmur2 partitioner so other
   producers agree on placement.
3. **The relay is ordered and at-least-once.** It relays the oldest unacknowledged outbox events
   first and retries the head event instead of skipping it, then acknowledges the event in its
   outbox checkpoint. A crash between produce and acknowledge produces the event again, so the log
   may hold duplicates and consumers deduplicate on `event_id`. The relay registers itself as an
   optional outbox consumer; registering it as required
   (`scout events consumer-register --consumer-id kafka-relay --required`) keeps
   `scout events prune` from deleting events it has not relayed. That is an operator's choice,
   because a required consumer that never runs again blocks pruning.
4. **The reducer stores offsets beside its effects.** `scout kafka reduce` writes each body to a
   content-addressed file, inserts the capture into its own capture spool (an idempotent insert),
   then records the event ID and the next offset in a SQLite state file in the same directory.
   A crash before that record replays the record, and the replayed insert is a no-op. Once a run's
   `crawl_completed` event and every `capture_committed` result before it are applied, the reducer
   materializes a snapshot with the same code as the live path and compares it with the live
   `corpus_snapshot_published` event.
5. **Direct-to-Postgres stays the default.** Compose runs Kafka only under the `kafka` (and
   `data`) profile, the Helm chart only with `kafka.enabled=true`, and no core component depends
   on it. Kafka is downstream of the authority, never in front of it. Parity is checked on every
   reducer run, not assumed.
6. **Client: `rskafka`** (pure Rust, MIT OR Apache-2.0). It needs no C toolchain or librdkafka.
   It has no consumer-group protocol, which this design does not need: the reducer stores its own
   offsets transactionally with its output, the pattern Kafka documents for consumers that keep
   state outside Kafka.

## Consequences

- Replay is real: a replica rebuilt from offset 0 with the fixture site switched off produces the
  same snapshot ID as the live crawl (tested in `tests/kafka_log.rs` and in the Compose `kafka`
  profile).
- Snapshot parity is the contract: same snapshot ID, which is a digest over the config digest and
  every Page, Alias and Passage record. The replica's Crawl Manifest differs in timestamps and in
  rejected/failed counts, which the log does not carry; neither is part of the snapshot identity.
- Lag is visible: `scout_kafka_relay_pending_events` (outbox events not yet in Kafka) and
  `scout_kafka_consumer_lag_records` (high watermark minus the reducer's next offset), with
  Grafana panels.
- The relay needs Postgres credentials and read access to the coordinator's data volume (for
  bodies), so it runs next to the coordinator. Bodies larger than the broker's
  `message.max.bytes` cannot be relayed; Compose and the chart raise it to fit the 16 MiB body cap.
- Not in scope: scheduled (Source-scoped) runs in the reducer, multiple brokers, TLS/SASL, and
  making Kafka the write path. Each would need its own decision.
