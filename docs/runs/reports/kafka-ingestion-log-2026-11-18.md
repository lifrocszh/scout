# Kafka ingestion log (issue #13)

Date: 2026-11-18

This adds Apache Kafka as an optional, replayable log behind the Postgres outbox
([ADR 0003](../../adr/0003-kafka-ingestion-log.md), [design notes](../../design.md#4-kafka-behind-the-outbox-not-in-front-of-postgres)).
The relay copies one run's outbox into Kafka; the reducer rebuilds the run's Corpus snapshot from
Kafka alone and checks it against the live snapshot. Both checks below passed locally (one
4-vCPU Linux container, Docker 29, `apache/kafka:4.1.2`, `postgres:17`).

## What was added

| File | Purpose |
| --- | --- |
| `docs/adr/0003-kafka-ingestion-log.md` | Why a log next to the frontier, what it costs, and why the relay reads the outbox instead of workers writing to Kafka. |
| `src/kafka/` | `log.rs` (rskafka wrapper, murmur2 partitioner), `record.rs` (wire format), `relay.rs`, `reducer.rs`, `reducer_state.rs` (offsets stored with the effects). |
| `src/distributed/frontier/outbox_relay.rs` | Ordered outbox reads for the relay (head-of-line, backoff never reorders). |
| `src/cli/kafka.rs` | `scout kafka relay` and `scout kafka reduce`. |
| `compose.yaml`, `docker/stack.sh` | `kafka` profile (also in `data`): Kafka in KRaft mode, `kafka-relay`, `kafka-reducer`. |
| `deploy/helm/scout/templates/kafka*.yaml` | `kafka.enabled=true`: Kafka StatefulSet, relay and reducer Deployments. |
| `deploy/grafana/dashboards/scout.json` | Row "Kafka ingestion log": relay lag, consumer lag per partition, records/s, parity. |
| `tests/kafka_log.rs` | Real crawl, relay and reducer each crashed once mid-delivery, replica checked byte for byte. |
| `scripts/ci/compose-kafka-checks.sh`, `.github/workflows/kafka.yml` | The Compose profile end to end with a slow reducer. |

## Replay parity with crashes (`tests/kafka_log.rs`)

A coordinator and two workers crawl a local six-page site (one exact duplicate, so the snapshot
has an Alias), and the live snapshot is sealed and published. Then the site is shut down and:

1. The relay aborts (`SCOUT_FAULT_KAFKA_ABORT=relay-after-produce:2`) after writing event 2 to
   Kafka and before acknowledging it. The restarted relay sends 7 of the 8 events: event 2 again,
   so the log holds one duplicate result.
2. The reducer aborts (`reduce-after-apply:3`) after inserting the third capture into its spool
   and before recording its offset. The restarted reducer re-reads it; the insert is a no-op.
3. A third reducer on an empty directory replays from offset 0.

Both replicas report `parity: true`, the live snapshot ID, 6 applied results and 6 spool rows; the
fresh one skipped the relay's duplicate. `pages.jsonl`, `aliases.jsonl` and `passages.jsonl` are
byte-identical to the live snapshot's, and the site saw no request after the crawl. Six
consecutive runs passed, 2.4-2.9 s each.

## Compose profile and backpressure

`scripts/ci/compose-kafka-checks.sh 2` (fixture site, two workers, `obs` and `kafka` profiles)
passed: crawl, index and serve finished as without Kafka; 12 captures relayed, applied once each;
replica snapshot `snapshot-193dba1b...` equals the live one; Prometheus saw the reducer's lag rise
and drain to 0, the relay's pending events at 0 and the parity gauge at 1.

Backpressure, measured with the same stack and `docker stats` streaming once a second:

| Reducer delay per result | Crawl (coordinator start to exit) | Worker peak memory | Reducer peak lag | Reducer finished after crawl.completed |
| --- | --- | --- | --- | --- |
| 0 ms | 4.24 s | 3.8 / 3.9 MiB | 0 (not sampled above 0) | 2.5 s |
| 1500 ms | 4.24 s | 3.5 / 3.9 MiB | 11 records | 20.3 s |

The slow consumer changed neither the crawl time nor the workers' memory: nothing upstream waits
for Kafka. Its lag went 11, 10, ... 1, 0 over about 19 s after the crawl had finished.

## Limits of this run

- In Compose the relay starts once Kafka's health check passes, which on the fixture crawl was
  after the crawl had finished, so the relay sent the whole run in one go (0.7 s). On a longer
  crawl it follows the outbox while the crawl runs.
- Worker memory samples are sparse (a worker lives about 4 s; 4-6 samples each).
- The Helm chart's Kafka resources are linted and schema-validated (`make helm-lint`) but were
  not run on a cluster.
- The image was assembled from a locally built release binary on `debian:trixie-slim` because
  Docker Hub rate-limited the full `Dockerfile` build here; CI builds the real image.
