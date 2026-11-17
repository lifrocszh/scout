# Observability: Prometheus metrics and Grafana (issue #7)

Adds `/metrics` to the coordinator, workers and `serve`, a Prometheus and Grafana pair under the
Compose `obs` profile, and an auto-provisioned dashboard. No new Rust dependency.

## What was added

| Where | What |
| --- | --- |
| `src/observability/metrics.rs` | The metric families as `static`s (`metrics::TASK_CLAIMS.inc(&[])`), counter, gauge and histogram primitives, and the Prometheus text rendering. No registry is passed around. |
| `src/observability/exporter.rs` | The `/metrics` HTTP endpoint on a small thread. Off unless `--metrics-bind` or `SCOUT_METRICS_BIND` is set. `SCOUT_METRICS_LINGER_SECS` keeps it up after the process finishes. |
| `src/distributed/coordinator/frontier_gauges.rs` | Samples frontier depth from Postgres on a private connection, every `SCOUT_METRICS_SAMPLE_SECS`. Runs only when `/metrics` is on. |
| Instrumentation | Coordinator: `database.rs` (pool wait), `rpc.rs` (fencing), `results.rs` (completions), `frontier/tasks.rs` (claims, renewals, failures, expiry), `frontier/request_policy.rs` (per-origin permits). Worker: `request()` in `worker.rs`. Serve: `search_response` in `http/mod.rs`. |
| `compose.yaml`, `deploy/` | Profile `obs`: `prom/prometheus:v3.13.5` and `grafana/grafana-oss:12.4.3`, scrape config, Grafana datasource and dashboard provisioning, `deploy/grafana/dashboards/scout.json` (18 panels). |
| `scripts/ci/compose-obs-checks.sh` | Asks Prometheus for the key series and checks Grafana. Called by `compose-smoke.sh`, which now runs the `obs` profile too. |
| `docker/sources.fixture-slow.toml` | The fixture crawl with 2 s spacing (about 30 s), to watch the dashboard fill in live. |

Metric names: `scout_pages_fetched_total`, `scout_task_claims_total`, `scout_task_completions_total`,
`scout_task_failures_total`, `scout_lease_renewals_total`, `scout_lease_expirations_total`,
`scout_stale_results_total`, `scout_origin_requests_total`, `scout_frontier_tasks`,
`scout_frontier_expired_leases`, `scout_db_pool_wait_seconds`, `scout_db_pool_connections`,
`scout_db_pool_connections_in_use`, `scout_search_duration_seconds`, `scout_search_requests_total`,
`scout_http_requests_total`, `scout_build_info`. Labels and meanings are in the README table.

## Design notes

- **Where the counters live.** At the places that already see the event, so no signature changed.
  The DB pool wait is recorded in `FrontierPool::with_store`. Lease fencing is counted once in
  `with_coordinator_db`, which every lease-validating RPC goes through, by matching the stale-lease
  status (now a shared constant); completions and the lost `fail_task` race are counted where the
  result is decided. Claim, renew and fail are counted in `FrontierStore`.
- **Lease expiry is counted at reclaim.** Expiry is lazy: nothing happens when a deadline passes,
  and the next claim takes the task over. `CLAIM_ONE_TASK` now also returns whether the claimed
  task was `leased` (an expired lease), and that bumps `scout_lease_expirations_total`. The
  `scout_frontier_expired_leases` gauge shows leases past their deadline that nobody has reclaimed.
- **Per-origin request rate is counted at the coordinator**, where a request permit is granted. That
  is the rate the politeness limits act on, it covers robots and sitemap requests, and it survives
  workers exiting. Origins are limited to the configured Sources.
- **Bounded labels.** Origin, kind, mode, outcome and RPC name only. A family holds at most 512
  series; more fold into one `overflow` series.
- **Closed label sets are seeded at zero** (outcomes, RPC names, search modes), because a counter
  that first appears with a nonzero value hides that first increase from `rate()`.
- **Scrapes never wait on the database.** Frontier depth comes from a sampler thread with its own
  connection, not from the scrape and not from the pool, so it cannot distort the pool-wait metric.
- **Short-lived processes.** A fixture crawl ends in seconds and Prometheus marks an exited
  target's series stale. The dashboard tiles use `max_over_time` / `last_over_time` over the
  selected range, and `SCOUT_METRICS_LINGER_SECS` gives a last scrape. The smoke test sets it to 8 s
  (scrape and discovery interval are 2 s each).
- **Compose.** The processes always serve `/metrics` on ports inside the Compose network; nothing
  publishes them, so without the profile the stack is the same as before. Prometheus and Grafana
  publish on loopback only. Grafana allows an anonymous viewer; edits need the admin login
  (`GRAFANA_ADMIN_PASSWORD`, default `admin`). Both images are OSI-licensed (Prometheus Apache-2.0,
  Grafana OSS AGPL-3.0, unmodified).

## Tests

- `observability::metrics` (13) and `observability::exporter` (3): rendering, escaping, cumulative
  histogram buckets, the series cap, seeding, role separation, name validity, HTTP routes, a stalled
  client not blocking the next scrape.
- `cli::tests` (2): `--metrics-bind` accepted by the three roles, rejected elsewhere.
- `frontier::tests::postgres_task_metrics_and_status_counts` (Postgres): claims, expiry reclaim,
  renewals, terminal and stale failures, and the status counts behind the gauges.
- `distributed_rpc_races::coordinator_metrics_count_retries_and_lease_fencing` (Postgres, real
  coordinator process): exact counts for claims, retry and terminal failure, a stale completion and
  a stale renewal fenced, pool wait observed, no connection left checked out.
- `compose-smoke.sh` / `compose-obs-checks.sh`: after a fixture crawl, Prometheus holds at least 11
  claims, accepted results, page fetches, page permits and succeeded tasks; 3 workers, the
  coordinator and serve were scraped; the rare-event series exist at zero; serve's `/metrics` shows
  the histogram; Grafana is healthy, its datasource is healthy, the dashboard has all 18 panels,
  and all 27 dashboard queries are valid PromQL.

## Verified locally (Docker 29.10, 4 CPUs)

The image for the local runs was `debian:trixie-slim` plus the natively built debug binary, not the
repository `Dockerfile` (a cold build takes about 7 minutes here). The Dockerfile is unchanged and
is built by CI.

| Check | Result |
| --- | --- |
| `compose-smoke.sh 3` with the obs profile | passed 4 of 4 runs, 35 s each (up in 19 s, including the 8 s linger) |
| Fixture crawl as seen by Prometheus | 13 claims and completions, 12 page fetches, 12 page + 1 robots + 1 sitemap permits, frontier 13 succeeded, 234 pool-wait observations over a pool of 4 |
| Per-container peak memory (obs included) | grafana 110 MiB, postgres 58, prometheus 37, coordinator 8, workers 5 to 6, serve 4; sum 234 MiB (budget 2048) |
| Live crawl (`sources.fixture-slow.toml`) | dashboard showed pending draining, 3 leased, about 0.5 pages/s and 4 pool connections in use mid-crawl |
| Linger set to 0 | smoke fails as intended (`pages fetched by workers: got '4', want >= 11`), logs dumped, nothing left running |
| `cargo fmt --check`, `cargo clippy --all-targets -D warnings`, `cargo test` | clean; 56 tests pass |
| `scripts/ci/postgres-tests.sh` (Postgres 16 locally) | all binaries pass |

## Not verified here

- **The repository `Dockerfile` build and `image.yml`.** Unchanged, so only CI exercises them.
- **Docker Hub rate limits.** The smoke test now pulls two more images (Prometheus, Grafana). This
  sandbox hit a `429` on one pull and succeeded on retry. GitHub runners have not shown this for
  `postgres:17` and `busybox`, but the new pulls are the first thing to look at if the job flakes.
- **Postgres 17.** Local tests ran on 16; CI uses 17.
- **A crawl longer than the fixture.** The sampler does one grouped scan of the run's tasks per
  interval; on a very large frontier raise `SCOUT_METRICS_SAMPLE_SECS`.

## Deferred

- **Final-attempt expiry is not in the counter.** A lease that expires on a task's last attempt is
  marked `failed` inside the claim statement, which returns nothing when no task is claimed, so it
  shows up in `scout_frontier_tasks{status="failed"}` but not in `scout_lease_expirations_total`.
- **A stale queued permit is not counted.** A fenced lease found while granting a queued request
  permit (`scheduling.rs`) is handled inside the dispatcher and not reported through
  `with_coordinator_db`, so `scout_stale_results_total` misses it. The immediate path is counted as
  `acquire_request_permit_lease_check`.
- Alert and recording rules, process CPU and memory metrics, fetch-duration and RPC-latency
  histograms, per-source gauges.
- Kubernetes scrape config (`ServiceMonitor`/annotations) belongs to the Helm chart in #8.
- Authentication or TLS on `/metrics` (it is meant for a private network).

## CI results

First run on the pull request (head `0df1480`, run 13): all jobs green, about 3.6 minutes wall time.

| Job | Result | Duration |
| --- | --- | --- |
| `lint` | success | 35 s |
| `test` | success | 1 min 8 s |
| `test-postgres` | success | 3 min 14 s |
| `python` | success | 7 s |
| `features` | success | 42 s |
| `search-quality` | success | 1 min 6 s |
| `compose-smoke` | success | 3 min 33 s |
| `features-macos` | skipped (manual only) | |

`compose-smoke` ran the obs checks on a GitHub runner: all 27 dashboard queries valid, 18 panels
provisioned, per-container peak memory summing to 226 MiB (grafana 119, postgres 52, prometheus 36,
coordinator 6, the rest 3 each) against the 2048 MiB budget. The Prometheus and Grafana pulls did not
hit a rate limit.
