# Failure-injection suite (issue #10)

Date: 2026-11-18

This adds a failure-injection suite for the Compose stack, a nightly CI job and a
[runbook](../../runbook.md). Each scenario crawls a fresh stack, injects one fault part-way
through and leaves recovery to the stack. All seven scenarios passed on a GitHub runner.

## What was added

| File | Purpose |
| --- | --- |
| `scripts/chaos/run.sh` | Runner: one fresh stack per scenario, a state-triggered fault, shared and per-scenario assertions, `results.jsonl` and `summary.md`. |
| `scripts/chaos/compose.chaos.yaml` | Override: synthetic fixture (`deploy/helm/scout/files/origins.py`, 4 origins x 40 pages, 400 ms per page), Toxiproxy in front of the coordinator, coordinator metrics kept up for a final scrape. |
| `scripts/chaos/sources.chaos.toml`, `toxiproxy.json` | Crawl config (2 s request timeout, so the lease is 4 s) and the proxy definition. |
| `src/distributed/worker.rs` | Test hook `SCOUT_FAULT_STALL_FIRST_LEASE_SECS`: the worker holds its first result that long without renewing, then reports it. |
| `compose.yaml` | Coordinator and workers now use `restart: on-failure:10`, so a crash recovers without a manual step. A clean exit (crawl finished) is not restarted. |
| `docker/stack.sh` | `SCOUT_COORDINATOR_HOST` (default `coordinator`): which host workers dial. The suite points it at Toxiproxy. |
| `.github/workflows/chaos.yml` | The fast set (baseline, worker-kill, stale-lease) on PRs that touch the crawl pipeline, the stack or the suite; every scenario nightly at 02:41 UTC; dispatch with any scenarios. |
| `docs/runbook.md` | Symptom, dashboard panel, diagnosis and action for each failure, with the measured recovery times. |

## What each scenario asserts

Every scenario checks four things:

- the crawl completes;
- the coordinator and every worker container end with exit 0;
- all 160 frontier tasks succeed with exactly one accepted result each, and every result was
  written by the lease epoch that holds the task (`scout_crawl_task_results.lease_epoch =
  scout_crawl_tasks.lease_epoch`);
- the materialized snapshot has 160 unique URLs, and its fingerprint (the sorted
  `normalized_final_url` and `content_identity` pairs from `pages.jsonl`) equals the baseline's.

| Scenario | Fault | Scenario-specific assertion | Recovery measured as |
| --- | --- | --- | --- |
| baseline | none | no lease expirations, no stale results | - |
| worker-kill | freeze a worker with `docker pause`; if it holds a lease, `kill -9` its container's init from the host | that lease is later accepted from another worker at a higher epoch; `scout_lease_expirations_total >= 1`; the container restarted | kill until every lease of the dead worker is reclaimed |
| stale-lease | an extra worker with the 10 s stall hook | `scout_stale_results_total{rpc="complete_task"} >= 1`; the worker logs `worker.task_stale` for that task; the task's result comes from another worker at a higher epoch | stall start until the task is reclaimed |
| coordinator-kill | `kill -9` of the coordinator container's init, after 40 pages | the coordinator restarted and logged `coordinator.resumed` | kill until the first new accepted page |
| postgres-pause | `docker compose pause postgres` for 10 s, after 40 pages | (shared assertions) | unpause until the first new accepted page |
| network-latency | Toxiproxy latency 100 ms +-20 ms, both directions, whole crawl | no lease expirations, no stale results | - |
| network-partition | Toxiproxy proxy disabled for 10 s, after 40 pages | at least one worker restarted | heal until the first new accepted page |

Faults are triggered on frontier state (40 of 160 pages succeeded) and never on a sleep.
Assertions are about outcomes, not timings. The one timing margin the suite relies on is in
network-latency: about 200 ms of extra round trip against a 1.3 s renewal interval and a 4 s
lease.

Why kill from the host: `docker kill` marks the container as manually stopped, and Docker's
restart policy then ignores it. That is what I saw before switching: the killed worker stayed
`Exited (137)`. A `kill -9` of the container's init looks like an OOM kill or a crash, and the
policy restarts it. The runner needs root or passwordless sudo for this, which GitHub runners
have.

## Results

CI on PR #27, [run 44253796650](https://github.com/lifrocszh/scout/actions/runs/44253796650),
`ubuntu-24.04`. The image build took 1 min 35 s from the GHA cache and the seven scenarios
7 min 13 s.

| Scenario | Crawl (s) | Recovery (s) | Stale results fenced | Leases reclaimed | Worker restarts | Coordinator restarts | Snapshot |
|---|---|---|---|---|---|---|---|
| baseline | 29.8 | - | 0 | 0 | 0 | 0 | `34ea78b75bcd` |
| worker-kill | 29.3 | 4.2 | 0 | 1 | 1 | 0 | `34ea78b75bcd` |
| stale-lease | 26.5 | 3.7 | 1 | 1 | 0 | 0 | `34ea78b75bcd` |
| coordinator-kill | 37.2 | 8.4 | 5 | 3 | 3 | 1 | `34ea78b75bcd` |
| postgres-pause | 39.8 | 0.2 | 6 | 3 | 0 | 0 | `34ea78b75bcd` |
| network-latency | 97.4 | 0 | 0 | 0 | 0 | 0 | `34ea78b75bcd` |
| network-partition | 40.3 | 0.8 | 0 | 3 | 3 | 0 | `34ea78b75bcd` |

The crawl time includes stack start-up and the coordinator's 6 s metrics linger. The counters
come from coordinator `/metrics`: the last scrape of each coordinator process, summed over
restarts.

Local runs (Docker 29 on a Linux VM, two runs each) agreed with CI:

| Scenario | Recovery (s) |
| --- | --- |
| worker-kill | 3.8, 4.3 |
| stale-lease | 3.6, 3.7 |
| coordinator-kill | 7.2, 7.1 |
| postgres-pause | 0.9, 0.2 |

The local runs used a plain TCP forwarder in place of Toxiproxy, because this environment could
not pull the Toxiproxy image. So network-latency and network-partition were verified on CI only.

## Observations

- **Postgres pause fences work on its own.** Lease deadlines use Postgres's clock, which keeps
  running while the server is frozen. Every lease held across the 10 s pause expired: after
  unpause, six late calls were fenced and three leases were reclaimed, with no lost or duplicated
  page. The coordinator has no database timeouts, so nothing failed during the pause; it simply
  waited.
- **Coordinator restarts produce some stale calls.** A worker whose gRPC channel reconnects to the
  restarted coordinator presents a lease from a worker ID the new process never registered. The
  call is fenced (`worker.task_stale`), and the worker moves on or exits and restarts.
- **Partitioned workers exit and restart; they do not retry in place.** Recovery after the heal
  was under a second, because the restarted workers were already polling the coordinator's port.
- **Latency below the renewal margin is free.** With 100 ms each way, no lease was lost. The crawl
  took about 3x longer, because each page takes several coordinator round trips (claim, request permits, result).

## Not covered

- Kubernetes: the issue scopes the suite to Compose, so there is no `kubectl delete pod` variant.
  The chart's Deployments restart pods the same way, but that recovery is not exercised by this
  suite.
- One fault per run: repeated faults that exhaust a task's attempts, and a coordinator crash
  during spool replay.
- The dashboard panels named in the runbook are mapped from the metrics they plot. The suite reads
  those metrics; it does not render Grafana.
