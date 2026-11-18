# Kubernetes on kind: crawl scaling from 1 to 8 workers (issue #8)

Measured on the Helm chart (`deploy/helm/scout`) on a kind cluster inside one GitHub-hosted runner.
Workflow run: [e2e-kind.yml, run 44200906315](https://github.com/lifrocszh/scout/actions/runs/44200906315)
(dispatch on `claude/kubernetes-kind-issue-8` at `d72f03b`; benchmark job 57 min 28 s, smoke job 7 min 26 s).
The per-run summaries (`results.json`) are in that run's `scaling-benchmark` artifact (kept 30 days); the tables below carry every headline value.

![Pages per second against workers, and CPU by role](kubernetes-scaling-2026-11-18.svg)

## Result

**Throughput scaled from 8.0 pages/s (1 worker) to 51.3 pages/s (8 workers), 6.44x, with no
failed tasks and no rejected pages. The curve has not flattened in 1 to 8 workers, but each added worker
buys less: efficiency fell from 95% to 90% to 80%.** The extra cost is spent
inside the coordinator and Postgres, not in CPU: all roles together used 1.61 of the node's
4 cores at 8 workers.

| workers | pages/s | speedup | efficiency | marginal pages/s per added worker |
|--:|--:|--:|--:|--:|
| 1 | 8.0 | 1.00x | 100% | |
| 2 | 15.1 | 1.90x | 95% | 7.2 |
| 4 | 28.8 | 3.61x | 90% | 6.8 |
| 8 | 51.3 | 6.44x | 80% | 5.6 |

Where it flattens: **not within this range** (the analyzer flags a flatten when a doubling buys less than 1.3x; the
smallest doubling here bought 1.78x). The next bend is beyond 8 workers and was not measured.

## What was measured

- **Cluster.** kind v0.33.0, one node, kubelet v1.32.2, 4 vCPU and
  16 GiB (`ubuntu-24.04` runner, kernel 7.0.0-1017-azure), helm v3.22.1.
  Coordinator, Postgres, workers and the origin pod all share that node. The chart sets CPU requests only, no CPU limits.
- **Workload.** A synthetic origin pod serves 32 origins (one port each, so each is its own politeness
  domain), 125 pages per origin, every response after 100 ms.
  Each run crawls 3000 pages with a fresh run ID and a fresh coordinator. Politeness is out of the way
  (`global_concurrency = 128`, `origin_concurrency = 4`), so what is measured is the coordinator, Postgres and the workers, not the origin.
- **Runs.** 1, 2, 4 and 8 workers, 3 repetitions each, in alternating order (up, then down, then up) so a warming node or a growing
  database does not favour one count. Four more runs (one per worker count, 1000 pages) have `SCOUT_DISTRIBUTED_METRICS=1`
  on the coordinator for per-RPC timing, and sample Postgres wait events and commits.
  Diagnostic runs are slower and short, so they explain the headline runs but are not part of them.
- **Window.** From the last worker's first task claim to `crawl.completed` in the event log, so pod start-up is excluded.
  Task latency is the worker's own timer, `worker.task_started` to the task's finish event. CPU is each cgroup's `cpu.stat` over the window.

## Headline numbers

Node: 4 vCPU, 16367516Ki memory, kubelet v1.32.2. Page budget 3000 per run, 32 origins, 100 ms per page.

| workers | runs | pages/s (mean) | min-max | speedup | efficiency | task p50 | task p95 |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 3 | 8.0 | 7.9-8.1 | 1.00x | 100% | 116 ms | 125 ms |
| 2 | 3 | 15.1 | 15.0-15.3 | 1.90x | 95% | 121 ms | 132 ms |
| 4 | 3 | 28.8 | 28.5-29.2 | 3.61x | 90% | 124 ms | 144 ms |
| 8 | 3 | 51.3 | 49.4-52.4 | 6.44x | 80% | 135 ms | 171 ms |

| workers | DB pool wait (mean) | DB pool wait (p95) | pool size | max in use | coordinator CPU | postgres CPU | workers CPU | origin pod CPU | all roles |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 0.000 ms | 0.10 ms | 4 | 2 | 0.10 cores | 0.23 cores | 0.02 cores | 0.01 cores | 0.37 cores |
| 2 | 0.000 ms | 0.09 ms | 4 | 3 | 0.15 cores | 0.38 cores | 0.04 cores | 0.01 cores | 0.59 cores |
| 4 | 0.008 ms | 0.10 ms | 4 | 4 | 0.24 cores | 0.66 cores | 0.08 cores | 0.02 cores | 0.99 cores |
| 8 | 0.161 ms | 1.40 ms | 4 | 4 | 0.38 cores | 1.06 cores | 0.14 cores | 0.02 cores | 1.61 cores |

Per run:

| workers | run | pages/s | window | tasks per worker (min-max) | failed tasks |
|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 8.09 | 371 s | 3000-3000 | 0 |
| 1 | 2 | 7.95 | 377 s | 3000-3000 | 0 |
| 1 | 3 | 7.87 | 381 s | 3000-3000 | 0 |
| 2 | 1 | 15.30 | 196 s | 1500-1501 | 0 |
| 2 | 2 | 15.03 | 200 s | 1500-1501 | 0 |
| 2 | 3 | 15.06 | 199 s | 1499-1501 | 0 |
| 4 | 1 | 29.21 | 103 s | 750-752 | 0 |
| 4 | 2 | 28.64 | 105 s | 748-753 | 0 |
| 4 | 3 | 28.55 | 105 s | 748-753 | 0 |
| 8 | 1 | 52.41 | 57 s | 373-378 | 0 |
| 8 | 2 | 52.15 | 58 s | 372-380 | 0 |
| 8 | 3 | 49.38 | 61 s | 371-379 | 0 |

Run-to-run spread (max minus min, as a share of the mean) is 2.7% at 1 worker, 1.8% at 2, 2.3% at 4 and 5.9% at 8 (min 49.4, max 52.4). Work was shared evenly: at 8 workers
each worker claimed 371 to 380 of 3000 tasks.

## Where the efficiency goes

A worker runs one task at a time (its default; the benchmark-only concurrency knob is not set), so one worker's cycle time is `workers / pages-per-second`. It grew by
30 ms between 1 and 8 workers:

| workers | cycle per task | mean task latency (worker's timer) | between tasks (cycle minus task latency) |
|--:|--:|--:|--:|
| 1 | 125.5 ms | 117.5 ms | 8.0 ms |
| 2 | 132.2 ms | 122.0 ms | 10.2 ms |
| 4 | 138.9 ms | 126.6 ms | 12.3 ms |
| 8 | 155.9 ms | 138.5 ms | 17.4 ms |

Of the 30 ms, 21 ms is inside the task timer (which includes its permit and robots RPCs to the coordinator) and 9 ms is outside it
(which includes claiming the next task). The workers are nearly idle: all of them together used 0.14 cores at 8 workers (0.02 at 1).

What the measurements rule out:

- **Not the node.** 1.61 of 4 cores at 8 workers (Postgres 1.06, coordinator 0.38, workers 0.14,
  origin 0.02).
- **Not the origin.** It answered after 100 ms throughout and its CPU stayed at 0.02 cores.
- **Not the connection pool.** The coordinator's 4 database connections were all in use at times from 4 workers on (max in use sampled: 4 of 4), but the mean wait for a connection was
  0.16 ms at 8 workers (1.38 ms per page, against 30 ms of cycle growth), p95 1.40 ms.

What it points to: **row locks in Postgres.** In the diagnostic runs, the mean number of backends waiting on a lock
(`Lock/transactionid` or `Lock/tuple`) was 0.00 at 1 worker, 0.01 at 2, 0.14 at 4 and 0.97 at 8. Over the same range
`acquire_request_permit` service time went from 4.2 to 8.6 ms at the median and from 6.8 to 21.9 ms at p95, and `dispatch` p95
from 5.3 to 16.5 ms. The code explains why those RPCs would queue: every `acquire_request_permit` locks the run's single row in
`scout_crawl_request_policy` with `FOR UPDATE` (`src/distributed/frontier/request_policy.rs`, about line 249), because the global concurrency, spacing
and attempt budget are decided atomically from it, and a completion that discovers new links takes the run's `scout_crawl_task_scheduler` row the same way (`results.rs`, phase `postgres_scheduler_lock_wait`;
page-only completions skip it, and I did not check how many of this workload's completions discover links).
So the permits of one run serialize by design. **That attribution is from reading the code next to the measured lock waits; no run isolated it**
(for example by spreading the permit state across rows), and the wait-event samples do not name the rows.

## What this does not show

- **Beyond 8 workers.** Not measured. The signals to watch there are efficiency, the `Lock/*` waits and the `acquire_request_permit` p95; a bigger node would be needed to tell lock limits from CPU limits.
- **An earlier plateau that did not recur.** A validation run before this one (same kernel, 800 pages) held at about 9.5 pages/s from 2 workers on, with every role well under one core.
  This run, on the same kernel image, did not show it, and neither did a later validation run on another runner image. I have no explanation. Treat these numbers as one clean
  measurement of one runner, and repeat the dispatch before relying on them.
- **Diagnostic runs are short.** One run per worker count, 1000 pages; at 8 workers the Postgres sampler took 34 samples. Read them as indications.
- **A real multi-origin crawl for contrast.** Not run; the origins here are synthetic with a fixed 100 ms latency and no bandwidth cost.
- **Lane sizes.** `--db-lanes` and `--completion-lanes` are wired through the dispatch inputs (`db_lanes`, `completion_lanes`) but no run used them: pool wait was too small to make lanes the suspect.
- **The HPA under load.** The smoke job checks that the HPA reports `ScalingActive`; no crawl was scaled by it.
- **One node.** Pod-to-pod traffic does not cross a real network.

## Reproduce

```bash
# CI: Actions > Kubernetes (kind) > Run workflow: benchmark on, workers "1 2 4 8", repetitions 3, pages 3000, diagnostics on.
# Locally (kind, helm, kubectl and Docker installed; `make kind-up` builds the image):
kind create cluster --config deploy/kind/cluster.yaml && docker build -t scout:kind . && kind load docker-image scout:kind --name scout
python3 scripts/benchmark-distributed-kubernetes.py --output benchmark-results --workers 1 2 4 8 --repetitions 3 --pages 3000 --diagnostics
python3 scripts/analyze-distributed-scaling.py benchmark-results/results.json --markdown --svg chart.svg
```

## Appendix: diagnostic runs

#### Diagnostic runs (RPC timing on, so slower than the headline runs)

| workers | pages/s | frontier pending (mean / min) | frontier leased (mean) | Postgres commits/s | active backends (mean) | idle in transaction (mean) |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | 7.6 | 1,031 / 32 | 0.9 | 795 | 0.13 | 0.06 |
| 2 | 14.4 | 1,032 / 50 | 1.8 | 1,199 | 0.26 | 0.05 |
| 4 | 26.5 | 1,015 / 32 | 3.4 | 1,950 | 0.59 | 0.34 |
| 8 | 44.0 | 1,013 / 58 | 7.3 | 2,393 | 1.79 | 0.62 |

1 workers, Postgres backends by state and wait event (mean per sample): `active:Client/ClientRead` 0.03, `active:IO/WalSync` 0.00, `active:cpu` 0.10, `idle in transaction:Client/ClientRead` 0.00, `idle in transaction:IO/WalWrite` 0.00

2 workers, Postgres backends by state and wait event (mean per sample): `active:Client/ClientRead` 0.05, `active:IO/WalSync` 0.04, `active:LWLock/WALWrite` 0.01, `active:Lock/transactionid` 0.01, `active:cpu` 0.16

4 workers, Postgres backends by state and wait event (mean per sample): `active:Client/ClientRead` 0.02, `active:IO/WalSync` 0.03, `active:Lock/transactionid` 0.14, `active:cpu` 0.40, `idle in transaction:Client/ClientRead` 0.09

8 workers, Postgres backends by state and wait event (mean per sample): `active:Client/ClientRead` 0.03, `active:IO/WalSync` 0.03, `active:Lock/transactionid` 0.53, `active:Lock/tuple` 0.44, `active:Timeout/VacuumDelay` 0.03

1 workers, coordinator RPCs by occupancy (service time per second of window; 1.0 is one core-equivalent):

| RPC | calls/s | service p50 | service p95 | server total p95 | core-lock wait p95 | DB-pool wait p95 | occupancy |
|--|--:|--:|--:|--:|--:|--:|--:|
| dispatch | 64.1 | 1.72 ms | 5.33 ms | 5.36 ms | 0.00 ms | 0.000 ms | 0.16 |
| acquire_request_permit | 7.9 | 4.20 ms | 6.80 ms | 6.94 ms | 0.00 ms | 0.000 ms | 0.04 |
| robots_decision | 15.5 | 1.75 ms | 3.20 ms | 3.24 ms | 0.00 ms | 0.000 ms | 0.03 |
| release_request_permit | 7.9 | 1.33 ms | 2.09 ms | 2.13 ms | 0.00 ms | 0.000 ms | 0.01 |
| acquire_request_permit_lease_check | 7.9 | 0.29 ms | 0.62 ms | 0.78 ms | 0.00 ms | 0.000 ms | 0.00 |
| complete_task | 7.6 | 0.05 ms | 0.08 ms | 12.43 ms | 0.00 ms | 0.000 ms | 0.00 |

2 workers, coordinator RPCs by occupancy (service time per second of window; 1.0 is one core-equivalent):

| RPC | calls/s | service p50 | service p95 | server total p95 | core-lock wait p95 | DB-pool wait p95 | occupancy |
|--|--:|--:|--:|--:|--:|--:|--:|
| dispatch | 92.1 | 1.77 ms | 6.41 ms | 6.43 ms | 0.00 ms | 0.000 ms | 0.25 |
| acquire_request_permit | 14.9 | 5.07 ms | 10.11 ms | 10.22 ms | 0.00 ms | 0.000 ms | 0.09 |
| robots_decision | 29.3 | 1.85 ms | 4.23 ms | 4.30 ms | 0.00 ms | 0.000 ms | 0.07 |
| release_request_permit | 14.9 | 1.28 ms | 3.60 ms | 3.77 ms | 0.00 ms | 0.000 ms | 0.03 |
| acquire_request_permit_lease_check | 14.9 | 0.35 ms | 0.83 ms | 0.98 ms | 0.00 ms | 0.000 ms | 0.01 |
| commit_robots_refresh | 0.5 | 1.76 ms | 13.80 ms | 13.82 ms | 0.00 ms | 0.000 ms | 0.00 |

4 workers, coordinator RPCs by occupancy (service time per second of window; 1.0 is one core-equivalent):

| RPC | calls/s | service p50 | service p95 | server total p95 | core-lock wait p95 | DB-pool wait p95 | occupancy |
|--|--:|--:|--:|--:|--:|--:|--:|
| dispatch | 142.1 | 1.86 ms | 8.70 ms | 8.73 ms | 0.00 ms | 0.000 ms | 0.44 |
| acquire_request_permit | 27.3 | 5.69 ms | 14.24 ms | 14.45 ms | 0.00 ms | 0.000 ms | 0.19 |
| robots_decision | 53.7 | 2.05 ms | 6.40 ms | 6.52 ms | 0.00 ms | 0.000 ms | 0.15 |
| release_request_permit | 27.3 | 1.34 ms | 6.49 ms | 6.52 ms | 0.00 ms | 0.000 ms | 0.06 |
| acquire_request_permit_lease_check | 27.3 | 0.40 ms | 1.36 ms | 1.75 ms | 0.00 ms | 0.000 ms | 0.02 |
| commit_robots_refresh | 0.8 | 2.14 ms | 5.97 ms | 7.91 ms | 0.00 ms | 0.705 ms | 0.00 |

8 workers, coordinator RPCs by occupancy (service time per second of window; 1.0 is one core-equivalent):

| RPC | calls/s | service p50 | service p95 | server total p95 | core-lock wait p95 | DB-pool wait p95 | occupancy |
|--|--:|--:|--:|--:|--:|--:|--:|
| dispatch | 154.5 | 2.17 ms | 16.49 ms | 16.89 ms | 0.00 ms | 0.241 ms | 0.69 |
| acquire_request_permit | 45.1 | 8.65 ms | 21.90 ms | 23.91 ms | 0.00 ms | 3.394 ms | 0.46 |
| robots_decision | 88.9 | 2.59 ms | 7.54 ms | 8.93 ms | 0.00 ms | 3.109 ms | 0.30 |
| release_request_permit | 45.2 | 1.99 ms | 11.52 ms | 12.65 ms | 0.00 ms | 0.612 ms | 0.16 |
| acquire_request_permit_lease_check | 45.0 | 0.57 ms | 2.14 ms | 3.85 ms | 0.00 ms | 2.432 ms | 0.04 |
| commit_robots_refresh | 1.3 | 2.72 ms | 6.49 ms | 8.68 ms | 0.00 ms | 3.940 ms | 0.00 |
