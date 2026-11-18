# Runbook: crawl failures

What each failure looks like, how to confirm it, and what to do. Every scenario below is
injected by the failure-injection suite (`scripts/chaos/run.sh`, issue #10): the counter
changes, log events and recovery times below were observed in those runs. The dashboard panels
are where those counters are plotted; the suite reads the metrics, not Grafana. The design they rely on (leases, fencing,
idempotent results) is in [design.md](design.md).

The short version: none of the failures below needs a manual step on Compose. Leases expire
and are reclaimed, late writes from a lost lease are fenced, and the coordinator and workers
restart on failure (`restart: on-failure:10` in `compose.yaml`). Intervene only when a
recovery below does not happen in the time given, or when the cause will recur.

## Where to look

**Dashboard.** Grafana "Scout overview" (`docker compose --profile obs up`, then
http://127.0.0.1:3000). The rows used here:

| Panel (row) | Query | Normal crawl |
| --- | --- | --- |
| Tasks pending / Tasks leased (Overview) | `scout_frontier_tasks{status=...}` | leased stays near the worker count |
| Lease expirations (Overview, Leases and fencing) | `scout_lease_expirations_total` | 0 |
| Stale results fenced (Overview); Stale-result (fencing) rejections / sec (Leases and fencing) | `scout_stale_results_total{rpc}` | 0 |
| Lease renewals / sec (Leases and fencing) | `scout_lease_renewals_total{outcome}` | only `renewed` |
| Task claims and completions / sec (Throughput) | `scout_task_claims_total`, `scout_task_completions_total` | steady |
| Frontier tasks by status (Queue depth) | `scout_frontier_tasks` | pending falls, succeeded rises |
| DB pool wait (Database) | `scout_db_pool_wait_seconds` | milliseconds |
| Scrape targets up (Overview) | `up` | one per process |

The coordinator's counters start from zero when it restarts; the tiles use `max_over_time`, so
after a restart read the time series rather than the tile.

**Logs.** Every process writes JSON events to stderr (`docker compose logs coordinator worker`);
the coordinator also keeps the run's events in `/data/logs/<run-id>.jsonl`. Events this page uses:

| Event | Process | Means |
| --- | --- | --- |
| `worker.task_stale` | worker | the coordinator fenced one of this worker's calls; the worker dropped the task and moved on |
| `worker.task_released` | worker | a stopping worker handed a lease back (graceful drain) |
| `worker.registered`, `worker.started` | coordinator, worker | a worker (re)joined; each start gets a new worker ID |
| `coordinator.resumed` | coordinator | a restarted coordinator picked up an existing run |
| `crawl.completed` | coordinator | the run finished |
| `scout crawl failed: storage_failed: ... transport error` | worker (plain text) | the worker lost the coordinator and exited non-zero; it restarts |

**Frontier.** The truth is in Postgres. From the Compose host:

```bash
psql() { docker compose exec -T postgres psql -U scout -d scout "$@"; }
# Tasks by status, and leases already past their deadline (claimable again).
psql -c "SELECT status, count(*), count(*) FILTER (WHERE status = 'leased' AND lease_deadline < clock_timestamp()) AS expired
         FROM scout_crawl_tasks WHERE crawl_run_id = '<run-id>' GROUP BY status"
# Tasks that changed hands, and who holds what now.
psql -c "SELECT task_id, status, lease_owner, lease_epoch, task_attempt_count, last_error
         FROM scout_crawl_tasks WHERE crawl_run_id = '<run-id>' AND (lease_epoch > 1 OR status = 'leased')
         ORDER BY task_id"
# Run status: active, completed or failed.
psql -c "SELECT crawl_run_id, status FROM scout_crawl_runs"
```

## Failures

Recovery times are from the suite (160 pages, 3 workers, 2 s request timeout so a lease is 4 s
and is renewed every ~1.3 s). With the default timeout (30 s, lease 60 s) every time that waits
for a lease to expire is about 15 times longer. See [Measured recovery](#measured-recovery).

### Worker crash mid-lease

A worker process dies while it holds a lease: OOM kill, node loss, `kill -9`.

- **Symptoms.** *Lease expirations* goes up by the number of leases the worker held, about one
  lease after the crash, when another worker reclaims them. Until then the task still counts as
  leased (the `scout_frontier_expired_leases` gauge, not on the dashboard, counts it as expired).
  No error appears in the worker's own log (it never ran again); `docker compose ps -a` shows a
  restart.
- **Diagnosis.** The frontier query shows the dead worker's ID as `lease_owner` with a deadline in
  the past, then the same task leased by another worker at `lease_epoch + 1`.
- **Action.** None. Another worker reclaims the task when the lease expires, and the container
  restarts with a new worker ID. If a worker keeps dying, find out why (OOM: `docker inspect -f
  '{{.State.OOMKilled}}'`); each lost lease costs a task attempt, and a task whose last attempt
  lapses is marked `failed`.
- **Suite asserts.** The killed worker held a lease at the moment it died (it is frozen with
  `docker pause` while the frontier is read, then killed); that task is later accepted from
  another worker at a higher epoch; `scout_lease_expirations_total >= 1`; the container
  restarted; every page captured once; snapshot equal to the baseline.

### Stale worker (stalled past its lease)

A worker stops renewing but is not dead: a long GC-like stall, a frozen container, a slow disk.
When it wakes up it reports a result for a lease that has already expired or moved on.

- **Symptoms.** *Lease expirations* +1 when its lease lapses; later *Stale results fenced* +1 with
  `rpc="complete_task"` (or `renew_lease` / `fail_task`, depending on which call arrives late).
  The worker logs `worker.task_stale` with the task ID and its old `lease_epoch`, then carries on
  with new tasks.
- **Diagnosis.** In the frontier, the task's `lease_epoch` is higher than the one in the
  `worker.task_stale` event, and its result row (`scout_crawl_task_results`) names another worker.
- **Action.** None for correctness: fencing threw the late write away. If stale results are
  frequent, leases are too short for the work: raise `request_timeout_seconds` (the lease is
  twice it), or find what stalls the workers.
- **Suite asserts.** A worker started with `SCOUT_FAULT_STALL_FIRST_LEASE_SECS=10` (a test hook:
  it holds its first result for 10 s without renewing) has its report rejected:
  `scout_stale_results_total{rpc="complete_task"} >= 1`, a `worker.task_stale` event for that
  task, and the task's accepted result comes from a different worker at a higher epoch.

### Coordinator crash

The coordinator process dies mid-crawl.

- **Symptoms.** *Scrape targets up* loses the coordinator; every panel fed by it has a gap, and
  its counters restart from zero. Workers log `scout crawl failed: storage_failed: ...
  transport error` and exit non-zero as their next call fails; Compose restarts them and they wait
  for the coordinator's port. The restarted coordinator logs `coordinator.resumed`. *Lease
  expirations* then counts the leases the old workers held, and a few `worker.task_stale` appear
  from workers whose call reached the new coordinator with a lease it does not recognise.
- **Diagnosis.** `docker compose ps -a` shows the coordinator's restart; `SELECT status FROM
  scout_crawl_runs` stays `active`. If the coordinator does not come back, its log says why (a
  failed spool replay marks the run `failed`).
- **Action.** None. The coordinator resumes the run from Postgres and the capture spool on the
  data volume, and workers rejoin. A run marked `failed` does not resume: crawl again with a new
  `SCOUT_CRAWL_RUN_ID`.
- **Suite asserts.** The coordinator restarted and logged `coordinator.resumed`; the crawl
  completed with every page captured once and the snapshot equal to the baseline.

### Postgres paused or unresponsive

Postgres stops answering without dropping connections (`docker pause`, a stuck disk, a
saturated host).

- **Symptoms.** Every coordinator RPC that needs the database waits; *Task claims and
  completions / sec* drops to zero; the frontier gauges stop changing (their sampler waits too).
  Nothing fails while it lasts: there are no database timeouts, so nothing logs an error. When
  Postgres answers again, the leases whose deadline passed meanwhile are dead: *Lease
  expirations* and *Stale results fenced* jump (`renew_lease`, `complete_task`), and workers log
  `worker.task_stale`.
- **Diagnosis.** `docker compose ps` shows Postgres `Paused` or unhealthy;
  `docker compose exec postgres pg_isready` hangs.
- **Action.** Fix Postgres; the crawl continues on its own. Lease deadlines use Postgres's clock,
  which kept running, so every lease held across a pause longer than the lease is lost and its
  task runs again.
- **Suite asserts.** After a 10 s pause, the crawl completed with every page captured once and the
  snapshot equal to the baseline; the recovery time is from unpause to the next accepted page.

### Slow network between workers and coordinator

Added latency on every worker-coordinator call.

- **Symptoms.** *Task claims and completions / sec* falls; nothing else changes. Lease
  expirations and stale results stay at zero while the round trip stays well inside the renewal
  interval (a third of the lease).
- **Diagnosis.** Throughput down with no lease or fencing activity and no worker restarts.
- **Action.** None for correctness. If the round trip approaches the renewal interval, leases
  start to lapse (see the stale worker entry); raise `request_timeout_seconds`.
- **Suite asserts.** With 100 ms +-20 ms each way (Toxiproxy) for the whole crawl: zero lease
  expirations, zero stale results, every page captured once, snapshot equal to the baseline.

### Network partition between workers and coordinator

Workers cannot reach the coordinator at all.

- **Symptoms.** Workers log `transport error` and exit non-zero; Compose restarts them and they
  wait for the coordinator's port. Claims and completions stop. After the partition heals the
  workers register again (new worker IDs) and *Lease expirations* counts the leases they held
  before.
- **Diagnosis.** Workers restarting with `transport error` while the coordinator is up and logging
  nothing new.
- **Action.** Fix the network. Recovery is automatic once workers can connect.
- **Suite asserts.** Toxiproxy cuts the link for 10 s: workers restarted, the crawl completed with
  every page captured once and the snapshot equal to the baseline; the recovery time is from the
  heal to the next accepted page.

## When recovery does not happen

- **A task is `failed`.** Each lease lost to a fault used one attempt (`max_attempts`, default 4).
  Repeated faults on the same task exhaust it; `last_error` says why. The run still completes
  without that page.
- **A container stays down.** `restart: on-failure:10` gives up after ten failed starts in a row;
  `docker compose logs <service>` has the reason. A clean exit (0) is never restarted: that is the
  crawl finishing.
- **The run is `failed`.** The coordinator marks it so when stored captures fail verification on
  restart. It cannot resume; crawl again with a new run ID.

## Measured recovery

Recovery is the time from the fault (or its end, for faults with a length) until the behaviour the
entry names: the dead or stalled worker's lease reclaimed by another worker, or the first new page
accepted.

| Failure | Recovery (CI) | Recovery (local) | Crawl (CI; clean: 29.8 s) | Leases reclaimed | Stale results fenced |
| --- | --- | --- | --- | --- | --- |
| Worker crash mid-lease | 4.2 s (lease reclaimed) | 3.8-4.3 s | 29.3 s | 1 | 0 |
| Stale worker, 10 s stall | 3.7 s (lease reclaimed) | 3.6-3.7 s | 26.5 s (one extra worker) | 1 | 1 |
| Coordinator crash | 8.4 s (first new page) | 7.1-7.4 s | 37.2 s | 3 | 5 |
| Postgres paused 10 s | 0.2 s after unpause | 0.2-0.9 s | 39.8 s | 3 | 6 |
| Latency 100 ms +-20 ms each way | none needed: no lease lost | not run | 97.4 s | 0 | 0 |
| Partition 10 s | 0.8 s after heal | not run | 40.3 s | 3 | 0 |

CI: the [Chaos workflow on PR #27](https://github.com/lifrocszh/scout/actions/runs/44253796650),
GitHub `ubuntu-24.04` runner, 2026-11-18; all seven scenarios took 7 min 13 s after a 1.5 min
image build. Local: Docker 29 on a Linux VM, two runs each; the network scenarios were not run
locally. Lease-bound recoveries track the 4 s lease; the counts of reclaimed leases and fenced
results depend on what was in flight when the fault hit, so expect them to vary between runs.
The [run report](runs/reports/chaos-suite-2026-11-18.md) has the details.

## Running the suite

```bash
scripts/chaos/run.sh            # all seven scenarios, ~7 min after the image is built
scripts/chaos/run.sh fast       # baseline, worker-kill, stale-lease
scripts/chaos/run.sh coordinator-kill postgres-pause
```

It needs Docker with Compose, `jq`, `curl`, and root or passwordless `sudo` (a crash is a
`kill -9` of the container's init from the host: `docker kill` counts as a manual stop, which
Docker's restart policy ignores). Results go to `target/chaos/` (`results.jsonl`, `summary.md`,
and per scenario the compose logs, coordinator metrics and snapshot pages). CI runs the fast set
on pull requests that touch the crawl pipeline or the stack, and every scenario nightly
(`.github/workflows/chaos.yml`).

On Kubernetes the same recoveries rely on the Deployment restarting pods; the suite does not run
there.
