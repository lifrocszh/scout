# Runbook: crawl failures

What each failure looks like, how to confirm it, and what to do. Every scenario below is
injected by the failure-injection suite (`scripts/chaos/run.sh`, issue #10): the counter
changes, log events and recovery times below were observed in those runs. The dashboard panels
are where those counters are plotted; the suite reads the metrics, not Grafana. The design they rely on (leases, fencing,
idempotent results) is in [design.md](design.md). The Prometheus alerts for them are under [Alerts](#alerts).

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

- **A task is dead-lettered.** Each lease lost to a fault used one attempt (`max_attempts`,
  default 4). Repeated faults on the same task exhaust it: it becomes `failed` with
  `dead_lettered_at` set, and `last_error` says why. The run still completes without that page.
  After fixing the cause, `scout dlq list --run-id <run>` shows them and
  `scout dlq requeue --run-id <run> --all` puts them back (not exercised by the suite).
- **A container stays down.** `restart: on-failure:10` gives up after ten failed starts in a row;
  `docker compose logs <service>` has the reason. A clean exit (0) is never restarted: that is the
  crawl finishing.
- **The run is `failed`.** The coordinator marks it so when stored captures fail verification on
  restart. It cannot resume; crawl again with a new run ID.

## Alerts

[`deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml) holds the Prometheus alert rules. The
Compose `obs` profile loads it (they show on Prometheus's Alerts page, http://127.0.0.1:9090/alerts);
for Kubernetes mount it next to `prometheus-k8s.yml` ([kubernetes.md](kubernetes.md#metrics)). Scout
ships no Alertmanager: add an `alerting:` block to the Prometheus config to be notified. The rules use
only the metrics listed in the README and the `up` series of the scrape jobs.

| Alert | Severity | Fires when |
| --- | --- | --- |
| [ScoutCrawlStalled](#scoutcrawlstalled) | critical | tasks are pending or leased, but no result was accepted for 10 minutes |
| [ScoutDeadLetterQueueGrowing](#scoutdeadletterqueuegrowing) | warning | the dead-letter queue grew in the last 15 minutes |
| [ScoutWorkersBelowExpected](#scoutworkersbelowexpected) | warning | fewer than 3 workers answer their scrape for 5 minutes during a crawl |
| [ScoutWorkerTargetDown](#scoutworkertargetdown) | warning | a worker target does not answer its scrape for 5 minutes during a crawl |
| [ScoutCoordinatorDown](#scoutcoordinatordown) | critical | the coordinator does not answer its scrape for 2 minutes |
| [ScoutServeDown](#scoutservedown) | critical | the search API, scraped successfully within the last day, does not answer for 2 minutes |
| [ScoutSearchErrorRateHigh](#scoutsearcherrorratehigh) | warning | over 5% of a mode's searches fail for 5 minutes (at least 20 searches in 5 minutes) |
| [ScoutKafkaRelayLagHigh](#scoutkafkarelaylaghigh) | warning | over 1000 outbox events wait for the Kafka relay for 10 minutes (Kafka only) |

"During a crawl" means the coordinator reports pending or leased tasks, so a finished crawl is
silent. Two numbers are specific to a deployment and marked `EDIT` in the file: the expected worker
count (3, the default of `--scale worker=3` and `worker.replicas`) and the relay lag. Edit the file and
restart Prometheus (`docker compose --profile obs restart prometheus`). The unit tests in
`alerts_test.yml` run with `scripts/ci/prometheus-rules.sh` (Docker, or `PROMTOOL=<binary>`), and in
CI; the Compose smoke test checks that Prometheus loads and evaluates every rule.

### ScoutCrawlStalled

Work is waiting, and nothing is finishing. The causes are the failures above, seen from the outside.

- **Check.** The *Scrape targets up* panel (or `up`) for a missing coordinator or workers; then
  the frontier query under [Where to look](#where-to-look): are tasks `leased` with expired deadlines
  (no live worker), or only `pending`? `rate(scout_task_claims_total[5m])` at zero means workers are not
  claiming (no workers, coordinator or Postgres unreachable); claims without completions mean tasks
  fail or run slowly (`scout_task_failures_total`, `scout_pages_fetched_total{outcome!="success"}`).
  `scout_origin_backoff_milliseconds` and `scout_origin_throttle_responses_total` high for the
  Source's origin means the site is slowing the crawl down on purpose.
- **Do.** No worker: [ScoutWorkersBelowExpected](#scoutworkersbelowexpected). The coordinator is gone:
  [Coordinator crash](#coordinator-crash). Claims stopped but both are up:
  [Postgres paused or unresponsive](#postgres-paused-or-unresponsive) or
  [Network partition](#network-partition-between-workers-and-coordinator). A throttled origin: raise
  `min_start_spacing_ms` or lower the origin's concurrency in `sources.toml` (see
  [crawl-behaviour.md](crawl-behaviour.md)); the crawl is slow, not broken.

### ScoutDeadLetterQueueGrowing

Tasks used up their attempts (`max_attempts`, default 4) and became `failed`; the run completes without
those pages. The alert resolves 15 minutes after the last one, even if the queue is not empty.

- **Check.** `scout dlq list --run-id <run>` and each task's `last_error`; the *Dead-letter queue* panel is
  `scout_frontier_tasks{status="failed"}`. A burst of lease expirations (*Lease expirations*) before it
  means workers or Postgres were struggling; a steady trickle with HTTP errors in `last_error` means
  the pages themselves fail.
- **Do.** Fix the cause, then `scout dlq requeue --run-id <run> --all` ([When recovery does not
  happen](#when-recovery-does-not-happen)). Requeueing pages that fail for their own reasons (a 404,
  a robots-disallowed path) only dead-letters them again.

### ScoutWorkersBelowExpected

Fewer workers answer their scrape than the 3 written in the rule, while there is work. The crawl still
completes with fewer workers, only slower.

- **Check.** Compose: `docker compose ps -a worker`; a worker that exited non-zero is restarted ten
  times, then left down (see [When recovery does not happen](#when-recovery-does-not-happen)).
  Kubernetes: `kubectl -n scout get pods -l app.kubernetes.io/component=worker` (Pending: no room on
  the nodes; CrashLoopBackOff or OOMKilled: `kubectl logs` and `describe pod`). A worker that exits
  with `transport error` lost the coordinator ([Coordinator crash](#coordinator-crash)).
- **Do.** Fix why they are down. If the smaller number is intended (a different `--scale`, `worker.replicas`,
  or the autoscaler's `minReplicas`), change the `3` in `alerts.yml`.

### ScoutWorkerTargetDown

A worker is a scrape target but does not answer `/metrics`: starting, crash-looping or wedged. It fires per
worker, so the labels name it (`instance`, and `pod` on Kubernetes). A worker that has disappeared from
service discovery is not a target, and only ScoutWorkersBelowExpected sees it.

- **Check.** Its logs; a pod that is Running but not Ready (`kubectl get pods`) has lost its
  coordinator, since a worker is ready while the coordinator answers its heartbeat. Prometheus cannot
  see readiness: Scout exports no readiness metric.
- **Do.** As ScoutWorkersBelowExpected. A rolling update or `kubectl delete pod` shows briefly; it
  stays within the 5 minutes of `for` unless the replacement does not start.

### ScoutCoordinatorDown

Nothing claims or completes tasks while the coordinator is down, and workers exit and restart as
their calls fail.

- **Check.** Compose: `docker compose ps -a coordinator`. `Exited (0)` is a finished crawl, not a
  failure: the coordinator exits when the run completes, so this alert also fires a few minutes after
  every Compose crawl (Kubernetes restarts it). Anything else: `docker compose logs coordinator`,
  `kubectl -n scout logs deploy/scout-coordinator --previous`, and `SELECT status FROM
  scout_crawl_runs` (`active` resumes, `failed` does not).
- **Do.** [Coordinator crash](#coordinator-crash). A deleted coordinator pod stops being a scrape
  target instead of going down, so this alert does not see it; ScoutCrawlStalled does.

### ScoutServeDown

`scout serve` answered scrapes within the last day and stopped; searches fail.

- **Check.** `curl http://127.0.0.1:8080/v1/readyz` (Compose; `503 not_ready` means no valid active
  Generation, not a dead process) and `docker compose logs serve`. `scout index verify --data-dir
  <dir>` checks the Generation on disk.
- **Do.** Restart the process (Compose restarts it unless stopped). If it starts but reports not ready,
  rebuild or re-activate a Generation (`scout index recover`, then `scout index build` and `activate`). The
  alert stays silent for a `serve` that was never up in the last day, which is every Compose stack until
  the crawl and the index build finish: check `docker compose ps` for that case.

### ScoutSearchErrorRateHigh

Searches that ran against the active Generation returned an error: `scout_search_requests_total{outcome="error"}`.
The label `mode` says which. Malformed requests (HTTP 400) are not counted, and neither is text with
Tantivy syntax in it: keyword queries are plain text and cannot be a syntax error.

- **Check.** `sum by (mode, outcome) (rate(scout_search_requests_total[5m]))` for the mode, then repeat
  a search in it: `curl -X POST http://127.0.0.1:8080/v1/search -H 'content-type: application/json' -d
  '{"query":"test","mode":"hybrid"}'`. `error.code` and `error.message` in the response say why (the
  per-request `search.completed` events in `<data-dir>/logs` hold only the status). The usual cause is
  a mode the Generation does not have: `hybrid` (the API's default) or `semantic` on a keyword-only
  index. The next is the embedding model failing to load for `semantic` and `hybrid` (the cache in
  `<data-dir>/models`, or no network on the first query).
- **Do.** Send `"mode":"keyword"` to a keyword-only index, or build a hybrid Generation
  (`scout index build --mode hybrid`) and activate it. For embedding errors, check the model cache.
  Searches with no active Generation are `scout_http_requests_total{route="search",status="503"}`, and
  a server at capacity is `{route="overloaded"}`: neither is counted here.

### ScoutKafkaRelayLagHigh

The relay is more than 1000 events behind the outbox for 10 minutes. Only with the `kafka` profile or
`kafka.enabled=true`; the crawl is not affected. See [Kafka ingestion log](#kafka-ingestion-log-optional):
"Relay lag grows". In short: Kafka is unreachable or a body file is missing, `scout_kafka_relay_failures_total` counts
the retries, and the relay resumes by itself when the broker is back.

## Kafka ingestion log (optional)

Only with the Compose `kafka` profile or the chart's `kafka.enabled=true`
([ADR 0003](adr/0003-kafka-ingestion-log.md)). The crawl never waits for Kafka, so none of these
stops a crawl; they only delay the replica.

- **Relay lag grows** (`scout_kafka_relay_pending_events` rising): Kafka is down or unreachable,
  or a body file is missing. `scout_kafka_relay_failures_total` counts the retries, the relay log
  names the event, and `last_error` in `scout_crawl_event_checkpoints` (consumer `kafka-relay`)
  holds the message. The relay retries the same event with backoff (up to 30 s) and resumes on its
  own once the broker is back; events are never skipped, so order is kept.
- **Consumer lag grows** (`scout_kafka_consumer_lag_records` rising): the reducer is slow or
  stopped. A restart resumes from its recorded offsets. To rebuild from scratch, give it an empty
  `--data-dir` (Compose: `docker compose rm -sf kafka-reducer && docker volume rm scout_kafka-replica`).
- **The reducer exits 1, "differs from the live snapshot"** (`scout_kafka_snapshot_parity` 0):
  the log and the live snapshot disagree. Compare `replica_snapshot_id` and `live_snapshot_id` in
  its JSON summary, then diff `corpora/<id>/pages.jsonl` of both. Treat it as a bug: the live
  snapshot, built from Postgres, is the authority.
- **"Sources config digest differs from the run's"**: the reducer was given another
  `sources.toml` than the crawl used; pass the run's.
- **A crash mid-delivery** needs nothing: the relay sends the unacknowledged event again and the
  reducer skips it by `event_id`; the reducer replays records after its last recorded offset, and
  re-applying one changes nothing (`tests/kafka_log.rs` crashes both and checks the replica).

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

## Kubernetes

The Compose suite does not run on Kubernetes, where a Deployment replaces a pod instead of a restart
policy restarting a container. One failure is injected there, by phase 3 of the kind smoke test
(`scripts/ci/kind-smoke.sh`, `make kind-smoke`; CI runs it as `.github/workflows/e2e-kind.yml` on
pull requests that touch the chart or the script, and nightly). Coordinator and Postgres pod failures,
node loss and `SIGKILL` of a worker are not injected on kind.

### Worker pod deleted mid-crawl

`kubectl delete pod`, a node drain, a rolling update or a scale-in stops a worker that holds a lease.
Unlike a crash, the kubelet sends SIGTERM first, and the worker is expected to leave cleanly.

- **Symptoms.** The pod goes unready and Terminating; the Deployment starts a replacement that is Ready
  after its init containers and a first heartbeat. The worker logs `worker.stopped`
  and exits 0. A task still running after `worker.drainSeconds` (default 20 s) is handed back first:
  `worker.task_released`, and another worker claims it at once (a task that finishes inside the window
  is simply completed). *Lease expirations* does not move and no `worker.task_stale` appears.
- **Diagnosis.** `kubectl -n scout get pods -l app.kubernetes.io/component=worker`. In the frontier, a
  handed-back task has `last_error = 'worker stopped before the task finished'` and `lease_epoch` 2 or
  more (a task that was only abandoned has neither: it waits for its lease to expire):
  ```bash
  kubectl -n scout exec pod/scout-postgres-0 -- psql -U scout -d scout -c \
    "SELECT task_id, status, lease_owner, lease_epoch, last_error FROM scout_crawl_tasks
     WHERE crawl_run_id = '<run-id>' AND lease_epoch > 1 ORDER BY task_id"
  ```
  The logs of a deleted pod go with it: follow them first (`kubectl logs -f <pod>`) or read them from
  your log collector. A pod that stays Terminating for the whole `terminationGracePeriodSeconds`
  (`drainSeconds` + 15) and has no `worker.stopped` did not handle SIGTERM and was killed; its lease is
  reclaimed only when it expires (60 s with the default request timeout).
- **Action.** None. Each hand-back uses one of the task's attempts (`max_attempts`, default 4), so
  repeated deletions of the pod holding the same task can dead-letter it; see
  [When recovery does not happen](#when-recovery-does-not-happen).
- **Smoke test asserts.** Three workers crawl 64 synthetic pages (3 s each). After 12 pages a worker
  that holds a lease is deleted with `drainSeconds=0`, so its task is always handed back rather than
  finished (the finish-inside-the-window case is `tests/worker_drain.rs`). If that worker held nothing at
  the instant of SIGTERM, another is deleted, up to three in all. Then: the deleted pod logged
  `worker.task_released` and `worker.stopped`; the coordinator recorded the release; the Deployment
  brought back three Ready workers, a new one for each deleted; the run completed with every task
  succeeded once (none failed or dead-lettered, one accepted result per task from the epoch that holds
  it); each handed-back task was accepted from a different worker at a higher epoch; and the snapshot
  holds exactly the fixture's 64 pages, each once. Waits poll observable state (pages succeeded, lease
  holder, Ready pods), with generous limits; there are no fixed sleeps.
