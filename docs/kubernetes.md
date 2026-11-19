# Scout on Kubernetes

The Helm chart in [`deploy/helm/scout`](../deploy/helm/scout) runs the coordinator, crawl workers, the
search API and Postgres on any Kubernetes 1.27+ cluster. `make kind-up` does it on a local
[kind](https://kind.sigs.k8s.io) cluster: free, no cloud account, and small enough for a laptop or a
standard GitHub runner. Only OSI-licensed software is involved (Helm, kind, kubectl, Postgres,
busybox, Python; no Terraform, no Bitnami charts, no KEDA).

```bash
make kind-up      # cluster + image + chart; returns when http://127.0.0.1:8080/v1/readyz is ready
make kind-smoke   # helm test, searches, scale workers 1 -> 8 mid-crawl, delete a worker mid-crawl
make kind-down
```

Needs Docker, `kind`, `kubectl` and `helm`. `BUILD_IMAGE=0 IMAGE_REPO=... IMAGE_TAG=...` skips the local
image build.

## What runs

| Workload | Kind | Notes |
| --- | --- | --- |
| `scout-postgres` | StatefulSet (1) + headless Service | Official `postgres:17` image, PVC for the data |
| `scout-coordinator` | Deployment (1, `Recreate`) + ClusterIP Service `:50051` | gRPC for the workers; the only pod with database credentials (with `kafka.enabled=true`, the Kafka relay and the index Job get them too) |
| `scout-worker` | Deployment (3) + PodDisruptionBudget, optional HPA | Stateless; scale with `kubectl scale` |
| `scout-index-<hash>` | Job | Waits for the crawl, then materializes it, builds a keyword-only Generation, activates and verifies it |
| `scout-serve` | Deployment (1) + Service `:8080` | The search API; ready once a Generation is active, and picks up new ones live |
| `scout-fixture` | Deployment (1) + Service | The site the default crawl fetches; `fixture.enabled=false` drops it |
| `scout-config` | ConfigMap | `sources.toml`, the index profile and the wrapper scripts |
| `scout-kafka` | StatefulSet (1) + headless Service `:9092` | Only with `kafka.enabled=true`: official `apache/kafka` image, KRaft, PVC for the log |
| `scout-kafka-relay` | Deployment (1, `Recreate`) | Only with `kafka.enabled=true`: copies the run's outbox into Kafka, then parks; on the coordinator's node (it reads bodies from the data PVC) |
| `scout-kafka-reducer` | Deployment (1) + PVC | Only with `kafka.enabled=true`: rebuilds the snapshot from Kafka alone and checks it against the live one, then parks |

Both the coordinator and `serve` have no authentication or TLS and the Services are ClusterIP: put
an authenticating ingress in front of `serve`, and never expose the coordinator.

### Why the official Postgres image and not CloudNativePG

A single StatefulSet is about 100 lines of the chart, starts in seconds, and installs nothing else.
CloudNativePG (Apache-2.0) is the better answer once you need replicas, failover and backups, but it
is an operator: CRDs, a controller Deployment and its own release to install and upgrade before the
first database pod exists, which doubles the moving parts of a kind smoke test that has to fit on one
runner. Scout's frontier is the only state in Postgres and the chart does not claim HA; point the
coordinator at a managed or operator-run Postgres by replacing the `postgres` workload and the
`scout.databaseEnv` helper.

## The crawl lifecycle

A crawl is one coordinator run, named by `crawl.runId`. The coordinator process has to exit when the
crawl completes: its last event, `crawl.concurrency`, is written at shutdown, and `scout index` refuses
a crawl log that does not end with it. A Deployment restarts a container that exits, so the pod runs
`files/coordinator.sh`, which runs `docker/stack.sh coordinator` (the wrapper Compose uses: run the
coordinator, stop it after `crawl.completed`) and then **parks**. A parked coordinator pod stays
Ready and answers its probes from a marker file; a restart of a finished crawl parks again without
re-running it. Workers that find the crawl finished idle (`SCOUT_WORKER_IDLE_WHEN_DONE`) instead of
exiting, so nothing restarts in a loop; with the coordinator stopped they report unready, and the
next crawl replaces them (their pod template carries the run ID).

To crawl again, set a new run ID: `helm upgrade ... --set crawl.runId=run-$(openssl rand -hex 32)`.
Changing the run ID restarts the coordinator and the workers; the index Job is renamed with its inputs,
so a new crawl gets a new Job.

The coordinator, the index Job and `serve` share one `ReadWriteOnce` volume, so `serve` and the Job are
scheduled onto the coordinator's node. For several nodes use a `ReadWriteMany` storage class
(`persistence.accessMode`).

## Probes, resources, disruption

| | Liveness | Readiness |
| --- | --- | --- |
| serve | `GET /v1/healthz` | `GET /v1/readyz` (503 until a Generation is active) |
| worker | `GET :9102/healthz` | `GET :9102/readyz`: 200 while the coordinator answers a heartbeat |
| coordinator | `GET :9101/healthz` | `GET :9101/readyz`: 200 once it serves RPCs (probes run in-pod, so they work when parked) |
| postgres | `pg_isready` | `pg_isready` |

`/healthz` and `/readyz` are served by the same listener as `/metrics` (`--metrics-bind`). Every pod has
requests and memory limits (CPU requests set what the HPA scales against), runs as the image's non-root
user with a read-only root filesystem, and carries Prometheus scrape annotations.

**Graceful stop.** Kubernetes sends SIGTERM, which the coordinator, workers and `serve` now handle
(as SIGINT already was). A worker turns unready, finishes its in-flight task for up to
`SCOUT_WORKER_DRAIN_SECS` (`worker.drainSeconds`, default 20), and if the task is still running it
hands the lease back (`fail_task`, so another worker claims it at once instead of waiting for the
lease to expire) before exiting 0. The pod's `terminationGracePeriodSeconds` is that plus 15. The
`PodDisruptionBudget` (`maxUnavailable: 1`) keeps a node drain from taking the pool down at once.

## Scaling workers

```bash
kubectl -n scout scale deploy/scout-worker --replicas=8   # by hand: new workers claim tasks as soon as they are ready
```

Or let the HPA do it on CPU: `make kind-metrics-server`, then
`helm upgrade ... --set worker.autoscaling.enabled=true`. Scaling in stops workers mid-task, which the
drain above handles; the HPA waits two minutes before removing one. Measured throughput against worker
count (8.0 pages/s with 1 worker, 51.3 with 8, on a 4-vCPU kind node) is in
[`runs/reports/kubernetes-scaling-2026-11-18.md`](runs/reports/kubernetes-scaling-2026-11-18.md).

## Kafka ingestion log

Off by default. `--set kafka.enabled=true` adds a single-node Kafka (KRaft, no ZooKeeper), the
outbox relay and the reducer ([ADR 0003](adr/0003-kafka-ingestion-log.md)); nothing else in the
chart changes or waits for them. The relay runs `docker/stack.sh kafka-relay` and parks once the
run's published snapshot is in Kafka. The reducer runs `docker/stack.sh kafka-reduce` on its own
PVC and parks once its replayed snapshot equals the live one; a mismatch exits non-zero, so the
pod restarts and shows as failing. `kafka.reducer.recordDelayMs` slows it down on purpose to watch
its lag in Grafana. The broker has one replica and no TLS or authentication, so keep it on the
cluster network. The chart's kind smoke test does not enable it; `make helm-lint` renders and
schema-validates it, and the Compose `kafka` profile runs the same roles end to end in CI.

## Metrics

Pods carry `prometheus.io/scrape` annotations, and
[`deploy/prometheus/prometheus-k8s.yml`](../deploy/prometheus/prometheus-k8s.yml) turns them into
scrape jobs named `scout-coordinator`, `scout-worker` and `scout-serve` (plus `scout-kafka-relay`
and `scout-kafka-reducer` with `kafka.enabled=true`) with plain Kubernetes service
discovery, so the Grafana dashboard in `deploy/grafana` works unchanged and no Prometheus Operator is
needed. A finished coordinator stops serving `/metrics` (`coordinator.metricsLingerSeconds` keeps it
up for a last scrape).

The alert rules in [`deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml) (what each means
and what to do: [runbook](runbook.md#alerts)) work on those job names. `prometheus-k8s.yml` loads them
with `rule_files: [alerts.yml]`, so Prometheus needs both files in one directory, for example a
ConfigMap mounted at `/etc/prometheus`:

```bash
kubectl -n monitoring create configmap prometheus-config \
  --from-file=prometheus.yml=deploy/prometheus/prometheus-k8s.yml \
  --from-file=alerts.yml=deploy/prometheus/alerts.yml
```

One number depends on your release: `ScoutWorkersBelowExpected` compares with 3 workers, the chart's
default `worker.replicas`; change it in `alerts.yml` if you scale differently.

## CI

`ci.yml` lints and schema-validates the chart on every pull request (`make helm-lint`).
`.github/workflows/e2e-kind.yml` creates a kind cluster, installs the chart and runs
`scripts/ci/kind-smoke.sh` on pull requests that touch `deploy/`, `docker/`, the Dockerfile or the
workflow, and nightly; run manually it also runs the scaling benchmark and uploads its results. The smoke
test also deletes a worker pod in the middle of a crawl and checks that nothing is lost
([runbook](runbook.md#worker-pod-deleted-mid-crawl)).
