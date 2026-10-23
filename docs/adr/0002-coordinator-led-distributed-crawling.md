# Coordinator-led distributed crawling

**Status:** accepted

## Context

Distributed crawls need more fetch capacity than one process may provide, while still enforcing one crawl-wide frontier, request budget, source allowlist, and robots policy. Workers can run as separate processes, but task ownership and outbound request policy must remain consistent across the whole worker fleet. PostgreSQL is available as durable shared state; gRPC is the control channel between workers and the coordinator.

## Decision

Run one coordinator process per crawl run and scale fetch capacity by adding worker processes. The coordinator owns the crawl control plane. Workers own HTTP fetching and content extraction. PostgreSQL is the durable authority for crawl state and request policy. Workers talk to the coordinator over gRPC and do not connect to PostgreSQL directly.

The design is intentionally coordinator-led, not active-active. PostgreSQL transactions protect durable shared decisions, while one coordinator process owns in-memory worker registrations, pending gRPC replies, and the scheduling dispatcher for that run.

## Component responsibilities

### Coordinator

At startup, the coordinator loads the same Sources TOML used by local crawling, validates the private bind address and transfer limits, opens `SCOUT_DATABASE_URL`, applies migrations, and creates or resumes a crawl run. A resumed run must use the same configuration digest. It persists the run policy and initial seed URLs before serving workers.

The coordinator serves the versioned gRPC API. Registration negotiates protocol version 3 and returns the run ID and effective worker settings, including request timeout, lease timing, extraction bounds, retry policy, and allowed Source boundaries. Other RPCs cover task claims and lease renewal, request permits, robots and sitemap coordination, task completion, and task failure.

One in-process dispatcher serializes scheduling decisions and resolves held task-claim and request-permit RPCs. The gRPC server runs on a multi-thread Tokio runtime. Control-plane PostgreSQL access uses a bounded connection pool (`SCOUT_COORDINATOR_DB_LANES`, default 4, configurable from 2 to 8). Result intake uses separate bounded PostgreSQL lanes (`SCOUT_COMPLETION_LANES`, default 4, configurable from 2 to 8); each lane has a bounded queue and its own connection.

### PostgreSQL

PostgreSQL stores crawl runs, the URL frontier, task status and lease epochs, result acceptance, source content counts, robots policy, sitemap discovery state, request permits, and request-spacing state. Constraints and transactions enforce uniqueness, idempotency, lease fencing, and shared request limits. The coordinator commits a task lease or request permit before replying to its worker.

PostgreSQL remains authoritative after coordinator restart. Pending RPC waits are process memory and disappear when that process stops. Active request permits remain reserved until a worker reports completion or the permit expires. Accepted capture metadata can rebuild the SQLite capture-spool projection after a crash; body files live in the crawl data directory.

### Workers

Each worker registers a worker ID, receives the crawl settings and Source policies, and repeatedly asks for one task lease per free task slot. Normal workers process one task at a time. A worker renews a lease while its task is active and reports either a completion or failure. Expired leases can be reclaimed, and stale lease epochs cannot submit accepted results.

Workers perform HTTP requests and extraction locally. They enforce Source URL boundaries while following redirects, consult the shared robots policy, discover sitemap URLs, extract page content, and collect discovered links. They send captures and discoveries back to the coordinator; they never write directly to the durable frontier.

## Crawl flow

1. **Start the run.** Coordinator loads policy, persists the run and seeds in PostgreSQL, prepares its capture storage, then starts the gRPC service and dispatcher.
2. **Assign work.** Each free worker slot issues a held `ClaimOne` RPC. The dispatcher chooses eligible work from the durable frontier. PostgreSQL records the lease owner, deadline, and incremented lease epoch before the coordinator returns the lease.
3. **Authorize each request.** Before every outbound HTTP request, the worker asks for a request permit. This includes page, robots, sitemap, redirect, and retry requests. The coordinator validates the task lease and request URL, then PostgreSQL atomically checks global concurrency, per-Origin concurrency, start spacing, retry budgets, and pending-request capacity. If policy blocks the request, the coordinator holds the RPC until a permit can be granted or the wait is cancelled. The worker starts HTTP only after receiving a sufficiently live permit, then reports or releases it; an unreported permit expires safely.
4. **Fetch and extract.** Worker fetches the allowlisted page, applies redirect and robots checks, extracts useful content, and collects discovered URLs. Network traffic never bypasses the shared permit policy.
5. **Accept the result.** Worker submits the result over gRPC. Coordinator validates the capture and discovered URLs, writes the body to crawl storage, then sends bounded result work to a completion lane. PostgreSQL accepts the result only for the current lease epoch and records eligible discoveries with the result. The coordinator projects accepted captures into SQLite and acknowledges completion after durable acceptance and projection. Duplicate submissions are idempotent; stale submissions are rejected.
6. **Finish and materialize.** Coordinator stops assigning work when the page target and Source quotas are met, or when the configured run duration expires. After coordinator and workers stop, the separate materialization step can turn the accepted capture spool into an immutable Corpus snapshot.

## Throughput, politeness, and backpressure

Adding worker processes increases available fetch and extraction capacity. It does not raise configured crawl limits. `global_concurrency` caps simultaneous requests across the run; each Origin has its own concurrency cap; `min_start_spacing_ms` limits request start frequency. Origins in a shared politeness group share start spacing. These limits apply across workers because PostgreSQL grants permits transactionally.

Coordinator has parallel internal work, but it is still one process with one scheduling dispatcher. The control database pool and completion lanes let independent RPC and result work proceed concurrently. Result queues and byte reservations are bounded. When completion capacity falls behind, the dispatcher pauses new task assignments and resumes after results drain. This protects coordinator memory and avoids assigning more work than it can accept.

The coordinator can therefore become the throughput ceiling when task scheduling, permit transactions, gRPC handling, result intake, local storage, or PostgreSQL cannot keep up. The central scheduling cursor and row locks still serialize some decisions. Public-site politeness limits may cap throughput before coordinator capacity does. Scale workers first; tune bounded coordinator lanes only after measurements show coordinator-side saturation.

## Multiple coordinator processes

Multiple coordinator processes serving the same crawl run are unsupported. The registered-worker set and pending held RPC replies are local to one process. A worker registered with one instance can have its lease or result rejected by another instance that lacks that registration state. PostgreSQL sharing alone does not share or route those RPCs. Crawl body storage and event output also rely on the coordinator's crawl data directory.

Supporting multiple coordinators would require explicit worker registration and RPC routing or shared ownership, coordinator fencing or leader election, safe coordination of held waits and scheduling, and shared durable body storage. It would also require tests for concurrent dispatch, permit limits, result acceptance, and failover. Until that work exists, use one coordinator process per run and add worker processes behind it.

## Consequences

- One durable frontier and one transactional request budget cover the entire worker fleet.
- Workers can scale independently and need only the private gRPC endpoint; PostgreSQL credentials stay with the coordinator.
- A coordinator restart can recover durable crawl state, but it drops in-flight RPC waits. Workers must reconnect and claim work again; leases and permits expire or resume under their durable deadlines.
- One coordinator process is a single availability and scheduling bottleneck. Internal lanes add bounded concurrency, not active-active coordinator capacity.
- Coordinator and worker binaries must agree on the negotiated protocol version. Multi-host deployments need synchronized clocks because workers validate permit and lease deadlines against coordinator timestamps.

## Implementation anchors

- [Coordinator startup and gRPC server](../src/distributed/coordinator.rs)
- [Coordinator RPCs](../src/distributed/coordinator/rpc.rs)
- [Scheduling dispatcher and held RPCs](../src/distributed/coordinator/scheduling.rs)
- [Bounded result intake](../src/distributed/coordinator/results.rs)
- [Worker registration, claims, and task loop](../src/distributed/worker.rs)
- [Durable frontier and request policy](../src/distributed/frontier.rs)
- [Protocol definition](../proto/coordinator.proto)
- [Coordination notes](../docs/coordination.md)
