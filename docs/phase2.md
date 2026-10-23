# Big Data + Search Intelligence (Phase 2): Databricks Delta Pipelines and Keyword Search Optimization

Source: [GitHub Issue #2](https://github.com/lifrocszh/scout/issues/2)

## Current status and scope closeout (2026-10-19)

The owner-approved breakdown in `.scratch/databricks/spec.md` is the current Phase 2 acceptance scope. Core Tickets 01–09 are complete; optional Tickets 10–11 are implemented and verified within their documented limits; conditional Ticket 12 is closed as **not planned**, not completed. The original GitHub Issue #2 is closed as **not planned** for the additional scope below. No remaining current Core ticket is open.

The original parent issue asked for more than the approved breakdown delivers. The following remain deferred and are not claimed complete: a Databricks-produced processed Corpus imported into Scout's local Generation pipeline; reuse of ONNX embeddings for unchanged passages; learned-ranking training and MLflow tracking; and a Rust learned-ranker artifact import/serving path. JSONL was an accepted interchange format, so the original preference for Parquet is not an unmet requirement. Continuous service and production-scale performance guarantees remain outside scope.

Key evidence: [Ticket 02 live workspace ingestion](runs/stage1b-databricks-2026-10-14/run-report.md), [Ticket 01 keyword baseline](runs/run-25-keyword-only-baseline-2026-10-19/run-report.md), [Ticket 05 PostgreSQL recovery/follow verification](runs/ticket05-durable-events-2026-10-19/run-report.md), and [Phase 2 closeout record](runs/phase2-closeout-2026-10-19.md).

Limits remain explicit: Ticket 11's near-duplicate query is opt-in pending a representative workspace profile; Ticket 04's live SQL was run on a small fixture, so production-scale performance and storage are unmeasured; Ticket 10's policy workflow was verified without executing a scheduled Source. Ticket 12 ran no candidate experiment, and Run 25 does not establish that search quality is sufficient.

## Goal

Integrate **Databricks Free Edition** as an offline data engineering and analytics platform downstream of Scout's distributed Rust crawler.

Prioritize **fast, keyword-only search using Tantivy/BM25**, while using Databricks for incremental data processing, historical crawl analysis, content quality assessment and adaptive recrawling recommendations.

Avoid duplicating capabilities already implemented in Scout, including extraction, normalization, exact deduplication and deterministic corpus generation.

**Core principles:**

- Rust owns crawling, canonical data identities, corpus generation, indexing and search serving.
- Databricks owns historical analytics, incremental processing and offline crawl intelligence.
- Databricks must not sit on the critical path for crawling, indexing or search.
- Keyword-only retrieval is the default; semantic/hybrid search remains optional.
- No mandatory embeddings, custom learning-to-rank training or paid model APIs.
- Favor reproducible, bounded batch processing over always-on infrastructure.
- Downstream processing should begin before upstream crawling completes, using durable events and bounded micro-batches.
- Incremental processing may produce provisional results, but only validated, immutable Corpus snapshots can be published as authoritative search inputs.
- Prefer PostgreSQL transactional outbox over introducing Kafka initially. Keep event contracts transport-independent.

## Architecture

**Local crawling and event pipeline:**

Distributed Rust Workers → Fetch/Extract → Durable Artifact Storage + PostgreSQL Transactional Outbox → Independent Downstream Consumers

**Local indexing path:**

Committed Captures → Incremental Staging → Completed Crawl → Deterministic Corpus Snapshot → Tantivy BM25 Index Build → Atomic Generation Activation

**Online query path (Local):**

HTTP Query → Active Tantivy Index → BM25 Retrieval → Ranked Results

**Databricks analytics path:**

Committed Capture Events → Bounded Micro-batch Export → Unity Catalog Volume → Spark Bronze/Silver/Gold Delta Tables → Incremental Analytics + Crawl Intelligence

**Optional feedback loop (Human-in-the-Loop):**

Databricks Recommendations → Scout Policy Validation → Conditional Human Approval → Rust Coordinator Scheduling

Crawling, local processing and analytics can overlap. Databricks may process provisional capture data, but authoritative historical state must be reconciled against completed, validated Corpus snapshots.

Databricks remains asynchronous and is never required for crawling, indexing or search serving.

## Implementation Checklist

This checklist is retained from the earlier Phase 2 plan. Some items have since been delivered under the approved tickets; other items were deferred. Its boxes were not reconciled individually and are not the current status tracker. Use the approved ticket issues and closeout record above; deferred items remain explicitly listed and are not marked complete.

### 1. Keyword-First Search

- [ ] Make keyword-only search the documented default for indexing and querying.
- [x] Ensure keyword-only index builds do not initialize embedding models or construct vector indexes.
- [x] Ensure keyword queries never invoke embedding inference.
- [x] Preserve optional semantic/hybrid functionality without making it a Phase 2 dependency.
- [x] Benchmark BM25 indexing throughput, index size, memory usage and p50/p95 query latency (Run 25; see the keyword-only baseline report).
- [x] Evaluate existing BM25 relevance using the frozen evaluation package.
- [ ] Experiment with lightweight keyword improvements, such as field boosts, analyzers and query normalization, only when supported by benchmark evidence; not planned unless a concrete product need or user search-quality requirement warrants reopening Ticket 12.

### 2. Data Contracts and Export

- [ ] Define a versioned interchange schema covering Source, Page, Passage, aliases, content hashes, crawl metadata, snapshot identity and operational events.
- [ ] Export immutable Scout Corpus snapshots and crawl metadata in Parquet (preferred) or JSONL.
- [ ] Preserve canonical Page/Passage identities, source ownership, provenance and manifest checksums.
- [x] Distinguish completed snapshots from incomplete crawls.
- [ ] Include batch IDs, schema versions and source snapshot references for idempotent ingestion.
- [ ] Support explicit batch uploads to a Unity Catalog Volume without exposing local Scout infrastructure publicly.
- [ ] Keep exported data read-only relative to Scout's canonical storage.

### 3. Databricks Delta Pipelines

Implement a lightweight Medallion Architecture.

**Bronze — Raw ingestion**

- [ ] Ingest exported Scout batches into append-only Bronze Delta tables.
- [ ] Preserve raw records, snapshot references, ingestion timestamps and lineage.
- [ ] Validate schema versions and required fields.
- [ ] Detect and reject conflicting batch identities.

**Silver — Historical state and changes**

- [ ] Build normalized historical Page and Source tables using Scout's existing canonical identities.
- [ ] Use Delta MERGE or equivalent idempotent processing for incremental updates.
- [ ] Identify new, modified and unchanged Pages through snapshot comparison and content hashes.
- [ ] Record removals only when supported by complete crawl coverage or explicit tombstones.
- [ ] Preserve historical versions and change timestamps.
- [ ] Avoid repeating Scout's exact-content deduplication or reassigning canonical identities.

**Gold — Analytics**

- [ ] Aggregate crawl coverage, Page changes, freshness and error rates.
- [ ] Calculate per-Source change frequency and historical crawl effectiveness.
- [ ] Produce content quality and duplicate-cluster reports where useful.
- [ ] Generate versioned recrawl recommendations and search-quality reports.

### 3.5. Event-Driven Incremental Processing

Enable downstream processing to begin before a crawl run completes.

- [ ] Define versioned, transport-independent events such as `CaptureCommitted`, `CrawlCompleted` and `CorpusSnapshotPublished`.
- [ ] Implement a PostgreSQL transactional outbox to durably record events alongside committed capture metadata.
- [ ] Ensure events reference fully persisted, immutable artifacts.
- [ ] Implement asynchronous consumers with independent checkpoints, bounded queues and retry handling.
- [ ] Group committed captures into bounded micro-batches for Databricks ingestion.
- [ ] Support idempotent event processing, replay and recovery after consumer failures.
- [ ] Track event lag, pending batches, retries and processing throughput.
- [ ] Apply bounded backpressure and retention policies without compromising crawler correctness.
- [ ] Reconcile provisional processing results against completed Corpus snapshots.
- [ ] Keep Kafka as a future transport option, not a Phase 2 dependency.

Databricks ingestion may use periodic or manually triggered micro-batches where Free Edition limitations prevent continuous execution.

### 4. Crawl Intelligence and Adaptive Recrawling

- [ ] Generate provisional crawl intelligence from incremental micro-batches without waiting for the entire crawl to complete.
- [ ] Require configurable human approval for significant scheduling policy changes, including substantial frequency increases and crawl-scope expansion.
- [ ] Automatically apply minor adjustments within previously approved policy limits.
- [ ] Record recommendation provenance, approval decisions and applied policy versions.
- [ ] Prevent provisional analytics from automatically changing authoritative crawl policies without validation.

### 5. Search Quality Evaluation

Retain the existing keyword search implementation as the baseline.

- [x] Freeze a reproducible BM25 evaluation baseline.
- [ ] Measure nDCG@10, MRR@10, Recall@100 and p95 query latency.
- [ ] Evaluate field boosts, tokenization and lightweight query improvements.
- [x] Record configurations, corpus identities, evaluation packages and benchmark results.
- [ ] Require measured relevance improvements without unacceptable latency regressions before promoting search changes.
- [x] Keep pretrained reranking and learned ranking as optional future experiments.

Custom LambdaMART/LightGBM training, MLflow integration and Rust-side learned-ranker inference are **not required** for Phase 2.

### 6. Reliability, Observability and Reproducibility

- [ ] Verify duplicate-batch ingestion does not duplicate logical records or change analytical results.
- [ ] Test changed Pages, missing Pages, explicit tombstones and incomplete crawls.
- [ ] Support replay and backfill from immutable exports.
- [ ] Record ingestion counts, changed rows, Spark execution time, processed bytes and storage usage.
- [ ] Create SQL/dashboard views for coverage, freshness, content changes, errors and crawl efficiency.
- [ ] Document Databricks Free Edition setup, upload procedures, quotas and operational limitations.
- [ ] Provide reproducible batch execution and benchmark commands.
- [x] Ensure Scout remains fully operational when Databricks is unavailable.

## Acceptance Criteria

1. **Keyword performance:** Scout can build and serve keyword-only indexes without loading embedding models or generating vectors. Baseline latency and resource usage are documented.

2. **Successful ingestion:** A completed Scout Corpus snapshot can be exported and ingested into validated Bronze/Silver/Gold Delta tables.

3. **Incremental correctness:** Replaying identical batches is idempotent. New, modified and explicitly deleted Pages produce correct historical results without altering Scout identities.

4. **Historical analytics:** Databricks produces reproducible coverage, freshness, error and change-frequency reports across multiple crawl snapshots.

5. **Crawl intelligence:** The pipeline can generate versioned recrawl recommendations. If optional Scout-side integration is enabled, invalid or stale recommendations are safely rejected.

6. **Search evaluation:** BM25 relevance and performance are measured reproducibly. Improvements are promoted only with supporting evidence.

7. **System independence:** Scout's crawling, corpus generation, indexing and search serving continue without Databricks connectivity.

8. **Cost constraints:** The workflow requires no paid model API, GPU training or continuously running Databricks compute.

9. **Overlapping execution:** Downstream processing begins while upstream crawling is still active. Events are durably delivered, processed idempotently and recoverable after failures.

10. **Human-in-the-loop:** Significant recrawl policy changes require approval. Routine scheduling remains automatic within approved limits, and all decisions are auditable.

## Non-Goals

- Mandatory semantic embeddings or vector indexing.
- Custom learning-to-rank model training and deployment.
- Hosted embedding or inference APIs.
- Reimplementing Scout's extraction, canonicalization or exact deduplication.
- Databricks-generated replacement Corpus snapshots.
- Moving the crawler, coordinator, PostgreSQL frontier or Rust search API into Databricks.
- Real-time Databricks queries during search or task scheduling.
- Kafka, always-on streaming or production-scale orchestration.
- Distributed search sharding.
- GPU training or a production cloud SLA.

## Dependencies

- Can begin using existing Scout single-node immutable Corpus snapshots.
- Compatible with Phase 1 distributed crawling through the same export contract.
- Independent of Phase 3 multi-machine deployment.
- Adaptive recrawl integration depends on stable coordinator scheduling interfaces.
- Semantic search and learned reranking are optional future extensions.

## Implementation Priority

**P0 — Core**

1. Keyword-only baseline and performance verification.
2. Versioned Scout export contract.
3. Databricks Bronze/Silver/Gold incremental ETL.
4. Historical crawl analytics and correctness tests.

**P1 — Enhancements** 5. Content quality and near-duplicate analysis. 6. Adaptive recrawl recommendation generation. 7. Optional coordinator integration.

**P2 — Future Research** 8. Advanced keyword relevance optimization. 9. Pretrained reranking experiments. 10. Learned ranking only if justified by evaluation evidence.

**Definition of Done:** Scout remains a fast, independent Rust keyword search engine, while Databricks provides reproducible historical analytics and actionable crawl intelligence without duplicating core crawler responsibilities.
