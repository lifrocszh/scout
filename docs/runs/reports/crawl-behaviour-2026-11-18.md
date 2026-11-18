# Crawl behaviour: near-duplicates, dead letters, adaptive rate limits (issue #12)

Adds SimHash near-duplicate reporting at the reduce step, a dead-letter queue with a
`scout dlq list|requeue` CLI, and adaptive per-origin rate limits for both crawlers, with metrics
and dashboard panels. The design is in [crawl-behaviour.md](../../crawl-behaviour.md).

## What changed

| Where | What |
| --- | --- |
| `src/crawl/throttle.rs` (new) | The pure pacing policy: classify a response, double or decay the backoff, the hold after a 429/503, and `Retry-After` parsing (seconds or HTTP date, via `httpdate`, already in the lockfile). |
| `src/crawl/fetch/request.rs`, `worker.rs` | The local crawler feeds every response into the policy. The backoff is added to origin spacing. |
| `src/distributed/frontier/throttle.rs` (new) | Persists the policy per run and origin. It runs in the permit release transaction, and the grant adds the backoff to the origin's spacing. |
| `proto/*.proto`, `worker.rs`, `coordinator/rpc.rs` | `ReleaseRequestPermitRequest.retry_after_ms` (field 6). Older workers send 0. The protocol version is unchanged. |
| `src/distributed/frontier/dead_letters.rs` (new), `src/cli/dlq.rs` (new), `app.rs` | List and requeue dead letters. |
| `migrations/0011_crawl_behaviour.sql` | `dead_lettered_at` and `dead_letter_requeues` on tasks, a constraint tying `failed` to `dead_lettered_at`, a dead-letter index, and `scout_crawl_origin_throttle`. Existing `failed` rows are backfilled. |
| `src/corpus/near_dup.rs` (new), `bounded.rs`, `http/mod.rs` | SimHash detector called once per representative Page and alias. Writes `near-duplicates.json`. Serve sets the `scout_corpus_*` gauges when it loads a Generation. |
| `src/observability/metrics.rs`, `coordinator/frontier_gauges.rs` | Six new metric families. |
| `deploy/grafana/dashboards/scout.json` | New "Crawl quality" row with 5 panels. Later rows move down. |
| `scripts/ci/compose-obs-checks.sh` | Checks that `scout_frontier_dead_letter_requeues` is exposed and that serve exports the Corpus page gauge for the fixture origin. |

`frontier.rs` only gained the migration entry, the `dead_lettered_at` assignment in the claim
query, and module declarations. The new responsibilities are in their own modules.

## Verification (local, 2026-11-18)

PostgreSQL 16 on localhost. CI uses 17.

| Check | Result |
| --- | --- |
| `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings` | clean |
| `cargo test --locked` | all pass (lib: 71 passed, 15 ignored Postgres tests) |
| `scripts/ci/postgres-tests.sh` (all 10 binaries, including the new `crawl_behaviour`) | `EXIT 0` |
| `SEARCH_QUALITY_MODE=keyword-only scripts/ci/search-quality.sh` | PASS. NDCG@10 0.8284, Recall@10 0.9531, MRR 0.8298, all +0.0000 against baseline |
| Hybrid search-quality gate | not run locally: the BGE Small download from Hugging Face is blocked in this container. CI runs it. |

Fixture crawl (`tests/crawl_behaviour.rs`, coordinator plus two workers, about 3.4 s):

- **Dedup counts.** On the docs origin, 3 Pages, 1 near duplicate (`/two` of `/one`, a one-sentence
  edit of a 400-word Page) and 1 exact-duplicate alias. On the throttled origin, 3 Pages and
  0 near duplicates.
- **Dead letters.** `/broken` (always HTTP 500) is the only entry in `scout dlq list`. It took
  2 task attempts of 2 HTTP attempts, so 4 requests. `requeue` on the completed run is refused.
- **Slowdown.** After the 429 (`Retry-After: 1`), every later request to that origin started at
  least 950 ms later, whichever worker sent it. `throttle_responses = 1`. Three healthy pages then
  took the backoff below 1 s. The docs origin was never throttled.

The search-quality fixture (64 Pages) has 0 near duplicates and 0 aliases. The report does not
affect snapshot identity, so the gate's corpus, index and metrics are unchanged.

## Limits

- Near duplicates are only compared within one origin. A snapshot resealed from preserved JSONL
  after an interrupted materialization gets no report.
- Requeue works only on `active` runs. A running coordinator notices requeued tasks on its next
  scheduling pass, not instantly.
- Serve sets the Corpus gauges when a search first loads the Generation, not at startup.
- The throttle constants (1 s floor, 60 s cap, 1/8 decay, 5 min `Retry-After` cap) are fixed,
  not configurable.
