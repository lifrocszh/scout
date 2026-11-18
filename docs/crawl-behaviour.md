# Crawl behaviour: near-duplicates, dead letters, adaptive rate limits

Three crawl-quality behaviours added for [issue #12](https://github.com/lifrocszh/scout/issues/12),
on top of robots handling, per-origin budgets and retry backoff. None needs a new service.

| Behaviour | Code | State | Metrics |
| --- | --- | --- | --- |
| Near-duplicate detection | `src/corpus/near_dup.rs`, called from the reduce step in `src/corpus/bounded.rs` | `crawls/<run>/near-duplicates.json` | `scout_corpus_*` (serve) |
| Dead-letter queue | `src/distributed/frontier/dead_letters.rs`, CLI in `src/cli/dlq.rs` | `failed` rows of `scout_crawl_tasks` with `dead_lettered_at` | `scout_frontier_tasks{status="failed"}`, `scout_frontier_dead_letter_requeues` |
| Adaptive rate limit | policy in `src/crawl/throttle.rs`; applied by `src/crawl/fetch/request.rs` (local) and `src/distributed/frontier/throttle.rs` (Postgres) | in memory (local), `scout_crawl_origin_throttle` (distributed) | `scout_origin_throttle_responses_total`, `scout_origin_backoff_milliseconds` |

Schema changes are migration `0011_crawl_behaviour.sql`, applied by `FrontierStore::apply_migrations`
like the others. Existing `failed` tasks get `dead_lettered_at = updated_at`.

## Near-duplicate detection

The reduce step (`materialize_spooled`) already folds exact duplicates (same extracted content in
one Source) into one Page with aliases. Near duplicates stay separate Pages, as
[CONTEXT.md](../CONTEXT.md) says, so this only measures them:

- Each Page gets a 64-bit SimHash of its lowercase word 3-shingles. Pages with fewer than three
  words get no fingerprint.
- Pages are visited in snapshot order. A Page whose fingerprint is within **6 bits** of an
  earlier Page of the **same origin** counts as a near duplicate of the earliest such Page.
  Six bits is about a 2–3% edit of a 400-word Page. Unrelated Pages that use the same vocabulary
  sit near 30 bits apart.
- Candidates come from seven pigeonhole bands, so a match always shares at least one band and
  only Pages sharing a band are compared.

The report (`scout.near-duplicates.v1`) has the counts per origin (`page_count`,
`near_duplicate_page_count`, `near_duplicate_rate`, `exact_duplicate_alias_count`) and up to 1,000
pairs. It is written next to `crawl-manifest.json` and is not part of the snapshot identity, so the
snapshot ID, the index and the search-quality gate stay the same. When a search first loads a
Generation, `serve` reads the report of that Generation's snapshot and sets the `scout_corpus_*`
gauges.

Not covered: near duplicates across origins (mirrors), and a snapshot sealed from an interrupted
materialization's preserved JSONL (`seal_existing_materialized_spool`), which writes no report.

## Dead-letter queue

A task that uses up `max_task_attempts` (a worker failure on the last attempt, or a lease that
expires on the last attempt) moves to `failed` and gets `dead_lettered_at`. A check constraint keeps
the two in step. That row is the dead letter; there is no second copy.

```bash
export SCOUT_DATABASE_URL=postgres://...
scout dlq list --run-id <run> [--limit 100]          # oldest first, with last_error
scout dlq requeue --run-id <run> --all
scout dlq requeue --run-id <run> --task-id 41 --task-id 42
```

Requeueing sets the task back to `pending`, due now, with a fresh attempt budget. It counts in
`dead_letter_requeues` and keeps `last_error`. It takes the scheduler and run locks in the same
order as run completion, so a requeue either lands before the run completes or is refused. Only an
`active` run can be requeued: once a run is `completed`, its Corpus may already be sealed. A
running coordinator picks up requeued tasks on its next scheduling pass (any worker RPC or
deadline). A stopped one picks them up when it resumes the run. Task IDs that are not dead letters
of the run come back in `skipped_task_ids`.

## Adaptive per-origin rate limit

One policy is used by both crawlers:

- **429 and 503** double the origin's *extra* start spacing, from at least 1 s up to 60 s. They
  also hold the origin's next start for that long, or for `Retry-After` if that is longer.
  `Retry-After` can be seconds or an HTTP date, and is capped at 5 min.
- **Other responses below 500** take an eighth off the extra spacing. It drops to zero below
  100 ms. Going from 60 s back to zero takes about fifty healthy responses.
- **Other 5xx and transport errors** do not change it.

The extra spacing adds to the configured `min_start_spacing_ms` (or the origin's own policy). It
applies per origin, so other origins, including others in the same politeness group, keep their
pace. Distributed: workers report the outcome and the parsed `Retry-After`
(`ReleaseRequestPermitRequest.retry_after_ms`, a new field older workers leave at zero) when they
release the request permit. The coordinator updates `scout_crawl_origin_throttle` in the same
transaction that completes the permit, under the run's policy lock, so a duplicate release does
not count twice. The hold reuses the `scout_crawl_request_spacing` gate that permits and the
dispatcher deadline already read. A worker also caps its own `Retry-After` sleep before a retry at
5 min.

## Tests

- Pure policy and parsing: `crawl::throttle::tests`, `corpus::near_dup::tests`, `cli::dlq::tests`,
  and `local_throttled_origin_backs_off_holds_and_recovers_slowly` for the local crawler.
- Postgres contract (`cargo test --lib -- --ignored`, through `scripts/ci/postgres-tests.sh`):
  `distributed::frontier::crawl_behaviour_tests` (dead-lettering, requeue, lease expiry on the last
  attempt, backoff/hold/recovery, duplicate releases).
- Fixture crawl: `tests/crawl_behaviour.rs` runs a coordinator, two workers and two local origins.
  It checks the near-duplicate and alias counts, the dead letter for an always-500 URL, the
  refused requeue on a completed run, and that every request after a 429 waits at least the 1 s
  hold. Timing is asserted only as lower bounds that the coordinator enforces.
