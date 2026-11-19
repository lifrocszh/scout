# Search-quality regression gate

CI job `search-quality` fails a PR that makes ranking worse. It crawls a small fixture corpus,
builds keyword, semantic and hybrid (RRF fusion) indexes, runs `scout evaluate` over 64 labelled
queries, and compares NDCG@10, Recall@10 and MRR per mode with `data/eval/baseline.json`. A drop of
more than **0.02** in any of the nine numbers fails the job. The before/after table is in the job
log and the job summary.

The gate catches regressions. It does not measure absolute quality: the corpus is tiny and the
labels are synthetic (see below). Real-corpus numbers stay in `docs/runs/`.

## Run it

```sh
cargo build --locked
scripts/ci/search-quality.sh                        # all three modes; downloads BGE Small once
SEARCH_QUALITY_MODE=keyword-only scripts/ci/search-quality.sh   # keyword only, no model, ~2 s
SEARCH_QUALITY_MAX_DROP=0.05 scripts/ci/search-quality.sh       # a different limit
```

Work happens in `data/search-quality/` (ignored by git). Its `models/` directory is kept between
runs and is what CI caches. The build verifies the model files against the hashes in
`data/eval/index-profile.toml`, so a stale or corrupt cache fails loudly instead of skewing results.

## What is in `data/eval/`

| File | Purpose |
| --- | --- |
| `corpus/` | 64 HTML pages: rust, python, kubernetes and postgresql, 16 each, plus `robots.txt` and `sitemap.xml`. Written for this fixture, not copied from the real docs. Served on `127.0.0.1:18099` (fixed, because Page IDs derive from URLs). |
| `sources.toml` | Crawl config for the four sources. |
| `index-profile.toml` | Same BGE Small profile as `docker/index-profile.toml` (keep the model pins in sync), with candidate depth 100 instead of 5 so top-10 metrics mean something. |
| `queries.jsonl` | The 64 `scout evaluate` queries: 4 sources x 4 intents x 4, 16 development and 48 held-out. This is the frozen shape `src/evaluation` already requires. |
| `judgments.jsonl` | Graded labels (0 to 3) keyed by URL path. `search-quality.sh` resolves each path to the crawled Page ID and fails if a labelled page was not crawled. |
| `metadata.json` | Package metadata; snapshot, Generation and model digest are filled in per run. |
| `baseline.json` | The committed numbers the gate compares with. |

### Labels

The corpus and labels were written together: grade 3 is the page written to answer the query, 2 a
page that answers it substantially, 1 a related page, unlisted pages 0. They are not independent
human judgments, so `metadata.json` declares `judgment_provenance.fixture_by_construction`. That
flag lets `scout evaluate` accept one judge, and is meant for this fixture only; real packages
still need two judges or an owner-reviewed LLM reviewer.

Queries are plain text, so any text works in `queries.jsonl`, `::` and quotes included (see
"Keyword queries" in the README). Quoted phrases are the one syntax; they change the ranking, so
leave them out unless a query is meant to test one.

## Updating the baseline on purpose

When a change legitimately moves the numbers (a better ranker, new fixture pages or queries, a new
model pin):

```sh
cargo build --locked
scripts/ci/search-quality.sh --update-baseline   # needs the default hybrid mode
git add data/eval/baseline.json
```

Commit it in the same PR as the change and explain the movement in the PR description. The command
writes the baseline from this run's results; run the gate once more afterwards to see it pass. If
CI and your machine disagree (they should not, keyword results are bit-identical), take
`results.json` from the `search-quality-results` artifact of the CI run instead:
`python3 scripts/ci/search_quality.py baseline --results results.json`.

An improvement of more than the limit shows as `improved` and does not fail. Ratchet the baseline up
when you see it, so the gain is protected.

## Changing the corpus or queries

Edit the pages, `queries.jsonl` and `judgments.jsonl` together, keep the 64-query frozen shape
(`validate_evaluation_queries`), then update the baseline as above. Page paths in the sitemap are
absolute URLs, so a path change needs `sitemap.xml` and the labels updated too.
