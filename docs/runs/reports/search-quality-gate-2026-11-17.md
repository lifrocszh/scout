# Search-quality regression gate (issue #6)

Adds CI job `search-quality`: it fails a PR when NDCG@10, Recall@10 or MRR of keyword, semantic or
hybrid search drops more than 0.02 below a committed baseline. Usage and baseline updates are in
[`docs/search-quality-gate.md`](../../search-quality-gate.md).

## What was added

| Path | Purpose |
| --- | --- |
| `data/eval/corpus/` | 64 original fixture pages (rust, python, kubernetes, postgresql; 16 each), `robots.txt`, `sitemap.xml`. 66 KB for all of `data/eval/`. |
| `data/eval/{queries,judgments}.jsonl`, `metadata.json` | 64 queries in the frozen `scout.evaluation.v1` shape (4 sources x 4 intents x 4; 16 development, 48 held-out) and 112 graded judgments. |
| `data/eval/{sources.toml,index-profile.toml}` | Crawl config and BGE Small index profile (same pins as `docker/index-profile.toml`, candidate depth 100). |
| `data/eval/baseline.json` | Committed numbers. |
| `scripts/ci/search-quality.sh`, `search_quality.py` | Driver (crawl, build, activate, verify, package, evaluate) and helper (package, summarize, compare, baseline). |
| `tests/search_quality_test.py` | 17 helper tests, run by the `python` job. |
| `.github/workflows/ci.yml` | New `search-quality` job. Other jobs unchanged except one added line in `python`. |
| `src/evaluation/mod.rs` | `recall_at_10` in per-query and aggregate metrics (additive in the report); `judgment_provenance.fixture_by_construction` accepted as the basis for a single judge. Tests in `src/lib.rs` and `tests/evaluation_contract.rs`. |
| `.gitignore`, `README.md` | `data/eval/` un-ignored; results table. |

No metric code was duplicated: every number comes from `scout evaluate`.

## Baseline (measured on a GitHub Actions runner)

| Mode | NDCG@10 | Recall@10 | MRR |
| --- | ---: | ---: | ---: |
| keyword | 0.8284 | 0.9531 | 0.8298 |
| semantic | 0.9122 | 0.9922 | 0.9062 |
| hybrid | 0.8974 | 0.9766 | 0.8971 |

Keyword was measured in a local 4-CPU sandbox and on the runner: identical to four decimals, and
identical across repeated local runs. Semantic and hybrid depend on ONNX inference, so the 0.02 limit
leaves room for hardware differences.

Per-intent keyword NDCG@10 (local): exact-api 0.965, conceptual 0.864, how-to 0.751,
semantic-paraphrase 0.734, so the fixture separates query types as intended.

## Verification

- Keyword mode locally, full flow: 2 s.
- **Gate fails on a real regression.** Temporarily truncating keyword retrieval to 3 Passages
  (reverted, not committed): NDCG@10 0.8284 to 0.6852, Recall@10 0.9531 to 0.6953, MRR 0.8298 to
  0.7396. Exit 1, table printed, baseline-update hint shown.
- **Not every change is a regression.** Ignoring the title and heading boosts moved NDCG@10 by
  +0.0211 and MRR by +0.0253: reported as `improved`, exit 0.
- `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, evaluation and benchmark
  contract tests, and the Python tests pass.
- First CI run (cold caches): `search-quality` 4 min 5 s, of which `cargo build` 3 min 25 s and the
  gate 25 s including the model download. It failed only on "no baseline" for semantic and hybrid,
  which is how their numbers were obtained; all other jobs passed.

## Findings

1. **Hybrid is below semantic on this fixture** (NDCG@10 0.8974 vs 0.9122). Same direction as the
   2k held-out result in Run 22. The gate protects each mode against itself and does not assert
   that hybrid wins.
2. **The title and heading boosts do not help here.** Removing them improves keyword NDCG@10 by
   0.02. One fixture says little about real documentation; worth re-checking on a real package.
3. **Keyword search rejects Tantivy syntax in user text.** `Vec::retain` or `kubectl logs --previous`
   is `invalid keyword query: Syntax Error`, and `scout evaluate` aborts on the first such query. The
   fixture queries avoid that syntax. Not fixed here.
4. **Labels are synthetic.** The corpus and grades were written together, so they are declared
   `fixture_by_construction`, not independent judgments.

## Deferred

- Escape or sanitize user text before the keyword query parser (finding 3).
- A larger or real-document fixture if the gate proves too coarse.
