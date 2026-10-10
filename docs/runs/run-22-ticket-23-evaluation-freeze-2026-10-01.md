# Run 22: Ticket 23 evaluation freeze and relevance metrics

Date: 2026-10-01

Status: Ticket 23 complete under the project owner's LLM-assisted review
decision. These labels are owner-reviewed LLM judgments, not independent
human ratings or human ground truth.

## Review decision and frozen packages

The project owner directed that Ticket 23 use LLM review instead of
independent human review and reported reviewing the LLM-generated reviews. The
review record is in
[`owner-review-record.json`](run-22-ticket-23-evaluation-freeze-2026-10-01/owner-review-record.json).
No row-specific overrides were supplied, so the reviewed recommendations are
used as final grades. Existing package identities were preserved; both frozen
packages have their own IDs.

The 2k package is `scout-2k-page-64-v3`, evaluated against Run 13's matching
verified Generation
`generation-49490253561adfa44305573404c501c18b473752a3e0794965607e787c3f53f1`
and Corpus snapshot
`snapshot-4ab6f3b944327df3e89fe29a1b535a52f62d4a1407fc98a472701e929d90d0e9`.
It retains the 16 development judgments and joins all 225 Run 17 LLM-reviewed
candidate judgments to 48 held-out queries and their Page IDs through the
private review key. The existing v2 package is unchanged.

The 10k package is `scout-10k-rebuild-page-64-v1`, evaluated against Run20's
verified Generation
`generation-168cac59a0009fabbd391d3f53d946d07736fbcad20963dbaccdcbd27a501a3c`
and Corpus snapshot
`snapshot-17ebc1e21779908ecfbe49bc749e4b15aa8965973cc0c5f1aa05c6e4b6015ba0`.
It preserves 32 unaffected targets and the 32 reviewed replacements. q-056
remains grade 2 for its partial answer.

Both packages retain the same frozen 64-query workload, SHA-256
`049763f2b69275e0afdfb6e3d21cb94f9edacc374f031eb49ef2ca9e0120bf31`.

| Package | Metadata SHA-256 | Judgments SHA-256 | Judgments |
| --- | --- | --- | ---: |
| 2k `scout-2k-page-64-v3` | `15b580152a2d6b1bb084c9ef2f6b4772a976ff472a82c296cbe64a198c265431` | `e2332a5b8063622e7e44828b70b4503a642596bc58cdbd0e5991294b20fb0f19` | 241 |
| 10k `scout-10k-rebuild-page-64-v1` | `c95780b8b0b54fe663f10945094a663b4f81b85ace707e542fb9b62679a99e3d` | `6956d309e0e24b90acc90370dff1500711a3f36f69f0f0adbbac837196fb07f6` | 64 |

The package materializer and exact review inputs are recorded in
[`materialize-evaluations.py`](run-22-ticket-23-evaluation-freeze-2026-10-01/materialize-evaluations.py).
It checks the active Generation pointers, Page membership in each frozen
Corpus, query/judgment coverage, and the private Run 17 ID mapping before
writing either package.

## Held-out metrics

| Checkpoint | Mode | nDCG@10 | MRR@10 | Recall@100 |
| --- | --- | ---: | ---: | ---: |
| 2k | Keyword | 0.4365 | 0.6007 | 0.3972 |
| 2k | Semantic | 0.7552 | 0.8090 | 0.7771 |
| 2k | Hybrid | 0.7744 | 0.8160 | 0.8378 |
| 10k | Keyword | 0.3028 | 0.2715 | 0.3958 |
| 10k | Semantic | 0.3614 | 0.3010 | 0.5417 |
| 10k | Hybrid | 0.4170 | 0.3501 | 0.6250 |

The 2k held-out gate did not pass. Hybrid nDCG@10 exceeded semantic by 0.0193,
below the 0.03 threshold; the paired-bootstrap 95% interval was
[-0.0153, 0.0563], so its lower bound also missed zero. Hybrid did improve
Recall@100 over semantic by 0.0608.

The 10k held-out gate passed its recorded thresholds. Hybrid nDCG@10 exceeded
semantic by 0.0556 (paired-bootstrap 95% interval [0.0127, 0.1007]) and
keyword by 0.1142 ([0.0512, 0.1796]); hybrid also led both alternatives on
MRR@10 and Recall@100. This remains 10k checkpoint evidence and reflects the
reviewed LLM labels only.

## Execution records

- 2k evaluation run: `run-125ce07ba72a25c2eaab4b5efac796ffd7bd076989701975e6d5b8820e73f34b`.
  Full report: [`two-k-evaluation-report.json`](run-22-ticket-23-evaluation-freeze-2026-10-01/two-k-evaluation-report.json), SHA-256 `328a9c3528b649bd43b640a34f6b4ab0a54c8d6cccaa152fdcc05e1e1d2066f1`.
- 10k evaluation run: `run-e77558b3e992637e00d05e589847fa8338bcdc58ae011fbc029f86db77bfbf7f`.
  Full report: [`ten-k-evaluation-report.json`](run-22-ticket-23-evaluation-freeze-2026-10-01/ten-k-evaluation-report.json), SHA-256 `a5b1cddea20f81fdf4b9f28cb873552af80a2e1a51f5c60481811da658983d79`.
- Release CLI rebuilt with `--features openvino-gpu --locked` so the evaluator accepts a single LLM reviewer only when owner review is explicitly recorded in package provenance.
- No tests were run.

## RSS guard update

The reusable Run20 Generation-build guard and the future Run21 #31 benchmark
guard both now use a 4 GiB process-tree RSS ceiling. Run20's 4 GiB
`MemAvailable` floor and Run21's 3.25 GiB `MemAvailable` floor, free-swap
floors, swap-out-growth stops, preflight gates, and hard timeout remain active.
Those host-wide stops can still end a run before it reaches 4 GiB RSS. Old
attempt summaries retain their recorded historical limits. No build or search
benchmark was rerun under the updated ceilings.
