# Run 19: distributed 500-page crawl and keyword-only default

Date: 2026-10-11
Scope: distributed-crawling Tickets 1–3
Profile: `data/configs/sources-500.toml`

## Distributed crawl

Run `scout-ticket123-500-20261011-r2` used one coordinator and three independent workers against the 500-page profile. The coordinator reported the 500 accepted-capture target reached. It completed 513 URL tasks: 500 captures and 13 page rejections.

Accepted captures by source were Rust 169, Python 180, Kubernetes 151, and PostgreSQL 0. One PostgreSQL seed task was still leased when the target stopped new claims, so this run did not meet that source's configured 125-page minimum. This exposed the remaining quota-tracking work for a later ticket.

Tickets 1–3 return captures to the coordinator but do not persist or materialize them. These 500 captures therefore did not produce a Corpus snapshot; durable capture acceptance and Corpus equivalence are Ticket 4.

## Keyword-only build default

Built the existing 522-page snapshot `snapshot-f875c…` in an isolated temporary data directory, omitting `--mode`. The build event reported `mode: "keyword-only"`; it staged a validated Generation with 522 pages and 5,879 passages. It was not activated. This verifies the default mode separately from the distributed crawl because Ticket 4 is not implemented yet.

## Automated checks

- `cargo check --all-targets` — passed.
- `cargo test --all-targets` — 39 passed, 3 ignored (PostgreSQL and separate-process tests run separately).
- PostgreSQL frontier/request-policy contracts — 4 passed.
- Three-worker coordinator/worker process smoke — 1 passed; observed shared origin concurrency and start spacing.
- `git diff --check` — passed.
