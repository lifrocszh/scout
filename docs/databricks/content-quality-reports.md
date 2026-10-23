# Content quality and duplicate reports

Run [the quality report SQL](../../scripts/databricks/content_quality_reports.sql)
after snapshot ingestion creates the history views. Replace
`workspace.default` with the target catalog and schema before running it. The
two Gold views cover all authoritative snapshots. The near-duplicate query is
opt-in and requires one `snapshot_id`; it does not change Pages, aliases, or
identity ownership.

## Screening indicators

`gold_page_content_quality_report` has one row per authoritative
`(snapshot_id, source_id, page_id)`. It carries the batch and snapshot IDs,
Source scope and extraction-contract hashes, snapshot and Crawl Manifest
checksums, observation time, URL, content hash, extraction method, and title,
description, and language metadata. Passage counts are deduplicated by Passage
ID across complete re-exports.

The review flags use fixed thresholds: extracted content under 200 characters,
a blank title, or no Passage. `needs_review_flag` is the OR of those flags.
These are reproducible triage signals, not a claim that other content is
valuable or that flagged content should be deleted.

```sql
SELECT *
FROM workspace.default.gold_page_content_quality_report
WHERE snapshot_id = 'snapshot-<64-hex-characters>'
ORDER BY needs_review_flag DESC, content_char_count, source_id, page_id;
```

`gold_exact_alias_groups` groups the already-published
`gold_page_alias_observations` by Source and representative Page ID. Its
`alias_members` retain Scout's alias Page IDs, URLs, and batch provenance. It
does not derive another exact-content deduplication or treat an alias as an
admitted Page.

```sql
SELECT *
FROM workspace.default.gold_exact_alias_groups
WHERE snapshot_id = 'snapshot-<64-hex-characters>'
ORDER BY source_id, representative_page_id;
```

## Opt-in near-duplicate candidates

The final query in `content_quality_reports.sql` compares at most 200 Pages
for one snapshot, selected deterministically by `(source_id, page_id)`. Each
Page contributes at most 1,000 extracted characters and 4,000 UTF-8 bytes.
It tokenizes Unicode letters and digits, forms unique five-word shingles, and
compares Pages with at least three shared shingles. A candidate needs a
Jaccard score of at least 0.80. Exact content hashes are excluded; shingle
hashes are only candidate evidence and never replace Scout Page IDs or exact
alias ownership.

The per-query bounds are 200 sampled Pages, 800,000 input text bytes, at most
99,200 distinct shingle rows (496 per Page), at most 396,800 pair-hit rows
(document frequency is capped at five), 19,900 unique Page pairs, and 500
reported candidates. Excluding shingles found in more than five sampled Pages
can miss boilerplate-heavy matches; deterministic sampling and the limits can
miss other candidates too. An empty result does not establish that no near
duplicates exist. Check each candidate's URLs, content hashes, and Source
lineage before acting.

The fixture records expected outputs and these logical capacity bounds. The
quality and opt-in near-duplicate SQL were executed on a deterministic 24-Page
fixture, but it is not representative. Query-profile time, bytes scanned, and
peak memory were unavailable and are recorded as `NULL`. Keep the
near-duplicate query opt-in; capture those metrics on representative snapshots
before raising any limit or scheduling it. The query reads authoritative
snapshot history only; provisional capture observations are not included.

See [Crawl coverage and effectiveness reports](crawl-reports.md) for the
historical freshness, coverage, errors, and effectiveness reports.
