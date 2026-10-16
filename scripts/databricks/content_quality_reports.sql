-- Run after snapshot_ingest.py has published the snapshot-history views.
-- Replace workspace.default with the catalog and schema used for ingestion.

CREATE OR REPLACE VIEW workspace.default.gold_page_content_quality_report AS
WITH passage_counts AS (
  SELECT p.snapshot_id, p.source_id, p.parent_page_id AS page_id,
         COUNT(DISTINCT p.record_id) AS passage_count
  FROM workspace.default.scout_silver_records p
  JOIN workspace.default.scout_ingestion_batches b
    ON b.batch_id = p.batch_id AND b.status = 'complete'
  WHERE p.record_type = 'passage'
  GROUP BY p.snapshot_id, p.source_id, p.parent_page_id
), page_fields AS (
  SELECT p.snapshot_id, p.source_id, p.page_id, p.batch_id,
         p.scope_sha256, p.extraction_contract_sha256, p.observed_at_utc,
         p.content_sha256, p.url AS normalized_final_url,
         s.source_snapshot_manifest_sha256, s.crawl_manifest_sha256,
         GET_JSON_OBJECT(p.record_json, '$.title') AS title,
         GET_JSON_OBJECT(p.record_json, '$.description') AS description,
         GET_JSON_OBJECT(p.record_json, '$.language') AS language,
         GET_JSON_OBJECT(p.record_json, '$.canonical_url') AS canonical_url,
         GET_JSON_OBJECT(p.record_json, '$.extraction_method') AS extraction_method,
         GET_JSON_OBJECT(p.record_json, '$.content') AS content
  FROM workspace.default.scout_page_observations p
  JOIN workspace.default.scout_source_observations s
    ON s.snapshot_id = p.snapshot_id AND s.source_id = p.source_id
)
SELECT p.snapshot_id, p.source_id, p.page_id, p.batch_id,
       p.scope_sha256, p.extraction_contract_sha256, p.observed_at_utc,
       p.source_snapshot_manifest_sha256, p.crawl_manifest_sha256,
       p.content_sha256, p.normalized_final_url, p.canonical_url,
       p.title, p.description, p.language, p.extraction_method,
       LENGTH(COALESCE(p.content, '')) AS content_char_count,
       OCTET_LENGTH(COALESCE(p.content, '')) AS content_utf8_bytes,
       COALESCE(c.passage_count, 0) AS passage_count,
       LENGTH(COALESCE(p.content, '')) < 200 AS short_content_flag,
       LENGTH(TRIM(COALESCE(p.title, ''))) = 0 AS missing_title_flag,
       COALESCE(c.passage_count, 0) = 0 AS no_passages_flag,
       (LENGTH(COALESCE(p.content, '')) < 200
        OR LENGTH(TRIM(COALESCE(p.title, ''))) = 0
        OR COALESCE(c.passage_count, 0) = 0) AS needs_review_flag,
       '200 extracted characters; nonblank title; at least one Passage' AS screen_thresholds,
       'screening signals only; not a content-value judgment' AS interpretation_limit
FROM page_fields p
LEFT JOIN passage_counts c
  ON c.snapshot_id = p.snapshot_id AND c.source_id = p.source_id AND c.page_id = p.page_id;

CREATE OR REPLACE VIEW workspace.default.gold_exact_alias_groups AS
SELECT snapshot_id, source_id, representative_page_id,
       scope_sha256, extraction_contract_sha256,
       COUNT(DISTINCT alias_page_id) AS alias_count,
       SORT_ARRAY(COLLECT_SET(NAMED_STRUCT(
         'alias_page_id', alias_page_id,
         'alias_url', alias_url,
         'batch_id', batch_id
       ))) AS alias_members,
       MIN(observed_at_utc) AS first_observed_at_utc,
       MAX(observed_at_utc) AS last_observed_at_utc
FROM workspace.default.gold_page_alias_observations
GROUP BY snapshot_id, source_id, representative_page_id,
         scope_sha256, extraction_contract_sha256;

-- Opt-in, read-only near-duplicate report for one snapshot. Replace the
-- snapshot ID, then run this query only after reviewing the documented bounds.
WITH page_sample AS (
  SELECT snapshot_id, source_id, page_id, batch_id,
         scope_sha256, extraction_contract_sha256, observed_at_utc,
         content_sha256, url, record_json
  FROM workspace.default.scout_page_observations
  WHERE snapshot_id = 'snapshot-replace-me'
  ORDER BY source_id, page_id
  LIMIT 200
), content_pages AS (
  SELECT *, GET_JSON_OBJECT(record_json, '$.content') AS content
  FROM page_sample
), bounded_pages AS (
  SELECT *
  FROM content_pages
  WHERE content IS NOT NULL
    AND LENGTH(content) <= 1000
    AND OCTET_LENGTH(content) <= 4000
), worded_pages AS (
  SELECT *,
         SPLIT(TRIM(REGEXP_REPLACE(LOWER(content), r'[^\p{L}\p{N}]+', ' ')), r'\s+') AS words
  FROM bounded_pages
), shingle_arrays AS (
  SELECT *,
         ARRAY_DISTINCT(TRANSFORM(
           SEQUENCE(1, SIZE(words) - 4),
           i -> SHA2(ARRAY_JOIN(SLICE(words, i, 5), ' '), 256)
         )) AS shingle_hashes
  FROM worded_pages
  WHERE SIZE(words) >= 5
), shingles AS (
  SELECT a.snapshot_id, a.source_id, a.page_id, a.batch_id,
         a.scope_sha256, a.extraction_contract_sha256, a.observed_at_utc,
         a.content_sha256, a.url, SIZE(a.shingle_hashes) AS shingle_count,
         shingle_hash
  FROM shingle_arrays a
  LATERAL VIEW EXPLODE(a.shingle_hashes) e AS shingle_hash
), page_stats AS (
  SELECT snapshot_id, source_id, page_id, batch_id,
         scope_sha256, extraction_contract_sha256, observed_at_utc,
         content_sha256, url, MAX(shingle_count) AS shingle_count
  FROM shingles
  GROUP BY snapshot_id, source_id, page_id, batch_id,
           scope_sha256, extraction_contract_sha256, observed_at_utc,
           content_sha256, url
), shingle_frequency AS (
  SELECT snapshot_id, shingle_hash, COUNT(*) AS document_frequency
  FROM shingles
  GROUP BY snapshot_id, shingle_hash
), shared_shingles AS (
  SELECT l.snapshot_id,
         l.source_id AS left_source_id, l.page_id AS left_page_id,
         r.source_id AS right_source_id, r.page_id AS right_page_id,
         COUNT(*) AS shared_five_word_shingle_count
  FROM shingles l
  JOIN shingles r
    ON r.snapshot_id = l.snapshot_id
   AND r.shingle_hash = l.shingle_hash
   AND (l.source_id < r.source_id
        OR (l.source_id = r.source_id AND l.page_id < r.page_id))
  JOIN shingle_frequency f
    ON f.snapshot_id = l.snapshot_id AND f.shingle_hash = l.shingle_hash
   AND f.document_frequency BETWEEN 2 AND 5
  GROUP BY l.snapshot_id, l.source_id, l.page_id, r.source_id, r.page_id
), scored AS (
  SELECT s.snapshot_id,
         l.source_id AS left_source_id, l.page_id AS left_page_id,
         l.batch_id AS left_batch_id, l.url AS left_url,
         l.scope_sha256 AS left_scope_sha256,
         l.extraction_contract_sha256 AS left_extraction_contract_sha256,
         l.content_sha256 AS left_content_sha256,
         r.source_id AS right_source_id, r.page_id AS right_page_id,
         r.batch_id AS right_batch_id, r.url AS right_url,
         r.scope_sha256 AS right_scope_sha256,
         r.extraction_contract_sha256 AS right_extraction_contract_sha256,
         r.content_sha256 AS right_content_sha256,
         l.shingle_count AS left_shingle_count,
         r.shingle_count AS right_shingle_count,
         s.shared_five_word_shingle_count,
         CAST(s.shared_five_word_shingle_count AS DOUBLE)
           / (l.shingle_count + r.shingle_count - s.shared_five_word_shingle_count)
             AS five_word_shingle_jaccard
  FROM shared_shingles s
  JOIN page_stats l
    ON l.snapshot_id = s.snapshot_id AND l.source_id = s.left_source_id AND l.page_id = s.left_page_id
  JOIN page_stats r
    ON r.snapshot_id = s.snapshot_id AND r.source_id = s.right_source_id AND r.page_id = s.right_page_id
  WHERE l.content_sha256 <> r.content_sha256
    AND s.shared_five_word_shingle_count >= 3
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    ORDER BY five_word_shingle_jaccard DESC,
             shared_five_word_shingle_count DESC,
             left_source_id, left_page_id, right_source_id, right_page_id
  ) AS candidate_rank
  FROM scored
  WHERE five_word_shingle_jaccard >= 0.80
)
SELECT *, 'five-word shingle Jaccard >= 0.80; exact content hashes excluded' AS candidate_basis
FROM ranked
WHERE candidate_rank <= 500;
