# Databricks notebook source
import re


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
for label, value in (("catalog", catalog), ("schema", schema)):
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
        raise ValueError(f"invalid {label} identifier")


def table(name):
    return ".".join(f"`{part.replace('`', '``')}`" for part in (catalog, schema, name))


event_batches = table("scout_crawl_event_batches")
event_records = table("scout_crawl_event_records")
event_identity = table("scout_crawl_event_identity")
ingestion_batches = table("scout_ingestion_batches")
silver = table("scout_silver_records")
page_history = table("scout_page_observations")
alias_history = table("scout_page_alias_observations")

complete_events = table("scout_reconciliation_complete_events")
reconciliation = table("gold_crawl_snapshot_reconciliation")
authoritative_pages = table("gold_authoritative_page_observations")
authoritative_aliases = table("gold_authoritative_page_alias_observations")
capture_reconciliation = table("gold_crawl_capture_reconciliation")
rejected_tasks = table("gold_crawl_rejected_task_observations")

# An identity can survive a failed notebook attempt. Resolve it through any
# exact replay whose own source batch completed; do not trust identity.batch_id.
spark.sql(f"""CREATE OR REPLACE VIEW {complete_events} AS
SELECT DISTINCT i.crawl_run_id, i.event_id, i.event_type, i.source_id,
       r.batch_id, r.record_json, r.disposition
FROM {event_identity} i
JOIN {event_records} r
  ON r.crawl_run_id = i.crawl_run_id AND r.record_id = i.event_id
 AND r.event_type = i.event_type AND r.record_json = i.record_json
JOIN {event_batches} b ON b.batch_id = r.batch_id
 AND b.crawl_run_id = r.crawl_run_id AND b.status = 'complete'
WHERE r.disposition IN ('accepted', 'duplicate')""")

spark.sql(f"""CREATE OR REPLACE VIEW {reconciliation} AS
WITH snapshot_rows AS (
  SELECT DISTINCT b.batch_id, b.snapshot_id, b.source_snapshot_manifest_sha256,
         b.crawl_manifest_sha256, b.crawl_manifest_file_sha256, b.crawl_manifest_file_bytes,
         b.expected_page_count, b.expected_alias_count,
         b.expected_passage_count,
         GET_JSON_OBJECT(c.record_json, '$.run_id') AS crawl_run_id,
         CAST(GET_JSON_OBJECT(c.record_json, '$.summary.captured_page_count') AS BIGINT) AS expected_capture_count,
         GET_JSON_OBJECT(c.record_json, '$.manifest_sha256') AS manifest_crawl_sha,
         GET_JSON_OBJECT(c.record_json, '$.status') AS crawl_status,
         GET_JSON_OBJECT(c.record_json, '$.snapshot_id') AS crawl_snapshot_id,
         GET_JSON_OBJECT(s.record_json, '$.status') AS snapshot_status,
         GET_JSON_OBJECT(s.record_json, '$.snapshot_id') AS snapshot_record_id,
         GET_JSON_OBJECT(s.record_json, '$.run_id') AS snapshot_run_id,
         GET_JSON_OBJECT(s.record_json, '$.crawl_manifest_sha256') AS snapshot_crawl_sha,
         CAST(GET_JSON_OBJECT(c.record_json, '$.summary.page_count') AS BIGINT) AS manifest_page_count,
         CAST(GET_JSON_OBJECT(c.record_json, '$.summary.alias_count') AS BIGINT) AS manifest_alias_count,
         CAST(GET_JSON_OBJECT(c.record_json, '$.summary.passage_count') AS BIGINT) AS manifest_passage_count
  FROM {ingestion_batches} b
  JOIN {silver} c ON c.batch_id = b.batch_id AND c.snapshot_id = b.snapshot_id
                 AND c.record_type = 'crawl_manifest'
  JOIN {silver} s ON s.batch_id = b.batch_id AND s.snapshot_id = b.snapshot_id
                 AND s.record_type = 'snapshot'
  WHERE b.status = 'complete'
    AND b.crawl_manifest_file_sha256 IS NOT NULL
    AND b.crawl_manifest_file_bytes IS NOT NULL
    AND c.record_id = GET_JSON_OBJECT(c.record_json, '$.run_id')
), snapshots AS (
  SELECT crawl_run_id, snapshot_id,
         MIN(batch_id) AS snapshot_batch_id,
         MIN(source_snapshot_manifest_sha256) AS source_snapshot_manifest_sha256,
         MIN(crawl_manifest_sha256) AS crawl_manifest_sha256,
         MIN(crawl_manifest_file_sha256) AS crawl_manifest_file_sha256,
         MIN(crawl_manifest_file_bytes) AS crawl_manifest_file_bytes,
         MIN(expected_page_count) AS expected_page_count,
         MIN(expected_alias_count) AS expected_alias_count,
         MIN(expected_passage_count) AS expected_passage_count,
         MIN(expected_capture_count) AS expected_capture_count,
         COUNT(DISTINCT TO_JSON(NAMED_STRUCT(
           'source_sha', source_snapshot_manifest_sha256, 'crawl_sha', crawl_manifest_sha256,
           'crawl_file_sha', crawl_manifest_file_sha256, 'crawl_file_bytes', crawl_manifest_file_bytes,
           'pages', expected_page_count, 'aliases', expected_alias_count,
           'passages', expected_passage_count, 'captures', expected_capture_count
         ))) AS metadata_variant_count,
         MIN(CASE WHEN crawl_status = 'complete' AND snapshot_status = 'complete'
                   AND crawl_snapshot_id = snapshot_id AND snapshot_record_id = snapshot_id
                   AND snapshot_run_id = crawl_run_id AND snapshot_crawl_sha = crawl_manifest_sha256
                   AND manifest_crawl_sha = crawl_manifest_sha256
                   AND expected_capture_count IS NOT NULL AND expected_capture_count >= 0
                   AND manifest_page_count = expected_page_count
                   AND manifest_alias_count = expected_alias_count
                   AND manifest_passage_count = expected_passage_count
                  THEN 1 ELSE 0 END) AS manifest_valid
  FROM snapshot_rows
  WHERE crawl_run_id IS NOT NULL
  GROUP BY crawl_run_id, snapshot_id
), all_publications AS (
  SELECT DISTINCT i.crawl_run_id, i.event_id,
         GET_JSON_OBJECT(i.record_json, '$.data.snapshot_id') AS snapshot_id,
         GET_JSON_OBJECT(i.record_json, '$.data.snapshot_manifest.sha256') AS snapshot_sha,
         GET_JSON_OBJECT(i.record_json, '$.data.crawl_manifest.sha256') AS crawl_sha,
         CAST(GET_JSON_OBJECT(i.record_json, '$.data.crawl_manifest.byte_size') AS BIGINT) AS crawl_bytes,
         CAST(GET_JSON_OBJECT(i.record_json, '$.data.page_count') AS BIGINT) AS page_count,
         CAST(GET_JSON_OBJECT(i.record_json, '$.data.alias_count') AS BIGINT) AS alias_count,
         CAST(GET_JSON_OBJECT(i.record_json, '$.data.passage_count') AS BIGINT) AS passage_count
  FROM {complete_events} i WHERE i.event_type = 'corpus_snapshot_published'
), complete_publications AS (
  SELECT DISTINCT crawl_run_id, event_id,
         GET_JSON_OBJECT(record_json, '$.data.snapshot_id') AS snapshot_id,
         GET_JSON_OBJECT(record_json, '$.data.snapshot_manifest.sha256') AS snapshot_sha,
         GET_JSON_OBJECT(record_json, '$.data.crawl_manifest.sha256') AS crawl_sha,
         CAST(GET_JSON_OBJECT(record_json, '$.data.crawl_manifest.byte_size') AS BIGINT) AS crawl_bytes,
         CAST(GET_JSON_OBJECT(record_json, '$.data.page_count') AS BIGINT) AS page_count,
         CAST(GET_JSON_OBJECT(record_json, '$.data.alias_count') AS BIGINT) AS alias_count,
         CAST(GET_JSON_OBJECT(record_json, '$.data.passage_count') AS BIGINT) AS passage_count
  FROM {complete_events} WHERE event_type = 'corpus_snapshot_published'
), capture_events AS (
  SELECT DISTINCT crawl_run_id, event_id,
         CAST(GET_JSON_OBJECT(record_json, '$.data.task_id') AS BIGINT) AS task_id,
         source_id,
         GET_JSON_OBJECT(record_json, '$.data.content_sha256') AS content_sha256,
         GET_JSON_OBJECT(record_json, '$.data.body_artifact.sha256') AS body_sha256
  FROM {complete_events} WHERE event_type = 'capture_committed'
), capture_counts AS (
  SELECT crawl_run_id, COUNT(DISTINCT event_id) AS capture_event_count,
         COUNT(DISTINCT task_id) AS distinct_task_count,
         SUM(CASE WHEN task_id IS NULL THEN 1 ELSE 0 END) AS missing_task_count
  FROM capture_events GROUP BY crawl_run_id
), snapshot_status AS (
  SELECT s.*, COALESCE(COUNT(DISTINCT p.event_id), 0) AS matching_publication_event_count,
         COALESCE(COUNT(DISTINCT ap.event_id), 0) AS publication_event_seen_count,
         COALESCE(cc.capture_event_count, 0) AS capture_event_count,
         COALESCE(cc.distinct_task_count, 0) AS distinct_task_count,
         COALESCE(cc.missing_task_count, 0) AS missing_task_count
  FROM snapshots s
  LEFT JOIN complete_publications p ON p.crawl_run_id = s.crawl_run_id
       AND p.snapshot_id = s.snapshot_id
       AND p.snapshot_sha = s.source_snapshot_manifest_sha256
       AND p.crawl_sha = s.crawl_manifest_file_sha256
       AND p.crawl_bytes = s.crawl_manifest_file_bytes
       AND p.page_count = s.expected_page_count AND p.alias_count = s.expected_alias_count
       AND p.passage_count = s.expected_passage_count
  LEFT JOIN all_publications ap ON ap.crawl_run_id = s.crawl_run_id AND ap.snapshot_id = s.snapshot_id
  LEFT JOIN capture_counts cc ON cc.crawl_run_id = s.crawl_run_id
  GROUP BY s.crawl_run_id, s.snapshot_id, s.snapshot_batch_id, s.source_snapshot_manifest_sha256,
       s.crawl_manifest_sha256, s.crawl_manifest_file_sha256, s.crawl_manifest_file_bytes,
       s.expected_page_count, s.expected_alias_count,
       s.expected_passage_count, s.expected_capture_count, s.metadata_variant_count,
       s.manifest_valid, cc.capture_event_count, cc.distinct_task_count, cc.missing_task_count
), snapshot_rows_out AS (
  SELECT crawl_run_id, snapshot_id, snapshot_batch_id, source_snapshot_manifest_sha256,
         crawl_manifest_sha256, crawl_manifest_file_sha256, crawl_manifest_file_bytes,
         expected_page_count, expected_alias_count, expected_passage_count,
         expected_capture_count, metadata_variant_count, manifest_valid,
         capture_event_count, distinct_task_count, missing_task_count,
         matching_publication_event_count, publication_event_seen_count,
         CASE WHEN metadata_variant_count != 1 THEN 'snapshot_metadata_conflict'
              WHEN manifest_valid != 1 THEN 'snapshot_manifest_mismatch'
              WHEN matching_publication_event_count = 0 AND publication_event_seen_count > 0
                THEN 'publication_binding_mismatch'
              WHEN matching_publication_event_count = 0 THEN 'awaiting_publication_event'
              WHEN capture_event_count < expected_capture_count THEN 'awaiting_capture_events'
              WHEN capture_event_count > expected_capture_count
                   OR distinct_task_count != capture_event_count OR missing_task_count > 0
                THEN 'capture_event_count_mismatch'
              ELSE 'authoritative' END AS authority_status
  FROM snapshot_status
), publication_without_snapshot AS (
  SELECT p.crawl_run_id, p.snapshot_id, CAST(NULL AS STRING) AS snapshot_batch_id,
         p.snapshot_sha AS source_snapshot_manifest_sha256, CAST(NULL AS STRING) AS crawl_manifest_sha256,
         p.crawl_sha AS crawl_manifest_file_sha256, p.crawl_bytes AS crawl_manifest_file_bytes,
         CAST(NULL AS BIGINT) AS expected_page_count, CAST(NULL AS BIGINT) AS expected_alias_count,
         CAST(NULL AS BIGINT) AS expected_passage_count, CAST(NULL AS BIGINT) AS expected_capture_count,
         CAST(NULL AS BIGINT) AS metadata_variant_count, CAST(NULL AS INT) AS manifest_valid,
         COALESCE(cc.capture_event_count, 0) AS capture_event_count,
         COALESCE(cc.distinct_task_count, 0) AS distinct_task_count,
         COALESCE(cc.missing_task_count, 0) AS missing_task_count,
         CAST(0 AS BIGINT) AS matching_publication_event_count,
         COUNT(DISTINCT p.event_id) AS publication_event_seen_count,
         'awaiting_validated_snapshot_batch' AS authority_status
  FROM all_publications p
  LEFT JOIN snapshots s ON s.crawl_run_id = p.crawl_run_id AND s.snapshot_id = p.snapshot_id
  LEFT JOIN capture_counts cc ON cc.crawl_run_id = p.crawl_run_id
  WHERE s.snapshot_id IS NULL
  GROUP BY p.crawl_run_id, p.snapshot_id, p.snapshot_sha, p.crawl_sha, p.crawl_bytes,
       cc.capture_event_count, cc.distinct_task_count, cc.missing_task_count
), provisional_runs AS (
  SELECT b.crawl_run_id, CAST(NULL AS STRING) AS snapshot_id,
         CAST(NULL AS STRING) AS snapshot_batch_id, CAST(NULL AS STRING) AS source_snapshot_manifest_sha256,
         CAST(NULL AS STRING) AS crawl_manifest_sha256, CAST(NULL AS STRING) AS crawl_manifest_file_sha256,
         CAST(NULL AS BIGINT) AS crawl_manifest_file_bytes, CAST(NULL AS BIGINT) AS expected_page_count,
         CAST(NULL AS BIGINT) AS expected_alias_count, CAST(NULL AS BIGINT) AS expected_passage_count,
         CAST(NULL AS BIGINT) AS expected_capture_count,
         CAST(NULL AS BIGINT) AS metadata_variant_count, CAST(NULL AS INT) AS manifest_valid,
         CAST(0 AS BIGINT) AS capture_event_count,
         CAST(0 AS BIGINT) AS distinct_task_count, CAST(0 AS BIGINT) AS missing_task_count,
         CAST(0 AS BIGINT) AS matching_publication_event_count, CAST(0 AS BIGINT) AS publication_event_seen_count,
         'provisional_no_validated_snapshot' AS authority_status
  FROM {event_batches} b
  LEFT JOIN snapshots s ON s.crawl_run_id = b.crawl_run_id
  LEFT JOIN all_publications p ON p.crawl_run_id = b.crawl_run_id
  WHERE s.snapshot_id IS NULL AND p.snapshot_id IS NULL
  GROUP BY b.crawl_run_id
)
SELECT * FROM snapshot_rows_out
UNION ALL SELECT * FROM publication_without_snapshot
UNION ALL SELECT * FROM provisional_runs""")

spark.sql(f"""CREATE OR REPLACE VIEW {authoritative_pages} AS
SELECT r.crawl_run_id, p.*
FROM {page_history} p JOIN {reconciliation} r
  ON r.snapshot_id = p.snapshot_id AND r.authority_status = 'authoritative'""")
spark.sql(f"""CREATE OR REPLACE VIEW {authoritative_aliases} AS
SELECT r.crawl_run_id, a.*
FROM {alias_history} a JOIN {reconciliation} r
  ON r.snapshot_id = a.snapshot_id AND r.authority_status = 'authoritative'""")

pages_schema = "ARRAY<STRUCT<requested_url:STRING,final_url:STRING,source_id:STRING,page_id:STRING,admission_status:STRING,capture_id:STRING,selected_capture_id:STRING,selection_reason:STRING,alias_of:STRING,body_sha256:STRING,content_sha256:STRING>>"
spark.sql(f"""CREATE OR REPLACE VIEW {capture_reconciliation} AS
WITH capture_events AS (
  SELECT DISTINCT crawl_run_id, event_id,
         CAST(GET_JSON_OBJECT(record_json, '$.data.task_id') AS BIGINT) AS task_id,
         source_id, GET_JSON_OBJECT(record_json, '$.data.content_sha256') AS content_sha256,
         GET_JSON_OBJECT(record_json, '$.data.body_artifact.sha256') AS body_sha256
  FROM {complete_events} WHERE event_type = 'capture_committed'
), manifests AS (
  SELECT DISTINCT b.snapshot_id, GET_JSON_OBJECT(c.record_json, '$.run_id') AS crawl_run_id,
         p.source_id, p.requested_url, p.page_id AS manifest_page_id, p.capture_id,
         p.selected_capture_id, p.admission_status, p.selection_reason, p.alias_of,
         p.body_sha256, p.content_sha256
  FROM {ingestion_batches} b
  JOIN {silver} c ON c.batch_id = b.batch_id AND c.snapshot_id = b.snapshot_id
                 AND c.record_type = 'crawl_manifest'
  JOIN {reconciliation} r ON r.snapshot_id = b.snapshot_id
                         AND r.crawl_run_id = GET_JSON_OBJECT(c.record_json, '$.run_id')
                         AND r.authority_status = 'authoritative'
  LATERAL VIEW EXPLODE(FROM_JSON(GET_JSON_OBJECT(c.record_json, '$.pages'), '{pages_schema}')) x AS p
  WHERE b.status = 'complete'
    AND b.source_snapshot_manifest_sha256 = r.source_snapshot_manifest_sha256
    AND b.crawl_manifest_sha256 = r.crawl_manifest_sha256
), manifest_outcomes AS (
  SELECT DISTINCT m.*,
         COALESCE(m.alias_of, selected.alias_of, selected.manifest_page_id, m.manifest_page_id)
           AS representative_page_id
  FROM manifests m
  LEFT JOIN manifests selected ON selected.crawl_run_id = m.crawl_run_id
       AND selected.snapshot_id = m.snapshot_id AND selected.source_id = m.source_id
       AND selected.capture_id = m.selected_capture_id
), ops AS (
  SELECT DISTINCT b.snapshot_id, GET_JSON_OBJECT(c.record_json, '$.run_id') AS crawl_run_id,
         CAST(GET_JSON_OBJECT(o.record_json, '$.data.task_id') AS BIGINT) AS task_id,
         GET_JSON_OBJECT(o.record_json, '$.data.source_id') AS source_id,
         GET_JSON_OBJECT(o.record_json, '$.data.url') AS requested_url,
         GET_JSON_OBJECT(o.record_json, '$.data.body_sha256') AS body_sha256
  FROM {ingestion_batches} b
  JOIN {silver} c ON c.batch_id = b.batch_id AND c.snapshot_id = b.snapshot_id
                 AND c.record_type = 'crawl_manifest'
  JOIN {reconciliation} r ON r.snapshot_id = b.snapshot_id
                         AND r.crawl_run_id = GET_JSON_OBJECT(c.record_json, '$.run_id')
                         AND r.authority_status = 'authoritative'
  JOIN {silver} o ON o.batch_id = b.batch_id AND o.snapshot_id = b.snapshot_id
                 AND o.record_type = 'operational'
  WHERE b.status = 'complete'
    AND b.source_snapshot_manifest_sha256 = r.source_snapshot_manifest_sha256
    AND b.crawl_manifest_sha256 = r.crawl_manifest_sha256
    AND GET_JSON_OBJECT(o.record_json, '$.event') = 'capture.accepted'
    AND GET_JSON_OBJECT(o.record_json, '$.outcome') = 'success'
    AND GET_JSON_OBJECT(o.record_json, '$.data.capture_received') = 'true'
), mapped AS (
  SELECT DISTINCT e.crawl_run_id, r.snapshot_id, e.event_id, m.capture_id,
         m.source_id, m.requested_url, m.body_sha256, m.manifest_page_id,
         m.selected_capture_id, m.admission_status,
         m.selection_reason, m.alias_of, m.representative_page_id
  FROM capture_events e
  JOIN {reconciliation} r ON r.crawl_run_id = e.crawl_run_id
                         AND r.authority_status = 'authoritative'
  JOIN ops o ON o.crawl_run_id = e.crawl_run_id AND o.snapshot_id = r.snapshot_id
            AND o.task_id = e.task_id AND o.source_id = e.source_id
            AND o.body_sha256 = e.body_sha256
  JOIN manifest_outcomes m ON m.crawl_run_id = e.crawl_run_id AND m.snapshot_id = r.snapshot_id
                  AND m.source_id = e.source_id AND m.requested_url = o.requested_url
                  AND m.body_sha256 = e.body_sha256 AND m.body_sha256 = o.body_sha256
                  AND m.content_sha256 = e.content_sha256
  JOIN {authoritative_pages} hp ON hp.snapshot_id = m.snapshot_id
       AND hp.source_id = m.source_id AND hp.page_id = m.representative_page_id
  LEFT JOIN {authoritative_aliases} ha ON ha.snapshot_id = m.snapshot_id
       AND ha.source_id = m.source_id AND ha.alias_page_id = m.manifest_page_id
       AND ha.representative_page_id = m.representative_page_id
  WHERE m.admission_status != 'exact-alias' OR ha.alias_page_id IS NOT NULL
), mapping_summary AS (
  SELECT crawl_run_id, snapshot_id, event_id,
         COUNT(DISTINCT TO_JSON(NAMED_STRUCT('capture_id', capture_id, 'source_id', source_id,
           'requested_url', requested_url, 'body_sha256', body_sha256))) AS candidate_capture_count,
         MAX(capture_id) AS capture_id, MAX(source_id) AS source_id, MAX(requested_url) AS requested_url,
         MAX(manifest_page_id) AS manifest_page_id, MAX(selected_capture_id) AS selected_capture_id,
         MAX(admission_status) AS admission_status, MAX(selection_reason) AS selection_reason,
         MAX(alias_of) AS alias_of, MAX(representative_page_id) AS representative_page_id
  FROM mapped GROUP BY crawl_run_id, snapshot_id, event_id
)
SELECT e.crawl_run_id, r.snapshot_id, e.event_id, e.task_id, e.source_id,
       e.body_sha256, e.content_sha256, m.capture_id, m.requested_url, m.manifest_page_id,
       m.selected_capture_id, m.admission_status, m.selection_reason,
       m.alias_of, m.representative_page_id, m.candidate_capture_count,
       CASE WHEN r.authority_status IS NULL OR r.authority_status != 'authoritative'
              THEN 'provisional_snapshot_or_capture_gate'
            WHEN m.candidate_capture_count IS NULL THEN 'provisional_unmatched'
            WHEN m.candidate_capture_count != 1 THEN 'provisional_ambiguous'
            WHEN m.admission_status = 'admitted' THEN 'authoritative_admitted'
            WHEN m.admission_status = 'exact-alias' THEN 'authoritative_alias'
            WHEN m.admission_status = 'superseded-capture' THEN 'authoritative_superseded'
            ELSE 'provisional_unknown_admission' END AS reconciliation_status,
       CASE WHEN m.candidate_capture_count = 1 THEN m.representative_page_id END AS canonical_page_id
FROM capture_events e
LEFT JOIN {reconciliation} r ON r.crawl_run_id = e.crawl_run_id
LEFT JOIN mapping_summary m ON m.crawl_run_id = e.crawl_run_id
                           AND m.snapshot_id = r.snapshot_id AND m.event_id = e.event_id""")

spark.sql(f"""CREATE OR REPLACE VIEW {rejected_tasks} AS
SELECT DISTINCT r.crawl_run_id, r.snapshot_id, o.record_id AS operational_record_id,
       CAST(GET_JSON_OBJECT(o.record_json, '$.data.task_id') AS BIGINT) AS task_id,
       GET_JSON_OBJECT(o.record_json, '$.data.source_id') AS source_id,
       GET_JSON_OBJECT(o.record_json, '$.data.url') AS requested_url,
       GET_JSON_OBJECT(o.record_json, '$.data.rejection_reason') AS rejection_reason,
       CASE WHEN r.authority_status = 'authoritative' THEN 'authoritative_rejected_task'
            ELSE 'provisional_rejected_task' END AS reconciliation_status
FROM {reconciliation} r
JOIN {ingestion_batches} b ON b.snapshot_id = r.snapshot_id AND b.status = 'complete'
JOIN {silver} c ON c.batch_id = b.batch_id AND c.snapshot_id = b.snapshot_id
               AND c.record_type = 'crawl_manifest'
JOIN {silver} o ON o.batch_id = b.batch_id AND o.snapshot_id = b.snapshot_id
               AND o.record_type = 'operational'
WHERE GET_JSON_OBJECT(c.record_json, '$.run_id') = r.crawl_run_id
  AND b.source_snapshot_manifest_sha256 = r.source_snapshot_manifest_sha256
  AND b.crawl_manifest_sha256 = r.crawl_manifest_sha256
  AND GET_JSON_OBJECT(o.record_json, '$.event') = 'page.rejected'""")
