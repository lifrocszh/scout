-- Run after snapshot_ingest.py and crawl_event_ingest.py have published their
-- history and provisional-event views. Replace workspace.default as needed.
-- One input row freezes policy bounds and exact evidence references per
-- (generation_id, source_id); recommendation views are deterministic for it.
SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS workspace.default.scout_recrawl_recommendation_inputs (
  generation_id STRING NOT NULL,
  source_id STRING NOT NULL,
  generated_at_utc STRING NOT NULL,
  expires_at_utc STRING NOT NULL,
  expected_policy_version STRING NOT NULL,
  minimum_interval_hours DOUBLE NOT NULL,
  current_interval_hours DOUBLE NOT NULL,
  maximum_interval_hours DOUBLE NOT NULL,
  max_adjustment_fraction DOUBLE NOT NULL,
  minimum_completed_snapshots INT NOT NULL,
  minimum_comparable_page_observations INT NOT NULL,
  maximum_evidence_age_days INT NOT NULL,
  maximum_provisional_event_age_seconds INT NOT NULL,
  completed_snapshot_ids_json STRING NOT NULL,
  provisional_run_id STRING,
  provisional_event_ids_json STRING NOT NULL
) USING DELTA;

CREATE OR REPLACE VIEW workspace.default.gold_recrawl_recommendation_candidates_internal AS
WITH raw_inputs AS (
  SELECT *,
         FROM_JSON(completed_snapshot_ids_json, 'ARRAY<STRING>') AS snapshot_ids,
         FROM_JSON(provisional_event_ids_json, 'ARRAY<STRING>') AS provisional_event_ids,
         TO_TIMESTAMP(generated_at_utc) AS generated_at,
         TO_TIMESTAMP(expires_at_utc) AS expires_at,
         COUNT(*) OVER (PARTITION BY generation_id, source_id) AS duplicate_input_count
  FROM workspace.default.scout_recrawl_recommendation_inputs
),
inputs AS (
  SELECT *,
         SORT_ARRAY(ARRAY_DISTINCT(snapshot_ids)) AS sorted_snapshot_ids,
         SORT_ARRAY(ARRAY_DISTINCT(provisional_event_ids)) AS sorted_provisional_event_ids,
         SIZE(snapshot_ids) AS requested_snapshot_count,
         SIZE(provisional_event_ids) AS requested_provisional_event_count
  FROM raw_inputs
),
complete_source_rows AS (
  SELECT h.*,
         ROW_NUMBER() OVER (
           PARTITION BY h.snapshot_id, h.source_id
           ORDER BY h.batch_id DESC
         ) AS batch_rank
  FROM workspace.default.scout_source_observations h
  JOIN workspace.default.scout_ingestion_batches b
    ON b.batch_id = h.batch_id AND b.snapshot_id = h.snapshot_id
   AND b.status = 'complete'
),
validated_sources AS (
  SELECT snapshot_id, source_id, batch_id, scope_sha256,
         extraction_contract_sha256, observed_at_utc,
         source_snapshot_manifest_sha256, crawl_manifest_sha256,
         stop_reason, failed_count, rejected_count
  FROM complete_source_rows WHERE batch_rank = 1
),
all_snapshot_refs AS (
  SELECT DISTINCT snapshot_id
  FROM validated_sources
),
requested_snapshots AS (
  SELECT i.generation_id, i.source_id, i.requested_snapshot_count, i.snapshot_ids,
         requested.requested_snapshot_id
  FROM inputs i
  LATERAL VIEW OUTER EXPLODE(i.snapshot_ids) requested AS requested_snapshot_id
),
resolved_snapshots AS (
  SELECT i.generation_id, i.source_id, i.requested_snapshot_count,
         i.requested_snapshot_id,
         s.snapshot_id, s.scope_sha256, s.extraction_contract_sha256,
         s.observed_at_utc, s.source_snapshot_manifest_sha256,
         s.crawl_manifest_sha256, s.stop_reason, s.failed_count,
         s.rejected_count,
         CASE WHEN a.snapshot_id IS NULL THEN 0 ELSE 1 END AS snapshot_exists
  FROM requested_snapshots i
  LEFT JOIN validated_sources s
    ON s.snapshot_id = i.requested_snapshot_id AND s.source_id = i.source_id
  LEFT JOIN all_snapshot_refs a ON a.snapshot_id = i.requested_snapshot_id
),
snapshot_stats AS (
  SELECT generation_id, source_id,
         COUNT(DISTINCT requested_snapshot_id) AS requested_snapshot_count,
         COUNT(DISTINCT snapshot_id) AS validated_snapshot_count,
         COUNT(DISTINCT CASE WHEN snapshot_exists = 1 THEN requested_snapshot_id END)
           AS existing_snapshot_count,
         SUM(CASE WHEN snapshot_exists = 1 AND snapshot_id IS NULL
                  THEN 1 ELSE 0 END) AS wrong_source_snapshot_count,
         COUNT(DISTINCT scope_sha256) AS scope_version_count,
         COUNT(DISTINCT extraction_contract_sha256) AS extraction_version_count,
         SUM(CASE WHEN snapshot_id IS NOT NULL
                       AND (stop_reason <> 'frontier_exhausted'
                            OR COALESCE(failed_count, -1) <> 0
                            OR COALESCE(rejected_count, -1) <> 0)
                  THEN 1 ELSE 0 END) AS incomplete_snapshot_count,
         MIN(TO_TIMESTAMP(observed_at_utc)) AS oldest_observed_at,
         MAX(TO_TIMESTAMP(observed_at_utc)) AS latest_observed_at,
         MIN(scope_sha256) AS scope_sha256,
         MIN(extraction_contract_sha256) AS extraction_contract_sha256
  FROM resolved_snapshots
  GROUP BY generation_id, source_id
),
snapshot_refs AS (
  SELECT generation_id, source_id,
         SORT_ARRAY(COLLECT_SET(NAMED_STRUCT(
           'snapshot_id', snapshot_id,
           'source_snapshot_manifest_sha256', source_snapshot_manifest_sha256,
           'crawl_manifest_sha256', crawl_manifest_sha256,
           'observed_at_utc', observed_at_utc
         ))) AS snapshot_refs
  FROM resolved_snapshots
  WHERE snapshot_id IS NOT NULL
  GROUP BY generation_id, source_id
),
change_stats AS (
  SELECT i.generation_id, i.source_id,
         COUNT(DISTINCT CASE WHEN c.change_status IN ('changed', 'unchanged')
                             THEN CONCAT(c.snapshot_id, '/', c.page_id) END)
           AS comparable_page_observations,
         COUNT(DISTINCT CASE WHEN c.change_status = 'changed'
                             THEN CONCAT(c.snapshot_id, '/', c.page_id) END)
           AS changed_page_observations
  FROM requested_snapshots i
  JOIN workspace.default.gold_page_change_observations c
    ON c.snapshot_id = i.requested_snapshot_id AND c.source_id = i.source_id
   AND ARRAY_CONTAINS(i.snapshot_ids, c.previous_snapshot_id)
  GROUP BY i.generation_id, i.source_id
),
accepted_events AS (
  SELECT DISTINCT r.crawl_run_id, r.record_id AS event_id, r.source_id,
         r.event_type,
         COALESCE(r.event_created_at_utc,
           CASE r.event_type
             WHEN 'capture_committed' THEN GET_JSON_OBJECT(r.record_json, '$.data.committed_at_utc')
             WHEN 'crawl_completed' THEN GET_JSON_OBJECT(r.record_json, '$.data.completed_at_utc')
             ELSE GET_JSON_OBJECT(r.record_json, '$.data.published_at_utc')
           END) AS event_created_at_utc
  FROM workspace.default.scout_crawl_event_records r
  JOIN workspace.default.scout_crawl_event_batches b
    ON b.batch_id = r.batch_id AND b.status = 'complete'
  WHERE r.record_type = 'crawl_event'
    AND r.disposition IN ('accepted', 'duplicate')
),
known_sources AS (
  SELECT source_id FROM validated_sources
  UNION
  SELECT source_id FROM accepted_events WHERE source_id IS NOT NULL
),
all_event_refs AS (
  SELECT DISTINCT event_id FROM accepted_events
),
requested_events AS (
  SELECT i.generation_id, i.source_id, i.provisional_run_id, i.generated_at,
         requested.requested_event_id
  FROM inputs i
  LATERAL VIEW OUTER EXPLODE(i.provisional_event_ids) requested AS requested_event_id
),
resolved_events AS (
  SELECT i.generation_id, i.source_id, i.provisional_run_id,
         i.generated_at, i.requested_event_id,
         e.event_id AS accepted_event_id,
         run_owner.event_id AS run_owner_event_id,
         run_owner.event_type AS run_owner_event_type,
         run_owner.source_id AS run_owner_source_id,
         e.crawl_run_id AS event_run_id, e.source_id AS event_source_id,
         e.event_type, e.event_created_at_utc,
         CASE WHEN a.event_id IS NULL THEN 0 ELSE 1 END AS event_exists
  FROM requested_events i
  LEFT JOIN accepted_events e
    ON e.event_id = i.requested_event_id
   AND e.crawl_run_id = i.provisional_run_id
   AND e.source_id = i.source_id
   AND e.event_type = 'capture_committed'
   AND TO_TIMESTAMP(e.event_created_at_utc) <= i.generated_at
  LEFT JOIN accepted_events run_owner
    ON run_owner.event_id = i.requested_event_id
   AND run_owner.crawl_run_id = i.provisional_run_id
  LEFT JOIN all_event_refs a ON a.event_id = i.requested_event_id
),
provisional_stats AS (
  SELECT generation_id, source_id,
         COUNT(DISTINCT requested_event_id) AS requested_event_count,
         COUNT(DISTINCT accepted_event_id) AS accepted_capture_event_count,
         COUNT(DISTINCT CASE WHEN event_exists = 1 THEN requested_event_id END)
           AS existing_event_count,
         SUM(CASE WHEN event_exists = 1 AND
                       (run_owner_event_id IS NULL OR
                        (run_owner_event_type = 'capture_committed' AND
                         (run_owner_source_id IS NULL OR run_owner_source_id <> source_id)))
                  THEN 1 ELSE 0 END) AS wrong_source_event_count,
         SUM(CASE WHEN run_owner_event_id IS NOT NULL
                       AND run_owner_event_type <> 'capture_committed'
                  THEN 1 ELSE 0 END) AS unsupported_event_type_count,
         MAX(TO_TIMESTAMP(event_created_at_utc)) AS latest_capture_event_at
  FROM resolved_events
  GROUP BY generation_id, source_id
),
run_state AS (
  SELECT i.generation_id, i.source_id, i.provisional_run_id,
         MAX(TO_TIMESTAMP(e.event_created_at_utc)) AS latest_run_event_at,
         MAX(CASE WHEN e.event_type IN ('crawl_completed', 'corpus_snapshot_published')
                  THEN 1 ELSE 0 END) AS terminal_event_seen
  FROM inputs i
  LEFT JOIN accepted_events e
    ON e.crawl_run_id = i.provisional_run_id
   AND TO_TIMESTAMP(e.event_created_at_utc) <= i.generated_at
  GROUP BY i.generation_id, i.source_id, i.provisional_run_id
),
combined AS (
  SELECT i.*, s.requested_snapshot_count AS resolved_snapshot_count,
         s.validated_snapshot_count, s.existing_snapshot_count,
         s.scope_version_count, s.extraction_version_count,
         s.incomplete_snapshot_count, s.oldest_observed_at,
         s.latest_observed_at, s.scope_sha256,
         s.extraction_contract_sha256,
         s.wrong_source_snapshot_count,
         COALESCE(sr.snapshot_refs, FROM_JSON('[]', 'ARRAY<STRUCT<snapshot_id:STRING,source_snapshot_manifest_sha256:STRING,crawl_manifest_sha256:STRING,observed_at_utc:STRING>>')) AS snapshot_refs,
         COALESCE(c.comparable_page_observations, 0) AS comparable_page_observations,
         COALESCE(c.changed_page_observations, 0) AS changed_page_observations,
         COALESCE(ps.requested_event_count, 0) AS resolved_event_count,
         COALESCE(ps.accepted_capture_event_count, 0) AS accepted_capture_event_count,
         COALESCE(ps.existing_event_count, 0) AS existing_event_count,
         COALESCE(ps.wrong_source_event_count, 0) AS wrong_source_event_count,
         COALESCE(ps.unsupported_event_type_count, 0) AS unsupported_event_type_count,
         ps.latest_capture_event_at,
         rs.latest_run_event_at, COALESCE(rs.terminal_event_seen, 0) AS terminal_event_seen,
         CASE WHEN k.source_id IS NULL THEN FALSE ELSE TRUE END AS source_in_evidence_scope,
         i.snapshot_ids AS parsed_snapshot_ids,
         i.provisional_event_ids AS parsed_provisional_event_ids
  FROM inputs i
  LEFT JOIN snapshot_stats s USING (generation_id, source_id)
  LEFT JOIN snapshot_refs sr USING (generation_id, source_id)
  LEFT JOIN change_stats c USING (generation_id, source_id)
  LEFT JOIN provisional_stats ps USING (generation_id, source_id)
  LEFT JOIN run_state rs USING (generation_id, source_id, provisional_run_id)
  LEFT JOIN known_sources k ON k.source_id = i.source_id
),
checked AS (
  SELECT *,
    CASE WHEN generated_at_utc RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
           AND expires_at_utc RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
           AND generated_at IS NOT NULL AND expires_at IS NOT NULL
           AND expires_at > generated_at
           AND generation_id IS NOT NULL AND LENGTH(TRIM(generation_id)) > 0
           AND source_id IS NOT NULL AND LENGTH(TRIM(source_id)) > 0
           AND expected_policy_version IS NOT NULL AND LENGTH(TRIM(expected_policy_version)) > 0
           AND duplicate_input_count = 1
           AND minimum_interval_hours > 0
           AND current_interval_hours BETWEEN minimum_interval_hours AND maximum_interval_hours
           AND maximum_interval_hours >= minimum_interval_hours
           AND max_adjustment_fraction BETWEEN 0 AND 0.5
           AND minimum_completed_snapshots BETWEEN 3 AND 5
           AND minimum_comparable_page_observations > 0
           AND maximum_evidence_age_days > 0
           AND maximum_provisional_event_age_seconds > 0
           AND snapshot_ids IS NOT NULL AND SIZE(snapshot_ids) <= 5
           AND provisional_event_ids IS NOT NULL AND SIZE(provisional_event_ids) <= 1000
           AND SIZE(ARRAY_DISTINCT(snapshot_ids)) = SIZE(snapshot_ids)
           AND SIZE(ARRAY_DISTINCT(provisional_event_ids)) = SIZE(provisional_event_ids)
           AND ((provisional_run_id IS NULL AND SIZE(provisional_event_ids) = 0)
                OR (provisional_run_id IS NOT NULL AND SIZE(snapshot_ids) = 0
                    AND SIZE(provisional_event_ids) > 0))
           AND (provisional_run_id IS NULL OR provisional_run_id RLIKE '^run-[0-9a-f]{64}$')
           AND (provisional_run_id IS NOT NULL OR SIZE(provisional_event_ids) = 0)
           AND FORALL(snapshot_ids, x -> x RLIKE '^snapshot-[0-9a-f]{64}$')
           AND FORALL(provisional_event_ids, x -> x RLIKE '^[A-Za-z0-9._:-]{1,512}$')
           AND minimum_interval_hours = ROUND(minimum_interval_hours, 3)
           AND current_interval_hours = ROUND(current_interval_hours, 3)
           AND maximum_interval_hours = ROUND(maximum_interval_hours, 3)
           AND max_adjustment_fraction = ROUND(max_adjustment_fraction, 6)
         THEN TRUE ELSE FALSE END AS valid_input,
    CASE WHEN provisional_run_id IS NOT NULL
           AND SIZE(provisional_event_ids) > 0
           AND resolved_event_count = SIZE(provisional_event_ids)
           AND accepted_capture_event_count = SIZE(provisional_event_ids)
           AND existing_event_count = SIZE(provisional_event_ids)
           AND terminal_event_seen = 0
           AND latest_run_event_at IS NOT NULL
           AND latest_run_event_at <= generated_at
           AND UNIX_TIMESTAMP(generated_at) - UNIX_TIMESTAMP(latest_run_event_at)
                 BETWEEN 0 AND maximum_provisional_event_age_seconds
         THEN TRUE ELSE FALSE END AS provisional_evidence_valid,
    CASE WHEN validated_snapshot_count >= minimum_completed_snapshots
           AND requested_snapshot_count = SIZE(snapshot_ids)
           AND validated_snapshot_count = SIZE(snapshot_ids)
           AND existing_snapshot_count = SIZE(snapshot_ids)
           AND scope_version_count = 1 AND extraction_version_count = 1
           AND COALESCE(incomplete_snapshot_count, 0) = 0
           AND comparable_page_observations >= minimum_comparable_page_observations
           AND latest_observed_at <= generated_at
           AND DATEDIFF(TO_DATE(generated_at), TO_DATE(latest_observed_at))
                 BETWEEN 0 AND maximum_evidence_age_days
         THEN TRUE ELSE FALSE END AS completed_evidence_valid
  FROM combined
),
proposal_base AS (
  SELECT *,
         CASE WHEN completed_evidence_valid AND comparable_page_observations > 0
              THEN CAST(changed_page_observations AS DOUBLE) / comparable_page_observations
              ELSE NULL END AS change_fraction,
    CASE WHEN valid_input AND provisional_run_id IS NOT NULL
                    AND provisional_evidence_valid THEN 'provisional'
              WHEN valid_input AND provisional_run_id IS NULL
                    AND completed_evidence_valid THEN 'completed_history'
              ELSE 'unsupported' END AS evidence_status
  FROM checked
),
proposal_target AS (
  SELECT *,
         CASE
           WHEN evidence_status = 'provisional' THEN current_interval_hours
           WHEN evidence_status <> 'completed_history' THEN current_interval_hours
           WHEN change_fraction >= 0.25 THEN current_interval_hours * 0.5
           WHEN change_fraction <= 0.02 THEN current_interval_hours * 1.5
           ELSE current_interval_hours
         END AS raw_target_hours
  FROM proposal_base
),
proposal_capped AS (
  SELECT *,
         GREATEST(minimum_interval_hours,
           LEAST(maximum_interval_hours,
             GREATEST(current_interval_hours * (1 - max_adjustment_fraction),
               LEAST(current_interval_hours * (1 + max_adjustment_fraction), raw_target_hours))
           )) AS capped_target_hours
  FROM proposal_target
),
proposal_quantized AS (
  SELECT *,
         CASE WHEN NOT valid_input THEN NULL
              WHEN raw_target_hours < current_interval_hours
                THEN CAST(CEIL(CAST(capped_target_hours AS DECIMAL(18,6)) * 1000) / 1000 AS DOUBLE)
              WHEN raw_target_hours > current_interval_hours
                THEN CAST(FLOOR(CAST(capped_target_hours AS DECIMAL(18,6)) * 1000) / 1000 AS DOUBLE)
              ELSE current_interval_hours END AS proposed_interval_hours
  FROM proposal_capped
),
recommendation_fields AS (
  SELECT *,
         ROUND(proposed_interval_hours - current_interval_hours, 3) AS adjustment_hours,
         CASE WHEN evidence_status = 'completed_history'
                    AND validated_snapshot_count >= 4
                    AND comparable_page_observations >= 100 THEN 'high'
              WHEN evidence_status = 'completed_history' THEN 'medium'
              WHEN evidence_status = 'provisional' THEN 'provisional'
              ELSE 'low' END AS confidence,
         FILTER(ARRAY(
           CASE WHEN evidence_status = 'completed_history'
                THEN 'deterministic change-fraction heuristic over complete, scope-compatible snapshots' END,
           CASE WHEN evidence_status = 'provisional'
                THEN 'active committed captures are provisional; hold current interval until validated Page comparisons exist' END,
           CASE WHEN evidence_status = 'unsupported'
                THEN 'insufficient, stale, incomplete, or incomparable evidence; hold current interval' END,
           CASE WHEN evidence_status = 'completed_history' AND change_fraction >= 0.25
                THEN 'at least 25% of comparable Page observations changed; shorten target by 50% before policy caps' END,
           CASE WHEN evidence_status = 'completed_history' AND change_fraction <= 0.02
                THEN 'at most 2% of comparable Page observations changed; lengthen target by 50% before policy caps' END,
           CASE WHEN evidence_status = 'completed_history' AND change_fraction > 0.02 AND change_fraction < 0.25
                THEN 'change fraction is between thresholds; retain current interval' END,
           CASE WHEN proposed_interval_hours <> raw_target_hours
                THEN 'policy bounds, adjustment fraction, or safe 0.001-hour rounding capped the raw target' END
         ), reason -> reason IS NOT NULL) AS reasons
  FROM proposal_quantized
),
canonical_key AS (
  SELECT *,
    TO_JSON(NAMED_STRUCT(
      'schema_version', 'scout.recrawl-recommendation.v1',
      'generation_id', generation_id,
      'source_id', source_id,
      'generated_at_utc', generated_at_utc,
      'expires_at_utc', expires_at_utc,
      'expected_policy_version', expected_policy_version,
      'interval_hours', NAMED_STRUCT(
        'minimum', CAST(CAST(minimum_interval_hours AS DECIMAL(18,3)) AS STRING),
        'current', CAST(CAST(current_interval_hours AS DECIMAL(18,3)) AS STRING),
        'maximum', CAST(CAST(maximum_interval_hours AS DECIMAL(18,3)) AS STRING),
        'proposed', COALESCE(CAST(CAST(proposed_interval_hours AS DECIMAL(18,3)) AS STRING), ''),
        'adjustment', COALESCE(CAST(CAST(adjustment_hours AS DECIMAL(18,3)) AS STRING), ''),
        'adjustment_fraction', COALESCE(CAST(CAST(ROUND(adjustment_hours / current_interval_hours, 6) AS DECIMAL(18,6)) AS STRING), '')
      ),
      'recommendation_settings', NAMED_STRUCT(
        'max_adjustment_fraction', CAST(CAST(max_adjustment_fraction AS DECIMAL(18,6)) AS STRING),
        'minimum_completed_snapshots', CAST(minimum_completed_snapshots AS STRING),
        'minimum_comparable_page_observations', CAST(minimum_comparable_page_observations AS STRING),
        'maximum_evidence_age_days', CAST(maximum_evidence_age_days AS STRING),
        'maximum_provisional_event_age_seconds', CAST(maximum_provisional_event_age_seconds AS STRING),
        'change_fraction_decrease_threshold', '0.250000',
        'change_fraction_increase_threshold', '0.020000',
        'decrease_target_multiplier', '0.500000',
        'increase_target_multiplier', '1.500000',
        'high_confidence_minimum_snapshots', '4',
        'high_confidence_minimum_comparable_page_observations', '100'
      ),
      'evidence', NAMED_STRUCT(
        'status', evidence_status,
        'snapshot_ids', CONCAT_WS(',', sorted_snapshot_ids),
        'snapshot_refs', snapshot_refs,
        'provisional_run_id', COALESCE(provisional_run_id, ''),
        'provisional_event_ids', CONCAT_WS(',', sorted_provisional_event_ids),
        'scope_sha256', COALESCE(scope_sha256, ''),
        'extraction_contract_sha256', COALESCE(extraction_contract_sha256, ''),
        'latest_observed_at_utc', COALESCE(DATE_FORMAT(latest_observed_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), ''),
        'latest_capture_event_at_utc', COALESCE(DATE_FORMAT(latest_capture_event_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), ''),
        'latest_run_event_at_utc', COALESCE(DATE_FORMAT(latest_run_event_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), ''),
        'comparable_page_observations', CAST(comparable_page_observations AS STRING),
        'changed_page_observations', CAST(changed_page_observations AS STRING),
        'change_fraction', COALESCE(CAST(CAST(ROUND(change_fraction, 6) AS DECIMAL(18,6)) AS STRING), '')
      )
    ), MAP('ignoreNullFields', 'false')) AS idempotency_basis
  FROM recommendation_fields
),
identified AS (
  SELECT *, CASE WHEN valid_input
                 THEN CONCAT('sha256:', SHA2(idempotency_basis, 256)) END AS idempotency_key
  FROM canonical_key
)
SELECT
  'scout.recrawl-recommendation.v1' AS schema_version,
  idempotency_key AS recommendation_id,
  idempotency_key,
  generation_id,
  source_id,
  generated_at_utc,
  expires_at_utc,
  expected_policy_version,
  minimum_interval_hours,
  current_interval_hours,
  maximum_interval_hours,
  proposed_interval_hours,
  adjustment_hours,
  CASE WHEN adjustment_hours > 0 THEN 'increase'
       WHEN adjustment_hours < 0 THEN 'decrease' ELSE 'hold' END AS adjustment_type,
  CASE WHEN current_interval_hours > 0
       THEN ROUND(adjustment_hours / current_interval_hours, 6) END AS adjustment_fraction,
  evidence_status,
  snapshot_refs,
  provisional_run_id,
  sorted_provisional_event_ids AS provisional_event_ids,
  scope_sha256,
  extraction_contract_sha256,
  DATE_FORMAT(latest_observed_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS latest_observed_at_utc,
  comparable_page_observations,
  changed_page_observations,
  ROUND(change_fraction, 6) AS change_fraction,
  confidence,
  reasons,
  TO_JSON(NAMED_STRUCT(
    'schema_version', 'scout.recrawl-recommendation.v1',
    'recommendation_id', idempotency_key,
    'idempotency_key', idempotency_key,
    'adjustment_type', CASE WHEN adjustment_hours > 0 THEN 'increase'
                            WHEN adjustment_hours < 0 THEN 'decrease' ELSE 'hold' END,
    'idempotency_basis', idempotency_basis,
    'generation_id', generation_id,
    'source_id', source_id,
    'generated_at_utc', generated_at_utc,
    'expires_at_utc', expires_at_utc,
    'expected_policy_version', expected_policy_version,
    'interval_hours', NAMED_STRUCT(
      'minimum', minimum_interval_hours,
      'current', current_interval_hours,
      'maximum', maximum_interval_hours,
      'proposed', proposed_interval_hours,
      'adjustment', adjustment_hours,
      'adjustment_fraction', CASE WHEN current_interval_hours > 0
                                  THEN ROUND(adjustment_hours / current_interval_hours, 6) END
    ),
    'evidence', NAMED_STRUCT(
      'status', evidence_status,
      'snapshot_refs', snapshot_refs,
      'provisional_run_id', provisional_run_id,
      'provisional_event_ids', sorted_provisional_event_ids,
      'scope_sha256', scope_sha256,
      'extraction_contract_sha256', extraction_contract_sha256,
      'latest_observed_at_utc', DATE_FORMAT(latest_observed_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
      'latest_capture_event_at_utc', DATE_FORMAT(latest_capture_event_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
      'latest_run_event_at_utc', DATE_FORMAT(latest_run_event_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
      'comparable_page_observations', comparable_page_observations,
      'changed_page_observations', changed_page_observations,
      'change_fraction', ROUND(change_fraction, 6)
    ),
    'confidence', confidence,
    'reasons', reasons
  ), MAP('ignoreNullFields', 'false')) AS recommendation_json,
  idempotency_basis,
  valid_input, provisional_evidence_valid, completed_evidence_valid,
  generated_at, expires_at, requested_snapshot_count,
  requested_provisional_event_count, maximum_evidence_age_days,
  existing_snapshot_count, validated_snapshot_count, scope_version_count,
  extraction_version_count, incomplete_snapshot_count,
  wrong_source_snapshot_count, resolved_event_count,
  accepted_capture_event_count, existing_event_count,
  wrong_source_event_count, unsupported_event_type_count,
  terminal_event_seen, latest_observed_at, duplicate_input_count,
  source_in_evidence_scope
FROM identified;

CREATE OR REPLACE VIEW workspace.default.gold_recrawl_recommendations AS
SELECT schema_version, recommendation_id, idempotency_key, idempotency_basis,
       generation_id, source_id, generated_at_utc, expires_at_utc,
       expected_policy_version, minimum_interval_hours, current_interval_hours,
       maximum_interval_hours, proposed_interval_hours, adjustment_hours,
       adjustment_type, adjustment_fraction, evidence_status, snapshot_refs,
       provisional_run_id, provisional_event_ids, scope_sha256,
       extraction_contract_sha256, latest_observed_at_utc,
       comparable_page_observations, changed_page_observations,
       change_fraction, confidence, reasons, recommendation_json
FROM workspace.default.gold_recrawl_recommendation_candidates_internal;

CREATE OR REPLACE VIEW workspace.default.gold_recrawl_recommendation_validation AS
WITH classified AS (
  SELECT *,
    CASE
      WHEN valid_input = FALSE THEN 'invalid'
      WHEN expires_at <= CURRENT_TIMESTAMP()
        OR (latest_observed_at IS NOT NULL
            AND DATEDIFF(TO_DATE(CURRENT_TIMESTAMP()), TO_DATE(latest_observed_at)) > maximum_evidence_age_days)
        THEN 'stale'
      WHEN wrong_source_snapshot_count > 0
        THEN 'out_of_scope'
      WHEN wrong_source_event_count > 0
        THEN 'out_of_scope'
      WHEN source_in_evidence_scope = FALSE
        THEN 'out_of_scope'
      WHEN unsupported_event_type_count > 0
        THEN 'unsupported'
      WHEN provisional_run_id IS NOT NULL AND provisional_evidence_valid
        THEN 'provisional_review_required'
      WHEN evidence_status = 'completed_history' AND completed_evidence_valid
        THEN 'valid'
      ELSE 'unsupported'
    END AS validation_status,
    CASE
      WHEN valid_input = FALSE THEN 'invalid input, policy bounds, timestamp, or duplicate generation/source row'
      WHEN expires_at <= CURRENT_TIMESTAMP() THEN 'recommendation expiry has passed'
      WHEN latest_observed_at IS NOT NULL
        AND DATEDIFF(TO_DATE(CURRENT_TIMESTAMP()), TO_DATE(latest_observed_at)) > maximum_evidence_age_days
        THEN 'completed evidence exceeds maximum_evidence_age_days'
      WHEN wrong_source_snapshot_count > 0
        THEN 'one or more snapshot references belong to another Source'
      WHEN wrong_source_event_count > 0
        THEN 'one or more event references belong to another Source or run'
      WHEN source_in_evidence_scope = FALSE
        THEN 'Source ID has no validated snapshot or accepted live-event evidence'
      WHEN unsupported_event_type_count > 0
        THEN 'provisional references include events other than capture_committed'
      WHEN provisional_run_id IS NOT NULL AND provisional_evidence_valid
        THEN 'provisional evidence requires explicit review after Corpus validation'
      WHEN evidence_status = 'completed_history' AND completed_evidence_valid
        THEN 'all selected snapshots are complete, compatible, fresh, and sufficiently comparable'
      WHEN provisional_run_id IS NOT NULL
        THEN 'provisional run is missing, terminal, stale, or lacks accepted committed captures'
      WHEN validated_snapshot_count < requested_snapshot_count
        THEN 'snapshot evidence is unavailable or not a complete ingested batch'
      WHEN scope_version_count > 1 OR extraction_version_count > 1
        THEN 'snapshot evidence crosses a Source scope or extraction-contract boundary'
      WHEN COALESCE(incomplete_snapshot_count, 0) > 0
        THEN 'snapshot evidence includes bounded, failed, or rejected crawl history'
      ELSE 'insufficient comparable Page observations or missing completed history'
    END AS validation_reason
  FROM workspace.default.gold_recrawl_recommendation_candidates_internal
)
SELECT recommendation_id, idempotency_key, generation_id, source_id,
       expected_policy_version, generated_at_utc, expires_at_utc,
       evidence_status, validation_status, validation_reason,
       proposed_interval_hours, recommendation_json
FROM classified;
