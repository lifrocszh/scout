-- One consistent, read-only snapshot. psql quotes all externally supplied values.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '1500ms';
WITH tasks AS MATERIALIZED (
    SELECT * FROM scout_crawl_tasks WHERE crawl_run_id = :'run_id'
), results AS MATERIALIZED (
    SELECT task_id, source_id, accepted_at, capture_metadata IS NOT NULL AS captured,
           body_size, content_hash, body_hash, accepted_discovered_count,
           rejected_discovered_count, rejection_reason
    FROM scout_crawl_task_results WHERE crawl_run_id = :'run_id'
), groups AS MATERIALIZED (
    SELECT source_id, accepted_at FROM scout_crawl_content_groups WHERE crawl_run_id = :'run_id'
), permits AS MATERIALIZED (
    SELECT worker_id, origin, schedule_key, request_kind, status,
           pending_deadline, expires_at, queued_at
    FROM scout_crawl_request_permits
    WHERE crawl_run_id = :'run_id' AND status IN ('pending', 'active')
), counts AS (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE status = 'pending') AS pending,
           count(*) FILTER (WHERE status = 'pending' AND retry_at <= now()) AS ready,
           count(*) FILTER (WHERE status = 'pending' AND retry_at > now()) AS delayed,
           count(*) FILTER (WHERE status = 'leased') AS leased,
           count(*) FILTER (WHERE status = 'leased' AND lease_deadline > now()) AS in_flight,
           count(*) FILTER (WHERE status = 'leased' AND lease_deadline <= now()) AS expired,
           count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
           count(*) FILTER (WHERE status = 'failed') AS failed
    FROM tasks
), outcomes AS (
    SELECT count(*) FILTER (WHERE captured) AS captures,
           count(*) FILTER (WHERE NOT captured) AS rejected,
           COALESCE(sum(accepted_discovered_count), 0) AS discoveries,
           COALESCE(sum(rejected_discovered_count), 0) AS rejected_discoveries
    FROM results
), source_counts AS (
    SELECT source_id, count(*) AS total,
           count(*) FILTER (WHERE status = 'pending') AS pending,
           count(*) FILTER (WHERE status = 'leased') AS leased,
           count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
           count(*) FILTER (WHERE status = 'failed') AS failed
    FROM tasks GROUP BY source_id
), origin_counts AS (
    SELECT origin, count(*) AS total,
           count(*) FILTER (WHERE status = 'pending') AS pending,
           count(*) FILTER (WHERE status = 'leased') AS leased,
           count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
           count(*) FILTER (WHERE status = 'failed') AS failed
    FROM tasks GROUP BY origin ORDER BY origin
), live_permits AS (
    SELECT worker_id, origin, schedule_key, request_kind,
           count(*) FILTER (WHERE status = 'active' AND expires_at > now()) AS active,
           count(*) FILTER (WHERE status = 'pending' AND pending_deadline > now()) AS waiting,
           count(*) FILTER (WHERE (status = 'active' AND expires_at <= now())
                              OR (status = 'pending' AND pending_deadline <= now())) AS expired,
           min(queued_at) FILTER (WHERE status = 'pending' AND pending_deadline > now()) AS oldest_wait
    FROM permits GROUP BY worker_id, origin, schedule_key, request_kind
), pages AS (
    SELECT t.task_id, t.source_id, t.origin, t.normalized_url, t.status,
           t.task_attempt_count, t.max_task_attempts, t.last_error,
           COALESCE(r.accepted_at, t.updated_at) AS outcome_at,
           r.source_id AS capture_source, r.body_size, r.content_hash, r.body_hash,
           r.accepted_discovered_count, r.rejected_discovered_count, r.rejection_reason,
           CASE WHEN r.capture_metadata IS NOT NULL THEN 'captured'
                WHEN r.task_id IS NOT NULL THEN 'rejected' ELSE t.status END AS outcome,
           r.capture_metadata::jsonb ->> 'final_url' AS final_url,
           r.capture_metadata::jsonb ->> 'title' AS title,
           r.capture_metadata::jsonb ->> 'status_code' AS http_status
    FROM tasks t LEFT JOIN scout_crawl_task_results r USING (task_id)
    WHERE :'view' = 'pages' AND (:'page_filter' = 'all'
       OR (:'page_filter' = 'captured' AND r.capture_metadata IS NOT NULL)
       OR (:'page_filter' = 'rejected' AND r.task_id IS NOT NULL AND r.capture_metadata IS NULL)
       OR (:'page_filter' IN ('pending', 'leased', 'failed') AND t.status = :'page_filter'))
    ORDER BY COALESCE(r.accepted_at, t.updated_at) DESC, t.task_id DESC
    LIMIT 6 OFFSET :'page_offset'::integer
), rates AS (
    SELECT seconds,
           (SELECT count(*) FROM tasks WHERE status IN ('succeeded', 'failed')
             AND updated_at >= now() - seconds * interval '1 second') AS tasks,
           (SELECT count(*) FROM results WHERE captured
             AND accepted_at >= now() - seconds * interval '1 second') AS captures,
           (SELECT count(*) FROM groups
             WHERE accepted_at >= now() - seconds * interval '1 second') AS unique_content
    FROM (VALUES (60), (300)) windows(seconds)
)
SELECT jsonb_build_object(
    'sampled_at', now(),
    'run', (SELECT to_jsonb(r) FROM scout_crawl_runs r WHERE crawl_run_id = :'run_id'),
    'counts', (SELECT to_jsonb(c) FROM counts c),
    'outcomes', (SELECT to_jsonb(o) FROM outcomes o),
    'unique_content', (SELECT count(*) FROM groups),
    'groups_by_source', COALESCE((SELECT jsonb_object_agg(source_id, n) FROM
        (SELECT source_id, count(*) n FROM groups GROUP BY source_id) g), '{}'::jsonb),
    'sources', COALESCE((SELECT jsonb_agg(s ORDER BY source_id) FROM source_counts s), '[]'::jsonb),
    'origins', COALESCE((SELECT jsonb_agg(o) FROM origin_counts o), '[]'::jsonb),
    'leases', COALESCE((SELECT jsonb_agg(l ORDER BY lease_owner, task_id) FROM
        (SELECT task_id, normalized_url, source_id, lease_owner, lease_epoch,
                lease_deadline, updated_at, task_attempt_count, last_error
         FROM tasks WHERE status = 'leased') l), '[]'::jsonb),
    'policy', (SELECT to_jsonb(p) FROM scout_crawl_request_policy p WHERE crawl_run_id = :'run_id'),
    'permits', COALESCE((SELECT jsonb_agg(p ORDER BY origin, worker_id) FROM live_permits p), '[]'::jsonb),
    'spacing', COALESCE((SELECT jsonb_agg(s ORDER BY schedule_key) FROM
        (SELECT schedule_key, next_allowed_at FROM scout_crawl_request_spacing
         WHERE crawl_run_id = :'run_id' AND next_allowed_at > now()) s), '[]'::jsonb),
    'rates', (SELECT jsonb_agg(r ORDER BY seconds) FROM rates r),
    'pages', COALESCE((SELECT jsonb_agg(p ORDER BY outcome_at DESC, task_id DESC) FROM pages p), '[]'::jsonb)
);
COMMIT;
