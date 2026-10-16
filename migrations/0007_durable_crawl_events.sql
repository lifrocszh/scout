ALTER TABLE scout_crawl_runs
    ADD COLUMN completed_at TIMESTAMPTZ,
    ADD COLUMN stop_reason TEXT;

UPDATE scout_crawl_runs
SET completed_at = created_at,
    stop_reason = 'legacy-completed'
WHERE status = 'completed' AND completed_at IS NULL;

CREATE TABLE scout_crawl_events (
    event_id TEXT PRIMARY KEY,
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL
        CHECK (event_type IN ('capture_committed', 'crawl_completed', 'corpus_snapshot_published')),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    dedupe_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT CHECK (payload_sha256 IS NULL OR payload_sha256 ~ '^[a-f0-9]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (crawl_run_id, event_type, dedupe_key)
);

CREATE INDEX scout_crawl_events_run_order
    ON scout_crawl_events(crawl_run_id, created_at, event_id);

CREATE TABLE scout_crawl_event_checkpoints (
    consumer_id TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES scout_crawl_events(event_id) ON DELETE CASCADE,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_error TEXT,
    acknowledged_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_id, event_id)
);

CREATE INDEX scout_crawl_event_checkpoints_due
    ON scout_crawl_event_checkpoints(consumer_id, next_attempt_at)
    WHERE acknowledged_at IS NULL;

-- Upgrade accepted captures into the durable feed. New captures insert their event
-- inside the result-acceptance transaction.
INSERT INTO scout_crawl_events(
    event_id, crawl_run_id, event_type, schema_version, dedupe_key, payload_json, created_at
)
SELECT
    'capture-committed.v1:' || result.crawl_run_id || ':' || result.task_id,
    result.crawl_run_id,
    'capture_committed',
    1,
    result.task_id::TEXT,
    json_build_object(
        'event_type', 'capture_committed',
        'data', json_build_object(
            'event_id', 'capture-committed.v1:' || result.crawl_run_id || ':' || result.task_id,
            'schema_version', 1,
            'crawl_run_id', result.crawl_run_id,
            'source_id', result.source_id,
            'task_id', result.task_id,
            'committed_at_utc', to_char(result.accepted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'result_sha256', result.result_sha256,
            'content_sha256', result.content_hash,
            'body_artifact', json_build_object(
                'uri', 'scout-artifact://crawl/' || result.crawl_run_id || '/bodies/' || result.body_hash,
                'sha256', result.body_hash,
                'byte_size', result.body_size
            )
        )
    )::TEXT,
    result.accepted_at
FROM scout_crawl_task_results AS result
WHERE result.capture_metadata IS NOT NULL
ON CONFLICT (crawl_run_id, event_type, dedupe_key) DO NOTHING;

INSERT INTO scout_crawl_events(
    event_id, crawl_run_id, event_type, schema_version, dedupe_key, payload_json, created_at
)
SELECT
    'crawl-completed.v1:' || run.crawl_run_id,
    run.crawl_run_id,
    'crawl_completed',
    1,
    'run',
    json_build_object(
        'event_type', 'crawl_completed',
        'data', json_build_object(
            'event_id', 'crawl-completed.v1:' || run.crawl_run_id,
            'schema_version', 1,
            'crawl_run_id', run.crawl_run_id,
            'completed_at_utc', to_char(run.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'stop_reason', COALESCE(run.stop_reason, 'legacy-completed'),
            'config_sha256', run.config_digest
        )
    )::TEXT,
    run.completed_at
FROM scout_crawl_runs AS run
WHERE run.status = 'completed' AND run.completed_at IS NOT NULL
ON CONFLICT (crawl_run_id, event_type, dedupe_key) DO NOTHING;
