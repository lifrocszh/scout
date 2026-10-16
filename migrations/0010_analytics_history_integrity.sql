ALTER TABLE scout_crawl_run_history
    ADD COLUMN expected_event_count BIGINT NOT NULL DEFAULT 0
        CHECK (expected_event_count >= 0),
    ADD COLUMN expected_event_coverage BIT(256) NOT NULL
        DEFAULT repeat('0', 256)::BIT(256);

ALTER TABLE scout_crawl_event_batches
    ADD COLUMN quarantine_count INTEGER CHECK (quarantine_count IS NULL OR quarantine_count >= 0);

ALTER TABLE scout_analytics_consumers
    ADD COLUMN ever_required BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE scout_analytics_consumers SET ever_required = is_required;

CREATE TABLE scout_analytics_prune_items (
    item_type TEXT NOT NULL CHECK (item_type IN ('event', 'batch_copy', 'body_artifact')),
    item_id TEXT NOT NULL,
    crawl_run_id TEXT NOT NULL,
    local_path TEXT NOT NULL DEFAULT '',
    expected_sha256 TEXT CHECK (expected_sha256 IS NULL OR expected_sha256 ~ '^[a-f0-9]{64}$'),
    expected_bytes BIGINT CHECK (expected_bytes IS NULL OR expected_bytes >= 0),
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (item_type, item_id, local_path),
    CHECK ((item_type = 'event' AND local_path = '') OR (item_type <> 'event' AND local_path <> ''))
);

CREATE FUNCTION scout_event_history_fingerprint(
    event_id TEXT, event_type TEXT, schema_version INTEGER, dedupe_key TEXT,
    payload_sha256 TEXT, created_at TIMESTAMPTZ
) RETURNS BIT(256) AS $$
    SELECT ('x' || encode(sha256(convert_to(jsonb_build_array(
        event_id, event_type, schema_version, dedupe_key, payload_sha256,
        to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    )::TEXT, 'UTF8')), 'hex'))::BIT(256)
$$ LANGUAGE SQL IMMUTABLE;

-- Backfill by scanning all retained events, not a timestamp watermark: run/event
-- insertions can commit out of creation order.
UPDATE scout_crawl_run_history AS run
SET expected_event_count = coverage.event_count,
    expected_event_coverage = coverage.event_coverage
FROM (
    SELECT crawl_run_id, COUNT(*)::BIGINT AS event_count,
           bit_xor(scout_event_history_fingerprint(
               event_id, event_type, schema_version, dedupe_key, payload_sha256, created_at
           )) AS event_coverage
    FROM scout_crawl_event_history
    GROUP BY crawl_run_id
) AS coverage
WHERE run.crawl_run_id = coverage.crawl_run_id;

CREATE FUNCTION scout_extend_crawl_event_history_coverage() RETURNS TRIGGER AS $$
BEGIN
    UPDATE scout_crawl_run_history
    SET expected_event_count = expected_event_count + 1,
        expected_event_coverage = expected_event_coverage # scout_event_history_fingerprint(
            NEW.event_id, NEW.event_type, NEW.schema_version, NEW.dedupe_key,
            NEW.payload_sha256, NEW.created_at
        )
    WHERE crawl_run_id = NEW.crawl_run_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'crawl run history missing for event run %', NEW.crawl_run_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER scout_crawl_events_coverage_after_insert
AFTER INSERT ON scout_crawl_events
FOR EACH ROW EXECUTE FUNCTION scout_extend_crawl_event_history_coverage();
