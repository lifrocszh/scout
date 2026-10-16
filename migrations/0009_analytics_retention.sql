CREATE TABLE scout_analytics_consumers (
    consumer_id TEXT PRIMARY KEY,
    is_required BOOLEAN NOT NULL DEFAULT FALSE,
    replay_window INTERVAL NOT NULL DEFAULT INTERVAL '90 days'
        CHECK (replay_window > INTERVAL '0 seconds'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    retired_at TIMESTAMPTZ,
    CHECK (consumer_id <> '')
);

INSERT INTO scout_analytics_consumers(consumer_id, is_required, replay_window)
VALUES ('databricks-live-v1', TRUE, INTERVAL '90 days')
ON CONFLICT (consumer_id) DO NOTHING;

-- Retain run identity after operators delete live crawl metadata.
CREATE TABLE scout_crawl_run_history (
    crawl_run_id TEXT PRIMARY KEY,
    config_digest TEXT NOT NULL CHECK (config_digest ~ '^[a-f0-9]{64}$'),
    created_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

INSERT INTO scout_crawl_run_history(crawl_run_id, config_digest, created_at)
SELECT crawl_run_id, config_digest, created_at FROM scout_crawl_runs
ON CONFLICT (crawl_run_id) DO NOTHING;

CREATE FUNCTION scout_record_crawl_run_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO scout_crawl_run_history(crawl_run_id, config_digest, created_at)
    VALUES (NEW.crawl_run_id, NEW.config_digest, NEW.created_at)
    ON CONFLICT (crawl_run_id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER scout_crawl_runs_history_after_insert
AFTER INSERT ON scout_crawl_runs
FOR EACH ROW EXECUTE FUNCTION scout_record_crawl_run_history();

CREATE FUNCTION scout_mark_crawl_run_deleted() RETURNS TRIGGER AS $$
BEGIN
    UPDATE scout_crawl_run_history SET deleted_at = COALESCE(deleted_at, clock_timestamp())
    WHERE crawl_run_id = OLD.crawl_run_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER scout_crawl_runs_history_before_delete
BEFORE DELETE ON scout_crawl_runs
FOR EACH ROW EXECUTE FUNCTION scout_mark_crawl_run_deleted();

CREATE TABLE scout_crawl_event_history (
    event_id TEXT PRIMARY KEY,
    crawl_run_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    dedupe_key TEXT NOT NULL,
    payload_sha256 TEXT CHECK (payload_sha256 IS NULL OR payload_sha256 ~ '^[a-f0-9]{64}$'),
    created_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX scout_crawl_event_history_run_order
    ON scout_crawl_event_history(crawl_run_id, created_at, event_id);

INSERT INTO scout_crawl_event_history(
    event_id, crawl_run_id, event_type, schema_version, dedupe_key, payload_sha256, created_at
)
SELECT event_id, crawl_run_id, event_type, schema_version, dedupe_key, payload_sha256, created_at
FROM scout_crawl_events
ON CONFLICT (event_id) DO NOTHING;

CREATE TABLE scout_crawl_event_artifacts (
    event_id TEXT NOT NULL,
    crawl_run_id TEXT NOT NULL,
    artifact_uri TEXT NOT NULL,
    artifact_kind TEXT NOT NULL CHECK (artifact_kind IN ('body', 'crawl_manifest', 'snapshot_manifest')),
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
    byte_size BIGINT NOT NULL CHECK (byte_size >= 0),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (event_id, artifact_uri)
);

INSERT INTO scout_crawl_event_artifacts(event_id, crawl_run_id, artifact_uri, artifact_kind, sha256, byte_size)
SELECT event_id, crawl_run_id,
       payload_json::JSONB #>> '{data,body_artifact,uri}', 'body',
       payload_json::JSONB #>> '{data,body_artifact,sha256}',
       (payload_json::JSONB #>> '{data,body_artifact,byte_size}')::BIGINT
FROM scout_crawl_events
WHERE event_type = 'capture_committed'
ON CONFLICT DO NOTHING;

INSERT INTO scout_crawl_event_artifacts(event_id, crawl_run_id, artifact_uri, artifact_kind, sha256, byte_size)
SELECT event_id, crawl_run_id,
       payload_json::JSONB #>> '{data,snapshot_manifest,uri}', 'snapshot_manifest',
       payload_json::JSONB #>> '{data,snapshot_manifest,sha256}',
       (payload_json::JSONB #>> '{data,snapshot_manifest,byte_size}')::BIGINT
FROM scout_crawl_events
WHERE event_type = 'corpus_snapshot_published'
ON CONFLICT DO NOTHING;

INSERT INTO scout_crawl_event_artifacts(event_id, crawl_run_id, artifact_uri, artifact_kind, sha256, byte_size)
SELECT event_id, crawl_run_id,
       payload_json::JSONB #>> '{data,crawl_manifest,uri}', 'crawl_manifest',
       payload_json::JSONB #>> '{data,crawl_manifest,sha256}',
       (payload_json::JSONB #>> '{data,crawl_manifest,byte_size}')::BIGINT
FROM scout_crawl_events
WHERE event_type = 'corpus_snapshot_published'
ON CONFLICT DO NOTHING;

CREATE TABLE scout_crawl_event_batches (
    batch_id TEXT PRIMARY KEY CHECK (batch_id ~ '^batch-[a-f0-9]{64}$'),
    consumer_id TEXT NOT NULL REFERENCES scout_analytics_consumers(consumer_id),
    crawl_run_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    records_sha256 TEXT NOT NULL CHECK (records_sha256 ~ '^[a-f0-9]{64}$'),
    coverage_sha256 TEXT NOT NULL CHECK (coverage_sha256 ~ '^[a-f0-9]{64}$'),
    record_count INTEGER NOT NULL CHECK (record_count > 0),
    records_bytes BIGINT NOT NULL CHECK (records_bytes > 0),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE scout_crawl_event_batch_locations (
    batch_id TEXT NOT NULL REFERENCES scout_crawl_event_batches(batch_id),
    local_path TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id, local_path)
);

CREATE TABLE scout_crawl_event_batch_events (
    batch_id TEXT NOT NULL REFERENCES scout_crawl_event_batches(batch_id),
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_sha256 TEXT CHECK (payload_sha256 IS NULL OR payload_sha256 ~ '^[a-f0-9]{64}$'),
    PRIMARY KEY (batch_id, event_id)
);

CREATE INDEX scout_crawl_event_batch_events_event ON scout_crawl_event_batch_events(event_id);

CREATE TABLE scout_crawl_event_ingestion_receipts (
    consumer_id TEXT NOT NULL REFERENCES scout_analytics_consumers(consumer_id),
    batch_id TEXT NOT NULL REFERENCES scout_crawl_event_batches(batch_id),
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    records_sha256 TEXT NOT NULL CHECK (records_sha256 ~ '^[a-f0-9]{64}$'),
    coverage_sha256 TEXT NOT NULL CHECK (coverage_sha256 ~ '^[a-f0-9]{64}$'),
    ingested_record_count INTEGER NOT NULL CHECK (ingested_record_count > 0),
    downstream_status TEXT NOT NULL CHECK (downstream_status = 'complete'),
    downstream_completed_at TEXT NOT NULL,
    acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_id, batch_id)
);

CREATE TABLE scout_crawl_event_archive_items (
    item_type TEXT NOT NULL CHECK (item_type IN ('batch', 'artifact')),
    item_id TEXT NOT NULL,
    archive_path TEXT NOT NULL,
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
    byte_size BIGINT NOT NULL CHECK (byte_size >= 0),
    verified_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (item_type, item_id)
);

CREATE FUNCTION scout_record_crawl_event_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO scout_crawl_event_history(
        event_id, crawl_run_id, event_type, schema_version, dedupe_key, payload_sha256, created_at
    ) VALUES (
        NEW.event_id, NEW.crawl_run_id, NEW.event_type, NEW.schema_version, NEW.dedupe_key,
        NEW.payload_sha256, NEW.created_at
    ) ON CONFLICT (event_id) DO NOTHING;

    IF NEW.event_type = 'capture_committed' THEN
        INSERT INTO scout_crawl_event_artifacts(event_id, crawl_run_id, artifact_uri, artifact_kind, sha256, byte_size)
        SELECT NEW.event_id, NEW.crawl_run_id,
               NEW.payload_json::JSONB #>> '{data,body_artifact,uri}', 'body',
               NEW.payload_json::JSONB #>> '{data,body_artifact,sha256}',
               (NEW.payload_json::JSONB #>> '{data,body_artifact,byte_size}')::BIGINT
        ON CONFLICT DO NOTHING;
    ELSIF NEW.event_type = 'corpus_snapshot_published' THEN
        INSERT INTO scout_crawl_event_artifacts(event_id, crawl_run_id, artifact_uri, artifact_kind, sha256, byte_size)
        SELECT NEW.event_id, NEW.crawl_run_id, item.artifact->>'uri',
               CASE item.key WHEN 'crawl_manifest' THEN 'crawl_manifest' ELSE 'snapshot_manifest' END,
               item.artifact->>'sha256', (item.artifact->>'byte_size')::BIGINT
        FROM jsonb_each(NEW.payload_json::JSONB->'data') AS item(key, artifact)
        WHERE item.key IN ('snapshot_manifest', 'crawl_manifest')
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER scout_crawl_events_history_after_insert
AFTER INSERT ON scout_crawl_events
FOR EACH ROW EXECUTE FUNCTION scout_record_crawl_event_history();
