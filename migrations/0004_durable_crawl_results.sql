CREATE TABLE scout_crawl_content_groups (
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    source_id TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[a-f0-9]{64}$'),
    identity BYTEA NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (crawl_run_id, source_id, content_hash)
);

CREATE TABLE scout_crawl_task_results (
    task_id BIGINT PRIMARY KEY REFERENCES scout_crawl_tasks(task_id) ON DELETE CASCADE,
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    source_id TEXT,
    content_hash TEXT CHECK (content_hash IS NULL OR content_hash ~ '^[a-f0-9]{64}$'),
    body_hash TEXT CHECK (body_hash IS NULL OR body_hash ~ '^[a-f0-9]{64}$'),
    body_size BIGINT CHECK (body_size IS NULL OR body_size >= 0),
    capture_metadata TEXT,
    idempotency_key TEXT NOT NULL CHECK (idempotency_key ~ '^[a-f0-9]{64}$'),
    lease_owner TEXT NOT NULL,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    result_sha256 TEXT NOT NULL CHECK (result_sha256 ~ '^[a-f0-9]{64}$'),
    discovered_urls TEXT[] NOT NULL DEFAULT '{}',
    accepted_discovered_count BIGINT NOT NULL DEFAULT 0 CHECK (accepted_discovered_count >= 0),
    rejected_discovered_count BIGINT NOT NULL DEFAULT 0 CHECK (rejected_discovered_count >= 0),
    rejection_reason TEXT NOT NULL DEFAULT '',
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (crawl_run_id, idempotency_key),
    CHECK (
        (capture_metadata IS NOT NULL AND source_id IS NOT NULL AND content_hash IS NOT NULL
         AND body_hash IS NOT NULL AND body_size IS NOT NULL AND rejection_reason = '')
        OR
        (capture_metadata IS NULL AND source_id IS NULL AND content_hash IS NULL
         AND body_hash IS NULL AND body_size IS NULL AND rejection_reason <> '')
    )
);

CREATE INDEX scout_crawl_task_results_captures
    ON scout_crawl_task_results(crawl_run_id, task_id)
    WHERE capture_metadata IS NOT NULL;
