-- One bounded discovery attempt owner per run/origin, fenced by a renewable URL task lease.
CREATE TABLE scout_crawl_sitemap_discovery (
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    origin TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('running', 'pending', 'completed', 'failed')),
    owner_task_id BIGINT NOT NULL REFERENCES scout_crawl_tasks(task_id) ON DELETE CASCADE,
    owner_lease_epoch BIGINT NOT NULL,
    scan_epoch BIGINT NOT NULL CHECK (scan_epoch > 0),
    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0 AND attempt_count <= max_attempts),
    result_sha256 TEXT CHECK (result_sha256 IS NULL OR result_sha256 ~ '^[a-f0-9]{64}$'),
    accepted_discovered_count BIGINT NOT NULL DEFAULT 0 CHECK (accepted_discovered_count >= 0),
    rejected_discovered_count BIGINT NOT NULL DEFAULT 0 CHECK (rejected_discovered_count >= 0),
    error_reason TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (crawl_run_id, origin)
);
