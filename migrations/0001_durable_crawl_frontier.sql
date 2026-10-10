CREATE TABLE scout_crawl_runs (
    crawl_run_id TEXT PRIMARY KEY,
    config_digest TEXT NOT NULL CHECK (config_digest ~ '^[a-f0-9]{64}$'),
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'completed', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE scout_crawl_tasks (
    task_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    source_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'leased', 'succeeded', 'failed')),
    task_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (task_attempt_count >= 0),
    max_task_attempts INTEGER NOT NULL CHECK (max_task_attempts > 0),
    retry_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_owner TEXT,
    lease_deadline TIMESTAMPTZ,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    idempotency_key TEXT NOT NULL UNIQUE CHECK (idempotency_key ~ '^[a-f0-9]{64}$'),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (crawl_run_id, source_id, normalized_url),
    CHECK (
        (status = 'leased' AND lease_owner IS NOT NULL AND lease_deadline IS NOT NULL)
        OR
        (status <> 'leased' AND lease_owner IS NULL AND lease_deadline IS NULL)
    ),
    CHECK (task_attempt_count <= max_task_attempts)
);

CREATE INDEX scout_crawl_tasks_claimable
    ON scout_crawl_tasks(crawl_run_id, retry_at, task_id)
    WHERE status = 'pending';

CREATE INDEX scout_crawl_tasks_expired_leases
    ON scout_crawl_tasks(crawl_run_id, lease_deadline, task_id)
    WHERE status = 'leased';
