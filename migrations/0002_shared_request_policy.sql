CREATE TABLE scout_crawl_request_policy (
    crawl_run_id TEXT PRIMARY KEY REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    global_concurrency INTEGER NOT NULL CHECK (global_concurrency > 0),
    origin_concurrency INTEGER NOT NULL CHECK (origin_concurrency > 0),
    min_start_spacing_ms BIGINT NOT NULL CHECK (min_start_spacing_ms >= 0),
    request_timeout_ms BIGINT NOT NULL CHECK (request_timeout_ms > 0),
    permit_safety_margin_ms BIGINT NOT NULL CHECK (permit_safety_margin_ms > 0),
    robots_cache_ms BIGINT NOT NULL CHECK (robots_cache_ms > 0),
    max_total_attempts BIGINT NOT NULL CHECK (max_total_attempts > 0),
    max_pending_requests INTEGER NOT NULL CHECK (max_pending_requests > 0),
    total_attempts BIGINT NOT NULL DEFAULT 0 CHECK (total_attempts >= 0),
    last_schedule_key TEXT
);

CREATE TABLE scout_crawl_request_spacing (
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    schedule_key TEXT NOT NULL,
    next_allowed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (crawl_run_id, schedule_key)
);

CREATE TABLE scout_crawl_request_permits (
    permit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    request_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    schedule_key TEXT NOT NULL,
    origin TEXT NOT NULL,
    request_kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'active', 'completed', 'expired', 'cancelled')),
    queued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    pending_deadline TIMESTAMPTZ,
    acquired_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    outcome TEXT,
    UNIQUE (crawl_run_id, request_id),
    CHECK (
        (status = 'pending' AND pending_deadline IS NOT NULL AND acquired_at IS NULL AND expires_at IS NULL)
        OR (status = 'active' AND pending_deadline IS NULL AND acquired_at IS NOT NULL AND expires_at IS NOT NULL)
        OR (status IN ('completed', 'expired', 'cancelled') AND pending_deadline IS NULL AND acquired_at IS NOT NULL AND expires_at IS NOT NULL AND finished_at IS NOT NULL)
    )
);

CREATE INDEX scout_crawl_request_permits_pending
    ON scout_crawl_request_permits(crawl_run_id, schedule_key, queued_at, request_id)
    WHERE status = 'pending';

CREATE INDEX scout_crawl_request_permits_active_origin
    ON scout_crawl_request_permits(crawl_run_id, origin)
    WHERE status = 'active';

CREATE INDEX scout_crawl_request_permits_active_expiry
    ON scout_crawl_request_permits(crawl_run_id, expires_at)
    WHERE status = 'active';

CREATE TABLE scout_crawl_robots_policy (
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    origin TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('refreshing', 'ready', 'denied')),
    policy TEXT NOT NULL DEFAULT '{"groups":[],"sitemaps":[]}',
    body_sha256 TEXT CHECK (body_sha256 IS NULL OR body_sha256 ~ '^[a-f0-9]{64}$'),
    fetched_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    refresh_owner TEXT,
    refresh_epoch BIGINT NOT NULL DEFAULT 0 CHECK (refresh_epoch >= 0),
    refresh_deadline TIMESTAMPTZ,
    PRIMARY KEY (crawl_run_id, origin),
    CHECK (
        (state = 'refreshing' AND refresh_owner IS NOT NULL AND refresh_deadline IS NOT NULL)
        OR (state <> 'refreshing' AND refresh_owner IS NULL AND refresh_deadline IS NULL AND fetched_at IS NOT NULL AND expires_at IS NOT NULL)
    )
);
