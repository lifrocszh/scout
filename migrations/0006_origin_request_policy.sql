ALTER TABLE scout_crawl_request_policy
    ADD COLUMN origin_policy_configured BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE scout_crawl_origin_request_policy (
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_request_policy(crawl_run_id) ON DELETE CASCADE,
    origin TEXT NOT NULL,
    schedule_key TEXT NOT NULL,
    origin_concurrency INTEGER NOT NULL CHECK (origin_concurrency > 0),
    min_start_spacing_ms BIGINT NOT NULL CHECK (min_start_spacing_ms >= 0),
    PRIMARY KEY (crawl_run_id, origin)
);

CREATE INDEX scout_crawl_origin_request_policy_schedule
    ON scout_crawl_origin_request_policy(crawl_run_id, schedule_key);
