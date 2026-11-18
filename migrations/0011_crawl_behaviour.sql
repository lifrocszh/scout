-- Dead-letter state: a task that exhausts max_task_attempts is `failed` and stamped with the
-- time it was dead-lettered; `scout dlq requeue` moves it back to `pending`.
ALTER TABLE scout_crawl_tasks
    ADD COLUMN dead_lettered_at TIMESTAMPTZ,
    ADD COLUMN dead_letter_requeues INTEGER NOT NULL DEFAULT 0
        CHECK (dead_letter_requeues >= 0);

UPDATE scout_crawl_tasks SET dead_lettered_at = updated_at WHERE status = 'failed';

ALTER TABLE scout_crawl_tasks
    ADD CONSTRAINT scout_crawl_tasks_dead_letter_state
        CHECK ((status = 'failed') = (dead_lettered_at IS NOT NULL));

CREATE INDEX scout_crawl_tasks_dead_letters
    ON scout_crawl_tasks(crawl_run_id, dead_lettered_at, task_id)
    WHERE status = 'failed';

-- Adaptive per-origin pacing (src/crawl/throttle.rs): extra start spacing added to the
-- configured spacing after 429/503 responses, decaying on healthy responses.
CREATE TABLE scout_crawl_origin_throttle (
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_request_policy(crawl_run_id) ON DELETE CASCADE,
    origin TEXT NOT NULL,
    backoff_ms BIGINT NOT NULL DEFAULT 0 CHECK (backoff_ms >= 0),
    throttle_responses BIGINT NOT NULL DEFAULT 0 CHECK (throttle_responses >= 0),
    last_throttled_at TIMESTAMPTZ,
    PRIMARY KEY (crawl_run_id, origin)
);
