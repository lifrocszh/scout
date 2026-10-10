ALTER TABLE scout_crawl_tasks ADD COLUMN schedule_key TEXT;

UPDATE scout_crawl_tasks SET schedule_key = origin;

ALTER TABLE scout_crawl_tasks ALTER COLUMN schedule_key SET NOT NULL;

CREATE TABLE scout_crawl_task_scheduler (
    crawl_run_id TEXT PRIMARY KEY REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    last_schedule_key TEXT
);

INSERT INTO scout_crawl_task_scheduler(crawl_run_id)
SELECT crawl_run_id FROM scout_crawl_runs;

CREATE INDEX scout_crawl_tasks_schedule_claimable
    ON scout_crawl_tasks(crawl_run_id, schedule_key, retry_at, task_id)
    WHERE status IN ('pending', 'leased');
