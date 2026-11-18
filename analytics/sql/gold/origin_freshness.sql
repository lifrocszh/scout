-- gold_origin_freshness: per Origin, the newest loaded Corpus snapshot that holds
-- its Pages, and the newest capture_committed event of the Sources crawling it.
-- Portable SQL (DuckDB and Databricks SQL); inputs are Silver tables.
WITH origin_snapshots AS (
    SELECT p.origin, s.snapshot_id, s.run_id, s.created_at_utc, COUNT(*) AS page_count
    FROM silver_pages AS p
    JOIN silver_snapshots AS s ON s.snapshot_id = p.snapshot_id
    GROUP BY p.origin, s.snapshot_id, s.run_id, s.created_at_utc
),
ranked AS (
    SELECT origin, snapshot_id, run_id, created_at_utc, page_count,
           ROW_NUMBER() OVER (PARTITION BY origin ORDER BY created_at_utc DESC, snapshot_id DESC) AS rn,
           COUNT(*) OVER (PARTITION BY origin) AS snapshot_count
    FROM origin_snapshots
),
source_origins AS (
    SELECT DISTINCT source_id, origin FROM silver_pages
),
captures AS (
    -- Capture events carry a Source, not a URL: attribute them to every Origin
    -- that Source's Pages live on.
    SELECT so.origin, MAX(e.occurred_at_utc) AS latest_capture_at_utc
    FROM silver_crawl_events AS e
    JOIN source_origins AS so ON so.source_id = e.source_id
    WHERE e.event_type = 'capture_committed'
    GROUP BY so.origin
)
SELECT r.origin,
       r.snapshot_id AS latest_snapshot_id,
       r.run_id AS latest_run_id,
       r.created_at_utc AS latest_snapshot_at_utc,
       c.latest_capture_at_utc,
       r.page_count,
       r.snapshot_count
FROM ranked AS r
LEFT JOIN captures AS c ON c.origin = r.origin
WHERE r.rn = 1
ORDER BY r.origin
