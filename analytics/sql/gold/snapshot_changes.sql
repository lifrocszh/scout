-- gold_snapshot_changes: Page changes per Source between each snapshot and the
-- previous loaded snapshot of the same Source (by creation time).
--   added / removed   Page IDs present in only one of the two snapshots
--   changed           same Page ID, different extracted content (content_sha256)
--   change_rate       changed / Pages present in both
--   churn_rate        (added + removed + changed) / Pages present in either
-- The first snapshot of a Source has no previous snapshot: all its Pages count
-- as added and change_rate is NULL.
WITH source_snapshots AS (
    SELECT DISTINCT p.source_id, s.snapshot_id, s.created_at_utc
    FROM silver_pages AS p
    JOIN silver_snapshots AS s ON s.snapshot_id = p.snapshot_id
),
paired AS (
    SELECT source_id, snapshot_id,
           LAG(snapshot_id) OVER (PARTITION BY source_id ORDER BY created_at_utc, snapshot_id) AS previous_snapshot_id
    FROM source_snapshots
),
both_sides AS (
    SELECT pr.snapshot_id, pr.previous_snapshot_id, pr.source_id, c.page_id,
           c.content_sha256 AS current_sha256, CAST(NULL AS VARCHAR) AS previous_sha256
    FROM paired AS pr
    JOIN silver_pages AS c ON c.snapshot_id = pr.snapshot_id AND c.source_id = pr.source_id
    UNION ALL
    SELECT pr.snapshot_id, pr.previous_snapshot_id, pr.source_id, p.page_id,
           CAST(NULL AS VARCHAR), p.content_sha256
    FROM paired AS pr
    JOIN silver_pages AS p ON p.snapshot_id = pr.previous_snapshot_id AND p.source_id = pr.source_id
),
per_page AS (
    SELECT snapshot_id, previous_snapshot_id, source_id, page_id,
           MAX(current_sha256) AS current_sha256, MAX(previous_sha256) AS previous_sha256
    FROM both_sides
    GROUP BY snapshot_id, previous_snapshot_id, source_id, page_id
),
counted AS (
    SELECT snapshot_id, previous_snapshot_id, source_id,
           COUNT(current_sha256) AS pages,
           COUNT(previous_sha256) AS previous_pages,
           SUM(CASE WHEN current_sha256 IS NOT NULL AND previous_sha256 IS NULL THEN 1 ELSE 0 END) AS added,
           SUM(CASE WHEN current_sha256 IS NULL AND previous_sha256 IS NOT NULL THEN 1 ELSE 0 END) AS removed,
           SUM(CASE WHEN current_sha256 <> previous_sha256 THEN 1 ELSE 0 END) AS changed,
           SUM(CASE WHEN current_sha256 = previous_sha256 THEN 1 ELSE 0 END) AS unchanged
    FROM per_page
    GROUP BY snapshot_id, previous_snapshot_id, source_id
)
SELECT snapshot_id, previous_snapshot_id, source_id, pages, previous_pages,
       added, removed, changed, unchanged,
       CASE WHEN previous_snapshot_id IS NULL OR changed + unchanged = 0 THEN NULL
            ELSE CAST(changed AS DOUBLE) / (changed + unchanged) END AS change_rate,
       CASE WHEN previous_snapshot_id IS NULL THEN NULL
            ELSE CAST(added + removed + changed AS DOUBLE) / (added + removed + changed + unchanged) END AS churn_rate
FROM counted
ORDER BY source_id, snapshot_id
