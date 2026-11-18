-- gold_duplicate_rates: per snapshot and Origin, exact duplicates (Page aliases
-- folded into a representative Page) and near duplicates (from the crawl's
-- near-duplicates.json report; NULL when the snapshot has no report).
--   exact_duplicate_rate  aliases / (Pages + aliases), i.e. share of captured URLs
--   near_duplicate_rate   near-duplicate Pages / Pages, as the report computes it
WITH pages AS (
    SELECT snapshot_id, origin, COUNT(*) AS pages FROM silver_pages GROUP BY snapshot_id, origin
),
aliases AS (
    SELECT snapshot_id, origin, COUNT(*) AS aliases FROM silver_page_aliases GROUP BY snapshot_id, origin
)
SELECT p.snapshot_id,
       p.origin,
       p.pages,
       COALESCE(a.aliases, 0) AS exact_duplicate_aliases,
       CAST(COALESCE(a.aliases, 0) AS DOUBLE) / (p.pages + COALESCE(a.aliases, 0)) AS exact_duplicate_rate,
       n.near_duplicate_page_count AS near_duplicate_pages,
       n.near_duplicate_rate
FROM pages AS p
LEFT JOIN aliases AS a ON a.snapshot_id = p.snapshot_id AND a.origin = p.origin
LEFT JOIN silver_near_duplicate_origins AS n ON n.snapshot_id = p.snapshot_id AND n.origin = p.origin
ORDER BY p.snapshot_id, p.origin
