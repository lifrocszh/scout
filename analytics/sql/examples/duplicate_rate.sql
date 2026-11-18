-- Duplicate rate: exact (alias) and near-duplicate share per Origin in the newest snapshot.
SELECT d.origin,
       d.snapshot_id,
       d.pages,
       d.exact_duplicate_aliases,
       ROUND(d.exact_duplicate_rate, 3) AS exact_duplicate_rate,
       d.near_duplicate_pages,
       ROUND(d.near_duplicate_rate, 3) AS near_duplicate_rate
FROM gold_duplicate_rates AS d
JOIN gold_origin_freshness AS f
  ON f.origin = d.origin AND f.latest_snapshot_id = d.snapshot_id
ORDER BY d.origin;
