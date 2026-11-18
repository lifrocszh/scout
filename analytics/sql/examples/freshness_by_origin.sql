-- Freshness by Origin: how old the newest snapshot and capture of each Origin are.
SELECT origin,
       latest_snapshot_id,
       latest_snapshot_at_utc,
       ROUND(date_diff('second', latest_snapshot_at_utc, current_timestamp) / 3600.0, 1) AS snapshot_age_hours,
       latest_capture_at_utc,
       page_count,
       snapshot_count
FROM gold_origin_freshness
ORDER BY snapshot_age_hours DESC, origin;
