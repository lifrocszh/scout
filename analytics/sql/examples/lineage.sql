-- Lineage: which crawl run and snapshot produced each table version. Gold versions
-- name the Silver table versions they were built from (input_ref).
SELECT table_name,
       table_version,
       rows_inserted,
       input_kind,
       COALESCE(snapshot_id, '') AS snapshot_id,
       COALESCE(run_id, '') AS run_id,
       CASE WHEN input_kind = 'gold_refresh' THEN input_ref ELSE '' END AS built_from
FROM lineage_loads
ORDER BY loaded_at_utc, table_name, table_version;
