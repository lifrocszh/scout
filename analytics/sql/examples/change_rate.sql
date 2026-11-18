-- Change rate: what changed in each Source since its previous snapshot.
SELECT source_id,
       snapshot_id,
       previous_snapshot_id,
       pages, added, removed, changed, unchanged,
       ROUND(change_rate, 3) AS change_rate,
       ROUND(churn_rate, 3) AS churn_rate
FROM gold_snapshot_changes
WHERE previous_snapshot_id IS NOT NULL
ORDER BY source_id, snapshot_id;
