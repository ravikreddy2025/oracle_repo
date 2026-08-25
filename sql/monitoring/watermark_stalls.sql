-- Tables whose watermark held for the last 5 runs
-- Successful runs that moved no watermark. Legitimate when a source is genuinely quiet, and the first symptom of a filter or predicate that silently matches nothing.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

WITH recent AS (
  SELECT
    table_fqn,
    env,
    run_id,
    started_at,
    watermark_to,
    ROW_NUMBER() OVER (PARTITION BY table_fqn, env ORDER BY started_at DESC) AS rn
  FROM prod_lakehouse.control.ingestion_tasks
  WHERE status = 'SUCCEEDED'
)
SELECT
  table_fqn,
  env,
  COUNT(*)                                                       AS successful_runs,
  SUM(CASE WHEN watermark_to IS NULL THEN 1 ELSE 0 END)          AS runs_with_no_new_data,
  MAX(started_at)                                                AS last_run_at
FROM recent
WHERE rn <= 5
GROUP BY table_fqn, env
HAVING SUM(CASE WHEN watermark_to IS NULL THEN 1 ELSE 0 END) = COUNT(*)
   AND COUNT(*) = 5
ORDER BY last_run_at;
