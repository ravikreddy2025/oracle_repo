-- Per-table outcomes, last 7 days
-- Success rate and failure count per table, using only the final attempt of each task so retries do not inflate the counts.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

WITH final_attempts AS (
  SELECT t.*
  FROM prod_lakehouse.control.ingestion_tasks t
  JOIN (
    SELECT run_id, table_fqn, MAX(attempt) AS attempt
    FROM prod_lakehouse.control.ingestion_tasks
    WHERE started_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    GROUP BY run_id, table_fqn
  ) latest
    ON t.run_id = latest.run_id
   AND t.table_fqn = latest.table_fqn
   AND t.attempt = latest.attempt
)
SELECT
  table_fqn,
  env,
  COUNT(*)                                                        AS runs,
  SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END)           AS succeeded,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)              AS failed,
  ROUND(100.0 * SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                  AS success_pct,
  MAX(attempt)                                                    AS max_attempts,
  ROUND(AVG(duration_ms) / 1000.0, 1)                             AS avg_seconds
FROM final_attempts
GROUP BY table_fqn, env
ORDER BY failed DESC, success_pct ASC, table_fqn;
