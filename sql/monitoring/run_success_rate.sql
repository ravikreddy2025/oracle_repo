-- Run outcomes, last 7 days
-- Daily run counts by status. A rising PARTIAL count means one table is sick; a rising FAILED count means the source or cluster is.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

SELECT
  DATE(started_at)                                   AS run_date,
  env,
  status,
  COUNT(*)                                           AS runs,
  ROUND(AVG(duration_ms) / 1000.0, 1)                AS avg_seconds
FROM prod_lakehouse.control.ingestion_runs
WHERE started_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY DATE(started_at), env, status
ORDER BY run_date DESC, env, status;
