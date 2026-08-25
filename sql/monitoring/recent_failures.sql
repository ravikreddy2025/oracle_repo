-- Recent failures
-- The most recent failed attempts with their error, for triage.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

SELECT
  started_at,
  env,
  table_fqn,
  run_id,
  attempt,
  extraction_mode,
  error_type,
  SUBSTRING(error_message, 1, 500)                               AS error_message,
  watermark_from
FROM prod_lakehouse.control.ingestion_tasks
WHERE status = 'FAILED'
ORDER BY started_at DESC
LIMIT 50;
