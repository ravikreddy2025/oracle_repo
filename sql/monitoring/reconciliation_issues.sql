-- Reconciliation and quality failures, last 7 days
-- Checks that failed or warned. WARNED rows did not stop the load, so they are the ones most likely to go unnoticed.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

SELECT
  checked_at,
  env,
  table_fqn,
  run_id,
  check_type,
  check_name,
  status,
  source_count,
  target_count,
  delta,
  details
FROM prod_lakehouse.control.reconciliation
WHERE status IN ('FAILED', 'WARNED')
  AND checked_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
ORDER BY checked_at DESC;
