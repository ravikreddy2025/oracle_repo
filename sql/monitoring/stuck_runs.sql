-- Runs still RUNNING after 6h
-- A run left open long past its timeout usually means the driver died before it could close its own row -- the data may be fine, but the control plane no longer reflects reality.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

SELECT
  run_id,
  env,
  trigger,
  started_at,
  ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(started_at)) / 3600.0, 2)
                                                                 AS hours_open,
  table_count
FROM prod_lakehouse.control.ingestion_runs
WHERE status = 'RUNNING'
  AND started_at < CURRENT_TIMESTAMP() - INTERVAL 6 HOURS
ORDER BY started_at;
