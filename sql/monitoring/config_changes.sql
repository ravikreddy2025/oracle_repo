-- Config changes, last 30 days
-- When a table's effective config last changed. Pairs with the volume trend: a shift in row counts on the day a config hash changed is rarely a coincidence.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

SELECT
  table_fqn,
  env,
  config_hash,
  first_seen_at,
  last_seen_at,
  last_run_id,
  config_sources
FROM prod_lakehouse.control.config_registry
WHERE first_seen_at >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
ORDER BY first_seen_at DESC, table_fqn;
