-- Table freshness vs SLA
-- Hours since each watermark last advanced, compared with the declared freshness SLA. Breaches sort to the top.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

WITH sla(table_fqn, env, sla_hours) AS (VALUES
  ('finance.gl_accounts', 'prod', CAST(NULL AS DOUBLE)),
  ('finance.gl_transactions', 'prod', 4),
  ('sales.order_events', 'prod', CAST(NULL AS DOUBLE))
)
SELECT
  w.table_fqn,
  w.env,
  w.watermark_value,
  w.updated_at                                                   AS watermark_updated_at,
  ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(w.updated_at)) / 3600.0, 2)
                                                                 AS hours_since_advance,
  s.sla_hours,
  CASE
    WHEN s.sla_hours IS NULL THEN NULL
    WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(w.updated_at)) / 3600.0 > s.sla_hours
      THEN TRUE
    ELSE FALSE
  END                                                            AS sla_breached
FROM prod_lakehouse.control.watermarks w
LEFT JOIN sla s
  ON s.table_fqn = w.table_fqn AND s.env = w.env
ORDER BY sla_breached DESC NULLS LAST, hours_since_advance DESC;
