-- Row volume by table, last 30 days
-- Daily rows written per table, with the deviation from that table's own median. A table that suddenly writes 10x or 0 rows is worth looking at even when the run reported success.
-- GENERATED from src/ingestion_framework/observability/monitoring.py

WITH daily AS (
  SELECT
    table_fqn,
    env,
    DATE(started_at)        AS load_date,
    SUM(rows_written)       AS rows_written,
    SUM(source_count)       AS source_count
  FROM prod_lakehouse.control.ingestion_tasks
  WHERE status = 'SUCCEEDED'
    AND started_at >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
  GROUP BY table_fqn, env, DATE(started_at)
),
baseline AS (
  SELECT table_fqn, env, PERCENTILE(rows_written, 0.5) AS median_rows
  FROM daily
  GROUP BY table_fqn, env
)
SELECT
  d.table_fqn,
  d.env,
  d.load_date,
  d.rows_written,
  d.source_count,
  ROUND(b.median_rows, 0)                                        AS median_rows,
  CASE
    WHEN b.median_rows IS NULL OR b.median_rows = 0 THEN NULL
    ELSE ROUND(100.0 * (d.rows_written - b.median_rows) / b.median_rows, 1)
  END                                                            AS pct_vs_median
FROM daily d
LEFT JOIN baseline b ON b.table_fqn = d.table_fqn AND b.env = d.env
ORDER BY d.load_date DESC, d.table_fqn;
