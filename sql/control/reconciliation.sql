-- GENERATED FILE -- do not edit.
-- Source: src/ingestion_framework/control/schema.py
-- Regenerate: python tools/emit_control_ddl.py --catalog ${catalog} --schema control

CREATE TABLE IF NOT EXISTS ${catalog}.control.reconciliation (
  run_id STRING NOT NULL,
  table_fqn STRING NOT NULL,
  env STRING NOT NULL,
  check_type STRING COMMENT 'row_count | expectation | null_key',
  check_name STRING,
  source_count BIGINT,
  target_count BIGINT,
  delta BIGINT,
  status STRING COMMENT 'PASSED | FAILED | WARNED',
  details STRING,
  checked_at TIMESTAMP
)
USING DELTA
COMMENT 'Source-vs-target counts and data-quality expectation results per run.'
CLUSTER BY (table_fqn, run_id)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
