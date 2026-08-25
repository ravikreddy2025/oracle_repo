-- GENERATED FILE -- do not edit.
-- Source: src/ingestion_framework/control/schema.py
-- Regenerate: python tools/emit_control_ddl.py --catalog ${catalog} --schema control

CREATE TABLE IF NOT EXISTS ${catalog}.control.watermarks (
  table_fqn STRING NOT NULL,
  env STRING NOT NULL,
  watermark_column STRING COMMENT 'Source column, or ORA_ROWSCN for SCN strategy',
  watermark_type STRING COMMENT 'timestamp | date | number',
  watermark_value STRING COMMENT 'Canonical string form; typed comparison happens in the framework',
  previous_value STRING COMMENT 'Value before the last advance, for recovery',
  run_id STRING COMMENT 'Run that last advanced this watermark (fencing token)',
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Current high-water mark per table and environment. One row per (table_fqn, env).'
CLUSTER BY (table_fqn, env)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
