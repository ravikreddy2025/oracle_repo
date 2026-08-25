-- GENERATED FILE -- do not edit.
-- Source: src/ingestion_framework/control/schema.py
-- Regenerate: python tools/emit_control_ddl.py --catalog ${catalog} --schema control

CREATE TABLE IF NOT EXISTS ${catalog}.control.ingestion_tasks (
  run_id STRING NOT NULL,
  table_fqn STRING NOT NULL,
  env STRING NOT NULL,
  attempt INT COMMENT '1-based; retries append attempts, they do not overwrite',
  status STRING COMMENT 'PENDING | RUNNING | SUCCEEDED | FAILED | SKIPPED',
  extraction_mode STRING,
  write_mode STRING,
  watermark_from STRING COMMENT 'Lower bound actually used (after overlap)',
  watermark_to STRING COMMENT 'New high-water mark observed in this batch',
  source_count BIGINT COMMENT 'Rows counted at source for reconciliation',
  rows_read BIGINT,
  rows_written BIGINT,
  rows_inserted BIGINT,
  rows_updated BIGINT,
  rows_deleted BIGINT,
  bytes_written BIGINT,
  config_hash STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_ms BIGINT,
  error_type STRING,
  error_message STRING
)
USING DELTA
COMMENT 'One row per table per run: the state machine and the metrics for that load.'
CLUSTER BY (table_fqn, run_id)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
