-- GENERATED FILE -- do not edit.
-- Source: src/ingestion_framework/control/schema.py
-- Regenerate: python tools/emit_control_ddl.py --catalog ${catalog} --schema control

CREATE TABLE IF NOT EXISTS ${catalog}.control.ingestion_runs (
  run_id STRING NOT NULL,
  env STRING NOT NULL,
  status STRING COMMENT 'RUNNING | SUCCEEDED | FAILED | PARTIAL',
  trigger STRING COMMENT 'schedule | manual | backfill | retry',
  triggered_by STRING,
  job_id STRING,
  job_run_id STRING,
  table_count INT,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_ms BIGINT,
  error_message STRING
)
USING DELTA
COMMENT 'One row per pipeline run (a batch of one or more tables).'
CLUSTER BY (run_id)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
