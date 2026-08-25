-- GENERATED FILE -- do not edit.
-- Source: src/ingestion_framework/control/schema.py
-- Regenerate: python tools/emit_control_ddl.py --catalog ${catalog} --schema control

CREATE SCHEMA IF NOT EXISTS ${catalog}.control COMMENT 'Ingestion framework control plane';

CREATE TABLE IF NOT EXISTS ${catalog}.control.config_registry (
  config_hash STRING NOT NULL COMMENT 'SHA-256 of the canonical effective config',
  table_fqn STRING NOT NULL COMMENT 'domain.table',
  env STRING NOT NULL,
  resolved_json STRING COMMENT 'The full effective config as JSON',
  config_sources ARRAY<STRING> COMMENT 'Files that contributed, in merge order',
  first_seen_at TIMESTAMP,
  last_seen_at TIMESTAMP,
  last_run_id STRING
)
USING DELTA
COMMENT 'Every distinct effective config the framework has run with, keyed by content hash.'
CLUSTER BY (table_fqn, env)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

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

CREATE TABLE IF NOT EXISTS ${catalog}.control.audit_log (
  event_id STRING NOT NULL,
  run_id STRING,
  table_fqn STRING,
  env STRING,
  event_type STRING NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  sequence INT COMMENT 'Order of events within a task, for stable replay',
  actor STRING COMMENT 'Service principal or user that caused the event',
  payload STRING COMMENT 'Event-specific detail as JSON'
)
USING DELTA
COMMENT 'Immutable event stream. Append-only: rows are never updated or deleted.'
CLUSTER BY (run_id, table_fqn)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

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
