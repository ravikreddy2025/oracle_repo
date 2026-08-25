-- GENERATED FILE -- do not edit.
-- Source: src/ingestion_framework/control/schema.py
-- Regenerate: python tools/emit_control_ddl.py --catalog ${catalog} --schema control

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
