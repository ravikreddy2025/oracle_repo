-- GENERATED FILE -- do not edit.
-- Source: src/ingestion_framework/control/schema.py
-- Regenerate: python tools/emit_control_ddl.py --catalog ${catalog} --schema control

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
