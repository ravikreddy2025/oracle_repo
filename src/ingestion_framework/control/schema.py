"""Definitions and DDL for the control-plane Delta tables.

This module is the single source of truth for the control schema: the runtime
creates tables from it, and ``tools/emit_control_ddl.py`` dumps the same
statements to ``sql/control/`` for DBAs and reviewers. There is no hand-written
DDL to drift out of step.

All statements are idempotent (``CREATE ... IF NOT EXISTS``) so ``init-control``
is safe to run on every deployment.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Unity Catalog identifier: letters, digits, underscores. Anything else would
# need backtick-quoting, and a control-plane name never legitimately does.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ControlSchemaError(ValueError):
    """Raised when a catalog/schema/table name is not a safe identifier."""


def qualify(*parts: str) -> str:
    """Join and validate identifier parts into a dotted name.

    Identifiers cannot be bound as query parameters, so every one that reaches
    a SQL string passes through here first.
    """
    for part in parts:
        if not part or not _IDENTIFIER.match(part):
            raise ControlSchemaError(
                f"{part!r} is not a valid unquoted identifier (letters, digits, underscore; "
                f"must not start with a digit)"
            )
    return ".".join(parts)


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    comment: str = ""
    nullable: bool = True

    def to_sql(self) -> str:
        parts = [self.name, self.type]
        if not self.nullable:
            parts.append("NOT NULL")
        if self.comment:
            parts.append(f"COMMENT '{_escape(self.comment)}'")
        return " ".join(parts)


@dataclass(frozen=True)
class TableDef:
    name: str
    comment: str
    columns: tuple[Column, ...]
    cluster_by: tuple[str, ...] = ()
    properties: dict[str, str] = field(default_factory=dict)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def create_sql(self, catalog: str, schema: str) -> str:
        fqn = qualify(catalog, schema, self.name)
        body = ",\n  ".join(c.to_sql() for c in self.columns)
        stmt = [f"CREATE TABLE IF NOT EXISTS {fqn} (", f"  {body}", ")", "USING DELTA"]
        if self.comment:
            stmt.append(f"COMMENT '{_escape(self.comment)}'")
        if self.cluster_by:
            for col in self.cluster_by:
                if col not in self.column_names:
                    raise ControlSchemaError(
                        f"{self.name}: cluster_by column {col!r} is not defined on the table"
                    )
            stmt.append(f"CLUSTER BY ({', '.join(self.cluster_by)})")
        properties = {"delta.enableDeletionVectors": "true", **self.properties}
        rendered = ", ".join(f"'{k}' = '{_escape(v)}'" for k, v in sorted(properties.items()))
        stmt.append(f"TBLPROPERTIES ({rendered})")
        return "\n".join(stmt)


def _escape(text: str) -> str:
    return str(text).replace("'", "''")


_TS = "TIMESTAMP"
_STR = "STRING"
_LONG = "BIGINT"


CONFIG_REGISTRY = TableDef(
    name="config_registry",
    comment="Every distinct effective config the framework has run with, keyed by content hash.",
    columns=(
        Column("config_hash", _STR, "SHA-256 of the canonical effective config", nullable=False),
        Column("table_fqn", _STR, "domain.table", nullable=False),
        Column("env", _STR, nullable=False),
        Column("resolved_json", _STR, "The full effective config as JSON"),
        Column("config_sources", "ARRAY<STRING>", "Files that contributed, in merge order"),
        Column("first_seen_at", _TS),
        Column("last_seen_at", _TS),
        Column("last_run_id", _STR),
    ),
    cluster_by=("table_fqn", "env"),
)

WATERMARKS = TableDef(
    name="watermarks",
    comment="Current high-water mark per table and environment. One row per (table_fqn, env).",
    columns=(
        Column("table_fqn", _STR, nullable=False),
        Column("env", _STR, nullable=False),
        Column("watermark_column", _STR, "Source column, or ORA_ROWSCN for SCN strategy"),
        Column("watermark_type", _STR, "timestamp | date | number"),
        Column(
            "watermark_value",
            _STR,
            "Canonical string form; typed comparison happens in the framework",
        ),
        Column("previous_value", _STR, "Value before the last advance, for recovery"),
        Column("run_id", _STR, "Run that last advanced this watermark (fencing token)"),
        Column("updated_at", _TS),
    ),
    cluster_by=("table_fqn", "env"),
)

INGESTION_RUNS = TableDef(
    name="ingestion_runs",
    comment="One row per pipeline run (a batch of one or more tables).",
    columns=(
        Column("run_id", _STR, nullable=False),
        Column("env", _STR, nullable=False),
        Column("status", _STR, "RUNNING | SUCCEEDED | FAILED | PARTIAL"),
        Column("trigger", _STR, "schedule | manual | backfill | retry"),
        Column("triggered_by", _STR),
        Column("job_id", _STR),
        Column("job_run_id", _STR),
        Column("table_count", "INT"),
        Column("started_at", _TS),
        Column("ended_at", _TS),
        Column("duration_ms", _LONG),
        Column("error_message", _STR),
    ),
    cluster_by=("run_id",),
)

INGESTION_TASKS = TableDef(
    name="ingestion_tasks",
    comment="One row per table per run: the state machine and the metrics for that load.",
    columns=(
        Column("run_id", _STR, nullable=False),
        Column("table_fqn", _STR, nullable=False),
        Column("env", _STR, nullable=False),
        Column("attempt", "INT", "1-based; retries append attempts, they do not overwrite"),
        Column("status", _STR, "PENDING | RUNNING | SUCCEEDED | FAILED | SKIPPED"),
        Column("extraction_mode", _STR),
        Column("write_mode", _STR),
        Column("watermark_from", _STR, "Lower bound actually used (after overlap)"),
        Column("watermark_to", _STR, "New high-water mark observed in this batch"),
        Column("source_count", _LONG, "Rows counted at source for reconciliation"),
        Column("rows_read", _LONG),
        Column("rows_written", _LONG),
        Column("rows_inserted", _LONG),
        Column("rows_updated", _LONG),
        Column("rows_deleted", _LONG),
        Column("bytes_written", _LONG),
        Column("config_hash", _STR),
        Column("started_at", _TS),
        Column("ended_at", _TS),
        Column("duration_ms", _LONG),
        Column("error_type", _STR),
        Column("error_message", _STR),
    ),
    cluster_by=("table_fqn", "run_id"),
)

AUDIT_LOG = TableDef(
    name="audit_log",
    comment="Immutable event stream. Append-only: rows are never updated or deleted.",
    columns=(
        Column("event_id", _STR, nullable=False),
        Column("run_id", _STR),
        Column("table_fqn", _STR),
        Column("env", _STR),
        Column("event_type", _STR, nullable=False),
        Column("event_ts", _TS, nullable=False),
        Column("sequence", "INT", "Order of events within a task, for stable replay"),
        Column("actor", _STR, "Service principal or user that caused the event"),
        Column("payload", _STR, "Event-specific detail as JSON"),
    ),
    cluster_by=("run_id", "table_fqn"),
)

RECONCILIATION = TableDef(
    name="reconciliation",
    comment="Source-vs-target counts and data-quality expectation results per run.",
    columns=(
        Column("run_id", _STR, nullable=False),
        Column("table_fqn", _STR, nullable=False),
        Column("env", _STR, nullable=False),
        Column("check_type", _STR, "row_count | expectation | null_key"),
        Column("check_name", _STR),
        Column("source_count", _LONG),
        Column("target_count", _LONG),
        Column("delta", _LONG),
        Column("status", _STR, "PASSED | FAILED | WARNED"),
        Column("details", _STR),
        Column("checked_at", _TS),
    ),
    cluster_by=("table_fqn", "run_id"),
)


ALL_TABLES: tuple[TableDef, ...] = (
    CONFIG_REGISTRY,
    WATERMARKS,
    INGESTION_RUNS,
    INGESTION_TASKS,
    AUDIT_LOG,
    RECONCILIATION,
)


def ddl_statements(catalog: str, schema: str) -> list[str]:
    """Every statement needed to stand up the control plane, in order."""
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {qualify(catalog, schema)} "
        f"COMMENT 'Ingestion framework control plane'"
    ]
    statements.extend(table.create_sql(catalog, schema) for table in ALL_TABLES)
    return statements
