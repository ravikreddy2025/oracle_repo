"""Control plane: run/task state, watermarks, audit trail, and config registry."""

from __future__ import annotations

from .audit import AuditEvent, AuditLog, EventType
from .control_store import (
    ControlStateError,
    ControlStore,
    RunContext,
    RunStatus,
    TaskMetrics,
    TaskRef,
    TaskStatus,
    assert_transition,
    derive_run_status,
    make_run_id,
)
from .schema import ALL_TABLES, ControlSchemaError, TableDef, ddl_statements, qualify
from .sql_client import RecordingSqlClient, SparkSqlClient, SqlClient
from .watermark import (
    WatermarkError,
    WatermarkRecord,
    WatermarkStore,
    WatermarkWindow,
    canonicalize,
    compute_window,
    should_advance,
)

__all__ = [
    "ALL_TABLES",
    "AuditEvent",
    "AuditLog",
    "ControlSchemaError",
    "ControlStateError",
    "ControlStore",
    "EventType",
    "RecordingSqlClient",
    "RunContext",
    "RunStatus",
    "SparkSqlClient",
    "SqlClient",
    "TableDef",
    "TaskMetrics",
    "TaskRef",
    "TaskStatus",
    "WatermarkError",
    "WatermarkRecord",
    "WatermarkStore",
    "WatermarkWindow",
    "assert_transition",
    "canonicalize",
    "compute_window",
    "ddl_statements",
    "derive_run_status",
    "make_run_id",
    "qualify",
    "should_advance",
]
