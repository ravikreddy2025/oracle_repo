"""The immutable audit event stream.

Events are buffered and flushed in batches: an audit trail that costs one Delta
commit per event would dominate the runtime of a small table's load. The buffer
is flushed at task boundaries and on failure, so a crash loses at most the
events of the task that crashed -- and that task's failure is itself recorded
by the control store.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Mapping

from .schema import AUDIT_LOG, qualify
from .sql_client import SqlClient


class EventType(str, Enum):
    """Every audit event the framework can emit."""

    RUN_STARTED = "RUN_STARTED"
    RUN_FINISHED = "RUN_FINISHED"
    TASK_STARTED = "TASK_STARTED"
    TASK_SKIPPED = "TASK_SKIPPED"
    CONFIG_RESOLVED = "CONFIG_RESOLVED"
    EXTRACT_STARTED = "EXTRACT_STARTED"
    EXTRACT_DONE = "EXTRACT_DONE"
    LOAD_STARTED = "LOAD_STARTED"
    LOAD_DONE = "LOAD_DONE"
    RECONCILIATION_DONE = "RECONCILIATION_DONE"
    WATERMARK_ADVANCED = "WATERMARK_ADVANCED"
    WATERMARK_HELD = "WATERMARK_HELD"
    WATERMARK_FORCED = "WATERMARK_FORCED"
    SCHEMA_EVOLVED = "SCHEMA_EVOLVED"
    TASK_SUCCEEDED = "TASK_SUCCEEDED"
    TASK_FAILED = "TASK_FAILED"
    TASK_RETRYING = "TASK_RETRYING"


@dataclass(frozen=True)
class AuditEvent:
    event_id: str
    event_type: str
    event_ts: datetime
    run_id: str | None = None
    table_fqn: str | None = None
    env: str | None = None
    sequence: int = 0
    actor: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "run_id": self.run_id,
            "table_fqn": self.table_fqn,
            "env": self.env,
            "event_type": self.event_type,
            "event_ts": self.event_ts,
            "sequence": self.sequence,
            "actor": self.actor,
            "payload": json.dumps(self.payload, sort_keys=True, default=str),
        }


class AuditLog:
    """Buffered, append-only writer for :class:`AuditEvent`.

    ``now`` and ``id_factory`` are injected so tests get deterministic output
    and so a replayed run can be stamped with the times it actually had.
    """

    def __init__(
        self,
        client: SqlClient,
        catalog: str,
        schema: str,
        *,
        run_id: str | None = None,
        env: str | None = None,
        actor: str | None = None,
        now: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
        buffer_limit: int = 100,
    ) -> None:
        self._client = client
        self._table = qualify(catalog, schema, AUDIT_LOG.name)
        self._run_id = run_id
        self._env = env
        self._actor = actor
        self._now = now or datetime.utcnow
        self._id_factory = id_factory or _uuid_hex
        self._buffer: list[AuditEvent] = []
        self._buffer_limit = buffer_limit
        self._sequence = 0

    @property
    def table(self) -> str:
        return self._table

    @property
    def pending(self) -> int:
        return len(self._buffer)

    def bind(self, *, run_id: str | None = None, env: str | None = None, actor: str | None = None) -> "AuditLog":
        """Attach run context so callers do not repeat it on every event."""
        if run_id is not None:
            self._run_id = run_id
        if env is not None:
            self._env = env
        if actor is not None:
            self._actor = actor
        return self

    def emit(
        self,
        event_type: EventType | str,
        *,
        table_fqn: str | None = None,
        payload: Mapping[str, Any] | None = None,
        run_id: str | None = None,
        flush: bool = False,
    ) -> AuditEvent:
        """Record an event. Buffered unless ``flush`` (or the buffer is full)."""
        self._sequence += 1
        event = AuditEvent(
            event_id=self._id_factory(),
            event_type=str(getattr(event_type, "value", event_type)),
            event_ts=self._now(),
            run_id=run_id or self._run_id,
            table_fqn=table_fqn,
            env=self._env,
            sequence=self._sequence,
            actor=self._actor,
            payload=dict(payload or {}),
        )
        self._buffer.append(event)
        if flush or len(self._buffer) >= self._buffer_limit:
            self.flush()
        return event

    def flush(self) -> int:
        """Write buffered events. Returns how many were written."""
        if not self._buffer:
            return 0
        rows = [event.to_row() for event in self._buffer]
        written = len(rows)
        self._buffer.clear()
        self._client.insert_rows(self._table, rows, AUDIT_LOG.column_names)
        return written

    def __enter__(self) -> "AuditLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Flush on the way out even when the body raised: the events leading up
        # to a failure are the ones worth having.
        self.flush()


def _uuid_hex() -> str:
    import uuid

    return uuid.uuid4().hex
