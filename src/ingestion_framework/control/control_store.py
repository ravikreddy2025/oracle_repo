"""Run, task, config-registry, and reconciliation state.

The task state machine is the framework's contract with itself: a task is
PENDING, becomes RUNNING, and ends SUCCEEDED, FAILED, or SKIPPED. Nothing
resurrects a terminal task -- a retry is a *new attempt*, appended, so the
history of what actually happened survives. Watermarks are advanced elsewhere
and only after a task reaches SUCCEEDED (see DESIGN 4).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Sequence

from .schema import (
    CONFIG_REGISTRY,
    INGESTION_RUNS,
    INGESTION_TASKS,
    RECONCILIATION,
    ddl_statements,
    qualify,
)
from .sql_client import SqlClient


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class RunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"


TERMINAL_TASK_STATUSES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.SKIPPED}
)

_ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.RUNNING, TaskStatus.SKIPPED}),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.SKIPPED}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.SKIPPED: frozenset(),
}


class ControlStateError(RuntimeError):
    """Raised on an illegal task-state transition."""


def assert_transition(current: TaskStatus | str, new: TaskStatus | str) -> None:
    """Guard the task state machine. Raises on an illegal move."""
    current, new = TaskStatus(current), TaskStatus(new)
    if new not in _ALLOWED_TRANSITIONS[current]:
        if current in TERMINAL_TASK_STATUSES:
            raise ControlStateError(
                f"task is already {current.value}; a terminal task cannot move to "
                f"{new.value} -- record a new attempt instead of overwriting history"
            )
        raise ControlStateError(f"illegal task transition {current.value} -> {new.value}")


def derive_run_status(task_statuses: Iterable[TaskStatus | str]) -> RunStatus:
    """Roll task outcomes up into the run's status."""
    statuses = [TaskStatus(s) for s in task_statuses]
    if not statuses:
        return RunStatus.SUCCEEDED
    if any(s in {TaskStatus.PENDING, TaskStatus.RUNNING} for s in statuses):
        return RunStatus.RUNNING
    failed = [s for s in statuses if s is TaskStatus.FAILED]
    if not failed:
        return RunStatus.SUCCEEDED
    # All-failed is a different operational picture from some-failed: one means
    # the source or cluster is down, the other means a specific table is sick.
    return RunStatus.FAILED if len(failed) == len(statuses) else RunStatus.PARTIAL


def make_run_id(env: str, started_at: datetime, suffix: str) -> str:
    """Human-scannable, sortable run id: ``prod-20260824T101500-a1b2c3``."""
    return f"{env}-{started_at.strftime('%Y%m%dT%H%M%S')}-{suffix[:6]}"


@dataclass
class TaskMetrics:
    """Counters a task reports when it finishes."""

    source_count: int | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None
    bytes_written: int | None = None
    watermark_from: str | None = None
    watermark_to: str | None = None

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TaskRef:
    """Identifies one attempt at one table within one run."""

    run_id: str
    table_fqn: str
    attempt: int = 1


@dataclass
class RunContext:
    run_id: str
    env: str
    started_at: datetime
    trigger: str = "manual"
    triggered_by: str | None = None
    table_count: int = 0
    tasks: list[TaskRef] = field(default_factory=list)


class ControlStore:
    """CRUD over the control-plane tables."""

    def __init__(
        self,
        client: SqlClient,
        catalog: str,
        schema: str,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._catalog = catalog
        self._schema = schema
        self._now = now or datetime.utcnow
        self.runs = qualify(catalog, schema, INGESTION_RUNS.name)
        self.tasks = qualify(catalog, schema, INGESTION_TASKS.name)
        self.config_registry = qualify(catalog, schema, CONFIG_REGISTRY.name)
        self.reconciliation = qualify(catalog, schema, RECONCILIATION.name)

    # -- bootstrap ---------------------------------------------------------

    def initialize(self) -> list[str]:
        """Create the control schema and tables if they do not exist."""
        statements = ddl_statements(self._catalog, self._schema)
        for statement in statements:
            self._client.execute(statement)
        return statements

    # -- runs --------------------------------------------------------------

    def start_run(
        self,
        run_id: str,
        env: str,
        *,
        trigger: str = "manual",
        triggered_by: str | None = None,
        table_count: int = 0,
        job_id: str | None = None,
        job_run_id: str | None = None,
        started_at: datetime | None = None,
    ) -> RunContext:
        started = started_at or self._now()
        self._client.execute(
            f"""
INSERT INTO {self.runs} (
  run_id, env, status, trigger, triggered_by, job_id, job_run_id,
  table_count, started_at, ended_at, duration_ms, error_message
) VALUES (
  :run_id, :env, :status, :trigger, :triggered_by, :job_id, :job_run_id,
  :table_count, :started_at, NULL, NULL, NULL
)
""".strip(),
            {
                "run_id": run_id,
                "env": env,
                "status": RunStatus.RUNNING.value,
                "trigger": trigger,
                "triggered_by": triggered_by,
                "job_id": job_id,
                "job_run_id": job_run_id,
                "table_count": table_count,
                "started_at": started,
            },
        )
        return RunContext(
            run_id=run_id,
            env=env,
            started_at=started,
            trigger=trigger,
            triggered_by=triggered_by,
            table_count=table_count,
        )

    def finish_run(
        self,
        run: RunContext,
        status: RunStatus | str | None = None,
        *,
        error_message: str | None = None,
        ended_at: datetime | None = None,
    ) -> RunStatus:
        """Close a run. With no explicit status, derive it from its tasks."""
        ended = ended_at or self._now()
        resolved = RunStatus(status) if status else derive_run_status(self.task_statuses(run.run_id))
        self._client.execute(
            f"""
UPDATE {self.runs} SET
  status = :status,
  ended_at = :ended_at,
  duration_ms = :duration_ms,
  error_message = :error_message
WHERE run_id = :run_id
""".strip(),
            {
                "status": resolved.value,
                "ended_at": ended,
                "duration_ms": _duration_ms(run.started_at, ended),
                "error_message": error_message,
                "run_id": run.run_id,
            },
        )
        return resolved

    def task_statuses(self, run_id: str) -> list[str]:
        rows = self._client.query(
            f"SELECT status FROM {self.tasks} t WHERE run_id = :run_id "
            f"AND attempt = (SELECT MAX(attempt) FROM {self.tasks} x "
            f"WHERE x.run_id = t.run_id AND x.table_fqn = t.table_fqn)",
            {"run_id": run_id},
        )
        return [row["status"] for row in rows]

    # -- tasks -------------------------------------------------------------

    def start_task(
        self,
        run_id: str,
        table_fqn: str,
        env: str,
        *,
        extraction_mode: str,
        write_mode: str,
        config_hash: str = "",
        attempt: int = 1,
        watermark_from: str | None = None,
        started_at: datetime | None = None,
    ) -> TaskRef:
        """Open a task in RUNNING. Retries pass a higher ``attempt``."""
        started = started_at or self._now()
        self._client.execute(
            f"""
INSERT INTO {self.tasks} (
  run_id, table_fqn, env, attempt, status, extraction_mode, write_mode,
  watermark_from, watermark_to, source_count, rows_read, rows_written,
  rows_inserted, rows_updated, rows_deleted, bytes_written, config_hash,
  started_at, ended_at, duration_ms, error_type, error_message
) VALUES (
  :run_id, :table_fqn, :env, :attempt, :status, :extraction_mode, :write_mode,
  :watermark_from, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, :config_hash,
  :started_at, NULL, NULL, NULL, NULL
)
""".strip(),
            {
                "run_id": run_id,
                "table_fqn": table_fqn,
                "env": env,
                "attempt": attempt,
                "status": TaskStatus.RUNNING.value,
                "extraction_mode": extraction_mode,
                "write_mode": write_mode,
                "watermark_from": watermark_from,
                "config_hash": config_hash,
                "started_at": started,
            },
        )
        return TaskRef(run_id=run_id, table_fqn=table_fqn, attempt=attempt)

    def finish_task(
        self,
        task: TaskRef,
        status: TaskStatus | str,
        *,
        metrics: TaskMetrics | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        started_at: datetime | None = None,
        ended_at: datetime | None = None,
        current_status: TaskStatus | str = TaskStatus.RUNNING,
    ) -> TaskStatus:
        """Close a task, enforcing the state machine before writing."""
        new_status = TaskStatus(status)
        assert_transition(current_status, new_status)

        ended = ended_at or self._now()
        metrics = metrics or TaskMetrics()
        params: dict[str, Any] = {
            "status": new_status.value,
            "ended_at": ended,
            "duration_ms": _duration_ms(started_at, ended) if started_at else None,
            "error_type": error_type,
            "error_message": _truncate(error_message),
            "run_id": task.run_id,
            "table_fqn": task.table_fqn,
            "attempt": task.attempt,
            **metrics.to_row(),
        }
        self._client.execute(
            f"""
UPDATE {self.tasks} SET
  status = :status,
  watermark_from = COALESCE(:watermark_from, watermark_from),
  watermark_to = :watermark_to,
  source_count = :source_count,
  rows_read = :rows_read,
  rows_written = :rows_written,
  rows_inserted = :rows_inserted,
  rows_updated = :rows_updated,
  rows_deleted = :rows_deleted,
  bytes_written = :bytes_written,
  ended_at = :ended_at,
  duration_ms = :duration_ms,
  error_type = :error_type,
  error_message = :error_message
WHERE run_id = :run_id AND table_fqn = :table_fqn AND attempt = :attempt
""".strip(),
            params,
        )
        return new_status

    def next_attempt(self, run_id: str, table_fqn: str) -> int:
        """The attempt number a retry should use."""
        rows = self._client.query(
            f"SELECT MAX(attempt) AS max_attempt FROM {self.tasks} "
            f"WHERE run_id = :run_id AND table_fqn = :table_fqn",
            {"run_id": run_id, "table_fqn": table_fqn},
        )
        current = rows[0].get("max_attempt") if rows else None
        return int(current) + 1 if current else 1

    def get_task(self, task: TaskRef) -> dict[str, Any] | None:
        rows = self._client.query(
            f"SELECT * FROM {self.tasks} "
            f"WHERE run_id = :run_id AND table_fqn = :table_fqn AND attempt = :attempt",
            {"run_id": task.run_id, "table_fqn": task.table_fqn, "attempt": task.attempt},
        )
        return rows[0] if rows else None

    # -- config registry ---------------------------------------------------

    def register_config(
        self,
        *,
        table_fqn: str,
        env: str,
        config_hash: str,
        resolved_json: str,
        config_sources: Sequence[str] = (),
        run_id: str | None = None,
        seen_at: datetime | None = None,
    ) -> None:
        """Record the config this run used, keyed by content hash.

        Re-running with an unchanged config touches ``last_seen_at`` rather than
        writing a duplicate, so the registry reads as 'config X was in effect
        from A to B'.
        """
        seen = seen_at or self._now()
        self._client.execute(
            f"""
MERGE INTO {self.config_registry} AS t
USING (
  SELECT :config_hash AS config_hash, :table_fqn AS table_fqn, :env AS env
) AS s
ON t.config_hash = s.config_hash AND t.table_fqn = s.table_fqn AND t.env = s.env
WHEN MATCHED THEN UPDATE SET
  t.last_seen_at = :seen_at,
  t.last_run_id = :run_id
WHEN NOT MATCHED THEN INSERT (
  config_hash, table_fqn, env, resolved_json, config_sources,
  first_seen_at, last_seen_at, last_run_id
) VALUES (
  s.config_hash, s.table_fqn, s.env, :resolved_json, :config_sources,
  :seen_at, :seen_at, :run_id
)
""".strip(),
            {
                "config_hash": config_hash,
                "table_fqn": table_fqn,
                "env": env,
                "resolved_json": resolved_json,
                "config_sources": list(config_sources),
                "seen_at": seen,
                "run_id": run_id,
            },
        )

    # -- reconciliation ----------------------------------------------------

    def record_checks(self, checks: Sequence[Mapping[str, Any]], *, checked_at: datetime | None = None) -> int:
        """Append reconciliation / expectation results for a task."""
        if not checks:
            return 0
        stamp = checked_at or self._now()
        rows = [{**dict(check), "checked_at": check.get("checked_at", stamp)} for check in checks]
        self._client.insert_rows(self.reconciliation, rows, RECONCILIATION.column_names)
        return len(rows)


def _duration_ms(start: datetime | None, end: datetime) -> int | None:
    if start is None:
        return None
    return int((end - start).total_seconds() * 1000)


def _truncate(text: str | None, limit: int = 8000) -> str | None:
    """Keep a stack trace from turning one bad row into an unreadable table."""
    if text is None:
        return None
    text = str(text)
    return text if len(text) <= limit else text[:limit] + f"... [truncated {len(text) - limit} chars]"


def json_payload(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)
