"""Run one table, once.

This module exists to own an ordering, and the ordering is the whole point:

    start task -> read watermark -> extract -> stage -> load -> reconcile
      -> advance watermark -> close task

The watermark moves **after** the load has committed and **only** if the task
succeeded (DESIGN 4). Any other order can lose data: advance-then-load loses
the batch if the load fails, and the next run resumes past rows that were never
written. A failed reconciliation is treated as a failed task for exactly the
same reason -- a mark that advances past rows we are not sure landed is worse
than a run that stops and tells someone.

Retries append a new attempt rather than reopening the old one, so the history
of what happened survives.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Sequence

from ..control.audit import AuditLog, EventType
from ..control.control_store import (
    ControlStore,
    TaskMetrics,
    TaskRef,
    TaskStatus,
)
from ..control.watermark import WatermarkStore
from ..engine.extractor import ExtractResult, OracleExtractor
from ..engine.loader import DeltaLoader, LoadResult
from ..engine.reconciler import ReconciliationReport, Reconciler, summarize
from ..engine.run_spec import RunSpec
from ..engine.transformer import StageResult, Transformer, view_names
from ..observability.alerts import (
    AlertDispatcher,
    build_failure_alert,
    build_reconciliation_alert,
)
from ..observability.logger import StructuredLogger


class ReconciliationFailure(RuntimeError):
    """Raised when checks fail, so the task fails and the watermark holds."""

    def __init__(self, report: ReconciliationReport) -> None:
        names = ", ".join(c.check_name for c in report.failures)
        super().__init__(f"reconciliation failed: {names}")
        self.report = report


@dataclass
class TaskOutcome:
    """Everything one attempt at one table produced."""

    table_fqn: str
    run_id: str
    status: TaskStatus
    attempt: int = 1
    metrics: TaskMetrics = field(default_factory=TaskMetrics)
    report: ReconciliationReport | None = None
    watermark_advanced: bool = False
    error: BaseException | None = None
    duration_ms: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.status is TaskStatus.SUCCEEDED


@dataclass
class Engine:
    """The collaborators a run needs. Bundled so tests can swap any one."""

    extractor: OracleExtractor
    transformer: Transformer
    loader: DeltaLoader
    reconciler: Reconciler
    control: ControlStore
    watermarks: WatermarkStore
    audit: AuditLog
    logger: StructuredLogger
    alerts: AlertDispatcher
    now: Callable[[], datetime] = datetime.utcnow
    batch_id_factory: Callable[[], str] = lambda: uuid.uuid4().hex[:12]
    sleep: Callable[[float], None] = time.sleep


class TableRunner:
    """Executes the ingest lifecycle for a single table."""

    def __init__(self, engine: Engine) -> None:
        self._e = engine

    # -- public API --------------------------------------------------------

    def run(self, spec: RunSpec, run_id: str, *, attempt: int = 1) -> TaskOutcome:
        """Run one attempt. Records state and never lets an error escape silently."""
        engine = self._e
        log = engine.logger.bind(
            table_fqn=spec.table_fqn, env=spec.env, run_id=run_id, attempt=attempt
        )

        if not spec.runtime.enabled:
            return self._skip(spec, run_id, attempt, log, "disabled by config")

        engine.control.register_config(
            table_fqn=spec.table_fqn,
            env=spec.env,
            config_hash=spec.config_hash,
            resolved_json="",
            run_id=run_id,
        )

        started_at = engine.now()
        task = engine.control.start_task(
            run_id,
            spec.table_fqn,
            spec.env,
            extraction_mode=spec.extraction.mode,
            write_mode=spec.target.write_mode,
            config_hash=spec.config_hash,
            attempt=attempt,
            started_at=started_at,
        )
        engine.audit.emit(
            EventType.TASK_STARTED,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload={"attempt": attempt, "mode": spec.extraction.mode},
        )
        log.info("task started", stage="start", write_mode=spec.target.write_mode)

        try:
            outcome = self._execute(spec, run_id, task, attempt, started_at, log)
        except BaseException as exc:  # noqa: BLE001 - recorded, then re-raised by the caller
            return self._fail(spec, run_id, task, attempt, started_at, log, exc)

        return outcome

    def run_with_retries(self, spec: RunSpec, run_id: str) -> TaskOutcome:
        """Run, retrying per config. Each retry is a fresh attempt row."""
        engine = self._e
        attempts = spec.runtime.retries + 1
        outcome: TaskOutcome | None = None

        for attempt in range(1, attempts + 1):
            outcome = self.run(spec, run_id, attempt=attempt)
            if outcome.succeeded or outcome.status is TaskStatus.SKIPPED:
                return outcome
            if attempt < attempts:
                backoff = spec.runtime.retry_backoff_seconds
                engine.audit.emit(
                    EventType.TASK_RETRYING,
                    table_fqn=spec.table_fqn,
                    run_id=run_id,
                    payload={"attempt": attempt, "next_attempt": attempt + 1, "backoff_s": backoff},
                )
                engine.logger.bind(table_fqn=spec.table_fqn, run_id=run_id).warning(
                    "retrying after failure", stage="retry", attempt=attempt, backoff_s=backoff
                )
                if backoff:
                    engine.sleep(backoff)

        assert outcome is not None
        self._alert_failure(spec, run_id, outcome)
        return outcome

    # -- the lifecycle -----------------------------------------------------

    def _execute(
        self,
        spec: RunSpec,
        run_id: str,
        task: TaskRef,
        attempt: int,
        started_at: datetime,
        log: StructuredLogger,
    ) -> TaskOutcome:
        engine = self._e
        batch_id = engine.batch_id_factory()
        views = view_names(spec.table_fqn, run_id, attempt)

        # 1. Where does this batch start?
        window = engine.watermarks.window(
            spec.table_fqn,
            spec.env,
            watermark_type=spec.extraction.incremental.watermark_type,
            overlap=spec.extraction.incremental.overlap_delta,
            lower_bound_default=spec.extraction.incremental.lower_bound_default,
        )
        log.info(
            "watermark window resolved",
            stage="watermark",
            lower_bound=window.lower_bound,
            overlap_applied=window.overlap_applied,
            first_run=window.is_first_run,
        )

        # 2. Extract.
        engine.audit.emit(EventType.EXTRACT_STARTED, table_fqn=spec.table_fqn, run_id=run_id)
        extract: ExtractResult = engine.extractor.extract(spec, window)
        engine.audit.emit(
            EventType.EXTRACT_DONE,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload={
                "source_count": extract.source_count,
                "lower_bound": extract.lower_bound,
                "upper_bound": extract.upper_bound,
                "partitions": extract.num_partitions,
            },
        )
        log.info(
            "extract complete",
            stage="extract",
            source_count=extract.source_count,
            partitions=extract.num_partitions,
        )

        # 3. Stage: audit columns and the dedupe the merge depends on.
        stage: StageResult = engine.transformer.stage(
            extract.dataframe,
            spec,
            run_id=run_id,
            batch_id=batch_id,
            ingested_at=engine.now(),
            views=views,
        )
        log.info(
            "batch staged",
            stage="transform",
            deduped=stage.query.dedupe_applied,
            duplicates_removed=stage.duplicates_removed,
            null_key_rows=stage.null_key_rows,
        )

        # 4. Load.
        engine.audit.emit(EventType.LOAD_STARTED, table_fqn=spec.table_fqn, run_id=run_id)
        load: LoadResult = engine.loader.write(stage.dataframe, spec, staged_view=views.staged)
        engine.audit.emit(
            EventType.LOAD_DONE,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload={
                "rows_written": load.rows_written,
                "rows_inserted": load.rows_inserted,
                "rows_updated": load.rows_updated,
                "table_created": load.table_created,
            },
        )
        if load.table_created:
            log.info("target table created", stage="load", target=load.target)
        log.info(
            "load complete",
            stage="load",
            rows_written=load.rows_written,
            rows_inserted=load.rows_inserted,
            rows_updated=load.rows_updated,
        )

        # 5. Reconcile before committing the watermark.
        report = engine.reconciler.run(
            spec,
            source_count=extract.source_count,
            duplicates_removed=stage.duplicates_removed,
            null_key_rows=stage.null_key_rows,
            rows_written=load.rows_written,
            view=views.staged,
        )
        engine.control.record_checks(
            report.to_rows(run_id=run_id, table_fqn=spec.table_fqn, env=spec.env)
        )
        engine.audit.emit(
            EventType.RECONCILIATION_DONE,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload=dict(summarize(report)),
        )

        metrics = TaskMetrics(
            source_count=extract.source_count,
            rows_read=extract.source_count,
            rows_written=load.rows_written,
            rows_inserted=load.rows_inserted,
            rows_updated=load.rows_updated,
            rows_deleted=load.rows_deleted,
            watermark_from=extract.lower_bound,
            watermark_to=extract.new_watermark,
        )

        if not report.ok:
            # Advancing past rows we are not sure landed is worse than stopping.
            log.error(
                "reconciliation failed", stage="reconcile", **dict(summarize(report))
            )
            self._e.alerts.dispatch(
                build_reconciliation_alert(
                    table_fqn=spec.table_fqn, env=spec.env, run_id=run_id,
                    failures=report.failures,
                ),
                spec.alerting.on_reconciliation_mismatch,
            )
            raise ReconciliationFailure(report)

        # 6. Only now may the watermark move.
        advanced = self._advance_watermark(spec, run_id, extract, log)

        ended_at = engine.now()
        engine.control.finish_task(
            task,
            TaskStatus.SUCCEEDED,
            metrics=metrics,
            started_at=started_at,
            ended_at=ended_at,
        )
        engine.audit.emit(
            EventType.TASK_SUCCEEDED,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload={"rows_written": load.rows_written, "watermark_advanced": advanced},
            flush=True,
        )
        duration = int((ended_at - started_at).total_seconds() * 1000)
        log.info("task succeeded", stage="finish", duration_ms=duration)

        return TaskOutcome(
            table_fqn=spec.table_fqn,
            run_id=run_id,
            status=TaskStatus.SUCCEEDED,
            attempt=attempt,
            metrics=metrics,
            report=report,
            watermark_advanced=advanced,
            duration_ms=duration,
        )

    # -- steps -------------------------------------------------------------

    def _advance_watermark(
        self, spec: RunSpec, run_id: str, extract: ExtractResult, log: StructuredLogger
    ) -> bool:
        engine = self._e
        if not spec.extraction.tracks_watermark:
            return False
        if extract.is_empty_batch:
            # Nothing new was observed, so there is nothing to move to. Holding
            # is the correct outcome, and it is recorded as such.
            engine.audit.emit(
                EventType.WATERMARK_HELD,
                table_fqn=spec.table_fqn,
                run_id=run_id,
                payload={"reason": "no new watermark in batch"},
            )
            log.info("watermark held", stage="watermark", reason="empty batch")
            return False

        incremental = spec.extraction.incremental
        advanced = engine.watermarks.advance(
            spec.table_fqn,
            spec.env,
            new_value=extract.new_watermark,
            run_id=run_id,
            watermark_column=incremental.effective_watermark_column,
            watermark_type="number" if incremental.uses_scn else incremental.watermark_type,
            updated_at=engine.now(),
        )
        engine.audit.emit(
            EventType.WATERMARK_ADVANCED if advanced else EventType.WATERMARK_HELD,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload={"value": extract.new_watermark, "advanced": advanced},
        )
        log.info(
            "watermark advanced" if advanced else "watermark held",
            stage="watermark",
            value=extract.new_watermark,
        )
        return advanced

    def _skip(
        self, spec: RunSpec, run_id: str, attempt: int, log: StructuredLogger, reason: str
    ) -> TaskOutcome:
        self._e.audit.emit(
            EventType.TASK_SKIPPED,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload={"reason": reason},
            flush=True,
        )
        log.info("task skipped", stage="skip", reason=reason)
        return TaskOutcome(
            table_fqn=spec.table_fqn, run_id=run_id, status=TaskStatus.SKIPPED, attempt=attempt
        )

    def _fail(
        self,
        spec: RunSpec,
        run_id: str,
        task: TaskRef,
        attempt: int,
        started_at: datetime,
        log: StructuredLogger,
        error: BaseException,
    ) -> TaskOutcome:
        engine = self._e
        ended_at = engine.now()
        report = getattr(error, "report", None)

        # Record the failure even if recording itself is fragile -- losing the
        # error is worse than a noisy log line.
        try:
            engine.control.finish_task(
                task,
                TaskStatus.FAILED,
                error_type=type(error).__name__,
                error_message=str(error),
                started_at=started_at,
                ended_at=ended_at,
            )
        except Exception as exc:  # pragma: no cover - defensive
            log.error(f"could not record task failure: {exc}", stage="finish")

        engine.audit.emit(
            EventType.TASK_FAILED,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload={"error_type": type(error).__name__, "error": str(error), "attempt": attempt},
            flush=True,
        )
        log.error(
            f"task failed: {type(error).__name__}: {error}",
            stage="finish",
            error_type=type(error).__name__,
        )

        return TaskOutcome(
            table_fqn=spec.table_fqn,
            run_id=run_id,
            status=TaskStatus.FAILED,
            attempt=attempt,
            report=report,
            error=error,
            duration_ms=int((ended_at - started_at).total_seconds() * 1000),
        )

    def _alert_failure(self, spec: RunSpec, run_id: str, outcome: TaskOutcome) -> None:
        if outcome.error is None:
            return
        self._e.alerts.dispatch(
            build_failure_alert(
                table_fqn=spec.table_fqn,
                env=spec.env,
                run_id=run_id,
                error=outcome.error,
                attempt=outcome.attempt,
            ),
            spec.alerting.on_failure,
        )
