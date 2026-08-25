"""Run a group of tables as one pipeline run.

Two things this adds over calling :class:`TableRunner` in a loop:

* **dependency ordering** -- ``table.depends_on`` is honoured, cycles are
  rejected up front rather than deadlocking, and a table whose dependency
  failed is skipped rather than loaded from a half-built parent;
* **run-level state** -- one ``ingestion_runs`` row that rolls the task
  outcomes up into SUCCEEDED / PARTIAL / FAILED.

Concurrency is opt-in and bounded per dependency level. It is safe because each
task stages into its own temp views; before that was true, two concurrent tables
would have silently overwritten each other's staged batch.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping, Sequence

from ..control.audit import EventType
from ..control.control_store import RunContext, RunStatus, TaskStatus, derive_run_status
from ..engine.run_spec import RunSpec
from .runner import Engine, TableRunner, TaskOutcome


class DependencyError(ValueError):
    """Raised when depends_on cannot be satisfied."""


@dataclass
class BatchResult:
    run_id: str
    env: str
    status: RunStatus
    outcomes: list[TaskOutcome] = field(default_factory=list)
    skipped_for_dependencies: dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> list[TaskOutcome]:
        return [o for o in self.outcomes if o.succeeded]

    @property
    def failed(self) -> list[TaskOutcome]:
        return [o for o in self.outcomes if o.status is TaskStatus.FAILED]

    def summary(self) -> dict[str, int | str]:
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "tables": len(self.outcomes),
            "succeeded": len(self.succeeded),
            "failed": len(self.failed),
            "skipped": len([o for o in self.outcomes if o.status is TaskStatus.SKIPPED]),
        }


# -- ordering ---------------------------------------------------------------


def order_tables(specs: Sequence[RunSpec]) -> list[list[RunSpec]]:
    """Group specs into dependency levels; each level may run concurrently.

    Dependencies naming a table outside this batch are ignored -- a group that
    only loads part of a domain is a normal thing to run, and refusing it
    because a sibling is absent would make targeted reruns impossible. Cycles
    are always an error.
    """
    by_fqn = {spec.table_fqn: spec for spec in specs}
    pending = {
        spec.table_fqn: {d for d in spec.table.depends_on if d in by_fqn} for spec in specs
    }

    for fqn, deps in pending.items():
        if fqn in deps:
            raise DependencyError(f"{fqn} depends on itself")

    levels: list[list[RunSpec]] = []
    done: set[str] = set()
    while pending:
        ready = sorted(fqn for fqn, deps in pending.items() if deps <= done)
        if not ready:
            stuck = ", ".join(sorted(pending))
            raise DependencyError(
                f"dependency cycle among: {stuck} -- no table in this set can start"
            )
        levels.append([by_fqn[fqn] for fqn in ready])
        done.update(ready)
        for fqn in ready:
            pending.pop(fqn)
    return levels


def blocked_by(spec: RunSpec, failed: Iterable[str]) -> list[str]:
    """Which of this table's dependencies failed."""
    failed = set(failed)
    return [d for d in spec.table.depends_on if d in failed]


# -- runner -----------------------------------------------------------------


class BatchRunner:
    """Runs many tables under one run id."""

    def __init__(
        self,
        engine: Engine,
        *,
        max_workers: int = 1,
        skip_dependents_on_failure: bool = True,
    ) -> None:
        self._e = engine
        self._runner = TableRunner(engine)
        self._max_workers = max(1, int(max_workers))
        self._skip_dependents = skip_dependents_on_failure

    def run(
        self,
        specs: Sequence[RunSpec],
        run_id: str,
        env: str,
        *,
        trigger: str = "manual",
        triggered_by: str | None = None,
        job_id: str | None = None,
        job_run_id: str | None = None,
    ) -> BatchResult:
        engine = self._e
        log = engine.logger.bind(run_id=run_id, env=env)
        levels = order_tables(specs)

        run: RunContext = engine.control.start_run(
            run_id,
            env,
            trigger=trigger,
            triggered_by=triggered_by,
            table_count=len(specs),
            job_id=job_id,
            job_run_id=job_run_id,
        )
        engine.audit.bind(run_id=run_id, env=env)
        result = BatchResult(run_id=run_id, env=env, status=RunStatus.RUNNING)
        failed: set[str] = set()

        # Everything after start_run() lives inside the try: once that row
        # exists, every path out of here must close it.
        try:
            engine.audit.emit(
                EventType.RUN_STARTED,
                run_id=run_id,
                payload={"tables": len(specs), "levels": len(levels), "trigger": trigger},
            )
            log.info(
                "run started",
                stage="run",
                tables=len(specs),
                levels=len(levels),
                max_workers=self._max_workers,
            )
            for depth, level in enumerate(levels):
                runnable, blocked = self._partition(level, failed)
                for spec, blockers in blocked:
                    reason = f"dependency failed: {', '.join(blockers)}"
                    result.skipped_for_dependencies[spec.table_fqn] = reason
                    result.outcomes.append(
                        self._skip_for_dependency(spec, run_id, reason, log)
                    )
                    failed.add(spec.table_fqn)  # its own dependents are blocked too

                if not runnable:
                    continue
                log.info("level starting", stage="run", depth=depth, tables=len(runnable))
                outcomes = self._run_level(runnable, run_id)
                result.outcomes.extend(outcomes)
                failed.update(o.table_fqn for o in outcomes if o.status is TaskStatus.FAILED)

            result.status = derive_run_status([o.status for o in result.outcomes])
        except BaseException:
            # An unexpected error here is not a task failure -- it is the run
            # itself coming apart, so record it as such and re-raise.
            result.status = RunStatus.FAILED
            raise
        finally:
            # A run row left in RUNNING forever is worse than one closed with a
            # partial picture, so this closes on every path out. Cleanup errors
            # are logged, never allowed to mask the original failure.
            try:
                engine.control.finish_run(run, result.status)
            except Exception as exc:  # pragma: no cover - defensive
                log.error(f"could not close the run row: {exc}", stage="run")
            try:
                engine.audit.emit(
                    EventType.RUN_FINISHED,
                    run_id=run_id,
                    payload=dict(result.summary()),
                    flush=True,
                )
            except Exception as exc:  # pragma: no cover - defensive
                log.error(f"could not write the run-finished event: {exc}", stage="run")

        log.info("run finished", stage="run", **result.summary())
        return result

    # -- internals ---------------------------------------------------------

    def _partition(
        self, level: Sequence[RunSpec], failed: set[str]
    ) -> tuple[list[RunSpec], list[tuple[RunSpec, list[str]]]]:
        if not self._skip_dependents:
            return list(level), []
        runnable: list[RunSpec] = []
        blocked: list[tuple[RunSpec, list[str]]] = []
        for spec in level:
            blockers = blocked_by(spec, failed)
            (blocked.append((spec, blockers)) if blockers else runnable.append(spec))
        return runnable, blocked

    def _run_level(self, specs: Sequence[RunSpec], run_id: str) -> list[TaskOutcome]:
        if self._max_workers == 1 or len(specs) == 1:
            return [self._runner.run_with_retries(spec, run_id) for spec in specs]
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = [pool.submit(self._runner.run_with_retries, spec, run_id) for spec in specs]
            return [future.result() for future in futures]

    def _skip_for_dependency(
        self, spec: RunSpec, run_id: str, reason: str, log
    ) -> TaskOutcome:
        self._e.audit.emit(
            EventType.TASK_SKIPPED,
            table_fqn=spec.table_fqn,
            run_id=run_id,
            payload={"reason": reason},
        )
        log.warning("table skipped", stage="skip", table_fqn=spec.table_fqn, reason=reason)
        return TaskOutcome(
            table_fqn=spec.table_fqn, run_id=run_id, status=TaskStatus.SKIPPED
        )


# -- selection --------------------------------------------------------------


def select_specs(
    specs: Sequence[RunSpec],
    *,
    tables: Sequence[str] | None = None,
    group: str | None = None,
    domain: str | None = None,
) -> list[RunSpec]:
    """Filter a resolved set of specs by table / schedule group / domain."""
    selected = list(specs)
    if tables:
        wanted = set(tables)
        known = {s.table_fqn for s in selected}
        unknown = sorted(wanted - known)
        if unknown:
            raise DependencyError(
                f"unknown table(s): {', '.join(unknown)}; known: {', '.join(sorted(known))}"
            )
        selected = [s for s in selected if s.table_fqn in wanted]
    if group:
        selected = [s for s in selected if s.schedule.group == group]
    if domain:
        selected = [s for s in selected if s.table.domain == domain]
    return selected
