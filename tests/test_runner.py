from __future__ import annotations

from datetime import datetime
from itertools import count

import pytest

from ingestion_framework.control.audit import AuditLog, EventType
from ingestion_framework.control.control_store import ControlStore, TaskStatus
from ingestion_framework.control.sql_client import RecordingSqlClient
from ingestion_framework.control.watermark import WatermarkStore, WatermarkWindow
from ingestion_framework.engine.extractor import ExtractResult
from ingestion_framework.engine.loader import LoadResult
from ingestion_framework.engine.reconciler import (
    CheckResult,
    CheckStatus,
    ReconciliationReport,
)
from ingestion_framework.engine.sql_builder import SourceQuery
from ingestion_framework.engine.transformer import StageQuery, StageResult
from ingestion_framework.observability.alerts import AlertDispatcher
from ingestion_framework.observability.logger import StructuredLogger
from ingestion_framework.orchestration.runner import (
    Engine,
    ReconciliationFailure,
    TableRunner,
)

from .test_sql_builder import spec

NOW = datetime(2026, 8, 24, 10, 0, 0)
RUN_ID = "prod-20260824T100000-a1b2c3"


# -- fakes ------------------------------------------------------------------


class FakeExtractor:
    def __init__(self, *, source_count=100, new_watermark="2026-08-24 10:00:00.000000", error=None):
        self.source_count = source_count
        self.new_watermark = new_watermark
        self.error = error
        self.calls: list[WatermarkWindow] = []

    def extract(self, spec, window):
        self.calls.append(window)
        if self.error:
            raise self.error
        return ExtractResult(
            dataframe=object(),
            query=SourceQuery(sql="SELECT 1", mode=spec.extraction.mode),
            lower_bound=window.lower_bound,
            upper_bound=self.new_watermark,
            new_watermark=self.new_watermark,
            source_count=self.source_count,
        )


class FakeTransformer:
    def __init__(self, *, duplicates=0, null_keys=0, error=None):
        self.duplicates = duplicates
        self.null_keys = null_keys
        self.error = error
        self.calls: list[dict] = []

    def stage(self, dataframe, spec, *, run_id, batch_id, ingested_at, count_rows=False, views=None):
        self.calls.append({"run_id": run_id, "batch_id": batch_id, "ingested_at": ingested_at})
        if self.error:
            raise self.error
        return StageResult(
            dataframe=object(),
            query=StageQuery(sql="SELECT 1", columns=("A",), data_columns=("A",), dedupe_applied=True),
            duplicates_removed=self.duplicates,
            null_key_rows=self.null_keys,
        )


class FakeLoader:
    def __init__(self, *, rows_written=100, error=None):
        self.rows_written = rows_written
        self.error = error
        self.calls = 0

    def write(self, dataframe, spec, *, staged_view=None):
        self.calls += 1
        if self.error:
            raise self.error
        return LoadResult(
            target=spec.target.fqn,
            write_mode=spec.target.write_mode,
            rows_written=self.rows_written,
            rows_inserted=self.rows_written,
            rows_updated=0,
        )


class FakeReconciler:
    def __init__(self, *, ok=True):
        self.ok = ok
        self.calls = 0

    def run(self, spec, **kwargs):
        self.calls += 1
        if self.ok:
            return ReconciliationReport(checks=[
                CheckResult("row_count", "source_vs_target", CheckStatus.PASSED, delta=0)
            ])
        return ReconciliationReport(checks=[
            CheckResult("row_count", "source_vs_target", CheckStatus.FAILED, delta=-5)
        ])


class SpyWatermarks(WatermarkStore):
    def __init__(self, client, *, stored=None, advances=True):
        super().__init__(client, "prod_lakehouse", "control")
        self.stored = stored
        self.advances = advances
        self.advance_calls: list[dict] = []

    def window(self, table_fqn, env, **kwargs):
        return WatermarkWindow(
            lower_bound=self.stored,
            stored_value=self.stored,
            overlap_applied=bool(self.stored),
            is_first_run=self.stored is None,
        )

    def advance(self, table_fqn, env, **kwargs):
        self.advance_calls.append(kwargs)
        return self.advances


def build_engine(**overrides):
    client = RecordingSqlClient()
    ids = count(1)
    engine = Engine(
        extractor=overrides.pop("extractor", FakeExtractor()),
        transformer=overrides.pop("transformer", FakeTransformer()),
        loader=overrides.pop("loader", FakeLoader()),
        reconciler=overrides.pop("reconciler", FakeReconciler()),
        control=ControlStore(client, "prod_lakehouse", "control", now=lambda: NOW),
        watermarks=overrides.pop("watermarks", SpyWatermarks(client)),
        audit=AuditLog(
            client, "prod_lakehouse", "control",
            run_id=RUN_ID, env="prod", now=lambda: NOW, id_factory=lambda: f"e{next(ids)}",
        ),
        logger=StructuredLogger(),
        alerts=overrides.pop("alerts", AlertDispatcher(StructuredLogger())),
        now=lambda: NOW,
        batch_id_factory=lambda: "batch-1",
        sleep=overrides.pop("sleep", lambda s: None),
    )
    for key, value in overrides.items():
        setattr(engine, key, value)
    return engine, client


def audit_types(client: RecordingSqlClient) -> list[str]:
    """Event types written to the audit table (reconciliation rows also land
    in client.inserted, and they have no event_type)."""
    return [
        row["event_type"]
        for table, rows in client.inserted
        if table.endswith("audit_log")
        for row in rows
    ]


class TestHappyPath:
    def test_task_succeeds_and_reports_metrics(self):
        engine, _ = build_engine()
        outcome = TableRunner(engine).run(spec(), RUN_ID)
        assert outcome.succeeded
        assert outcome.metrics.source_count == 100
        assert outcome.metrics.rows_written == 100
        assert outcome.watermark_advanced

    def test_lifecycle_events_are_emitted_in_order(self):
        engine, client = build_engine()
        TableRunner(engine).run(spec(), RUN_ID)
        types = audit_types(client)
        for event in [
            EventType.TASK_STARTED, EventType.EXTRACT_STARTED, EventType.EXTRACT_DONE,
            EventType.LOAD_STARTED, EventType.LOAD_DONE, EventType.RECONCILIATION_DONE,
            EventType.WATERMARK_ADVANCED, EventType.TASK_SUCCEEDED,
        ]:
            assert event.value in types, f"{event.value} missing from {types}"
        assert types.index("EXTRACT_DONE") < types.index("LOAD_STARTED")

    def test_config_is_registered_for_the_run(self):
        engine, client = build_engine()
        TableRunner(engine).run(spec(), RUN_ID)
        assert client.statements_matching("MERGE INTO prod_lakehouse.control.config_registry")

    def test_reconciliation_results_are_recorded(self):
        engine, client = build_engine()
        TableRunner(engine).run(spec(), RUN_ID)
        tables = [table for table, _ in client.inserted]
        assert "prod_lakehouse.control.reconciliation" in tables

    def test_batch_id_reaches_the_transformer(self):
        transformer = FakeTransformer()
        engine, _ = build_engine(transformer=transformer)
        TableRunner(engine).run(spec(), RUN_ID)
        assert transformer.calls[0]["batch_id"] == "batch-1"
        assert transformer.calls[0]["run_id"] == RUN_ID


class TestWatermarkOrdering:
    """The ordering guarantee this module exists to own."""

    def test_watermark_advances_only_after_the_load(self):
        engine, client = build_engine()
        TableRunner(engine).run(spec(), RUN_ID)
        types = audit_types(client)
        assert types.index("LOAD_DONE") < types.index("WATERMARK_ADVANCED")

    def test_watermark_does_not_advance_when_the_load_fails(self):
        # Advance-then-load would leave the next run resuming past rows that
        # were never written.
        watermarks = SpyWatermarks(RecordingSqlClient())
        engine, _ = build_engine(loader=FakeLoader(error=RuntimeError("disk full")),
                                 watermarks=watermarks)
        outcome = TableRunner(engine).run(spec(), RUN_ID)
        assert outcome.status is TaskStatus.FAILED
        assert watermarks.advance_calls == []

    def test_watermark_does_not_advance_when_the_extract_fails(self):
        watermarks = SpyWatermarks(RecordingSqlClient())
        engine, _ = build_engine(extractor=FakeExtractor(error=RuntimeError("ORA-12541")),
                                 watermarks=watermarks)
        TableRunner(engine).run(spec(), RUN_ID)
        assert watermarks.advance_calls == []

    def test_watermark_does_not_advance_when_reconciliation_fails(self):
        # A mark past rows we are not sure landed is worse than a stopped run.
        watermarks = SpyWatermarks(RecordingSqlClient())
        engine, _ = build_engine(reconciler=FakeReconciler(ok=False), watermarks=watermarks)
        outcome = TableRunner(engine).run(spec(), RUN_ID)
        assert outcome.status is TaskStatus.FAILED
        assert isinstance(outcome.error, ReconciliationFailure)
        assert watermarks.advance_calls == []

    def test_empty_batch_holds_the_mark_and_still_succeeds(self):
        watermarks = SpyWatermarks(RecordingSqlClient())
        engine, client = build_engine(
            extractor=FakeExtractor(source_count=0, new_watermark=None), watermarks=watermarks
        )
        outcome = TableRunner(engine).run(spec(), RUN_ID)
        assert outcome.succeeded
        assert not outcome.watermark_advanced
        assert watermarks.advance_calls == []
        assert "WATERMARK_HELD" in audit_types(client)

    def test_full_load_never_advances_a_watermark(self):
        watermarks = SpyWatermarks(RecordingSqlClient())
        s = spec(extraction__mode="full", extraction__columns="*", extraction__incremental={})
        engine, _ = build_engine(watermarks=watermarks)
        outcome = TableRunner(engine).run(s, RUN_ID)
        assert outcome.succeeded
        assert watermarks.advance_calls == []

    def test_advance_carries_the_run_id_as_a_fence(self):
        watermarks = SpyWatermarks(RecordingSqlClient())
        engine, _ = build_engine(watermarks=watermarks)
        TableRunner(engine).run(spec(), RUN_ID)
        assert watermarks.advance_calls[0]["run_id"] == RUN_ID

    def test_held_watermark_is_reported_when_the_guard_rejects_it(self):
        watermarks = SpyWatermarks(RecordingSqlClient(), advances=False)
        engine, client = build_engine(watermarks=watermarks)
        outcome = TableRunner(engine).run(spec(), RUN_ID)
        assert outcome.succeeded and not outcome.watermark_advanced
        assert "WATERMARK_HELD" in audit_types(client)


class TestFailures:
    def test_failure_is_recorded_with_type_and_message(self):
        engine, client = build_engine(extractor=FakeExtractor(error=RuntimeError("ORA-12541")))
        outcome = TableRunner(engine).run(spec(), RUN_ID)
        assert outcome.status is TaskStatus.FAILED
        params = client.params_for("UPDATE prod_lakehouse.control.ingestion_tasks")
        assert params["status"] == "FAILED"
        assert params["error_type"] == "RuntimeError"
        assert "ORA-12541" in params["error_message"]

    def test_failure_flushes_the_audit_trail(self):
        # The events leading up to a failure are the ones worth having.
        engine, client = build_engine(loader=FakeLoader(error=RuntimeError("boom")))
        TableRunner(engine).run(spec(), RUN_ID)
        types = audit_types(client)
        assert "TASK_FAILED" in types and "EXTRACT_DONE" in types

    def test_error_does_not_escape_the_runner(self):
        engine, _ = build_engine(transformer=FakeTransformer(error=ValueError("bad schema")))
        outcome = TableRunner(engine).run(spec(), RUN_ID)
        assert isinstance(outcome.error, ValueError)

    def test_reconciliation_failure_keeps_the_report(self):
        engine, _ = build_engine(reconciler=FakeReconciler(ok=False))
        outcome = TableRunner(engine).run(spec(), RUN_ID)
        assert outcome.report is not None and not outcome.report.ok


class TestSkip:
    def test_disabled_table_is_skipped_without_touching_the_source(self):
        extractor = FakeExtractor()
        engine, client = build_engine(extractor=extractor)
        outcome = TableRunner(engine).run(spec(runtime={"enabled": False}), RUN_ID)
        assert outcome.status is TaskStatus.SKIPPED
        assert extractor.calls == []
        assert "TASK_SKIPPED" in audit_types(client)

    def test_skipped_table_opens_no_task_row(self):
        engine, client = build_engine()
        TableRunner(engine).run(spec(runtime={"enabled": False}), RUN_ID)
        assert not client.statements_matching("INSERT INTO prod_lakehouse.control.ingestion_tasks")


class TestRetries:
    def test_success_on_the_first_attempt_does_not_retry(self):
        loader = FakeLoader()
        engine, _ = build_engine(loader=loader)
        outcome = TableRunner(engine).run_with_retries(spec(), RUN_ID)
        assert outcome.succeeded and outcome.attempt == 1
        assert loader.calls == 1

    def test_retries_up_to_the_configured_count(self):
        loader = FakeLoader(error=RuntimeError("transient"))
        engine, _ = build_engine(loader=loader)
        outcome = TableRunner(engine).run_with_retries(spec(runtime={"retries": 2}), RUN_ID)
        assert outcome.status is TaskStatus.FAILED
        assert outcome.attempt == 3  # 1 initial + 2 retries
        assert loader.calls == 3

    def test_each_retry_is_a_new_attempt_row(self):
        engine, client = build_engine(loader=FakeLoader(error=RuntimeError("transient")))
        TableRunner(engine).run_with_retries(spec(runtime={"retries": 2}), RUN_ID)
        inserts = [
            p for k, s, p in client.calls
            if "INSERT INTO prod_lakehouse.control.ingestion_tasks" in s
        ]
        assert [p["attempt"] for p in inserts] == [1, 2, 3]

    def test_backoff_is_honoured_between_attempts(self):
        slept: list[float] = []
        engine, _ = build_engine(
            loader=FakeLoader(error=RuntimeError("x")), sleep=slept.append
        )
        TableRunner(engine).run_with_retries(
            spec(runtime={"retries": 2, "retry_backoff_seconds": 30}), RUN_ID
        )
        assert slept == [30, 30]  # not after the final attempt

    def test_retry_events_are_audited(self):
        engine, client = build_engine(loader=FakeLoader(error=RuntimeError("x")))
        TableRunner(engine).run_with_retries(spec(runtime={"retries": 1}), RUN_ID)
        assert "TASK_RETRYING" in audit_types(client)

    def test_skipped_table_is_not_retried(self):
        engine, _ = build_engine()
        outcome = TableRunner(engine).run_with_retries(
            spec(runtime={"enabled": False, "retries": 3}), RUN_ID
        )
        assert outcome.status is TaskStatus.SKIPPED


class RecordingDispatcher(AlertDispatcher):
    def __init__(self):
        super().__init__(StructuredLogger())
        self.dispatched: list[tuple] = []

    def dispatch(self, alert, channels):
        self.dispatched.append((alert, list(channels)))
        return super().dispatch(alert, channels)


class TestAlerting:
    def test_failure_alert_fires_after_the_final_retry(self):
        alerts = RecordingDispatcher()
        engine, _ = build_engine(loader=FakeLoader(error=RuntimeError("boom")), alerts=alerts)
        s = spec(runtime={"retries": 1}, alerting={"on_failure": ["email:oncall@example.com"]})
        TableRunner(engine).run_with_retries(s, RUN_ID)
        assert len(alerts.dispatched) == 1  # once, not once per attempt
        alert, channels = alerts.dispatched[0]
        assert alert.event.value == "task_failed"
        assert channels == ["email:oncall@example.com"]

    def test_no_alert_when_the_task_succeeds(self):
        alerts = RecordingDispatcher()
        engine, _ = build_engine(alerts=alerts)
        TableRunner(engine).run_with_retries(
            spec(alerting={"on_failure": ["email:x@y.com"]}), RUN_ID
        )
        assert alerts.dispatched == []

    def test_reconciliation_mismatch_alerts_on_its_own_channel(self):
        alerts = RecordingDispatcher()
        engine, _ = build_engine(reconciler=FakeReconciler(ok=False), alerts=alerts)
        s = spec(
            runtime={"retries": 0},
            alerting={"on_reconciliation_mismatch": ["slack:#data-quality"],
                      "on_failure": ["email:oncall@example.com"]},
        )
        TableRunner(engine).run_with_retries(s, RUN_ID)
        events = {a.event.value for a, _ in alerts.dispatched}
        assert "reconciliation_mismatch" in events
        recon = next(a for a, _ in alerts.dispatched if a.event.value == "reconciliation_mismatch")
        assert "source_vs_target" in recon.body
